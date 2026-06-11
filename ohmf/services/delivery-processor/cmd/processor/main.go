package main

import (
	"context"
	"encoding/json"
	"errors"
	"log"
	"os"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/redis/go-redis/v9"
	"github.com/segmentio/kafka-go"
)

type persistedEvent struct {
	EventID         string   `json:"event_id"`
	MessageID       string   `json:"message_id"`
	ConversationID  string   `json:"conversation_id"`
	SenderUserID    string   `json:"sender_user_id"`
	ServerOrder     int64    `json:"server_order"`
	Transport       string   `json:"transport"`
	Status          string   `json:"status"`
	PersistedAtMS   int64    `json:"persisted_at_ms"`
	DeliveryTargets []string `json:"delivery_targets"`
	TraceID         string   `json:"trace_id"`
}

const insertDeliveredReceiptSQL = `
	INSERT INTO message_deliveries (
		message_id,
		recipient_user_id,
		transport,
		state,
		submitted_at,
		updated_at
	) VALUES ($1::uuid, $2::uuid, $3, 'DELIVERED', now(), now())
	ON CONFLICT (message_id, recipient_user_id)
	WHERE recipient_user_id IS NOT NULL AND state = 'DELIVERED'
	DO NOTHING
	RETURNING id
`

type deliveryRecorder interface {
	RecordDelivered(context.Context, persistedEvent, string) (bool, error)
}

type presenceStore interface {
	IsOnline(context.Context, string) (bool, error)
	Publish(context.Context, string, []byte) error
}

type deliveryPublisher interface {
	Publish(context.Context, string, []byte) error
}

type queryRower interface {
	QueryRow(context.Context, string, ...any) pgx.Row
}

type deliveryMetricsObserver interface {
	RecordDuplicate()
}

type noopDeliveryMetricsObserver struct{}

func (noopDeliveryMetricsObserver) RecordDuplicate() {}

type pgDeliveryRecorder struct {
	db queryRower
}

func (r pgDeliveryRecorder) RecordDelivered(ctx context.Context, evt persistedEvent, recipientID string) (bool, error) {
	var deliveryID string
	err := r.db.QueryRow(ctx, insertDeliveredReceiptSQL, evt.MessageID, recipientID, evt.Transport).Scan(&deliveryID)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return false, nil
		}
		return false, err
	}
	return true, nil
}

type redisPresenceStore struct {
	client *redis.Client
}

func (s redisPresenceStore) IsOnline(ctx context.Context, recipientID string) (bool, error) {
	ok, err := s.client.Exists(ctx, "presence:user:"+recipientID).Result()
	if err != nil {
		return false, err
	}
	return ok > 0, nil
}

func (s redisPresenceStore) Publish(ctx context.Context, channel string, body []byte) error {
	return s.client.Publish(ctx, channel, body).Err()
}

type kafkaDeliveryPublisher struct {
	writer *kafka.Writer
}

func (p kafkaDeliveryPublisher) Publish(ctx context.Context, key string, body []byte) error {
	return p.writer.WriteMessages(ctx, kafka.Message{
		Key:   []byte(key),
		Value: body,
		Time:  time.Now().UTC(),
	})
}

func main() {
	ctx := context.Background()
	brokers := splitCSV(getenv("APP_KAFKA_BROKERS", "localhost:9092"))
	persistedTopic := getenv("APP_KAFKA_PERSISTED_TOPIC", "msg.persisted.v1")
	deliveryTopic := getenv("APP_KAFKA_DELIVERY_TOPIC", "msg.delivery.v1")
	dlqTopic := getenv("APP_KAFKA_DELIVERY_DLQ_TOPIC", "msg.delivery.dlq.v1")
	startMetricsServer(getenv("APP_METRICS_ADDR", ":9092"))
	poolCfg, err := pgxpool.ParseConfig(getenv("APP_DB_DSN", "postgres://ohmf:ohmf@localhost:5432/ohmf?sslmode=disable"))
	if err != nil {
		log.Fatalf("postgres config failed: %v", err)
	}
	poolCfg.ConnConfig.Tracer = &dbQueryTracer{}
	pg, err := pgxpool.NewWithConfig(ctx, poolCfg)
	if err != nil {
		log.Fatalf("postgres init failed: %v", err)
	}
	defer pg.Close()
	if err := pg.Ping(ctx); err != nil {
		log.Fatalf("postgres ping failed: %v", err)
	}

	reader := kafka.NewReader(kafka.ReaderConfig{
		Brokers:     brokers,
		GroupID:     getenv("APP_KAFKA_GROUP_ID", "delivery-processor-v1"),
		Topic:       persistedTopic,
		StartOffset: kafka.LastOffset,
		MaxWait:     500 * time.Millisecond,
	})
	defer reader.Close()

	deliveryWriter := writer(brokers, deliveryTopic)
	defer deliveryWriter.Close()
	dlqWriter := writer(brokers, dlqTopic)
	defer dlqWriter.Close()

	rdb := redis.NewClient(&redis.Options{Addr: getenv("APP_REDIS_ADDR", "localhost:6379")})
	defer rdb.Close()
	if err := rdb.Ping(ctx).Err(); err != nil {
		log.Fatalf("redis ping failed: %v", err)
	}

	log.Printf("delivery-processor started")
	for {
		msg, err := reader.FetchMessage(ctx)
		if err != nil {
			recordProcessorRetry("fetch_failed")
			log.Printf("fetch failed: %v", err)
			time.Sleep(300 * time.Millisecond)
			continue
		}
		startedAt := time.Now()
		if err := process(ctx, pg, rdb, deliveryWriter, msg); err != nil {
			recordProcessorResult("failure", time.Since(startedAt))
			log.Printf("process failed: %v", err)
			_ = publishDLQ(ctx, dlqWriter, msg, err)
		} else {
			recordProcessorResult("success", time.Since(startedAt))
		}
		if err := reader.CommitMessages(ctx, msg); err != nil {
			recordProcessorRetry("commit_failed")
			log.Printf("commit failed: %v", err)
		}
	}
}

func process(ctx context.Context, pg *pgxpool.Pool, rdb *redis.Client, deliveryWriter *kafka.Writer, msg kafka.Message) error {
	return processMessageWithObserver(ctx, pgDeliveryRecorder{db: pg}, redisPresenceStore{client: rdb}, kafkaDeliveryPublisher{writer: deliveryWriter}, msg, noopDeliveryMetricsObserver{})
}

func processMessage(ctx context.Context, deliveries deliveryRecorder, presence presenceStore, publisher deliveryPublisher, msg kafka.Message) error {
	return processMessageWithObserver(ctx, deliveries, presence, publisher, msg, noopDeliveryMetricsObserver{})
}

func processMessageWithObserver(ctx context.Context, deliveries deliveryRecorder, presence presenceStore, publisher deliveryPublisher, msg kafka.Message, observer deliveryMetricsObserver) error {
	var evt persistedEvent
	if err := json.Unmarshal(msg.Value, &evt); err != nil {
		return err
	}
	if len(evt.DeliveryTargets) == 0 {
		return nil
	}

	statusUpdatedAt := time.Now().UTC().Format(time.RFC3339Nano)
	for _, recipientID := range evt.DeliveryTargets {
		if strings.TrimSpace(recipientID) == "" {
			continue
		}
		online, err := presence.IsOnline(ctx, recipientID)
		if err != nil {
			return err
		}
		if !online {
			continue
		}
		created, err := deliveries.RecordDelivered(ctx, evt, recipientID)
		if err != nil {
			return err
		}
		if !created {
			observer.RecordDuplicate()
			recordProcessorRetry("duplicate")
			continue
		}
		delivery := map[string]any{
			"event_id":          evt.EventID,
			"recipient_user_id": recipientID,
			"sender_user_id":    evt.SenderUserID,
			"message_id":        evt.MessageID,
			"conversation_id":   evt.ConversationID,
			"server_order":      evt.ServerOrder,
			"status":            "DELIVERED",
			"status_updated_at": statusUpdatedAt,
			"trace_id":          evt.TraceID,
		}
		body, _ := json.Marshal(delivery)
		if err := publisher.Publish(ctx, recipientID, body); err != nil {
			return err
		}
		_ = presence.Publish(ctx, "delivery:user:"+recipientID, body)
		if strings.TrimSpace(evt.SenderUserID) != "" && evt.SenderUserID != recipientID {
			_ = presence.Publish(ctx, "delivery:user:"+evt.SenderUserID, body)
		}
	}
	return nil
}

func publishDLQ(ctx context.Context, dlqWriter *kafka.Writer, msg kafka.Message, cause error) error {
	body, _ := json.Marshal(map[string]any{
		"cause":     cause.Error(),
		"topic":     msg.Topic,
		"partition": msg.Partition,
		"offset":    msg.Offset,
		"value":     string(msg.Value),
		"at":        time.Now().UTC().Format(time.RFC3339Nano),
	})
	return dlqWriter.WriteMessages(ctx, kafka.Message{
		Key:   msg.Key,
		Value: body,
		Time:  time.Now().UTC(),
	})
}

func writer(brokers []string, topic string) *kafka.Writer {
	return &kafka.Writer{
		Addr:         kafka.TCP(brokers...),
		Topic:        topic,
		Balancer:     &kafka.Hash{},
		RequiredAcks: kafka.RequireAll,
		BatchTimeout: 10 * time.Millisecond,
	}
}

func splitCSV(v string) []string {
	raw := strings.Split(v, ",")
	out := make([]string, 0, len(raw))
	for _, item := range raw {
		trim := strings.TrimSpace(item)
		if trim == "" {
			continue
		}
		out = append(out, trim)
	}
	if len(out) == 0 {
		return []string{"localhost:9092"}
	}
	return out
}

func getenv(k, d string) string {
	if v := strings.TrimSpace(os.Getenv(k)); v != "" {
		return v
	}
	return d
}

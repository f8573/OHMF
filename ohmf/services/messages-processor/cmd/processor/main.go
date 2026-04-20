package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"os"
	"strings"
	"time"

	"github.com/gocql/gocql"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/redis/go-redis/v9"
	"github.com/segmentio/kafka-go"
)

type config struct {
	KafkaBrokers           string
	KafkaIngressTopic      string
	KafkaPersistedTopic    string
	KafkaMicroserviceTopic string
	KafkaSMSDispatchTopic  string
	KafkaDLQTopic          string
	KafkaGroupID           string
	PostgresDSN            string
	RedisAddr              string
	CassandraHosts         string
	CassandraKeyspace      string
	ShadowPostgresWrite    bool
	AsyncCassandraProjection bool
}

type ingressEvent struct {
	EventID           string         `json:"event_id"`
	MessageID         string         `json:"message_id"`
	ConversationID    string         `json:"conversation_id"`
	SenderUserID      string         `json:"sender_user_id"`
	IdempotencyKey    string         `json:"idempotency_key"`
	Endpoint          string         `json:"endpoint"`
	ClientGeneratedID string         `json:"client_generated_id,omitempty"`
	ContentType       string         `json:"content_type"`
	Content           map[string]any `json:"content"`
	TransportIntent   string         `json:"transport_intent"`
	RecipientPhone    string         `json:"recipient_phone"`
	SentAtMS          int64          `json:"sent_at_ms"`
	TraceID           string         `json:"trace_id"`
}

type persistedEvent struct {
	EventID           string   `json:"event_id"`
	MessageID         string   `json:"message_id"`
	ConversationID    string   `json:"conversation_id"`
	SenderUserID      string   `json:"sender_user_id"`
	ContentType       string   `json:"content_type"`
	Content           map[string]any `json:"content"`
	ServerOrder       int64    `json:"server_order"`
	Transport         string   `json:"transport"`
	Status            string   `json:"status"`
	PersistedAtMS     int64    `json:"persisted_at_ms"`
	DeliveryTargets   []string `json:"delivery_targets"`
	TraceID           string   `json:"trace_id"`
	ClientGeneratedID string   `json:"client_generated_id,omitempty"`
}

type ackPayload struct {
	EventID           string `json:"event_id"`
	MessageID         string `json:"message_id"`
	ConversationID    string `json:"conversation_id"`
	ServerOrder       int64  `json:"server_order"`
	Status            string `json:"status"`
	Transport         string `json:"transport"`
	PersistedAtMS     int64  `json:"persisted_at_ms"`
	ClientGeneratedID string `json:"client_generated_id,omitempty"`
}

type conversationMeta struct {
	Type           string
	Participants   []string
	ExternalPhones []string
}

type messageCreatedDomainEventPayload struct {
	MessageID         string         `json:"message_id"`
	ConversationID    string         `json:"conversation_id"`
	ConversationType  string         `json:"conversation_type"`
	SenderUserID      string         `json:"sender_user_id"`
	ContentType       string         `json:"content_type"`
	Content           map[string]any `json:"content"`
	ClientGeneratedID string         `json:"client_generated_id,omitempty"`
	Transport         string         `json:"transport"`
	ServerOrder       int64          `json:"server_order"`
	CreatedAt         string         `json:"created_at"`
	Participants      []string       `json:"participants"`
	ExternalPhones    []string       `json:"external_phones,omitempty"`
}

type processor struct {
	cfg           config
	pg            *pgxpool.Pool
	redis         *redis.Client
	cassandra     *gocql.Session
	reader        *kafka.Reader
	persistedW    *kafka.Writer
	microserviceW *kafka.Writer
	smsW          *kafka.Writer
	dlqW          *kafka.Writer
}

const domainEventMessageCreated = "message_created"
const ackChannelName = "msg:ack:events"

func main() {
	ctx := context.Background()
	cfg := loadConfig()
	startMetricsServer(getenv("APP_METRICS_ADDR", ":9091"))

	poolCfg, err := pgxpool.ParseConfig(cfg.PostgresDSN)
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

	rdb := redis.NewClient(&redis.Options{Addr: cfg.RedisAddr})
	if err := rdb.Ping(ctx).Err(); err != nil {
		log.Fatalf("redis ping failed: %v", err)
	}
	defer rdb.Close()

	cass, err := newCassandra(cfg)
	if err != nil && !cfg.AsyncCassandraProjection {
		log.Fatalf("cassandra init failed: %v", err)
	}
	if cass != nil {
		defer cass.Close()
	}
	if cass != nil {
		if err := ensureCassandraSchema(ctx, cass, cfg.CassandraKeyspace); err != nil {
			log.Fatalf("cassandra schema failed: %v", err)
		}
	}

	brokers := splitCSV(cfg.KafkaBrokers)
	p := &processor{
		cfg:       cfg,
		pg:        pg,
		redis:     rdb,
		cassandra: cass,
		reader: kafka.NewReader(kafka.ReaderConfig{
			Brokers:     brokers,
			GroupID:     cfg.KafkaGroupID,
			Topic:       cfg.KafkaIngressTopic,
			StartOffset: kafka.LastOffset,
			MaxWait:     500 * time.Millisecond,
		}),
		persistedW:    writer(brokers, cfg.KafkaPersistedTopic),
		microserviceW: writer(brokers, cfg.KafkaMicroserviceTopic),
		smsW:          writer(brokers, cfg.KafkaSMSDispatchTopic),
		dlqW:          writer(brokers, cfg.KafkaDLQTopic),
	}
	defer p.reader.Close()
	defer p.persistedW.Close()
	defer p.microserviceW.Close()
	defer p.smsW.Close()
	defer p.dlqW.Close()

	log.Printf("messages-processor started")
	for {
		msg, err := p.reader.FetchMessage(ctx)
		if err != nil {
			recordProcessorRetry("fetch_failed")
			log.Printf("fetch failed: %v", err)
			time.Sleep(300 * time.Millisecond)
			continue
		}
		startedAt := time.Now()
		if !msg.Time.IsZero() && startedAt.After(msg.Time) {
			recordKafkaConsumeLag(startedAt.Sub(msg.Time))
		}
		if err := p.processMessage(ctx, msg); err != nil {
			recordProcessorResult("failure", time.Since(startedAt))
			log.Printf("process failed: %v", err)
			_ = p.publishDLQ(ctx, msg, err)
		} else {
			recordProcessorResult("success", time.Since(startedAt))
		}
		if err := p.reader.CommitMessages(ctx, msg); err != nil {
			recordProcessorRetry("commit_failed")
			log.Printf("commit failed: %v", err)
		}
	}
}

func (p *processor) processMessage(ctx context.Context, msg kafka.Message) error {
	var evt ingressEvent
	if err := json.Unmarshal(msg.Value, &evt); err != nil {
		return fmt.Errorf("decode ingress: %w", err)
	}
	if evt.ConversationID == "" || evt.SenderUserID == "" || evt.IdempotencyKey == "" || evt.Endpoint == "" {
		return fmt.Errorf("invalid ingress event")
	}

	txStartedAt := time.Now()
	tx, err := p.pg.BeginTx(ctx, pgx.TxOptions{})
	if err != nil {
		return err
	}
	defer tx.Rollback(ctx)

	var member int
	if err := tx.QueryRow(ctx, `
		SELECT 1
		FROM conversation_members
		WHERE conversation_id = $1::uuid AND user_id = $2::uuid
	`, evt.ConversationID, evt.SenderUserID).Scan(&member); err != nil {
		return fmt.Errorf("membership validation failed: %w", err)
	}

	var existingPayload []byte
	var existingStatus int
	err = tx.QueryRow(ctx, `
		SELECT response_payload, status_code
		FROM idempotency_keys
		WHERE actor_user_id = $1::uuid AND endpoint = $2 AND key = $3
	`, evt.SenderUserID, evt.Endpoint, evt.IdempotencyKey).Scan(&existingPayload, &existingStatus)
	if err != nil && err != pgx.ErrNoRows {
		return err
	}

	var (
		serverOrder int64
		messageID   string
		persistedAt = time.Now().UTC()
		newMessage  bool
	)
	if err == nil && existingStatus == 201 {
		var existing struct {
			MessageID   string `json:"message_id"`
			ServerOrder int64  `json:"server_order"`
		}
		if uErr := json.Unmarshal(existingPayload, &existing); uErr == nil && existing.MessageID != "" {
			messageID = existing.MessageID
			serverOrder = existing.ServerOrder
		}
	}

	if messageID == "" {
		newMessage = true
		messageID = evt.MessageID
		err = tx.QueryRow(ctx, `
			UPDATE conversation_counters
			SET next_server_order = next_server_order + 1, updated_at = now()
			WHERE conversation_id = $1::uuid
			RETURNING next_server_order - 1
		`, evt.ConversationID).Scan(&serverOrder)
		if err != nil {
			return err
		}

		if p.cfg.ShadowPostgresWrite {
			contentJSON, _ := json.Marshal(evt.Content)
			_, err = tx.Exec(ctx, `
				INSERT INTO messages (id, conversation_id, sender_user_id, content_type, content, client_generated_id, transport, server_order, created_at)
				VALUES ($1::uuid, $2::uuid, $3::uuid, $4, $5::jsonb, $6, $7, $8, now())
				ON CONFLICT (id) DO NOTHING
			`, messageID, evt.ConversationID, evt.SenderUserID, evt.ContentType, string(contentJSON), evt.ClientGeneratedID, mapTransport(evt.TransportIntent), serverOrder)
			if err != nil {
				return err
			}
			_, err = tx.Exec(ctx, `
				UPDATE conversations
				SET last_message_id = $2::uuid, updated_at = now()
				WHERE id = $1::uuid
			`, evt.ConversationID, messageID)
			if err != nil {
				return err
			}
		}
	}

	payload, _ := json.Marshal(map[string]any{
		"message_id":          messageID,
		"conversation_id":     evt.ConversationID,
		"sender_user_id":      evt.SenderUserID,
		"content_type":        evt.ContentType,
		"content":             evt.Content,
		"client_generated_id": evt.ClientGeneratedID,
		"transport":           mapTransport(evt.TransportIntent),
		"server_order":        serverOrder,
		"status":              "SENT",
		"created_at":          persistedAt.Format(time.RFC3339),
	})
	_, err = tx.Exec(ctx, `
		INSERT INTO idempotency_keys (actor_user_id, endpoint, key, response_payload, status_code, expires_at)
		VALUES ($1::uuid, $2, $3, $4::jsonb, 201, now() + interval '24 hour')
		ON CONFLICT (actor_user_id, endpoint, key)
		DO UPDATE SET response_payload = EXCLUDED.response_payload, status_code = EXCLUDED.status_code
	`, evt.SenderUserID, evt.Endpoint, evt.IdempotencyKey, string(payload))
	if err != nil {
		return err
	}

	recipients, err := loadRecipients(ctx, tx, evt.ConversationID, evt.SenderUserID)
	if err != nil {
		return err
	}
	if newMessage {
		meta, err := loadConversationMeta(ctx, tx, evt.ConversationID)
		if err != nil {
			return err
		}
		if err := appendDomainEvent(ctx, tx, evt.ConversationID, evt.SenderUserID, domainEventMessageCreated, messageCreatedDomainEventPayload{
			MessageID:         messageID,
			ConversationID:    evt.ConversationID,
			ConversationType:  meta.Type,
			SenderUserID:      evt.SenderUserID,
			ContentType:       evt.ContentType,
			Content:           evt.Content,
			ClientGeneratedID: evt.ClientGeneratedID,
			Transport:         mapTransport(evt.TransportIntent),
			ServerOrder:       serverOrder,
			CreatedAt:         persistedAt.Format(time.RFC3339),
			Participants:      meta.Participants,
			ExternalPhones:    meta.ExternalPhones,
		}); err != nil {
			return err
		}
	}

	if err := tx.Commit(ctx); err != nil {
		recordProcessorTransaction("error", time.Since(txStartedAt))
		return err
	}
	recordProcessorTransaction("ok", time.Since(txStartedAt))

	ackStartedAt := time.Now()
	if err := p.publishPersistedAck(ctx, ackPayload{
		EventID:           evt.EventID,
		MessageID:         messageID,
		ConversationID:    evt.ConversationID,
		ServerOrder:       serverOrder,
		Status:            "SENT",
		Transport:         mapTransport(evt.TransportIntent),
		PersistedAtMS:     persistedAt.UnixMilli(),
		ClientGeneratedID: evt.ClientGeneratedID,
	}); err != nil {
		recordAckPublish("error", time.Since(ackStartedAt))
		return err
	}
	recordAckPublish("ok", time.Since(ackStartedAt))

	persisted := persistedEvent{
		EventID:         evt.EventID,
		MessageID:       messageID,
		ConversationID:  evt.ConversationID,
		SenderUserID:    evt.SenderUserID,
		ContentType:     evt.ContentType,
		Content:         evt.Content,
		ServerOrder:     serverOrder,
		Transport:       mapTransport(evt.TransportIntent),
		Status:          "SENT",
		PersistedAtMS:   persistedAt.UnixMilli(),
		DeliveryTargets: recipients,
		TraceID:         evt.TraceID,
	}
	body, _ := json.Marshal(persisted)
	if err := p.persistedW.WriteMessages(ctx, kafka.Message{
		Key:   []byte(evt.ConversationID),
		Value: body,
		Time:  time.Now().UTC(),
	}); err != nil {
		return err
	}
	if err := p.microserviceW.WriteMessages(ctx, kafka.Message{
		Key:   []byte(evt.ConversationID),
		Value: body,
		Time:  time.Now().UTC(),
	}); err != nil {
		return err
	}
	if mapTransport(evt.TransportIntent) == "SMS" {
		if err := p.smsW.WriteMessages(ctx, kafka.Message{
			Key:   []byte(evt.ConversationID),
			Value: body,
			Time:  time.Now().UTC(),
		}); err != nil {
			return err
		}
	}
	for _, recipientID := range recipients {
		if strings.TrimSpace(recipientID) == "" {
			continue
		}
		_ = p.redis.Publish(ctx, "message:user:"+recipientID, payload).Err()
	}
	if p.cassandra != nil {
		projectionStartedAt := time.Now()
		if err := p.writeCassandra(ctx, evt, messageID, serverOrder, persistedAt); err != nil {
			recordCassandraProjection("error", time.Since(projectionStartedAt))
			return err
		}
		recordCassandraProjection("ok", time.Since(projectionStartedAt))
	}
	return nil
}

func (p *processor) writeCassandra(ctx context.Context, evt ingressEvent, messageID string, serverOrder int64, persistedAt time.Time) error {
	msgUUID, err := gocql.ParseUUID(messageID)
	if err != nil {
		return err
	}
	convUUID, err := gocql.ParseUUID(evt.ConversationID)
	if err != nil {
		return err
	}
	senderUUID, err := gocql.ParseUUID(evt.SenderUserID)
	if err != nil {
		return err
	}
	contentJSON, _ := json.Marshal(evt.Content)
	bucket := persistedAt.UTC().Format("20060102")
	return p.cassandra.Query(`
		INSERT INTO messages_by_conversation
		(conversation_id, bucket_yyyymmdd, server_order, message_id, sender_user_id, content_type, content_json, transport, created_at)
		VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
	`, convUUID, bucket, serverOrder, msgUUID, senderUUID, evt.ContentType, string(contentJSON), mapTransport(evt.TransportIntent), persistedAt).WithContext(ctx).Exec()
}

func (p *processor) publishDLQ(ctx context.Context, msg kafka.Message, cause error) error {
	payload, _ := json.Marshal(map[string]any{
		"cause":     cause.Error(),
		"topic":     msg.Topic,
		"partition": msg.Partition,
		"offset":    msg.Offset,
		"value":     string(msg.Value),
		"at":        time.Now().UTC().Format(time.RFC3339Nano),
	})
	return p.dlqW.WriteMessages(ctx, kafka.Message{
		Key:   msg.Key,
		Value: payload,
		Time:  time.Now().UTC(),
	})
}

func loadRecipients(ctx context.Context, tx pgx.Tx, conversationID, senderID string) ([]string, error) {
	rows, err := tx.Query(ctx, `
		SELECT user_id::text
		FROM conversation_members
		WHERE conversation_id = $1::uuid
		  AND user_id <> $2::uuid
	`, conversationID, senderID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := make([]string, 0, 4)
	for rows.Next() {
		var id string
		if err := rows.Scan(&id); err != nil {
			return nil, err
		}
		out = append(out, id)
	}
	return out, rows.Err()
}

func loadConversationMeta(ctx context.Context, tx pgx.Tx, conversationID string) (conversationMeta, error) {
	var meta conversationMeta
	if err := tx.QueryRow(ctx, `SELECT type FROM conversations WHERE id = $1::uuid`, conversationID).Scan(&meta.Type); err != nil {
		return conversationMeta{}, err
	}

	memberRows, err := tx.Query(ctx, `
		SELECT user_id::text
		FROM conversation_members
		WHERE conversation_id = $1::uuid
		ORDER BY joined_at
	`, conversationID)
	if err != nil {
		return conversationMeta{}, err
	}
	for memberRows.Next() {
		var userID string
		if err := memberRows.Scan(&userID); err != nil {
			memberRows.Close()
			return conversationMeta{}, err
		}
		meta.Participants = append(meta.Participants, userID)
	}
	if err := memberRows.Err(); err != nil {
		memberRows.Close()
		return conversationMeta{}, err
	}
	memberRows.Close()

	phoneRows, err := tx.Query(ctx, `
		SELECT ec.phone_e164
		FROM conversation_external_members cem
		JOIN external_contacts ec ON ec.id = cem.external_contact_id
		WHERE cem.conversation_id = $1::uuid
		ORDER BY ec.phone_e164
	`, conversationID)
	if err != nil {
		return conversationMeta{}, err
	}
	for phoneRows.Next() {
		var phone string
		if err := phoneRows.Scan(&phone); err != nil {
			phoneRows.Close()
			return conversationMeta{}, err
		}
		meta.ExternalPhones = append(meta.ExternalPhones, phone)
	}
	if err := phoneRows.Err(); err != nil {
		phoneRows.Close()
		return conversationMeta{}, err
	}
	phoneRows.Close()

	return meta, nil
}

func appendDomainEvent(ctx context.Context, tx pgx.Tx, conversationID, actorUserID, eventType string, payload any) error {
	body, err := json.Marshal(payload)
	if err != nil {
		return err
	}
	_, err = tx.Exec(ctx, `
		INSERT INTO domain_events (conversation_id, actor_user_id, event_type, payload)
		VALUES ($1::uuid, NULLIF($2, '')::uuid, $3, $4::jsonb)
	`, conversationID, actorUserID, eventType, string(body))
	return err
}

func loadConfig() config {
	return config{
		KafkaBrokers:           getenv("APP_KAFKA_BROKERS", "localhost:9092"),
		KafkaIngressTopic:      getenv("APP_KAFKA_INGRESS_TOPIC", "msg.ingress.v1"),
		KafkaPersistedTopic:    getenv("APP_KAFKA_PERSISTED_TOPIC", "msg.persisted.v1"),
		KafkaMicroserviceTopic: getenv("APP_KAFKA_MICROSERVICE_TOPIC", "microservice.events.v1"),
		KafkaSMSDispatchTopic:  getenv("APP_KAFKA_SMS_DISPATCH_TOPIC", "msg.sms.dispatch.v1"),
		KafkaDLQTopic:          getenv("APP_KAFKA_DLQ_TOPIC", "msg.ingress.dlq.v1"),
		KafkaGroupID:           getenv("APP_KAFKA_GROUP_ID", "messages-processor-v1"),
		PostgresDSN:            getenv("APP_DB_DSN", "postgres://ohmf:ohmf@localhost:5432/ohmf?sslmode=disable"),
		RedisAddr:              getenv("APP_REDIS_ADDR", "localhost:6379"),
		CassandraHosts:         getenv("APP_CASSANDRA_HOSTS", "localhost:9042"),
		CassandraKeyspace:      getenv("APP_CASSANDRA_KEYSPACE", "ohmf_messages"),
		ShadowPostgresWrite:    getenv("APP_SHADOW_POSTGRES_WRITE", "true") == "true",
		AsyncCassandraProjection: getenv("APP_ASYNC_CASSANDRA_PROJECTION", "false") == "true",
	}
}

func newCassandra(cfg config) (*gocql.Session, error) {
	if cfg.AsyncCassandraProjection {
		return nil, nil
	}
	cluster := gocql.NewCluster(splitCSV(cfg.CassandraHosts)...)
	cluster.Consistency = gocql.Quorum
	cluster.Timeout = 5 * time.Second
	cluster.ConnectTimeout = 5 * time.Second

	systemSession, err := cluster.CreateSession()
	if err != nil {
		return nil, err
	}
	defer systemSession.Close()
	createKeyspace := fmt.Sprintf(`CREATE KEYSPACE IF NOT EXISTS %s WITH replication = {'class':'SimpleStrategy', 'replication_factor': 1}`, cfg.CassandraKeyspace)
	if err := systemSession.Query(createKeyspace).Exec(); err != nil {
		return nil, err
	}

	cluster.Keyspace = cfg.CassandraKeyspace
	return cluster.CreateSession()
}

func ensureCassandraSchema(ctx context.Context, s *gocql.Session, keyspace string) error {
	_ = keyspace
	return s.Query(`
		CREATE TABLE IF NOT EXISTS messages_by_conversation (
			conversation_id uuid,
			bucket_yyyymmdd text,
			server_order bigint,
			message_id uuid,
			sender_user_id uuid,
			content_type text,
			content_json text,
			transport text,
			created_at timestamp,
			PRIMARY KEY ((conversation_id, bucket_yyyymmdd), server_order)
		) WITH CLUSTERING ORDER BY (server_order DESC)
		AND compaction = {'class': 'TimeWindowCompactionStrategy'}
		AND default_time_to_live = 31536000
	`).WithContext(ctx).Exec()
}

func mapTransport(intent string) string {
	if strings.EqualFold(strings.TrimSpace(intent), "sms") {
		return "SMS"
	}
	return "OHMF"
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

func (p *processor) publishPersistedAck(ctx context.Context, ack ackPayload) error {
	body, err := json.Marshal(ack)
	if err != nil {
		return err
	}
	if err := p.redis.Set(ctx, "msg:ack:"+ack.EventID, string(body), 24*time.Hour).Err(); err != nil {
		return err
	}
	return p.redis.Publish(ctx, ackChannelName, body).Err()
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

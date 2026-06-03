package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"os"
	"strings"
	"time"

	"github.com/gocql/gocql"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
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
	HTTPAddr               string
	PostgresDSN            string
	RedisAddr              string
	CassandraHosts         string
	CassandraKeyspace      string
	ShadowPostgresWrite    bool
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

const (
	ackKeyPrefix    = "msg:ack:"
	ackSignalPrefix = "msg:ack:notify:"
)

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
	pg            processorDB
	redis         redisStore
	cassandra     cassandraStore
	reader        kafkaReader
	persistedW    kafkaMessageWriter
	microserviceW kafkaMessageWriter
	smsW          kafkaMessageWriter
	dlqW          kafkaMessageWriter
	obs           *processorObservability
}

const domainEventMessageCreated = "message_created"

const (
	sideEffectPersistedPublished = "persisted_published"
	sideEffectMicroserviceSent   = "microservice_published"
	sideEffectSMSDispatchSent    = "sms_dispatch_published"
	sideEffectRecipientFanout    = "recipient_fanout_published"
)

type processorDB interface {
	BeginTx(context.Context, pgx.TxOptions) (pgx.Tx, error)
	Exec(context.Context, string, ...any) (pgconn.CommandTag, error)
}

type redisStore interface {
	Set(context.Context, string, any, time.Duration) error
	Publish(context.Context, string, any) error
}

type cassandraStore interface {
	WriteMessage(context.Context, ingressEvent, string, int64, time.Time) error
	Close()
}

type kafkaReader interface {
	FetchMessage(context.Context) (kafka.Message, error)
	CommitMessages(context.Context, ...kafka.Message) error
	Close() error
}

type kafkaMessageWriter interface {
	WriteMessages(context.Context, ...kafka.Message) error
	Close() error
}

type redisClient struct {
	client *redis.Client
}

func (r redisClient) Set(ctx context.Context, key string, value any, expiration time.Duration) error {
	return r.client.Set(ctx, key, value, expiration).Err()
}

func (r redisClient) Publish(ctx context.Context, channel string, value any) error {
	return r.client.Publish(ctx, channel, value).Err()
}

type cassandraSession struct {
	session *gocql.Session
}

func (c cassandraSession) Close() {
	c.session.Close()
}

func (c cassandraSession) WriteMessage(ctx context.Context, evt ingressEvent, messageID string, serverOrder int64, persistedAt time.Time) error {
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
	return c.session.Query(`
		INSERT INTO messages_by_conversation
		(conversation_id, bucket_yyyymmdd, server_order, message_id, sender_user_id, content_type, content_json, transport, created_at)
		VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
	`, convUUID, bucket, serverOrder, msgUUID, senderUUID, evt.ContentType, string(contentJSON), mapTransport(evt.TransportIntent), persistedAt).WithContext(ctx).Exec()
}

type storedResponsePayload struct {
	MessageID         string                  `json:"message_id"`
	ConversationID    string                  `json:"conversation_id"`
	SenderUserID      string                  `json:"sender_user_id"`
	ContentType       string                  `json:"content_type"`
	Content           map[string]any          `json:"content"`
	ClientGeneratedID string                  `json:"client_generated_id,omitempty"`
	Transport         string                  `json:"transport"`
	ServerOrder       int64                   `json:"server_order"`
	Status            string                  `json:"status"`
	CreatedAt         string                  `json:"created_at"`
	SideEffects       storedResponseSideState `json:"side_effects,omitempty"`
}

type storedResponseSideState struct {
	PersistedPublished    bool `json:"persisted_published,omitempty"`
	MicroservicePublished bool `json:"microservice_published,omitempty"`
	SMSDispatchPublished  bool `json:"sms_dispatch_published,omitempty"`
	RecipientFanoutSent   bool `json:"recipient_fanout_published,omitempty"`
}

type retryableProcessError struct {
	err error
}

func (e *retryableProcessError) Error() string {
	return e.err.Error()
}

func (e *retryableProcessError) Unwrap() error {
	return e.err
}

func retryableError(format string, args ...any) error {
	return &retryableProcessError{err: fmt.Errorf(format, args...)}
}

func isRetryableProcessError(err error) bool {
	var target *retryableProcessError
	return errors.As(err, &target)
}

func main() {
	ctx := context.Background()
	cfg := loadConfig()

	pg, err := pgxpool.New(ctx, cfg.PostgresDSN)
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
	if err != nil {
		log.Fatalf("cassandra init failed: %v", err)
	}
	defer cass.Close()
	if err := ensureCassandraSchema(ctx, cass, cfg.CassandraKeyspace); err != nil {
		log.Fatalf("cassandra schema failed: %v", err)
	}

	brokers := splitCSV(cfg.KafkaBrokers)
	p := &processor{
		cfg:       cfg,
		pg:        pg,
		redis:     redisClient{client: rdb},
		cassandra: cassandraSession{session: cass},
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
	p.obs = newProcessorObservability(
		"messages",
		cfg.HTTPAddr,
		splitCSV(cfg.KafkaBrokers),
		[]dependencyCheck{
			{name: "postgres", check: func(ctx context.Context) error { return pg.Ping(ctx) }},
			{name: "redis", check: func(ctx context.Context) error { return rdb.Ping(ctx).Err() }},
			{name: "cassandra", check: func(ctx context.Context) error {
				var releaseVersion string
				return cass.Query(`SELECT release_version FROM system.local`).WithContext(ctx).Scan(&releaseVersion)
			}},
		},
	)
	p.obs.start()

	log.Printf("messages-processor started")
	for {
		msg, err := p.reader.FetchMessage(ctx)
		if err != nil {
			log.Printf("fetch failed: %v", err)
			time.Sleep(300 * time.Millisecond)
			continue
		}
		p.obs.setConsumerLag(messageLag(msg))
		p.handleFetchedMessage(ctx, msg)
	}
}

func (p *processor) handleFetchedMessage(ctx context.Context, msg kafka.Message) {
	startedAt := time.Now()
	if err := p.processMessage(ctx, msg); err != nil {
		p.obs.recordError(time.Since(startedAt))
		log.Printf("process failed: %v", err)
		if isRetryableProcessError(err) {
			log.Printf("recoverable processor failure; leaving offset uncommitted for retry")
			return
		}
		if dlqErr := p.publishDLQ(ctx, msg, err); dlqErr != nil {
			log.Printf("dlq publish failed: %v", dlqErr)
			return
		}
		p.obs.recordDLQPublish()
	} else {
		p.obs.recordSuccess(time.Since(startedAt))
	}
	if err := p.reader.CommitMessages(ctx, msg); err != nil {
		log.Printf("commit failed: %v", err)
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
		sideEffects storedResponseSideState
	)
	if err == nil && existingStatus == 201 {
		p.obs.recordDuplicate()
		var existing storedResponsePayload
		if uErr := json.Unmarshal(existingPayload, &existing); uErr == nil && existing.MessageID != "" {
			messageID = existing.MessageID
			serverOrder = existing.ServerOrder
			sideEffects = existing.SideEffects
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

	payload, _ := json.Marshal(storedResponsePayload{
		MessageID:         messageID,
		ConversationID:    evt.ConversationID,
		SenderUserID:      evt.SenderUserID,
		ContentType:       evt.ContentType,
		Content:           evt.Content,
		ClientGeneratedID: evt.ClientGeneratedID,
		Transport:         mapTransport(evt.TransportIntent),
		ServerOrder:       serverOrder,
		Status:            "SENT",
		CreatedAt:         persistedAt.Format(time.RFC3339),
		SideEffects:       sideEffects,
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
		return retryableError("postgres commit failed: %w", err)
	}

	if err := p.cassandra.WriteMessage(ctx, evt, messageID, serverOrder, persistedAt); err != nil {
		return retryableError("cassandra write failed after postgres commit: %w", err)
	}

	ackBody, _ := json.Marshal(ackPayload{
		EventID:           evt.EventID,
		MessageID:         messageID,
		ConversationID:    evt.ConversationID,
		ServerOrder:       serverOrder,
		Status:            "SENT",
		Transport:         mapTransport(evt.TransportIntent),
		PersistedAtMS:     persistedAt.UnixMilli(),
		ClientGeneratedID: evt.ClientGeneratedID,
	})
	if err := p.redis.Set(ctx, ackKeyPrefix+evt.EventID, string(ackBody), 24*time.Hour); err != nil {
		return retryableError("redis ack failed after persistence: %w", err)
	}
	if err := p.redis.Publish(ctx, ackSignalPrefix+evt.EventID, string(ackBody)); err != nil {
		return retryableError("redis ack notify failed after persistence: %w", err)
	}

	persisted := persistedEvent{
		EventID:         evt.EventID,
		MessageID:       messageID,
		ConversationID:  evt.ConversationID,
		SenderUserID:    evt.SenderUserID,
		ServerOrder:     serverOrder,
		Transport:       mapTransport(evt.TransportIntent),
		Status:          "SENT",
		PersistedAtMS:   persistedAt.UnixMilli(),
		DeliveryTargets: recipients,
		TraceID:         evt.TraceID,
	}
	body, _ := json.Marshal(persisted)
	if !sideEffects.PersistedPublished {
		if err := p.persistedW.WriteMessages(ctx, kafka.Message{
			Key:   []byte(evt.ConversationID),
			Value: body,
			Time:  time.Now().UTC(),
		}); err != nil {
			return retryableError("persisted topic publish failed after persistence: %w", err)
		}
		if err := p.markSideEffect(ctx, evt, sideEffectPersistedPublished); err != nil {
			return retryableError("mark persisted topic side effect failed: %w", err)
		}
	}
	if !sideEffects.MicroservicePublished {
		if err := p.microserviceW.WriteMessages(ctx, kafka.Message{
			Key:   []byte(evt.ConversationID),
			Value: body,
			Time:  time.Now().UTC(),
		}); err != nil {
			return retryableError("microservice topic publish failed after persistence: %w", err)
		}
		if err := p.markSideEffect(ctx, evt, sideEffectMicroserviceSent); err != nil {
			return retryableError("mark microservice topic side effect failed: %w", err)
		}
	}
	if mapTransport(evt.TransportIntent) == "SMS" && !sideEffects.SMSDispatchPublished {
		if err := p.smsW.WriteMessages(ctx, kafka.Message{
			Key:   []byte(evt.ConversationID),
			Value: body,
			Time:  time.Now().UTC(),
		}); err != nil {
			return retryableError("sms dispatch publish failed after persistence: %w", err)
		}
		if err := p.markSideEffect(ctx, evt, sideEffectSMSDispatchSent); err != nil {
			return retryableError("mark sms dispatch side effect failed: %w", err)
		}
	}
	if !sideEffects.RecipientFanoutSent {
		for _, recipientID := range recipients {
			if strings.TrimSpace(recipientID) == "" {
				continue
			}
			_ = p.redis.Publish(ctx, "message:user:"+recipientID, payload)
		}
		if err := p.markSideEffect(ctx, evt, sideEffectRecipientFanout); err != nil {
			return retryableError("mark recipient fanout side effect failed: %w", err)
		}
	}
	return nil
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

func (p *processor) markSideEffect(ctx context.Context, evt ingressEvent, effect string) error {
	_, err := p.pg.Exec(ctx, `
		UPDATE idempotency_keys
		SET response_payload = jsonb_set(
			COALESCE(response_payload, '{}'::jsonb),
			$4::text[],
			'true'::jsonb,
			true
		)
		WHERE actor_user_id = $1::uuid AND endpoint = $2 AND key = $3
	`, evt.SenderUserID, evt.Endpoint, evt.IdempotencyKey, []string{"side_effects", effect})
	return err
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
		HTTPAddr:               getenv("APP_HTTP_ADDR", ":18088"),
		PostgresDSN:            getenv("APP_DB_DSN", "postgres://ohmf:ohmf@localhost:5432/ohmf?sslmode=disable"),
		RedisAddr:              getenv("APP_REDIS_ADDR", "localhost:6379"),
		CassandraHosts:         getenv("APP_CASSANDRA_HOSTS", "localhost:9042"),
		CassandraKeyspace:      getenv("APP_CASSANDRA_KEYSPACE", "ohmf_messages"),
		ShadowPostgresWrite:    getenv("APP_SHADOW_POSTGRES_WRITE", "true") == "true",
	}
}

func newCassandra(cfg config) (*gocql.Session, error) {
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

func messageLag(msg kafka.Message) float64 {
	if msg.HighWaterMark <= 0 {
		return 0
	}
	lag := msg.HighWaterMark - msg.Offset - 1
	if lag < 0 {
		return 0
	}
	return float64(lag)
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

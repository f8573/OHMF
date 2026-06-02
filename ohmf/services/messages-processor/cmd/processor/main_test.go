package main

import (
	"context"
	"encoding/json"
	"errors"
	"regexp"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	pgxmock "github.com/pashagolub/pgxmock/v4"
	"github.com/segmentio/kafka-go"
)

func TestHandleFetchedMessageRetriesCassandraFailureAfterPostgresPersistence(t *testing.T) {
	ctx := context.Background()
	mock := newProcessorMockDB(t)
	evt := testIngressEvent()
	expectFreshMessage(mock, evt)

	reader := &stubKafkaReader{}
	dlq := &stubKafkaWriter{}
	p := newTestProcessor(mock, reader, dlq)
	p.cassandra = &stubCassandraStore{failures: 1, err: errors.New("cassandra unavailable")}

	p.handleFetchedMessage(ctx, kafkaMessageFromIngress(t, evt))

	if reader.commitCount != 0 {
		t.Fatalf("expected no offset commit on recoverable cassandra failure, got %d", reader.commitCount)
	}
	if len(dlq.messages) != 0 {
		t.Fatalf("expected no DLQ publish on recoverable cassandra failure, got %d", len(dlq.messages))
	}
	assertMockExpectations(t, mock)
}

func TestHandleFetchedMessageRetriesRedisAckFailureAfterPersistence(t *testing.T) {
	ctx := context.Background()
	mock := newProcessorMockDB(t)
	evt := testIngressEvent()
	expectFreshMessage(mock, evt)

	reader := &stubKafkaReader{}
	dlq := &stubKafkaWriter{}
	redis := &stubRedisStore{setFailures: 1, setErr: errors.New("redis unavailable")}
	p := newTestProcessor(mock, reader, dlq)
	p.redis = redis

	p.handleFetchedMessage(ctx, kafkaMessageFromIngress(t, evt))

	if reader.commitCount != 0 {
		t.Fatalf("expected no offset commit on recoverable redis ack failure, got %d", reader.commitCount)
	}
	if len(dlq.messages) != 0 {
		t.Fatalf("expected no DLQ publish on recoverable redis ack failure, got %d", len(dlq.messages))
	}
	if redis.setCalls != 1 {
		t.Fatalf("expected one redis ack attempt, got %d", redis.setCalls)
	}
	assertMockExpectations(t, mock)
}

func TestHandleFetchedMessageRetriesKafkaPublishFailureAfterPersistence(t *testing.T) {
	ctx := context.Background()
	mock := newProcessorMockDB(t)
	evt := testIngressEvent()
	expectFreshMessage(mock, evt)
	expectSideEffectMark(mock, sideEffectPersistedPublished)
	expectDuplicateMessage(mock, evt, storedResponseSideState{PersistedPublished: true})
	expectSideEffectMark(mock, sideEffectMicroserviceSent)
	expectSideEffectMark(mock, sideEffectRecipientFanout)

	reader := &stubKafkaReader{}
	dlq := &stubKafkaWriter{}
	persisted := &stubKafkaWriter{}
	microservice := &stubKafkaWriter{failures: 1, err: errors.New("kafka publish failed")}
	p := newTestProcessor(mock, reader, dlq)
	p.persistedW = persisted
	p.microserviceW = microservice

	msg := kafkaMessageFromIngress(t, evt)
	p.handleFetchedMessage(ctx, msg)
	p.handleFetchedMessage(ctx, msg)

	if reader.commitCount != 1 {
		t.Fatalf("expected offset commit only after successful retry, got %d", reader.commitCount)
	}
	if len(dlq.messages) != 0 {
		t.Fatalf("expected no DLQ publish on recoverable kafka failure, got %d", len(dlq.messages))
	}
	if len(persisted.messages) != 1 {
		t.Fatalf("expected persisted topic to publish once across retry, got %d", len(persisted.messages))
	}
	if len(microservice.messages) != 1 {
		t.Fatalf("expected microservice topic to publish successfully once across retry, got %d", len(microservice.messages))
	}
	if microservice.attempts != 2 {
		t.Fatalf("expected microservice topic to be attempted twice, got %d", microservice.attempts)
	}
	assertMockExpectations(t, mock)
}

func TestHandleFetchedMessageSkipsUnsafeDuplicateDownstreamPublishesOnRedelivery(t *testing.T) {
	ctx := context.Background()
	mock := newProcessorMockDB(t)
	evt := testIngressEvent()
	expectFreshMessage(mock, evt)
	expectSideEffectMark(mock, sideEffectPersistedPublished)
	expectSideEffectMark(mock, sideEffectMicroserviceSent)
	expectSideEffectMark(mock, sideEffectRecipientFanout)
	expectDuplicateMessage(mock, evt, storedResponseSideState{
		PersistedPublished:    true,
		MicroservicePublished: true,
		RecipientFanoutSent:   true,
	})

	reader := &stubKafkaReader{}
	dlq := &stubKafkaWriter{}
	redis := &stubRedisStore{}
	persisted := &stubKafkaWriter{}
	microservice := &stubKafkaWriter{}
	p := newTestProcessor(mock, reader, dlq)
	p.redis = redis
	p.persistedW = persisted
	p.microserviceW = microservice

	msg := kafkaMessageFromIngress(t, evt)
	p.handleFetchedMessage(ctx, msg)
	p.handleFetchedMessage(ctx, msg)

	if reader.commitCount != 2 {
		t.Fatalf("expected both successful deliveries to commit offsets, got %d", reader.commitCount)
	}
	if len(persisted.messages) != 1 {
		t.Fatalf("expected persisted topic to publish once across duplicate redelivery, got %d", len(persisted.messages))
	}
	if len(microservice.messages) != 1 {
		t.Fatalf("expected microservice topic to publish once across duplicate redelivery, got %d", len(microservice.messages))
	}
	if redis.setCalls != 2 {
		t.Fatalf("expected redis ack overwrite to be attempted on both deliveries, got %d", redis.setCalls)
	}
	if len(redis.publishChannels) != 1 {
		t.Fatalf("expected recipient fanout to publish once across duplicate redelivery, got %d", len(redis.publishChannels))
	}
	if len(dlq.messages) != 0 {
		t.Fatalf("expected no DLQ publish on successful duplicate redelivery, got %d", len(dlq.messages))
	}
	assertMockExpectations(t, mock)
}

func newTestProcessor(db processorDB, reader kafkaReader, dlq kafkaMessageWriter) *processor {
	return &processor{
		cfg: config{
			ShadowPostgresWrite: true,
		},
		pg:            db,
		redis:         &stubRedisStore{},
		cassandra:     &stubCassandraStore{},
		reader:        reader,
		persistedW:    &stubKafkaWriter{},
		microserviceW: &stubKafkaWriter{},
		smsW:          &stubKafkaWriter{},
		dlqW:          dlq,
		obs:           newProcessorObservability("messages", ":0", []string{"127.0.0.1:1"}, nil),
	}
}

func newProcessorMockDB(t *testing.T) pgxmock.PgxPoolIface {
	t.Helper()
	mock, err := pgxmock.NewPool()
	if err != nil {
		t.Fatalf("pgxmock.NewPool failed: %v", err)
	}
	return mock
}

func expectFreshMessage(mock pgxmock.PgxPoolIface, evt ingressEvent) {
	mock.ExpectBegin()
	mock.ExpectQuery("FROM conversation_members").
		WithArgs(evt.ConversationID, evt.SenderUserID).
		WillReturnRows(pgxmock.NewRows([]string{"one"}).AddRow(1))
	mock.ExpectQuery("FROM idempotency_keys").
		WithArgs(evt.SenderUserID, evt.Endpoint, evt.IdempotencyKey).
		WillReturnError(pgx.ErrNoRows)
	mock.ExpectQuery("UPDATE conversation_counters").
		WithArgs(evt.ConversationID).
		WillReturnRows(pgxmock.NewRows([]string{"next_server_order"}).AddRow(int64(7)))
	mock.ExpectExec("INSERT INTO messages").
		WithArgs(
			evt.MessageID,
			evt.ConversationID,
			evt.SenderUserID,
			evt.ContentType,
			pgxmock.AnyArg(),
			evt.ClientGeneratedID,
			mapTransport(evt.TransportIntent),
			int64(7),
		).
		WillReturnResult(pgxmock.NewResult("INSERT", 1))
	mock.ExpectExec("UPDATE conversations").
		WithArgs(evt.ConversationID, evt.MessageID).
		WillReturnResult(pgxmock.NewResult("UPDATE", 1))
	mock.ExpectExec("INSERT INTO idempotency_keys").
		WithArgs(evt.SenderUserID, evt.Endpoint, evt.IdempotencyKey, pgxmock.AnyArg()).
		WillReturnResult(pgxmock.NewResult("INSERT", 1))
	mock.ExpectQuery("SELECT user_id::text\\s+FROM conversation_members").
		WithArgs(evt.ConversationID, evt.SenderUserID).
		WillReturnRows(pgxmock.NewRows([]string{"user_id"}).AddRow("33333333-3333-3333-3333-333333333333"))
	mock.ExpectQuery("SELECT type FROM conversations").
		WithArgs(evt.ConversationID).
		WillReturnRows(pgxmock.NewRows([]string{"type"}).AddRow("GROUP"))
	mock.ExpectQuery("ORDER BY joined_at").
		WithArgs(evt.ConversationID).
		WillReturnRows(pgxmock.NewRows([]string{"user_id"}).
			AddRow(evt.SenderUserID).
			AddRow("33333333-3333-3333-3333-333333333333"))
	mock.ExpectQuery("JOIN external_contacts").
		WithArgs(evt.ConversationID).
		WillReturnRows(pgxmock.NewRows([]string{"phone_e164"}))
	mock.ExpectExec("INSERT INTO domain_events").
		WithArgs(evt.ConversationID, evt.SenderUserID, domainEventMessageCreated, pgxmock.AnyArg()).
		WillReturnResult(pgxmock.NewResult("INSERT", 1))
	mock.ExpectCommit()
}

func expectDuplicateMessage(mock pgxmock.PgxPoolIface, evt ingressEvent, sideEffects storedResponseSideState) {
	mock.ExpectBegin()
	mock.ExpectQuery("FROM conversation_members").
		WithArgs(evt.ConversationID, evt.SenderUserID).
		WillReturnRows(pgxmock.NewRows([]string{"one"}).AddRow(1))
	payload, _ := json.Marshal(storedResponsePayload{
		MessageID:      evt.MessageID,
		ConversationID: evt.ConversationID,
		SenderUserID:   evt.SenderUserID,
		ContentType:    evt.ContentType,
		Content:        evt.Content,
		Transport:      mapTransport(evt.TransportIntent),
		ServerOrder:    7,
		Status:         "SENT",
		CreatedAt:      time.Now().UTC().Format(time.RFC3339),
		SideEffects:    sideEffects,
	})
	mock.ExpectQuery("FROM idempotency_keys").
		WithArgs(evt.SenderUserID, evt.Endpoint, evt.IdempotencyKey).
		WillReturnRows(pgxmock.NewRows([]string{"response_payload", "status_code"}).AddRow(payload, 201))
	mock.ExpectExec("INSERT INTO idempotency_keys").
		WithArgs(evt.SenderUserID, evt.Endpoint, evt.IdempotencyKey, pgxmock.AnyArg()).
		WillReturnResult(pgxmock.NewResult("UPDATE", 1))
	mock.ExpectQuery("SELECT user_id::text\\s+FROM conversation_members").
		WithArgs(evt.ConversationID, evt.SenderUserID).
		WillReturnRows(pgxmock.NewRows([]string{"user_id"}).AddRow("33333333-3333-3333-3333-333333333333"))
	mock.ExpectCommit()
}

func expectSideEffectMark(mock pgxmock.PgxPoolIface, effect string) {
	mock.ExpectExec(regexp.QuoteMeta("UPDATE idempotency_keys")).
		WithArgs(pgxmock.AnyArg(), pgxmock.AnyArg(), pgxmock.AnyArg(), []string{"side_effects", effect}).
		WillReturnResult(pgxmock.NewResult("UPDATE", 1))
}

func assertMockExpectations(t *testing.T, mock pgxmock.PgxPoolIface) {
	t.Helper()
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("unmet pg expectations: %v", err)
	}
}

func testIngressEvent() ingressEvent {
	return ingressEvent{
		EventID:           "evt-1",
		MessageID:         "11111111-1111-1111-1111-111111111111",
		ConversationID:    "22222222-2222-2222-2222-222222222222",
		SenderUserID:      "aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa",
		IdempotencyKey:    "idem-1",
		Endpoint:          "/v1/messages/send",
		ClientGeneratedID: "client-1",
		ContentType:       "text/plain",
		Content:           map[string]any{"text": "hello"},
		TransportIntent:   "OHMF",
		SentAtMS:          1700000000000,
		TraceID:           "trace-1",
	}
}

func kafkaMessageFromIngress(t *testing.T, evt ingressEvent) kafka.Message {
	t.Helper()
	body, err := json.Marshal(evt)
	if err != nil {
		t.Fatalf("marshal ingressEvent: %v", err)
	}
	return kafka.Message{
		Topic:     "msg.ingress.v1",
		Partition: 0,
		Offset:    12,
		Key:       []byte(evt.ConversationID),
		Value:     body,
	}
}

type stubKafkaReader struct {
	commitCount int
}

func (s *stubKafkaReader) FetchMessage(context.Context) (kafka.Message, error) {
	return kafka.Message{}, errors.New("not implemented")
}

func (s *stubKafkaReader) CommitMessages(_ context.Context, _ ...kafka.Message) error {
	s.commitCount++
	return nil
}

func (s *stubKafkaReader) Close() error {
	return nil
}

type stubKafkaWriter struct {
	attempts int
	failures int
	err      error
	messages []kafka.Message
}

func (s *stubKafkaWriter) WriteMessages(_ context.Context, msgs ...kafka.Message) error {
	s.attempts++
	if s.failures > 0 {
		s.failures--
		if s.err != nil {
			return s.err
		}
		return errors.New("forced write failure")
	}
	s.messages = append(s.messages, msgs...)
	return nil
}

func (s *stubKafkaWriter) Close() error {
	return nil
}

type stubRedisStore struct {
	setCalls        int
	setFailures     int
	setErr          error
	publishChannels []string
}

func (s *stubRedisStore) Set(_ context.Context, _ string, _ any, _ time.Duration) error {
	s.setCalls++
	if s.setFailures > 0 {
		s.setFailures--
		if s.setErr != nil {
			return s.setErr
		}
		return errors.New("forced redis set failure")
	}
	return nil
}

func (s *stubRedisStore) Publish(_ context.Context, channel string, _ any) error {
	s.publishChannels = append(s.publishChannels, channel)
	return nil
}

type stubCassandraStore struct {
	failures int
	err      error
}

func (s *stubCassandraStore) WriteMessage(context.Context, ingressEvent, string, int64, time.Time) error {
	if s.failures > 0 {
		s.failures--
		if s.err != nil {
			return s.err
		}
		return errors.New("forced cassandra failure")
	}
	return nil
}

func (s *stubCassandraStore) Close() {}

package messages

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"sync"
	"testing"
	"time"

	miniredis "github.com/alicebob/miniredis/v2"
	"github.com/jackc/pgx/v5"
	pgxmock "github.com/pashagolub/pgxmock/v4"
	"github.com/redis/go-redis/v9"
)

func TestAsyncPipelineWaitAckReturnsPublishedAck(t *testing.T) {
	mr, err := miniredis.Run()
	if err != nil {
		t.Fatalf("start miniredis: %v", err)
	}
	defer mr.Close()

	rdb := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	defer rdb.Close()

	pipeline := &AsyncPipeline{redis: rdb}
	want := PersistedAck{
		EventID:        "event-1",
		MessageID:      "message-1",
		ConversationID: "conversation-1",
		ServerOrder:    41,
		Status:         "SENT",
		Transport:      "OHMF",
		PersistedAtMS:  1700000000000,
	}
	errCh := make(chan error, 1)

	go func() {
		time.Sleep(20 * time.Millisecond)
		errCh <- writeAckForTest(rdb, want.EventID, want)
	}()

	got, ok, err := pipeline.WaitAck(context.Background(), want.EventID, 500*time.Millisecond)
	if err != nil {
		t.Fatalf("WaitAck failed: %v", err)
	}
	if !ok {
		t.Fatalf("expected ack to arrive before timeout")
	}
	if got != want {
		t.Fatalf("unexpected ack: %+v", got)
	}
	if err := <-errCh; err != nil {
		t.Fatalf("write ack: %v", err)
	}
}

func TestAsyncPipelineWaitAckTimeoutDoesNotBusyPoll(t *testing.T) {
	mr, err := miniredis.Run()
	if err != nil {
		t.Fatalf("start miniredis: %v", err)
	}
	defer mr.Close()

	rdb := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	defer rdb.Close()

	hook := newRedisCommandCounter()
	rdb.AddHook(hook)

	pipeline := &AsyncPipeline{redis: rdb}
	start := time.Now()
	_, ok, err := pipeline.WaitAck(context.Background(), "event-timeout", 1200*time.Millisecond)
	if err != nil {
		t.Fatalf("WaitAck returned error: %v", err)
	}
	if ok {
		t.Fatalf("expected timeout without ack")
	}
	if elapsed := time.Since(start); elapsed < time.Second {
		t.Fatalf("WaitAck returned too early: %v", elapsed)
	}
	if got := hook.Count("get"); got > 4 {
		t.Fatalf("expected sparse fallback reads plus final timeout check, got %d GET commands", got)
	}
}

func TestAsyncPipelineWaitAckTreatsRedisGetDeadlineAsTimeout(t *testing.T) {
	mr, err := miniredis.Run()
	if err != nil {
		t.Fatalf("start miniredis: %v", err)
	}
	defer mr.Close()

	rdb := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	defer rdb.Close()

	rdb.AddHook(redisDelayHook{delay: 40 * time.Millisecond})

	pipeline := &AsyncPipeline{redis: rdb}
	_, ok, err := pipeline.WaitAck(context.Background(), "event-read-timeout", 10*time.Millisecond)
	if err != nil {
		t.Fatalf("WaitAck returned error instead of timeout: %v", err)
	}
	if ok {
		t.Fatalf("expected timeout without ack")
	}
}

func TestSendAsyncReturnsCanonicalMessageWhenAckArrives(t *testing.T) {
	mock, err := pgxmock.NewPool()
	if err != nil {
		t.Fatal(err)
	}
	defer mock.Close()

	mr, err := miniredis.Run()
	if err != nil {
		t.Fatalf("start miniredis: %v", err)
	}
	defer mr.Close()

	rdb := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	defer rdb.Close()

	producer := &recordingIngressProducer{published: make(chan IngressEvent, 2)}
	pipeline := NewAsyncPipeline(producer, rdb)
	svc := &Service{
		db:           mock,
		useKafkaSend: true,
		async:        pipeline,
		ackTimeout:   500 * time.Millisecond,
	}

	expectAsyncTextSend(mock, 202, 201)
	errCh := make(chan error, 1)

	go func() {
		evt := <-producer.published
		errCh <- writeAckForTest(rdb, evt.EventID, PersistedAck{
			EventID:        evt.EventID,
			MessageID:      evt.MessageID,
			ConversationID: evt.ConversationID,
			ServerOrder:    77,
			Status:         "SENT",
			Transport:      "OHMF",
			PersistedAtMS:  1700000000123,
		})
	}()

	result, err := svc.Send(context.Background(), "user-1", "device-1", "conversation-1", "idem-1", "text", map[string]any{"text": "hi"}, "client-1", "trace-1", "")
	if err != nil {
		t.Fatalf("Send failed: %v", err)
	}
	if result.Queued {
		t.Fatalf("expected canonical response, got queued result")
	}
	if result.Message.ServerOrder != 77 {
		t.Fatalf("expected canonical server_order, got %d", result.Message.ServerOrder)
	}
	if result.Message.Status != "SENT" {
		t.Fatalf("expected SENT status, got %q", result.Message.Status)
	}
	if result.Message.Transport != "OHMF" {
		t.Fatalf("expected OHMF transport, got %q", result.Message.Transport)
	}
	if result.Message.ClientGeneratedID != "client-1" {
		t.Fatalf("expected client_generated_id to survive ack reconciliation, got %q", result.Message.ClientGeneratedID)
	}
	if err := <-errCh; err != nil {
		t.Fatalf("write ack: %v", err)
	}

	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("unmet expectations: %v", err)
	}
}

func TestSendAsyncReturnsCanonicalMessageAfterAckTimeoutOnceDurableStateIsVisible(t *testing.T) {
	mock, err := pgxmock.NewPool()
	if err != nil {
		t.Fatal(err)
	}
	defer mock.Close()

	mr, err := miniredis.Run()
	if err != nil {
		t.Fatalf("start miniredis: %v", err)
	}
	defer mr.Close()

	rdb := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	defer rdb.Close()

	producer := &recordingIngressProducer{published: make(chan IngressEvent, 1)}
	pipeline := NewAsyncPipeline(producer, rdb)
	svc := &Service{
		db:           mock,
		useKafkaSend: true,
		async:        pipeline,
		ackTimeout:   40 * time.Millisecond,
	}

	expectMembershipOK(mock)
	expectUnblocked(mock)
	mock.ExpectQuery(`SELECT type, COALESCE\(encryption_state, 'PLAINTEXT'\), COALESCE\(is_mls_encrypted, false\) FROM conversations WHERE id = \$1::uuid`).
		WithArgs("conversation-1").
		WillReturnRows(pgxmock.NewRows([]string{"type", "encryption_state", "is_mls_encrypted"}).AddRow("GROUP", "PLAINTEXT", false))
	mock.ExpectQuery(`SELECT response_payload, COALESCE\(status_code, 201\)`).
		WithArgs("user-1", "/v1/messages", "idem-1").
		WillReturnError(pgx.ErrNoRows)
	mock.ExpectExec(`INSERT INTO idempotency_keys \(actor_user_id, endpoint, key, response_payload, status_code, expires_at\)`).
		WithArgs("user-1", "/v1/messages", "idem-1", pgxmock.AnyArg(), 202).
		WillReturnResult(pgxmock.NewResult("INSERT", 1))
	mock.ExpectQuery(`SELECT response_payload, COALESCE\(status_code, 201\)`).
		WithArgs("user-1", "/v1/messages", "idem-1").
		WillReturnRows(pgxmock.NewRows([]string{"response_payload", "status_code"}).AddRow(mustJSONMessage(t, Message{
			MessageID:         "message-durable-1",
			ConversationID:    "conversation-1",
			SenderUserID:      "user-1",
			SenderDeviceID:    "device-1",
			ContentType:       "text",
			Content:           map[string]any{"text": "hi"},
			ClientGeneratedID: "client-1",
			Transport:         "OHMF",
			ServerOrder:       91,
			Status:            "SENT",
			CreatedAt:         "2026-06-09T00:00:00Z",
		}), 201))

	result, err := svc.Send(context.Background(), "user-1", "device-1", "conversation-1", "idem-1", "text", map[string]any{"text": "hi"}, "client-1", "trace-1", "")
	if err != nil {
		t.Fatalf("Send failed: %v", err)
	}
	if result.Queued {
		t.Fatalf("expected canonical success after durable state became visible, got queued result")
	}
	if result.Message.MessageID != "message-durable-1" {
		t.Fatalf("expected durable message to be returned, got %+v", result.Message)
	}
	if result.Message.ServerOrder != 91 || result.Message.Status != "SENT" {
		t.Fatalf("unexpected durable message payload: %+v", result.Message)
	}

	expectMembershipOK(mock)
	expectUnblocked(mock)
	mock.ExpectQuery(`SELECT type, COALESCE\(encryption_state, 'PLAINTEXT'\), COALESCE\(is_mls_encrypted, false\) FROM conversations WHERE id = \$1::uuid`).
		WithArgs("conversation-1").
		WillReturnRows(pgxmock.NewRows([]string{"type", "encryption_state", "is_mls_encrypted"}).AddRow("GROUP", "PLAINTEXT", false))
	mock.ExpectQuery(`SELECT response_payload, COALESCE\(status_code, 201\)`).
		WithArgs("user-1", "/v1/messages", "idem-1").
		WillReturnRows(pgxmock.NewRows([]string{"response_payload", "status_code"}).AddRow(mustJSONMessage(t, Message{
			MessageID:         "message-durable-1",
			ConversationID:    "conversation-1",
			SenderUserID:      "user-1",
			SenderDeviceID:    "device-1",
			ContentType:       "text",
			Content:           map[string]any{"text": "hi"},
			ClientGeneratedID: "client-1",
			Transport:         "OHMF",
			ServerOrder:       91,
			Status:            "SENT",
			CreatedAt:         "2026-06-09T00:00:00Z",
		}), 201))

	retry, err := svc.Send(context.Background(), "user-1", "device-1", "conversation-1", "idem-1", "text", map[string]any{"text": "hi"}, "client-1", "trace-1", "")
	if err != nil {
		t.Fatalf("retry Send failed: %v", err)
	}
	if retry.Queued {
		t.Fatalf("expected retry to return original message, not queued")
	}
	if retry.Message.MessageID != result.Message.MessageID || retry.Message.ServerOrder != result.Message.ServerOrder {
		t.Fatalf("expected retry to return original message, got %+v vs %+v", retry.Message, result.Message)
	}

	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("unmet expectations: %v", err)
	}
}

func TestSendAsyncRedisAckFailureBeforeDurablePersistenceFailsSafely(t *testing.T) {
	mock, err := pgxmock.NewPool()
	if err != nil {
		t.Fatal(err)
	}
	defer mock.Close()

	mr, err := miniredis.Run()
	if err != nil {
		t.Fatalf("start miniredis: %v", err)
	}
	defer mr.Close()

	rdb := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	defer rdb.Close()
	rdb.AddHook(redisGetErrorHook{err: errors.New("redis unavailable")})

	producer := &recordingIngressProducer{published: make(chan IngressEvent, 1)}
	pipeline := NewAsyncPipeline(producer, rdb)
	svc := &Service{
		db:           mock,
		useKafkaSend: true,
		async:        pipeline,
		ackTimeout:   40 * time.Millisecond,
	}

	expectMembershipOK(mock)
	expectUnblocked(mock)
	mock.ExpectQuery(`SELECT type, COALESCE\(encryption_state, 'PLAINTEXT'\), COALESCE\(is_mls_encrypted, false\) FROM conversations WHERE id = \$1::uuid`).
		WithArgs("conversation-1").
		WillReturnRows(pgxmock.NewRows([]string{"type", "encryption_state", "is_mls_encrypted"}).AddRow("GROUP", "PLAINTEXT", false))
	mock.ExpectQuery(`SELECT response_payload, COALESCE\(status_code, 201\)`).
		WithArgs("user-1", "/v1/messages", "idem-1").
		WillReturnError(pgx.ErrNoRows)
	mock.ExpectExec(`INSERT INTO idempotency_keys \(actor_user_id, endpoint, key, response_payload, status_code, expires_at\)`).
		WithArgs("user-1", "/v1/messages", "idem-1", pgxmock.AnyArg(), 202).
		WillReturnResult(pgxmock.NewResult("INSERT", 1))
	mock.ExpectQuery(`SELECT response_payload, COALESCE\(status_code, 201\)`).
		WithArgs("user-1", "/v1/messages", "idem-1").
		WillReturnRows(pgxmock.NewRows([]string{"response_payload", "status_code"}).AddRow(mustJSONMessage(t, Message{
			MessageID:         "message-provisional-1",
			ConversationID:    "conversation-1",
			SenderUserID:      "user-1",
			SenderDeviceID:    "device-1",
			ContentType:       "text",
			Content:           map[string]any{"text": "hi"},
			ClientGeneratedID: "client-1",
			Transport:         "OHMF",
			ServerOrder:       0,
			Status:            "QUEUED",
			CreatedAt:         "2026-06-09T00:00:00Z",
		}), 202))

	_, err = svc.Send(context.Background(), "user-1", "device-1", "conversation-1", "idem-1", "text", map[string]any{"text": "hi"}, "client-1", "trace-1", "")
	if err == nil {
		t.Fatalf("expected send to fail safely before durable persistence is visible")
	}

	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("unmet expectations: %v", err)
	}
}

func TestSendAsyncReturnsAckWhenPostAckIdempotencyPersistFails(t *testing.T) {
	mock, err := pgxmock.NewPool()
	if err != nil {
		t.Fatal(err)
	}
	defer mock.Close()

	mr, err := miniredis.Run()
	if err != nil {
		t.Fatalf("start miniredis: %v", err)
	}
	defer mr.Close()

	rdb := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	defer rdb.Close()

	producer := &recordingIngressProducer{published: make(chan IngressEvent, 1)}
	pipeline := NewAsyncPipeline(producer, rdb)
	svc := &Service{
		db:           mock,
		useKafkaSend: true,
		async:        pipeline,
		ackTimeout:   500 * time.Millisecond,
	}

	expectMembershipOK(mock)
	expectUnblocked(mock)
	mock.ExpectQuery(`SELECT type, COALESCE\(encryption_state, 'PLAINTEXT'\), COALESCE\(is_mls_encrypted, false\) FROM conversations WHERE id = \$1::uuid`).
		WithArgs("conversation-1").
		WillReturnRows(pgxmock.NewRows([]string{"type", "encryption_state", "is_mls_encrypted"}).AddRow("GROUP", "PLAINTEXT", false))
	mock.ExpectQuery(`SELECT response_payload, COALESCE\(status_code, 201\)`).
		WithArgs("user-1", "/v1/messages", "idem-1").
		WillReturnError(pgx.ErrNoRows)
	mock.ExpectExec(`INSERT INTO idempotency_keys \(actor_user_id, endpoint, key, response_payload, status_code, expires_at\)`).
		WithArgs("user-1", "/v1/messages", "idem-1", pgxmock.AnyArg(), 202).
		WillReturnResult(pgxmock.NewResult("INSERT", 1))
	mock.ExpectExec(`INSERT INTO idempotency_keys \(actor_user_id, endpoint, key, response_payload, status_code, expires_at\)`).
		WithArgs("user-1", "/v1/messages", "idem-1", pgxmock.AnyArg(), 201).
		WillReturnError(errors.New("idempotency store unavailable"))

	errCh := make(chan error, 1)
	go func() {
		evt := <-producer.published
		errCh <- writeAckForTest(rdb, evt.EventID, PersistedAck{
			EventID:        evt.EventID,
			MessageID:      evt.MessageID,
			ConversationID: evt.ConversationID,
			ServerOrder:    79,
			Status:         "SENT",
			Transport:      "OHMF",
			PersistedAtMS:  1700000000222,
		})
	}()

	result, err := svc.Send(context.Background(), "user-1", "device-1", "conversation-1", "idem-1", "text", map[string]any{"text": "hi"}, "client-1", "trace-1", "")
	if err != nil {
		t.Fatalf("Send failed: %v", err)
	}
	if result.Queued {
		t.Fatalf("expected canonical response, got queued result")
	}
	if result.Message.MessageID == "" || result.Message.ServerOrder != 79 {
		t.Fatalf("expected persisted ack response, got %+v", result.Message)
	}
	if err := <-errCh; err != nil {
		t.Fatalf("write ack: %v", err)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("unmet expectations: %v", err)
	}
}

func TestSendAsyncTimeoutReturnsProvisionalAndLateAckRemainsReadable(t *testing.T) {
	mock, err := pgxmock.NewPool()
	if err != nil {
		t.Fatal(err)
	}
	defer mock.Close()

	mr, err := miniredis.Run()
	if err != nil {
		t.Fatalf("start miniredis: %v", err)
	}
	defer mr.Close()

	rdb := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	defer rdb.Close()

	producer := &recordingIngressProducer{published: make(chan IngressEvent, 1)}
	pipeline := NewAsyncPipeline(producer, rdb)
	svc := &Service{
		db:           mock,
		useKafkaSend: true,
		async:        pipeline,
		ackTimeout:   40 * time.Millisecond,
	}

	expectAsyncTextSend(mock, 202)
	mock.ExpectQuery(`SELECT response_payload, COALESCE\(status_code, 201\)`).
		WithArgs("user-1", "/v1/messages", "idem-1").
		WillReturnRows(pgxmock.NewRows([]string{"response_payload", "status_code"}).AddRow(mustJSONMessage(t, Message{
			MessageID:         "message-provisional-1",
			ConversationID:    "conversation-1",
			SenderUserID:      "user-1",
			SenderDeviceID:    "device-1",
			ContentType:       "text",
			Content:           map[string]any{"text": "hi"},
			ClientGeneratedID: "client-1",
			Transport:         "OHMF",
			ServerOrder:       0,
			Status:            "QUEUED",
			CreatedAt:         "2026-06-09T00:00:00Z",
		}), 202))

	result, err := svc.Send(context.Background(), "user-1", "device-1", "conversation-1", "idem-1", "text", map[string]any{"text": "hi"}, "client-1", "trace-1", "")
	if err != nil {
		t.Fatalf("Send failed: %v", err)
	}
	if !result.Queued {
		t.Fatalf("expected provisional queued response on timeout")
	}
	if result.AckTimeoutMS != 40 {
		t.Fatalf("expected ack_timeout_ms=40, got %d", result.AckTimeoutMS)
	}
	if result.Message.ServerOrder != 0 {
		t.Fatalf("expected provisional server_order=0, got %d", result.Message.ServerOrder)
	}
	if result.Message.Status != "QUEUED" {
		t.Fatalf("expected provisional status QUEUED, got %q", result.Message.Status)
	}

	evt := producer.Last()
	if evt.EventID == "" {
		t.Fatalf("expected ingress event to be published")
	}
	lateAck := PersistedAck{
		EventID:        evt.EventID,
		MessageID:      evt.MessageID,
		ConversationID: evt.ConversationID,
		ServerOrder:    88,
		Status:         "SENT",
		Transport:      "OHMF",
		PersistedAtMS:  1700000000456,
	}
	body, err := json.Marshal(lateAck)
	if err != nil {
		t.Fatalf("marshal late ack: %v", err)
	}
	if err := rdb.Set(context.Background(), AckRedisKey(evt.EventID), string(body), 24*time.Hour).Err(); err != nil {
		t.Fatalf("persist late ack: %v", err)
	}

	got, ok, err := pipeline.WaitAck(context.Background(), evt.EventID, 50*time.Millisecond)
	if err != nil {
		t.Fatalf("WaitAck failed for late ack: %v", err)
	}
	if !ok {
		t.Fatalf("expected late durable ack to remain readable")
	}
	if got.ServerOrder != lateAck.ServerOrder || got.MessageID != lateAck.MessageID {
		t.Fatalf("unexpected late ack: %+v", got)
	}

	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("unmet expectations: %v", err)
	}
}

func expectAsyncTextSend(mock pgxmock.PgxPoolIface, idempotencyStatuses ...int) {
	expectMembershipOK(mock)
	expectUnblocked(mock)
	mock.ExpectQuery(`SELECT type, COALESCE\(encryption_state, 'PLAINTEXT'\), COALESCE\(is_mls_encrypted, false\) FROM conversations WHERE id = \$1::uuid`).
		WithArgs("conversation-1").
		WillReturnRows(pgxmock.NewRows([]string{"type", "encryption_state", "is_mls_encrypted"}).AddRow("GROUP", "PLAINTEXT", false))
	mock.ExpectQuery(`SELECT response_payload, COALESCE\(status_code, 201\)`).
		WithArgs("user-1", "/v1/messages", "idem-1").
		WillReturnError(pgx.ErrNoRows)
	for _, status := range idempotencyStatuses {
		mock.ExpectExec(`INSERT INTO idempotency_keys \(actor_user_id, endpoint, key, response_payload, status_code, expires_at\)`).
			WithArgs("user-1", "/v1/messages", "idem-1", pgxmock.AnyArg(), status).
			WillReturnResult(pgxmock.NewResult("INSERT", 1))
	}
}

func writeAckForTest(rdb *redis.Client, eventID string, ack PersistedAck) error {
	body, err := json.Marshal(ack)
	if err != nil {
		return err
	}
	if err := rdb.Set(context.Background(), AckRedisKey(eventID), string(body), 24*time.Hour).Err(); err != nil {
		return err
	}
	if err := rdb.Publish(context.Background(), ackSignalChannel(eventID), string(body)).Err(); err != nil {
		return err
	}
	return nil
}

func mustJSONMessage(t *testing.T, v any) string {
	t.Helper()
	body, err := json.Marshal(v)
	if err != nil {
		t.Fatalf("marshal json: %v", err)
	}
	return string(body)
}

type recordingIngressProducer struct {
	mu        sync.Mutex
	last      IngressEvent
	published chan IngressEvent
}

func (p *recordingIngressProducer) PublishIngress(_ context.Context, _ string, payload any) error {
	evt, ok := payload.(IngressEvent)
	if !ok {
		return errors.New("unexpected ingress payload type")
	}
	p.mu.Lock()
	p.last = evt
	p.mu.Unlock()
	if p.published != nil {
		p.published <- evt
	}
	return nil
}

func (p *recordingIngressProducer) Last() IngressEvent {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.last
}

type redisCommandCounter struct {
	mu     sync.Mutex
	counts map[string]int
}

func newRedisCommandCounter() *redisCommandCounter {
	return &redisCommandCounter{counts: make(map[string]int)}
}

func (h *redisCommandCounter) DialHook(next redis.DialHook) redis.DialHook {
	return next
}

func (h *redisCommandCounter) ProcessHook(next redis.ProcessHook) redis.ProcessHook {
	return func(ctx context.Context, cmd redis.Cmder) error {
		h.record(cmd.Name())
		return next(ctx, cmd)
	}
}

func (h *redisCommandCounter) ProcessPipelineHook(next redis.ProcessPipelineHook) redis.ProcessPipelineHook {
	return func(ctx context.Context, cmds []redis.Cmder) error {
		for _, cmd := range cmds {
			h.record(cmd.Name())
		}
		return next(ctx, cmds)
	}
}

func (h *redisCommandCounter) Count(name string) int {
	h.mu.Lock()
	defer h.mu.Unlock()
	return h.counts[strings.ToLower(name)]
}

func (h *redisCommandCounter) record(name string) {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.counts[strings.ToLower(name)]++
}

type redisDelayHook struct {
	delay time.Duration
}

func (h redisDelayHook) DialHook(next redis.DialHook) redis.DialHook {
	return next
}

func (h redisDelayHook) ProcessHook(next redis.ProcessHook) redis.ProcessHook {
	return func(ctx context.Context, cmd redis.Cmder) error {
		if strings.EqualFold(cmd.Name(), "get") {
			time.Sleep(h.delay)
		}
		return next(ctx, cmd)
	}
}

func (h redisDelayHook) ProcessPipelineHook(next redis.ProcessPipelineHook) redis.ProcessPipelineHook {
	return next
}

type redisGetErrorHook struct {
	err error
}

func (h redisGetErrorHook) DialHook(next redis.DialHook) redis.DialHook {
	return next
}

func (h redisGetErrorHook) ProcessHook(next redis.ProcessHook) redis.ProcessHook {
	return func(ctx context.Context, cmd redis.Cmder) error {
		if strings.EqualFold(cmd.Name(), "get") {
			return h.err
		}
		return next(ctx, cmd)
	}
}

func (h redisGetErrorHook) ProcessPipelineHook(next redis.ProcessPipelineHook) redis.ProcessPipelineHook {
	return next
}

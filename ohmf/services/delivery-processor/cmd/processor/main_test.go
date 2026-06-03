package main

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/segmentio/kafka-go"
)

func TestProcessMessageSkipsDuplicateDeliveryEvent(t *testing.T) {
	ctx := context.Background()
	store := newMemoryDeliveryRecorder()
	presence := &stubPresenceStore{online: map[string]bool{"recipient-1": true}}
	publisher := &stubDeliveryPublisher{}
	observer := &stubDeliveryMetricsObserver{}
	msg := kafkaMessage(t, persistedEvent{
		EventID:         "evt-1",
		MessageID:       "11111111-1111-1111-1111-111111111111",
		ConversationID:  "22222222-2222-2222-2222-222222222222",
		SenderUserID:    "33333333-3333-3333-3333-333333333333",
		ServerOrder:     7,
		Transport:       "OHMF",
		DeliveryTargets: []string{"recipient-1"},
		TraceID:         "trace-1",
	})

	if err := processMessageWithObserver(ctx, store, presence, publisher, msg, observer); err != nil {
		t.Fatalf("first processMessage failed: %v", err)
	}
	if err := processMessageWithObserver(ctx, store, presence, publisher, msg, observer); err != nil {
		t.Fatalf("second processMessage failed: %v", err)
	}

	if got := len(store.deliveries); got != 1 {
		t.Fatalf("expected 1 delivery row, got %d", got)
	}
	if got := len(publisher.keys); got != 1 {
		t.Fatalf("expected 1 kafka delivery event, got %d", got)
	}
	if got := countString(presence.channels, "delivery:user:recipient-1"); got != 1 {
		t.Fatalf("expected recipient pubsub notification once, got %d", got)
	}
	if got := countString(presence.channels, "delivery:user:33333333-3333-3333-3333-333333333333"); got != 1 {
		t.Fatalf("expected sender pubsub notification once, got %d", got)
	}
	if observer.duplicates != 1 {
		t.Fatalf("expected duplicate observer to count 1 duplicate, got %d", observer.duplicates)
	}
}

func TestProcessMessageSkipsDuplicateKafkaRedelivery(t *testing.T) {
	ctx := context.Background()
	store := newMemoryDeliveryRecorder()
	presence := &stubPresenceStore{online: map[string]bool{
		"recipient-1": true,
		"recipient-2": true,
	}}
	publisher := &stubDeliveryPublisher{}
	msg := kafkaMessage(t, persistedEvent{
		EventID:         "evt-2",
		MessageID:       "aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa",
		ConversationID:  "bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb",
		SenderUserID:    "cccccccc-cccc-cccc-cccc-cccccccccccc",
		ServerOrder:     9,
		Transport:       "OHMF",
		DeliveryTargets: []string{"recipient-1", "recipient-2"},
		TraceID:         "trace-2",
	})

	if err := processMessage(ctx, store, presence, publisher, msg); err != nil {
		t.Fatalf("first processMessage failed: %v", err)
	}
	if err := processMessage(ctx, store, presence, publisher, msg); err != nil {
		t.Fatalf("redelivery processMessage failed: %v", err)
	}

	if got := len(store.deliveries); got != 2 {
		t.Fatalf("expected 2 delivery rows, got %d", got)
	}
	if got := len(publisher.keys); got != 2 {
		t.Fatalf("expected 2 kafka delivery events, got %d", got)
	}
	if got := countString(publisher.keys, "recipient-1"); got != 1 {
		t.Fatalf("expected recipient-1 kafka event once, got %d", got)
	}
	if got := countString(publisher.keys, "recipient-2"); got != 1 {
		t.Fatalf("expected recipient-2 kafka event once, got %d", got)
	}
}

func TestProcessMessageRetryAfterPartialRecipientFailureOnlyPublishesRemainingRecipient(t *testing.T) {
	ctx := context.Background()
	store := newMemoryDeliveryRecorder()
	store.failures["recipient-2"] = 1
	presence := &stubPresenceStore{online: map[string]bool{
		"recipient-1": true,
		"recipient-2": true,
	}}
	publisher := &stubDeliveryPublisher{}
	msg := kafkaMessage(t, persistedEvent{
		EventID:         "evt-3",
		MessageID:       "dddddddd-dddd-dddd-dddd-dddddddddddd",
		ConversationID:  "eeeeeeee-eeee-eeee-eeee-eeeeeeeeeeee",
		SenderUserID:    "ffffffff-ffff-ffff-ffff-ffffffffffff",
		ServerOrder:     11,
		Transport:       "OHMF",
		DeliveryTargets: []string{"recipient-1", "recipient-2"},
		TraceID:         "trace-3",
	})

	if err := processMessage(ctx, store, presence, publisher, msg); err == nil {
		t.Fatal("expected first processMessage call to fail")
	}
	if err := processMessage(ctx, store, presence, publisher, msg); err != nil {
		t.Fatalf("retry processMessage failed: %v", err)
	}

	if got := len(store.deliveries); got != 2 {
		t.Fatalf("expected 2 delivery rows after retry, got %d", got)
	}
	if got := len(publisher.keys); got != 2 {
		t.Fatalf("expected 2 kafka delivery events after retry, got %d", got)
	}
	if got := countString(publisher.keys, "recipient-1"); got != 1 {
		t.Fatalf("expected recipient-1 kafka event once, got %d", got)
	}
	if got := countString(publisher.keys, "recipient-2"); got != 1 {
		t.Fatalf("expected recipient-2 kafka event once, got %d", got)
	}
}

func TestPGDeliveryRecorderTreatsConflictAsDuplicate(t *testing.T) {
	rower := &stubQueryRower{
		rows: []pgx.Row{
			stubRow{id: "delivery-1"},
			stubRow{err: pgx.ErrNoRows},
		},
	}
	recorder := pgDeliveryRecorder{db: rower}
	evt := persistedEvent{
		MessageID: "12121212-1212-1212-1212-121212121212",
		Transport: "OHMF",
	}

	created, err := recorder.RecordDelivered(context.Background(), evt, "34343434-3434-3434-3434-343434343434")
	if err != nil {
		t.Fatalf("first RecordDelivered failed: %v", err)
	}
	if !created {
		t.Fatal("expected first RecordDelivered call to create a row")
	}

	created, err = recorder.RecordDelivered(context.Background(), evt, "34343434-3434-3434-3434-343434343434")
	if err != nil {
		t.Fatalf("second RecordDelivered failed: %v", err)
	}
	if created {
		t.Fatal("expected duplicate RecordDelivered call to be ignored")
	}
	if !strings.Contains(rower.sql, "ON CONFLICT (message_id, recipient_user_id)") {
		t.Fatalf("expected conflict guard in SQL, got %q", rower.sql)
	}
	if !strings.Contains(rower.sql, "state = 'DELIVERED'") {
		t.Fatalf("expected DELIVERED predicate in SQL, got %q", rower.sql)
	}
}

func TestObservabilityHandlers(t *testing.T) {
	obs := newProcessorObservability("delivery", ":0", []string{"127.0.0.1:1"}, []dependencyCheck{
		{name: "postgres", check: func(context.Context) error { return nil }},
		{name: "redis", check: func(context.Context) error { return nil }},
	})
	obs.recordSuccess(20 * time.Millisecond)
	obs.recordError(40 * time.Millisecond)
	obs.recordDLQPublish()
	obs.recordDuplicate()
	obs.setConsumerLag(3)

	server := httptest.NewServer(obs.handler())
	defer server.Close()

	healthResp, err := http.Get(server.URL + "/healthz")
	if err != nil {
		t.Fatalf("healthz request failed: %v", err)
	}
	defer healthResp.Body.Close()
	if healthResp.StatusCode != http.StatusOK {
		t.Fatalf("expected 200 from healthz, got %d", healthResp.StatusCode)
	}

	readyResp, err := http.Get(server.URL + "/readyz")
	if err != nil {
		t.Fatalf("readyz request failed: %v", err)
	}
	defer readyResp.Body.Close()
	if readyResp.StatusCode != http.StatusServiceUnavailable {
		t.Fatalf("expected 503 from readyz with failing kafka check, got %d", readyResp.StatusCode)
	}

	metricsResp, err := http.Get(server.URL + "/metrics")
	if err != nil {
		t.Fatalf("metrics request failed: %v", err)
	}
	defer metricsResp.Body.Close()
	body, err := io.ReadAll(metricsResp.Body)
	if err != nil {
		t.Fatalf("read metrics body: %v", err)
	}
	text := string(body)
	for _, want := range []string{
		"ohmf_delivery_processor_processed_total 1",
		"ohmf_delivery_processor_errors_total 1",
		"ohmf_delivery_processor_dlq_published_total 1",
		"ohmf_delivery_processor_duplicates_total 1",
		"ohmf_delivery_processor_last_success_timestamp_seconds",
		"ohmf_delivery_processor_consumer_lag_messages 3",
	} {
		if !strings.Contains(text, want) {
			t.Fatalf("expected metrics output to contain %q", want)
		}
	}
}

type memoryDeliveryRecorder struct {
	deliveries map[string]struct{}
	failures   map[string]int
}

func newMemoryDeliveryRecorder() *memoryDeliveryRecorder {
	return &memoryDeliveryRecorder{
		deliveries: make(map[string]struct{}),
		failures:   make(map[string]int),
	}
}

func (r *memoryDeliveryRecorder) RecordDelivered(_ context.Context, evt persistedEvent, recipientID string) (bool, error) {
	if remaining := r.failures[recipientID]; remaining > 0 {
		r.failures[recipientID] = remaining - 1
		return false, errors.New("forced insert failure")
	}
	key := evt.MessageID + "|" + recipientID
	if _, exists := r.deliveries[key]; exists {
		return false, nil
	}
	r.deliveries[key] = struct{}{}
	return true, nil
}

type stubPresenceStore struct {
	online   map[string]bool
	channels []string
}

func (s *stubPresenceStore) IsOnline(_ context.Context, recipientID string) (bool, error) {
	return s.online[recipientID], nil
}

func (s *stubPresenceStore) Publish(_ context.Context, channel string, _ []byte) error {
	s.channels = append(s.channels, channel)
	return nil
}

type stubDeliveryPublisher struct {
	keys []string
}

func (p *stubDeliveryPublisher) Publish(_ context.Context, key string, _ []byte) error {
	p.keys = append(p.keys, key)
	return nil
}

type stubDeliveryMetricsObserver struct {
	duplicates int
}

func (o *stubDeliveryMetricsObserver) RecordDuplicate() {
	o.duplicates++
}

type stubQueryRower struct {
	rows []pgx.Row
	sql  string
}

func (r *stubQueryRower) QueryRow(_ context.Context, sql string, _ ...any) pgx.Row {
	r.sql = sql
	row := r.rows[0]
	r.rows = r.rows[1:]
	return row
}

type stubRow struct {
	id  string
	err error
}

func (r stubRow) Scan(dest ...any) error {
	if r.err != nil {
		return r.err
	}
	if len(dest) > 0 {
		if out, ok := dest[0].(*string); ok {
			*out = r.id
		}
	}
	return nil
}

func kafkaMessage(t *testing.T, evt persistedEvent) kafka.Message {
	t.Helper()
	body, err := json.Marshal(evt)
	if err != nil {
		t.Fatalf("marshal persistedEvent: %v", err)
	}
	return kafka.Message{Value: body}
}

func countString(items []string, want string) int {
	count := 0
	for _, item := range items {
		if item == want {
			count++
		}
	}
	return count
}

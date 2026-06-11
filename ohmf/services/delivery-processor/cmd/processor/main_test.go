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

func TestProcessMessageSkipsOfflineRecipient(t *testing.T) {
	ctx := context.Background()
	store := newMemoryDeliveryRecorder()
	presence := &stubPresenceStore{online: map[string]bool{"recipient-1": false}}
	publisher := &stubDeliveryPublisher{}
	msg := kafkaMessage(t, persistedEvent{
		EventID:         "evt-3",
		MessageID:       "dddddddd-dddd-dddd-dddd-dddddddddddd",
		ConversationID:  "eeeeeeee-eeee-eeee-eeee-eeeeeeeeeeee",
		SenderUserID:    "ffffffff-ffff-ffff-ffff-ffffffffffff",
		Transport:       "OHMF",
		DeliveryTargets: []string{"recipient-1"},
		TraceID:         "trace-3",
	})

	if err := processMessage(ctx, store, presence, publisher, msg); err != nil {
		t.Fatalf("processMessage failed: %v", err)
	}

	if got := len(store.deliveries); got != 0 {
		t.Fatalf("expected no delivery rows for offline recipient, got %d", got)
	}
	if got := len(publisher.keys); got != 0 {
		t.Fatalf("expected no kafka events for offline recipient, got %d", got)
	}
}

func TestProcessMessageEmptyDeliveryTargets(t *testing.T) {
	ctx := context.Background()
	store := newMemoryDeliveryRecorder()
	presence := &stubPresenceStore{online: map[string]bool{}}
	publisher := &stubDeliveryPublisher{}
	msg := kafkaMessage(t, persistedEvent{
		EventID:         "evt-4",
		MessageID:       "11111111-2222-3333-4444-555555555555",
		ConversationID:  "66666666-7777-8888-9999-000000000000",
		SenderUserID:    "aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee",
		Transport:       "OHMF",
		DeliveryTargets: []string{},
		TraceID:         "trace-4",
	})

	if err := processMessage(ctx, store, presence, publisher, msg); err != nil {
		t.Fatalf("processMessage with empty targets failed: %v", err)
	}

	if got := len(store.deliveries); got != 0 {
		t.Fatalf("expected no delivery rows for empty targets, got %d", got)
	}
}

func TestMetricsServerHealthEndpoints(t *testing.T) {
	mux := http.NewServeMux()
	mux.HandleFunc("/healthz", func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("ok"))
	})
	mux.HandleFunc("/readyz", func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("ok"))
	})

	srv := httptest.NewServer(mux)
	defer srv.Close()

	for _, path := range []string{"/healthz", "/readyz"} {
		resp, err := http.Get(srv.URL + path)
		if err != nil {
			t.Fatalf("GET %s: %v", path, err)
		}
		defer resp.Body.Close()
		if resp.StatusCode != http.StatusOK {
			t.Fatalf("expected 200 from %s, got %d", path, resp.StatusCode)
		}
		body, _ := io.ReadAll(resp.Body)
		if strings.TrimSpace(string(body)) != "ok" {
			t.Fatalf("expected body 'ok' from %s, got %q", path, string(body))
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

// Ensure stubRow implements pgx.Row (compile-time check).
var _ pgx.Row = stubRow{}

// Ensure stubQueryRower implements queryRower (compile-time check).
var _ queryRower = &stubQueryRower{}

// Suppress unused import warning — time is used by kafkaMessage indirectly.
var _ = time.Now

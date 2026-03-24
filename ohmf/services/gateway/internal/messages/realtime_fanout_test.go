package messages

import (
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"sort"
	"testing"
	"time"

	miniredis "github.com/alicebob/miniredis/v2"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/redis/go-redis/v9"
)

func TestSendPublishesRealtimeMessageToRecipients(t *testing.T) {
	dsn := os.Getenv("TEST_DATABASE_URL")
	if dsn == "" {
		t.Skip("skipping DB integration test; set TEST_DATABASE_URL to run")
	}

	ctx := context.Background()
	pool, err := pgxpool.New(ctx, dsn)
	if err != nil {
		t.Fatalf("connect: %v", err)
	}
	defer pool.Close()

	applyAllMigrations(t, ctx, pool)

	mr, err := miniredis.Run()
	if err != nil {
		t.Fatalf("start miniredis: %v", err)
	}
	defer mr.Close()

	rdb := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	defer rdb.Close()

	senderID := insertTestUser(t, ctx, pool)
	recipientID := insertTestUser(t, ctx, pool)
	conversationID := uuid.NewString()

	if _, err := pool.Exec(ctx, `INSERT INTO conversations (id, type) VALUES ($1, 'PRIVATE')`, conversationID); err != nil {
		t.Fatalf("insert conversation: %v", err)
	}
	if _, err := pool.Exec(ctx, `INSERT INTO conversation_counters (conversation_id) VALUES ($1)`, conversationID); err != nil {
		t.Fatalf("insert counter: %v", err)
	}
	if _, err := pool.Exec(ctx, `INSERT INTO conversation_members (conversation_id, user_id) VALUES ($1, $2::uuid), ($1, $3::uuid)`, conversationID, senderID, recipientID); err != nil {
		t.Fatalf("insert members: %v", err)
	}

	pubsub := rdb.Subscribe(ctx, "message:user:"+recipientID)
	defer pubsub.Close()
	if _, err := pubsub.ReceiveTimeout(ctx, time.Second); err != nil {
		t.Fatalf("subscribe recipient channel: %v", err)
	}
	channel := pubsub.Channel()

	deliveryPubsub := rdb.Subscribe(ctx, "delivery:user:"+senderID)
	defer deliveryPubsub.Close()
	if _, err := deliveryPubsub.ReceiveTimeout(ctx, time.Second); err != nil {
		t.Fatalf("subscribe sender delivery channel: %v", err)
	}
	deliveryChannel := deliveryPubsub.Channel()

	svc := NewService(pool, Options{Redis: rdb})
	if err := rdb.Set(ctx, "presence:user:"+recipientID, "1", time.Minute).Err(); err != nil {
		t.Fatalf("set recipient presence: %v", err)
	}
	result, err := svc.Send(ctx, senderID, "", conversationID, uuid.NewString(), "text", map[string]any{"text": "hello"}, "", "trace-test", "127.0.0.1")
	if err != nil {
		t.Fatalf("send message: %v", err)
	}

	select {
	case published := <-channel:
		var payload Message
		if err := json.Unmarshal([]byte(published.Payload), &payload); err != nil {
			t.Fatalf("unmarshal published payload: %v", err)
		}
		if payload.MessageID != result.Message.MessageID {
			t.Fatalf("expected published message_id %q, got %q", result.Message.MessageID, payload.MessageID)
		}
		if payload.ConversationID != conversationID {
			t.Fatalf("expected published conversation_id %q, got %q", conversationID, payload.ConversationID)
		}
		if payload.SenderUserID != senderID {
			t.Fatalf("expected published sender_user_id %q, got %q", senderID, payload.SenderUserID)
		}
		if payload.Status != "SENT" {
			t.Fatalf("expected published status SENT, got %q", payload.Status)
		}
		if payload.Transport != "OTT" {
			t.Fatalf("expected published transport OTT, got %q", payload.Transport)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for recipient realtime publish")
	}

	select {
	case published := <-deliveryChannel:
		var payload map[string]any
		if err := json.Unmarshal([]byte(published.Payload), &payload); err != nil {
			t.Fatalf("unmarshal delivery payload: %v", err)
		}
		if payload["message_id"] != result.Message.MessageID {
			t.Fatalf("expected delivery message_id %q, got %#v", result.Message.MessageID, payload["message_id"])
		}
		if payload["conversation_id"] != conversationID {
			t.Fatalf("expected delivery conversation_id %q, got %#v", conversationID, payload["conversation_id"])
		}
		if payload["status"] != "DELIVERED" {
			t.Fatalf("expected delivery status DELIVERED, got %#v", payload["status"])
		}
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for sender delivery publish")
	}
}

func applyAllMigrations(t *testing.T, ctx context.Context, pool *pgxpool.Pool) {
	t.Helper()

	if _, err := pool.Exec(ctx, `
		CREATE TABLE IF NOT EXISTS test_applied_migrations (
			name TEXT PRIMARY KEY,
			applied_at TIMESTAMPTZ NOT NULL DEFAULT now()
		)
	`); err != nil {
		t.Fatalf("ensure test_applied_migrations table: %v", err)
	}

	patterns := []string{
		filepath.Join("..", "..", "migrations", "*.up.sql"),
		filepath.Join("..", "migrations", "*.up.sql"),
		filepath.Join("migrations", "*.up.sql"),
	}

	var paths []string
	for _, pattern := range patterns {
		matches, err := filepath.Glob(pattern)
		if err != nil {
			t.Fatalf("glob migrations %q: %v", pattern, err)
		}
		if len(matches) > 0 {
			paths = matches
			break
		}
	}
	if len(paths) == 0 {
		t.Fatal("no gateway migrations found")
	}

	sort.Strings(paths)
	for _, path := range paths {
		name := filepath.Base(path)
		var alreadyApplied bool
		if err := pool.QueryRow(ctx, `
			SELECT EXISTS (
				SELECT 1
				FROM test_applied_migrations
				WHERE name = $1
			)
		`, name).Scan(&alreadyApplied); err != nil {
			t.Fatalf("check applied migration %q: %v", name, err)
		}
		if alreadyApplied {
			continue
		}

		body, err := os.ReadFile(path)
		if err != nil {
			t.Fatalf("read migration %q: %v", path, err)
		}
		if _, err := pool.Exec(ctx, string(body)); err != nil {
			t.Fatalf("apply migration %q: %v", path, err)
		}
		if _, err := pool.Exec(ctx, `
			INSERT INTO test_applied_migrations (name)
			VALUES ($1)
			ON CONFLICT (name) DO NOTHING
		`, name); err != nil {
			t.Fatalf("record applied migration %q: %v", name, err)
		}
	}
}

func insertTestUser(t *testing.T, ctx context.Context, pool *pgxpool.Pool) string {
	t.Helper()

	var userID string
	phone := "+test-" + uuid.NewString()
	if err := pool.QueryRow(ctx, `INSERT INTO users (primary_phone_e164) VALUES ($1) RETURNING id::text`, phone).Scan(&userID); err != nil {
		t.Fatalf("insert user %q: %v", phone, err)
	}
	return userID
}

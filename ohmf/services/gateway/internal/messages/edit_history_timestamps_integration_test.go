package messages

import (
	"context"
	"os"
	"testing"

	miniredis "github.com/alicebob/miniredis/v2"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/redis/go-redis/v9"
	"ohmf/services/gateway/internal/replication"
)

func TestEditHistoryPreservesSentDeliveredReadTimestamps(t *testing.T) {
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

	store := replication.NewStore(pool, rdb)
	svc := NewService(pool, Options{Redis: rdb, Replication: store})

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

	sendResult, err := svc.Send(ctx, senderID, "", conversationID, uuid.NewString(), "text", map[string]any{"text": "before"}, "", "trace-edit-history-test", "127.0.0.1")
	if err != nil {
		t.Fatalf("send message: %v", err)
	}

	if err := store.AdvanceDeliveredCheckpoint(ctx, recipientID, "", conversationID, sendResult.Message.ServerOrder); err != nil {
		t.Fatalf("advance delivered checkpoint: %v", err)
	}
	if err := svc.MarkRead(ctx, recipientID, conversationID, sendResult.Message.ServerOrder); err != nil {
		t.Fatalf("mark read: %v", err)
	}

	beforeEdit, err := svc.List(ctx, senderID, conversationID)
	if err != nil {
		t.Fatalf("list messages before edit: %v", err)
	}
	if len(beforeEdit) != 1 {
		t.Fatalf("expected 1 message before edit, got %d", len(beforeEdit))
	}
	if beforeEdit[0].SentAt == "" || beforeEdit[0].DeliveredAt == "" || beforeEdit[0].ReadAt == "" {
		t.Fatalf("expected timestamps before edit, got sent=%q delivered=%q read=%q", beforeEdit[0].SentAt, beforeEdit[0].DeliveredAt, beforeEdit[0].ReadAt)
	}

	if err := svc.EditMessage(ctx, senderID, "", sendResult.Message.MessageID, map[string]any{"text": "after"}); err != nil {
		t.Fatalf("edit message: %v", err)
	}

	history, err := svc.GetMessageEditHistory(ctx, senderID, sendResult.Message.MessageID)
	if err != nil {
		t.Fatalf("get edit history: %v", err)
	}
	if len(history) != 1 {
		t.Fatalf("expected 1 edit history row, got %d", len(history))
	}
	if history[0].SentAt == "" || history[0].DeliveredAt == "" || history[0].ReadAt == "" {
		t.Fatalf("expected preserved timestamps, got sent=%q delivered=%q read=%q", history[0].SentAt, history[0].DeliveredAt, history[0].ReadAt)
	}
	assertSameInstant(t, "sent_at", beforeEdit[0].SentAt, history[0].SentAt)
	assertSameInstant(t, "delivered_at", beforeEdit[0].DeliveredAt, history[0].DeliveredAt)
	assertSameInstant(t, "read_at", beforeEdit[0].ReadAt, history[0].ReadAt)
}

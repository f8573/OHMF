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

func TestReactionHistoryAppendsAddAndRemoveEvents(t *testing.T) {
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
	emoji := "\U0001F44D"

	if _, err := pool.Exec(ctx, `INSERT INTO conversations (id, type) VALUES ($1, 'PRIVATE')`, conversationID); err != nil {
		t.Fatalf("insert conversation: %v", err)
	}
	if _, err := pool.Exec(ctx, `INSERT INTO conversation_counters (conversation_id) VALUES ($1)`, conversationID); err != nil {
		t.Fatalf("insert counter: %v", err)
	}
	if _, err := pool.Exec(ctx, `INSERT INTO conversation_members (conversation_id, user_id) VALUES ($1, $2::uuid), ($1, $3::uuid)`, conversationID, senderID, recipientID); err != nil {
		t.Fatalf("insert members: %v", err)
	}

	sendResult, err := svc.Send(ctx, senderID, "", conversationID, uuid.NewString(), "text", map[string]any{"text": "history"}, "", "trace-reaction-history-test", "127.0.0.1")
	if err != nil {
		t.Fatalf("send message: %v", err)
	}

	if err := store.AdvanceDeliveredCheckpoint(ctx, recipientID, "", conversationID, sendResult.Message.ServerOrder); err != nil {
		t.Fatalf("advance delivered checkpoint: %v", err)
	}
	if err := svc.MarkRead(ctx, recipientID, conversationID, sendResult.Message.ServerOrder); err != nil {
		t.Fatalf("mark read: %v", err)
	}

	beforeReaction, err := svc.List(ctx, senderID, conversationID)
	if err != nil {
		t.Fatalf("list messages before reaction: %v", err)
	}
	if len(beforeReaction) != 1 {
		t.Fatalf("expected 1 message before reaction history, got %d", len(beforeReaction))
	}

	if err := svc.AddReaction(ctx, senderID, sendResult.Message.MessageID, emoji); err != nil {
		t.Fatalf("add reaction: %v", err)
	}
	if err := svc.RemoveReaction(ctx, senderID, sendResult.Message.MessageID, emoji); err != nil {
		t.Fatalf("remove reaction: %v", err)
	}

	history, err := svc.GetMessageReactionHistory(ctx, senderID, sendResult.Message.MessageID)
	if err != nil {
		t.Fatalf("get reaction history: %v", err)
	}
	if len(history) != 2 {
		t.Fatalf("expected 2 reaction history rows, got %d", len(history))
	}
	if history[0].Action != "removed" || history[1].Action != "added" {
		t.Fatalf("expected remove/add ordering, got %#v then %#v", history[0].Action, history[1].Action)
	}
	for _, item := range history {
		if item.Emoji != emoji {
			t.Fatalf("expected emoji %q, got %q", emoji, item.Emoji)
		}
		if item.SentAt != beforeReaction[0].SentAt || item.DeliveredAt != beforeReaction[0].DeliveredAt || item.ReadAt != beforeReaction[0].ReadAt {
			t.Fatalf("expected preserved timestamps sent=%q delivered=%q read=%q, got sent=%q delivered=%q read=%q", beforeReaction[0].SentAt, beforeReaction[0].DeliveredAt, beforeReaction[0].ReadAt, item.SentAt, item.DeliveredAt, item.ReadAt)
		}
	}
}

package messages

import (
	"context"
	"encoding/json"
	"os"
	"testing"
	"time"

	miniredis "github.com/alicebob/miniredis/v2"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/redis/go-redis/v9"
	"ohmf/services/gateway/internal/replication"
)

func TestAddReactionAllowsEncryptedDMAndPublishesSyncEvent(t *testing.T) {
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
	messageID := uuid.NewString()
	emoji := "\U0001F44D"

	if _, err := pool.Exec(ctx, `
		INSERT INTO conversations (id, type, encryption_state)
		VALUES ($1, 'DM', 'ENCRYPTED')
	`, conversationID); err != nil {
		t.Fatalf("insert conversation: %v", err)
	}
	if _, err := pool.Exec(ctx, `INSERT INTO conversation_counters (conversation_id, next_server_order) VALUES ($1, 2)`, conversationID); err != nil {
		t.Fatalf("insert counter: %v", err)
	}
	if _, err := pool.Exec(ctx, `INSERT INTO conversation_members (conversation_id, user_id) VALUES ($1, $2::uuid), ($1, $3::uuid)`, conversationID, senderID, recipientID); err != nil {
		t.Fatalf("insert members: %v", err)
	}
	if _, err := pool.Exec(ctx, `
		INSERT INTO messages (id, conversation_id, sender_user_id, content_type, content, transport, server_order)
		VALUES ($1::uuid, $2::uuid, $3::uuid, 'encrypted', '{}'::jsonb, 'OHMF', 1)
	`, messageID, conversationID, senderID); err != nil {
		t.Fatalf("insert encrypted message: %v", err)
	}

	userEventPubsub := rdb.Subscribe(ctx, store.ChannelForUser(recipientID))
	defer userEventPubsub.Close()
	if _, err := userEventPubsub.ReceiveTimeout(ctx, time.Second); err != nil {
		t.Fatalf("subscribe user event channel: %v", err)
	}

	if err := svc.AddReaction(ctx, senderID, messageID, emoji); err != nil {
		t.Fatalf("add reaction to encrypted dm: %v", err)
	}

	if processed, err := store.ProcessBatch(ctx, 100); err != nil {
		t.Fatalf("process reaction sync batch: %v", err)
	} else if processed == 0 {
		t.Fatal("expected reaction domain event to be processed")
	}

	select {
	case published := <-userEventPubsub.Channel():
		var evt replication.Event
		if err := json.Unmarshal([]byte(published.Payload), &evt); err != nil {
			t.Fatalf("unmarshal reaction user event: %v", err)
		}
		if evt.Type != replication.UserEventConversationMessageReactionsUpdated {
			t.Fatalf("expected reaction user event type %q, got %q", replication.UserEventConversationMessageReactionsUpdated, evt.Type)
		}
		if evt.Payload["message_id"] != messageID {
			t.Fatalf("expected message_id %q, got %#v", messageID, evt.Payload["message_id"])
		}
		reactions, _ := evt.Payload["reactions"].(map[string]any)
		if reactions[emoji] != float64(1) {
			t.Fatalf("expected reaction count 1, got %#v", reactions[emoji])
		}
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for encrypted reaction sync event")
	}

	items, err := svc.List(ctx, recipientID, conversationID)
	if err != nil {
		t.Fatalf("list messages after encrypted reaction: %v", err)
	}
	if len(items) != 1 {
		t.Fatalf("expected 1 listed message, got %d", len(items))
	}
	if items[0].Reactions[emoji] != 1 {
		t.Fatalf("expected listed reaction count 1, got %#v", items[0].Reactions[emoji])
	}
}

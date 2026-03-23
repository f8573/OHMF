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

func TestDeleteMessagePublishesSyncEventAndProjectsTombstone(t *testing.T) {
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

	userEventPubsub := rdb.Subscribe(ctx, store.ChannelForUser(recipientID))
	defer userEventPubsub.Close()
	if _, err := userEventPubsub.ReceiveTimeout(ctx, time.Second); err != nil {
		t.Fatalf("subscribe user event channel: %v", err)
	}

	sendResult, err := svc.Send(ctx, senderID, "", conversationID, uuid.NewString(), "text", map[string]any{"text": "hello"}, "", "trace-delete-test", "127.0.0.1")
	if err != nil {
		t.Fatalf("send message: %v", err)
	}

	if processed, err := store.ProcessBatch(ctx, 100); err != nil {
		t.Fatalf("process create sync batch: %v", err)
	} else if processed == 0 {
		t.Fatal("expected create domain event to be processed")
	}

	select {
	case published := <-userEventPubsub.Channel():
		var evt replication.Event
		if err := json.Unmarshal([]byte(published.Payload), &evt); err != nil {
			t.Fatalf("unmarshal created user event: %v", err)
		}
		if evt.Type != replication.UserEventConversationMessageAppended {
			t.Fatalf("expected initial user event type %q, got %q", replication.UserEventConversationMessageAppended, evt.Type)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for initial sync event")
	}

	var unreadCountBefore int64
	var previewBefore string
	if err := pool.QueryRow(ctx, `
		SELECT unread_count, COALESCE(last_message_preview, '')
		FROM user_conversation_state
		WHERE user_id = $1::uuid AND conversation_id = $2::uuid
	`, recipientID, conversationID).Scan(&unreadCountBefore, &previewBefore); err != nil {
		t.Fatalf("load user conversation state before delete: %v", err)
	}
	if unreadCountBefore != 1 {
		t.Fatalf("expected unread count 1 before delete, got %d", unreadCountBefore)
	}
	if previewBefore != "hello" {
		t.Fatalf("expected preview 'hello' before delete, got %q", previewBefore)
	}

	if err := svc.DeleteMessage(ctx, senderID, sendResult.Message.MessageID); err != nil {
		t.Fatalf("delete message: %v", err)
	}

	if processed, err := store.ProcessBatch(ctx, 100); err != nil {
		t.Fatalf("process delete sync batch: %v", err)
	} else if processed == 0 {
		t.Fatal("expected delete domain event to be processed")
	}

	select {
	case published := <-userEventPubsub.Channel():
		var evt replication.Event
		if err := json.Unmarshal([]byte(published.Payload), &evt); err != nil {
			t.Fatalf("unmarshal deleted user event: %v", err)
		}
		if evt.Type != replication.UserEventConversationMessageDeleted {
			t.Fatalf("expected delete user event type %q, got %q", replication.UserEventConversationMessageDeleted, evt.Type)
		}
		if evt.Payload["preview"] != "Message deleted" {
			t.Fatalf("expected delete preview tombstone, got %#v", evt.Payload["preview"])
		}
		messagePayload, _ := evt.Payload["message"].(map[string]any)
		if messagePayload["message_id"] != sendResult.Message.MessageID {
			t.Fatalf("expected deleted message_id %q, got %#v", sendResult.Message.MessageID, messagePayload["message_id"])
		}
		if deleted, _ := messagePayload["deleted"].(bool); !deleted {
			t.Fatalf("expected deleted flag in sync payload, got %#v", messagePayload["deleted"])
		}
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for delete sync event")
	}

	items, err := svc.List(ctx, recipientID, conversationID)
	if err != nil {
		t.Fatalf("list messages after delete: %v", err)
	}
	if len(items) != 1 {
		t.Fatalf("expected 1 listed message, got %d", len(items))
	}
	if !items[0].Deleted {
		t.Fatalf("expected listed message to be marked deleted")
	}
	if items[0].DeletedAt == "" {
		t.Fatalf("expected listed message deleted_at")
	}
	if len(items[0].Content) != 0 {
		t.Fatalf("expected deleted message content to be empty, got %#v", items[0].Content)
	}
	if items[0].VisibilityState != "SOFT_DELETED" {
		t.Fatalf("expected visibility state SOFT_DELETED, got %q", items[0].VisibilityState)
	}

	var unreadCountAfter int64
	var previewAfter string
	if err := pool.QueryRow(ctx, `
		SELECT unread_count, COALESCE(last_message_preview, '')
		FROM user_conversation_state
		WHERE user_id = $1::uuid AND conversation_id = $2::uuid
	`, recipientID, conversationID).Scan(&unreadCountAfter, &previewAfter); err != nil {
		t.Fatalf("load user conversation state after delete: %v", err)
	}
	if unreadCountAfter != 0 {
		t.Fatalf("expected unread count 0 after delete, got %d", unreadCountAfter)
	}
	if previewAfter != "Message deleted" {
		t.Fatalf("expected preview tombstone after delete, got %q", previewAfter)
	}
}

func TestEditMessagePublishesSyncEventAndProjectsUpdatedContent(t *testing.T) {
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

	userEventPubsub := rdb.Subscribe(ctx, store.ChannelForUser(recipientID))
	defer userEventPubsub.Close()
	if _, err := userEventPubsub.ReceiveTimeout(ctx, time.Second); err != nil {
		t.Fatalf("subscribe user event channel: %v", err)
	}

	sendResult, err := svc.Send(ctx, senderID, "", conversationID, uuid.NewString(), "text", map[string]any{"text": "before"}, "", "trace-edit-test", "127.0.0.1")
	if err != nil {
		t.Fatalf("send message: %v", err)
	}

	if processed, err := store.ProcessBatch(ctx, 100); err != nil {
		t.Fatalf("process create sync batch: %v", err)
	} else if processed == 0 {
		t.Fatal("expected create domain event to be processed")
	}

	select {
	case <-userEventPubsub.Channel():
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for initial sync event")
	}

	if err := svc.EditMessage(ctx, senderID, "", sendResult.Message.MessageID, map[string]any{"text": "after"}); err != nil {
		t.Fatalf("edit message: %v", err)
	}

	if processed, err := store.ProcessBatch(ctx, 100); err != nil {
		t.Fatalf("process edit sync batch: %v", err)
	} else if processed == 0 {
		t.Fatal("expected edit domain event to be processed")
	}

	select {
	case published := <-userEventPubsub.Channel():
		var evt replication.Event
		if err := json.Unmarshal([]byte(published.Payload), &evt); err != nil {
			t.Fatalf("unmarshal edited user event: %v", err)
		}
		if evt.Type != replication.UserEventConversationMessageEdited {
			t.Fatalf("expected edit user event type %q, got %q", replication.UserEventConversationMessageEdited, evt.Type)
		}
		if evt.Payload["preview"] != "after" {
			t.Fatalf("expected edit preview 'after', got %#v", evt.Payload["preview"])
		}
		messagePayload, _ := evt.Payload["message"].(map[string]any)
		content, _ := messagePayload["content"].(map[string]any)
		if content["text"] != "after" {
			t.Fatalf("expected edited text 'after', got %#v", content["text"])
		}
		if messagePayload["edited_at"] == nil {
			t.Fatalf("expected edited_at in sync payload")
		}
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for edit sync event")
	}

	items, err := svc.List(ctx, recipientID, conversationID)
	if err != nil {
		t.Fatalf("list messages after edit: %v", err)
	}
	if len(items) != 1 {
		t.Fatalf("expected 1 listed message, got %d", len(items))
	}
	if items[0].Content["text"] != "after" {
		t.Fatalf("expected listed message text 'after', got %#v", items[0].Content["text"])
	}
	if items[0].EditedAt == "" {
		t.Fatalf("expected listed message edited_at")
	}

	var previewAfter string
	if err := pool.QueryRow(ctx, `
		SELECT COALESCE(last_message_preview, '')
		FROM user_conversation_state
		WHERE user_id = $1::uuid AND conversation_id = $2::uuid
	`, recipientID, conversationID).Scan(&previewAfter); err != nil {
		t.Fatalf("load user conversation state after edit: %v", err)
	}
	if previewAfter != "after" {
		t.Fatalf("expected preview 'after' after edit, got %q", previewAfter)
	}
}

func TestAddReactionPublishesSyncEventAndProjectsReactionCounts(t *testing.T) {
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

	userEventPubsub := rdb.Subscribe(ctx, store.ChannelForUser(recipientID))
	defer userEventPubsub.Close()
	if _, err := userEventPubsub.ReceiveTimeout(ctx, time.Second); err != nil {
		t.Fatalf("subscribe user event channel: %v", err)
	}

	sendResult, err := svc.Send(ctx, senderID, "", conversationID, uuid.NewString(), "text", map[string]any{"text": "hello"}, "", "trace-reaction-test", "127.0.0.1")
	if err != nil {
		t.Fatalf("send message: %v", err)
	}

	if processed, err := store.ProcessBatch(ctx, 100); err != nil {
		t.Fatalf("process create sync batch: %v", err)
	} else if processed == 0 {
		t.Fatal("expected create domain event to be processed")
	}

	select {
	case <-userEventPubsub.Channel():
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for initial sync event")
	}

	if err := svc.AddReaction(ctx, senderID, sendResult.Message.MessageID, "👍"); err != nil {
		t.Fatalf("add reaction: %v", err)
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
		if evt.Payload["message_id"] != sendResult.Message.MessageID {
			t.Fatalf("expected message_id %q, got %#v", sendResult.Message.MessageID, evt.Payload["message_id"])
		}
		reactions, _ := evt.Payload["reactions"].(map[string]any)
		if reactions["👍"] != float64(1) {
			t.Fatalf("expected reaction count 1, got %#v", reactions["👍"])
		}
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for reaction sync event")
	}

	items, err := svc.List(ctx, recipientID, conversationID)
	if err != nil {
		t.Fatalf("list messages after reaction: %v", err)
	}
	if len(items) != 1 {
		t.Fatalf("expected 1 listed message, got %d", len(items))
	}
	if items[0].Reactions["👍"] != 1 {
		t.Fatalf("expected listed reaction count 1, got %#v", items[0].Reactions["👍"])
	}
}

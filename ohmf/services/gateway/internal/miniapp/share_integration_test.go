package miniapp

import (
	"encoding/json"
	"testing"
	"time"

	miniredis "github.com/alicebob/miniredis/v2"
	"github.com/google/uuid"
	"github.com/redis/go-redis/v9"
	"ohmf/services/gateway/internal/config"
	"ohmf/services/gateway/internal/replication"
	"ohmf/services/gateway/internal/testutil"
)

func TestShareSessionPublishesRecipientFanoutAndSyncEvent(t *testing.T) {
	ctx, pool := testutil.OpenAndMigrateGatewayPool(t)
	defer pool.Close()

	mr, err := miniredis.Run()
	if err != nil {
		t.Fatalf("start miniredis: %v", err)
	}
	defer mr.Close()

	rdb := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	defer rdb.Close()

	store := replication.NewStore(pool, rdb)
	svc := NewService(pool, config.Config{}, rdb, store)

	senderID := testutil.InsertTestUser(t, ctx, pool)
	recipientID := testutil.InsertTestUser(t, ctx, pool)
	appID := "com.example.share." + uuid.NewString()
	insertMiniappCapableDevice(t, ctx, pool, senderID)
	insertMiniappCapableDevice(t, ctx, pool, recipientID)

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

	manifestID, err := svc.RegisterManifest(ctx, senderID, testManifest(appID))
	if err != nil {
		t.Fatalf("register manifest: %v", err)
	}

	messagePubsub := rdb.Subscribe(ctx, "message:user:"+recipientID)
	defer messagePubsub.Close()
	if _, err := messagePubsub.ReceiveTimeout(ctx, time.Second); err != nil {
		t.Fatalf("subscribe message channel: %v", err)
	}

	userEventPubsub := rdb.Subscribe(ctx, store.ChannelForUser(recipientID))
	defer userEventPubsub.Close()
	if _, err := userEventPubsub.ReceiveTimeout(ctx, time.Second); err != nil {
		t.Fatalf("subscribe user event channel: %v", err)
	}

	result, err := svc.ShareSession(ctx, senderID, ShareInput{
		ManifestID:         manifestID,
		ConversationID:     conversationID,
		GrantedPermissions: []string{"conversation.send_message"},
		StateSnapshot:      map[string]any{"counter": 3},
		ResumeExisting:     true,
	})
	if err != nil {
		t.Fatalf("share session: %v", err)
	}

	select {
	case published := <-messagePubsub.Channel():
		var payload map[string]any
		if err := json.Unmarshal([]byte(published.Payload), &payload); err != nil {
			t.Fatalf("unmarshal recipient message payload: %v", err)
		}
		if payload["conversation_id"] != conversationID {
			t.Fatalf("expected conversation_id %q, got %#v", conversationID, payload["conversation_id"])
		}
		if payload["content_type"] != contentTypeAppCard {
			t.Fatalf("expected content_type %q, got %#v", contentTypeAppCard, payload["content_type"])
		}
		if payload["transport"] != "OTT" {
			t.Fatalf("expected transport OTT, got %#v", payload["transport"])
		}
		content, _ := payload["content"].(map[string]any)
		preview, _ := content["message_preview"].(map[string]any)
		if preview["type"] != "static_image" {
			t.Fatalf("expected static_image preview, got %#v", preview["type"])
		}
		if preview["fit_mode"] != "scale" {
			t.Fatalf("expected scale fit_mode, got %#v", preview["fit_mode"])
		}
		previewState, _ := content["preview_state"].(map[string]any)
		if previewState["counter"] != float64(3) {
			t.Fatalf("expected preview_state.counter 3, got %#v", previewState["counter"])
		}
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for recipient message publish")
	}

	processed, err := store.ProcessBatch(ctx, 100)
	if err != nil {
		t.Fatalf("process sync batch: %v", err)
	}
	if processed == 0 {
		t.Fatal("expected a processed domain event for shared app card")
	}

	select {
	case published := <-userEventPubsub.Channel():
		var evt replication.Event
		if err := json.Unmarshal([]byte(published.Payload), &evt); err != nil {
			t.Fatalf("unmarshal user event payload: %v", err)
		}
		if evt.Type != replication.UserEventConversationMessageAppended {
			t.Fatalf("expected user event type %q, got %q", replication.UserEventConversationMessageAppended, evt.Type)
		}
		messagePayload, _ := evt.Payload["message"].(map[string]any)
		if messagePayload["message_id"] != result["message"].(map[string]any)["message_id"] {
			t.Fatalf("expected synced message_id %#v, got %#v", result["message"].(map[string]any)["message_id"], messagePayload["message_id"])
		}
		if messagePayload["content_type"] != contentTypeAppCard {
			t.Fatalf("expected synced content_type %q, got %#v", contentTypeAppCard, messagePayload["content_type"])
		}
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for recipient sync event publish")
	}
}

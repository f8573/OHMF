package conversations

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"testing"
	"time"

	miniredis "github.com/alicebob/miniredis/v2"
	"github.com/go-chi/chi/v5"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/redis/go-redis/v9"
	"ohmf/services/gateway/internal/middleware"
	"ohmf/services/gateway/internal/replication"
	"ohmf/services/gateway/internal/testutil"
	"ohmf/services/gateway/internal/users"
)

func TestUpdatePreferencesPersistsAndPublishesStateEvent(t *testing.T) {
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
	svc := NewService(pool, store, nil)

	actorID := insertTestUser(t, ctx, pool)
	targetID := insertTestUser(t, ctx, pool)
	conversationID := uuid.NewString()

	if _, err := pool.Exec(ctx, `INSERT INTO conversations (id, type) VALUES ($1, 'DM')`, conversationID); err != nil {
		t.Fatalf("insert conversation: %v", err)
	}
	if _, err := pool.Exec(ctx, `INSERT INTO conversation_counters (conversation_id) VALUES ($1)`, conversationID); err != nil {
		t.Fatalf("insert counter: %v", err)
	}
	if _, err := pool.Exec(ctx, `INSERT INTO conversation_members (conversation_id, user_id) VALUES ($1, $2::uuid), ($1, $3::uuid)`, conversationID, actorID, targetID); err != nil {
		t.Fatalf("insert members: %v", err)
	}

	pubsub := rdb.Subscribe(ctx, store.ChannelForUser(actorID))
	defer pubsub.Close()
	if _, err := pubsub.ReceiveTimeout(ctx, time.Second); err != nil {
		t.Fatalf("subscribe actor channel: %v", err)
	}

	updated, err := svc.UpdatePreferences(ctx, actorID, conversationID, stringPtr("Best Friend"), boolPtr(true), nil, nil, nil)
	if err != nil {
		t.Fatalf("update preferences: %v", err)
	}
	if updated.Nickname != "Best Friend" {
		t.Fatalf("expected nickname to persist, got %q", updated.Nickname)
	}
	if !updated.Closed {
		t.Fatal("expected conversation to be closed")
	}

	select {
	case message := <-pubsub.Channel():
		var evt replication.Event
		if err := json.Unmarshal([]byte(message.Payload), &evt); err != nil {
			t.Fatalf("unmarshal state event: %v", err)
		}
		if evt.Type != replication.UserEventConversationStateUpdated {
			t.Fatalf("expected event type %q, got %q", replication.UserEventConversationStateUpdated, evt.Type)
		}
		if evt.Payload["nickname"] != "Best Friend" {
			t.Fatalf("expected nickname payload, got %#v", evt.Payload["nickname"])
		}
		if closed, _ := evt.Payload["closed"].(bool); !closed {
			t.Fatalf("expected closed payload true, got %#v", evt.Payload["closed"])
		}
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for conversation state event")
	}
}

func TestUpdateMetadataPersistsDescriptionAndPreservesEffectPolicy(t *testing.T) {
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

	actorID := insertTestUser(t, ctx, pool)
	conversationID := uuid.NewString()

	if _, err := pool.Exec(ctx, `INSERT INTO conversations (id, type, allow_message_effects) VALUES ($1, 'GROUP', false)`, conversationID); err != nil {
		t.Fatalf("insert conversation: %v", err)
	}
	if _, err := pool.Exec(ctx, `INSERT INTO conversation_counters (conversation_id) VALUES ($1)`, conversationID); err != nil {
		t.Fatalf("insert counter: %v", err)
	}
	if _, err := pool.Exec(ctx, `INSERT INTO conversation_members (conversation_id, user_id, role) VALUES ($1, $2::uuid, 'OWNER')`, conversationID, actorID); err != nil {
		t.Fatalf("insert member: %v", err)
	}

	svc := NewService(pool, nil, nil)
	h := NewHandler(svc)

	body := `{"description":"Launch coordination thread"}`
	req := httptest.NewRequest(http.MethodPatch, "/v1/conversations/"+conversationID+"/metadata", strings.NewReader(body))
	req = req.WithContext(middleware.WithUserID(req.Context(), actorID))
	routeCtx := chi.NewRouteContext()
	routeCtx.URLParams.Add("id", conversationID)
	req = req.WithContext(context.WithValue(req.Context(), chi.RouteCtxKey, routeCtx))

	rec := httptest.NewRecorder()
	h.UpdateMetadata(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("expected status 200, got %d: %s", rec.Code, rec.Body.String())
	}

	var updated Conversation
	if err := json.NewDecoder(rec.Body).Decode(&updated); err != nil {
		t.Fatalf("decode response: %v", err)
	}
	if updated.Description != "Launch coordination thread" {
		t.Fatalf("expected description to persist, got %q", updated.Description)
	}
	if updated.AllowMessageEffects {
		t.Fatalf("expected effect policy to remain disabled, got %#v", updated.AllowMessageEffects)
	}
	if updated.ConversationID != conversationID {
		t.Fatalf("expected conversation id %s, got %s", conversationID, updated.ConversationID)
	}
}

func TestBlockUserPublishesCrossDeviceConversationState(t *testing.T) {
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
	convSvc := NewService(pool, store, nil)
	userSvc := users.NewService(pool, store)

	actorID := insertTestUser(t, ctx, pool)
	targetID := insertTestUser(t, ctx, pool)
	conversationID := uuid.NewString()

	if _, err := pool.Exec(ctx, `INSERT INTO conversations (id, type) VALUES ($1, 'DM')`, conversationID); err != nil {
		t.Fatalf("insert conversation: %v", err)
	}
	if _, err := pool.Exec(ctx, `INSERT INTO conversation_counters (conversation_id) VALUES ($1)`, conversationID); err != nil {
		t.Fatalf("insert counter: %v", err)
	}
	if _, err := pool.Exec(ctx, `INSERT INTO conversation_members (conversation_id, user_id) VALUES ($1, $2::uuid), ($1, $3::uuid)`, conversationID, actorID, targetID); err != nil {
		t.Fatalf("insert members: %v", err)
	}

	actorPubsub := rdb.Subscribe(ctx, store.ChannelForUser(actorID))
	defer actorPubsub.Close()
	if _, err := actorPubsub.ReceiveTimeout(ctx, time.Second); err != nil {
		t.Fatalf("subscribe actor channel: %v", err)
	}

	targetPubsub := rdb.Subscribe(ctx, store.ChannelForUser(targetID))
	defer targetPubsub.Close()
	if _, err := targetPubsub.ReceiveTimeout(ctx, time.Second); err != nil {
		t.Fatalf("subscribe target channel: %v", err)
	}

	if err := userSvc.BlockUser(ctx, actorID, targetID); err != nil {
		t.Fatalf("block user: %v", err)
	}

	assertConversationStateEvent(t, actorPubsub.Channel(), func(evt replication.Event) {
		if evt.Type != replication.UserEventConversationStateUpdated {
			t.Fatalf("expected state event for actor, got %q", evt.Type)
		}
		if blockedByViewer, _ := evt.Payload["blocked_by_viewer"].(bool); !blockedByViewer {
			t.Fatalf("expected blocked_by_viewer for actor, got %#v", evt.Payload["blocked_by_viewer"])
		}
	})
	assertConversationStateEvent(t, targetPubsub.Channel(), func(evt replication.Event) {
		if evt.Type != replication.UserEventConversationStateUpdated {
			t.Fatalf("expected state event for target, got %q", evt.Type)
		}
		if blockedByOther, _ := evt.Payload["blocked_by_other"].(bool); !blockedByOther {
			t.Fatalf("expected blocked_by_other for target, got %#v", evt.Payload["blocked_by_other"])
		}
	})

	actorView, err := convSvc.Get(ctx, actorID, conversationID)
	if err != nil {
		t.Fatalf("load actor projection: %v", err)
	}
	if !actorView.Blocked || !actorView.BlockedByViewer {
		t.Fatalf("expected actor projection blocked by viewer, got %#v", actorView)
	}

	targetView, err := convSvc.Get(ctx, targetID, conversationID)
	if err != nil {
		t.Fatalf("load target projection: %v", err)
	}
	if !targetView.Blocked || !targetView.BlockedByOther {
		t.Fatalf("expected target projection blocked by other, got %#v", targetView)
	}
}

func assertConversationStateEvent(t *testing.T, ch <-chan *redis.Message, assertFn func(replication.Event)) {
	t.Helper()
	select {
	case message := <-ch:
		var evt replication.Event
		if err := json.Unmarshal([]byte(message.Payload), &evt); err != nil {
			t.Fatalf("unmarshal state event: %v", err)
		}
		assertFn(evt)
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for conversation state event")
	}
}

func applyAllMigrations(t *testing.T, ctx context.Context, pool *pgxpool.Pool) {
	t.Helper()

	testutil.ResetAndMigrateGateway(t, ctx, pool)
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

func stringPtr(value string) *string { return &value }

func boolPtr(value bool) *bool { return &value }

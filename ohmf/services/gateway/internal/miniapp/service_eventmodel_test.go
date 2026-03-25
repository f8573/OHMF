package miniapp

import (
	"context"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
	"ohmf/services/gateway/internal/config"
	"ohmf/services/gateway/internal/testutil"
)

func TestSessionCreatedEvent(t *testing.T) {
	ctx, pool, svc := setupEventModelTest(t)
	defer pool.Close()

	ownerID := testutil.InsertTestUser(t, ctx, pool)
	sessionID := createEventModelSession(t, ctx, pool, svc, ownerID)

	events := mustSessionEvents(t, ctx, svc, sessionID)
	if len(events) == 0 {
		t.Fatal("expected at least one event")
	}

	firstEvent := events[0]
	if firstEvent.EventType != EventTypeSessionCreated {
		t.Fatalf("expected event_type=%s, got %s", EventTypeSessionCreated, firstEvent.EventType)
	}
	if firstEvent.ActorID == nil || *firstEvent.ActorID != ownerID {
		t.Fatalf("expected actor_id=%s, got %v", ownerID, firstEvent.ActorID)
	}
	if firstEvent.Body["participant_count"] != float64(1) {
		t.Fatalf("expected participant_count=1, got %v", firstEvent.Body["participant_count"])
	}
}

func TestSessionJoinedEvent(t *testing.T) {
	ctx, pool, svc := setupEventModelTest(t)
	defer pool.Close()

	ownerID := testutil.InsertTestUser(t, ctx, pool)
	joinerID := testutil.InsertTestUser(t, ctx, pool)
	sessionID := createEventModelSession(t, ctx, pool, svc, ownerID, joinerID)

	if _, err := svc.JoinSession(ctx, joinerID, sessionID, []string{"storage.session"}); err != nil {
		t.Fatalf("JoinSession failed: %v", err)
	}

	joinEvent := mustEventByType(t, mustSessionEvents(t, ctx, svc, sessionID), EventTypeSessionJoined)
	if joinEvent.ActorID == nil || *joinEvent.ActorID != joinerID {
		t.Fatalf("expected actor_id=%s, got %v", joinerID, joinEvent.ActorID)
	}
}

func TestSnapshotWrittenEvent(t *testing.T) {
	ctx, pool, svc := setupEventModelTest(t)
	defer pool.Close()

	ownerID := testutil.InsertTestUser(t, ctx, pool)
	sessionID := createEventModelSession(t, ctx, pool, svc, ownerID)

	newState := map[string]any{"level": 5, "score": 1000}
	version, err := svc.SnapshotSession(ctx, sessionID, newState, 0, ownerID)
	if err != nil {
		t.Fatalf("SnapshotSession failed: %v", err)
	}

	snapshotEvent := mustEventByType(t, mustSessionEvents(t, ctx, svc, sessionID), EventTypeSnapshotWritten)
	if snapshotEvent.ActorID == nil || *snapshotEvent.ActorID != ownerID {
		t.Fatalf("expected actor_id=%s, got %v", ownerID, snapshotEvent.ActorID)
	}
	if int(snapshotEvent.Body["state_version"].(float64)) != version {
		t.Fatalf("expected state_version=%d, got %v", version, snapshotEvent.Body["state_version"])
	}
}

func TestAppendOnlyEnforcement(t *testing.T) {
	ctx, pool, _ := setupEventModelTest(t)
	defer pool.Close()

	ownerID := testutil.InsertTestUser(t, ctx, pool)
	svc := NewService(pool, config.Config{}, nil, nil)
	sessionID := createEventModelSession(t, ctx, pool, svc, ownerID)

	seq, err := svc.AppendEvent(ctx, sessionID, ownerID, EventTypeStorageUpdated, "write", map[string]any{"ok": true})
	if err != nil {
		t.Fatalf("AppendEvent failed: %v", err)
	}

	if _, err := pool.Exec(ctx, `UPDATE miniapp_events SET body = '{}' WHERE app_session_id = $1::uuid AND event_seq = $2`, sessionID, seq); err == nil {
		t.Fatal("expected UPDATE to fail")
	}
	if _, err := pool.Exec(ctx, `DELETE FROM miniapp_events WHERE app_session_id = $1::uuid AND event_seq = $2`, sessionID, seq); err == nil {
		t.Fatal("expected DELETE to fail")
	}
}

func TestEventOrdering(t *testing.T) {
	ctx, pool, svc := setupEventModelTest(t)
	defer pool.Close()

	ownerID := testutil.InsertTestUser(t, ctx, pool)
	joinerID := testutil.InsertTestUser(t, ctx, pool)
	sessionID := createEventModelSession(t, ctx, pool, svc, ownerID, joinerID)

	if _, err := svc.JoinSession(ctx, joinerID, sessionID, []string{"storage.session"}); err != nil {
		t.Fatalf("JoinSession failed: %v", err)
	}
	for version := 1; version <= 3; version++ {
		if _, err := svc.SnapshotSession(ctx, sessionID, map[string]any{"v": version}, 0, ownerID); err != nil {
			t.Fatalf("SnapshotSession version %d failed: %v", version, err)
		}
	}

	events := mustSessionEvents(t, ctx, svc, sessionID)
	if len(events) < 5 {
		t.Fatalf("expected at least 5 events, got %d", len(events))
	}
	for i := 0; i < len(events)-1; i++ {
		if events[i].EventSeq >= events[i+1].EventSeq {
			t.Fatalf("events out of order: %d then %d", events[i].EventSeq, events[i+1].EventSeq)
		}
	}
}

func TestGetSessionEventsFiltering(t *testing.T) {
	ctx, pool, svc := setupEventModelTest(t)
	defer pool.Close()

	ownerID := testutil.InsertTestUser(t, ctx, pool)
	joinerIDs := []string{
		testutil.InsertTestUser(t, ctx, pool),
		testutil.InsertTestUser(t, ctx, pool),
		testutil.InsertTestUser(t, ctx, pool),
	}
	memberIDs := append([]string{ownerID}, joinerIDs...)
	sessionID := createEventModelSession(t, ctx, pool, svc, memberIDs...)

	for _, joinerID := range joinerIDs {
		if _, err := svc.JoinSession(ctx, joinerID, sessionID, []string{"storage.session"}); err != nil {
			t.Fatalf("JoinSession failed for %s: %v", joinerID, err)
		}
	}

	eventType := EventTypeSessionJoined
	events, err := svc.GetSessionEvents(ctx, sessionID, &eventType, 100, 0, nil)
	if err != nil {
		t.Fatalf("GetSessionEvents with filter failed: %v", err)
	}
	if len(events) != len(joinerIDs) {
		t.Fatalf("expected %d session_joined events, got %d", len(joinerIDs), len(events))
	}
	for _, event := range events {
		if event.EventType != EventTypeSessionJoined {
			t.Fatalf("expected only %s events, got %s", EventTypeSessionJoined, event.EventType)
		}
	}
}

func TestGetSessionEventsPagination(t *testing.T) {
	ctx, pool, svc := setupEventModelTest(t)
	defer pool.Close()

	ownerID := testutil.InsertTestUser(t, ctx, pool)
	sessionID := createEventModelSession(t, ctx, pool, svc, ownerID)

	for version := 1; version <= 20; version++ {
		if _, err := svc.SnapshotSession(ctx, sessionID, map[string]any{"i": version}, 0, ownerID); err != nil {
			t.Fatalf("SnapshotSession version %d failed: %v", version, err)
		}
	}

	events1, err := svc.GetSessionEvents(ctx, sessionID, nil, 5, 0, nil)
	if err != nil {
		t.Fatalf("GetSessionEvents page 1 failed: %v", err)
	}
	if len(events1) != 5 {
		t.Fatalf("expected 5 events, got %d", len(events1))
	}

	events2, err := svc.GetSessionEvents(ctx, sessionID, nil, 5, 5, nil)
	if err != nil {
		t.Fatalf("GetSessionEvents page 2 failed: %v", err)
	}
	if len(events2) != 5 {
		t.Fatalf("expected 5 events, got %d", len(events2))
	}

	for _, event1 := range events1 {
		for _, event2 := range events2 {
			if event1.EventSeq == event2.EventSeq {
				t.Fatalf("event %d appeared in both pages", event1.EventSeq)
			}
		}
	}
}

func setupEventModelTest(t *testing.T) (context.Context, *pgxpool.Pool, *Service) {
	t.Helper()

	ctx, pool := testutil.OpenAndMigrateGatewayPool(t)
	return ctx, pool, NewService(pool, config.Config{}, nil, nil)
}

func createEventModelSession(t *testing.T, ctx context.Context, pool *pgxpool.Pool, svc *Service, memberIDs ...string) string {
	t.Helper()

	if len(memberIDs) == 0 {
		t.Fatal("at least one member is required")
	}
	for _, userID := range memberIDs {
		insertMiniappCapableDevice(t, ctx, pool, userID)
	}

	conversationID := createTestConversation(t, ctx, pool, memberIDs...)
	appID := "com.example.eventmodel." + uuid.NewString()
	manifestID, err := svc.RegisterManifest(ctx, memberIDs[0], testManifest(appID))
	if err != nil {
		t.Fatalf("RegisterManifest failed: %v", err)
	}

	session, _, err := svc.CreateSession(ctx, CreateSessionInput{
		ManifestID:     manifestID,
		AppID:          appID,
		ConversationID: conversationID,
		Viewer: SessionParticipant{
			UserID: memberIDs[0],
			Role:   "PLAYER",
		},
		Participants: []SessionParticipant{
			{UserID: memberIDs[0], Role: "PLAYER"},
		},
		GrantedPermissions: []string{"storage.session", "realtime.session"},
		StateSnapshot:      map[string]any{},
		TTL:                30 * time.Minute,
	})
	if err != nil {
		t.Fatalf("CreateSession failed: %v", err)
	}

	sessionID, _ := session["app_session_id"].(string)
	if sessionID == "" {
		t.Fatal("CreateSession did not return app_session_id")
	}
	return sessionID
}

func mustSessionEvents(t *testing.T, ctx context.Context, svc *Service, sessionID string) []SessionEvent {
	t.Helper()

	events, err := svc.GetSessionEvents(ctx, sessionID, nil, 100, 0, nil)
	if err != nil {
		t.Fatalf("GetSessionEvents failed: %v", err)
	}
	return events
}

func mustEventByType(t *testing.T, events []SessionEvent, eventType string) *SessionEvent {
	t.Helper()

	for i := range events {
		if events[i].EventType == eventType {
			return &events[i]
		}
	}
	t.Fatalf("expected a %s event", eventType)
	return nil
}

func testManifest(appID string) map[string]any {
	manifest := validManifest()
	manifest["app_id"] = appID
	return manifest
}

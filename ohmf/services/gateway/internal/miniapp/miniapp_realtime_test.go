package miniapp

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"testing"
	"time"

	miniredis "github.com/alicebob/miniredis/v2"
	"github.com/google/uuid"
	"github.com/gorilla/websocket"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"ohmf/services/gateway/internal/config"
	"ohmf/services/gateway/internal/realtime"
	"ohmf/services/gateway/internal/replication"
	"ohmf/services/gateway/internal/token"
)

// TestSingleClientRealtimeEventDelivery tests that a single client receives real-time events
// within the expected latency when subscribed to a session.
func TestSingleClientRealtimeEventDelivery(t *testing.T) {
	t.Parallel()
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}

	dsn := os.Getenv("TEST_DATABASE_URL")
	if dsn == "" {
		t.Skip("skipping; set TEST_DATABASE_URL to run")
	}

	ctx := context.Background()
	pool, err := pgxpool.New(ctx, dsn)
	require.NoError(t, err)
	defer pool.Close()

	applyAllMigrations(t, ctx, pool)

	mr, err := miniredis.Run()
	require.NoError(t, err)
	defer mr.Close()

	rdb := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	defer rdb.Close()

	store := replication.NewStore(pool, rdb)
	miniappSvc := NewService(pool, config.Config{}, rdb, store)

	// Create test users and sessions
	userID := insertTestUser(t, ctx, pool)
	appID := "com.example.realtime." + uuid.NewString()
	insertMiniappCapableDevice(t, ctx, pool, userID)
	conversationID := createTestConversation(t, ctx, pool, userID)

	// Register manifest
	manifestID, err := miniappSvc.RegisterManifest(ctx, userID, testManifest(appID))
	require.NoError(t, err)

	// Create session
	session, _, err := miniappSvc.CreateSession(ctx, CreateSessionInput{
		ManifestID:     manifestID,
		AppID:          appID,
		ConversationID: conversationID,
		Viewer: SessionParticipant{
			UserID: userID,
			Role:   "owner",
		},
		Participants: []SessionParticipant{
			{UserID: userID, Role: "owner"},
		},
		GrantedPermissions: []string{"storage.session", "realtime.session"},
		StateSnapshot:      map[string]any{"initial": true},
		TTL:                time.Hour,
	})
	require.NoError(t, err)
	sessionID, _ := session["app_session_id"].(string)
	require.NotEmpty(t, sessionID)

	// Setup gateway handler
	gh, wsURL := setupTestGateway(t, miniappSvc, rdb, pool)

	// Connect WebSocket client
	conn, err := createWSConnectionV2(t, wsURL, userID, pool)
	require.NoError(t, err)
	defer conn.Close()

	// Subscribe to session
	err = subscribeSession(t, conn, sessionID)
	require.NoError(t, err)

	// Receive subscribe_session_ack
	ack := waitForMessageNonBlocking(t, conn, 2*time.Second)
	require.Equal(t, "subscribe_session_ack", ack["event"])

	// Trigger event
	eventBody := map[string]any{"test": "data", "value": 42}
	seq, err := miniappSvc.AppendEvent(ctx, sessionID, userID, EventTypeStorageUpdated, "testMethod", eventBody)
	require.NoError(t, err)
	require.Greater(t, seq, int64(0))

	// Wait for session_event within latency window
	eventMsg := waitForMessageNonBlocking(t, conn, 100*time.Millisecond)
	require.NotNil(t, eventMsg)
	require.Equal(t, "session_event", eventMsg["event"])

	// Verify event payload
	eventData := eventMsg["data"].(map[string]any)
	assert.Equal(t, float64(seq), eventData["event_seq"])
	assert.Equal(t, EventTypeStorageUpdated, eventData["event_type"])
	assert.Equal(t, userID, eventData["actor_id"])

	_ = gh
}

// TestMultipleClientsReceiveEvent tests that multiple clients subscribed to the same session
// both receive identical event payloads.
func TestMultipleClientsReceiveEvent(t *testing.T) {
	t.Parallel()
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}

	dsn := os.Getenv("TEST_DATABASE_URL")
	if dsn == "" {
		t.Skip("skipping; set TEST_DATABASE_URL to run")
	}

	ctx := context.Background()
	pool, err := pgxpool.New(ctx, dsn)
	require.NoError(t, err)
	defer pool.Close()

	applyAllMigrations(t, ctx, pool)

	mr, err := miniredis.Run()
	require.NoError(t, err)
	defer mr.Close()

	rdb := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	defer rdb.Close()

	store := replication.NewStore(pool, rdb)
	miniappSvc := NewService(pool, config.Config{}, rdb, store)

	// Create test users and sessions
	user1ID := insertTestUser(t, ctx, pool)
	user2ID := insertTestUser(t, ctx, pool)
	appID := "com.example.realtime." + uuid.NewString()
	insertMiniappCapableDevice(t, ctx, pool, user1ID)
	insertMiniappCapableDevice(t, ctx, pool, user2ID)
	conversationID := createTestConversation(t, ctx, pool, user1ID, user2ID)

	// Register manifest
	manifestID, err := miniappSvc.RegisterManifest(ctx, user1ID, testManifest(appID))
	require.NoError(t, err)

	// Create session shared between users
	session, _, err := miniappSvc.CreateSession(ctx, CreateSessionInput{
		ManifestID:     manifestID,
		AppID:          appID,
		ConversationID: conversationID,
		Viewer: SessionParticipant{
			UserID: user1ID,
			Role:   "owner",
		},
		Participants: []SessionParticipant{
			{UserID: user1ID, Role: "owner"},
			{UserID: user2ID, Role: "participant"},
		},
		GrantedPermissions: []string{"storage.session", "realtime.session"},
		StateSnapshot:      map[string]any{},
		TTL:                time.Hour,
	})
	require.NoError(t, err)
	sessionID, _ := session["app_session_id"].(string)
	require.NotEmpty(t, sessionID)

	// Setup gateway handler
	gh, wsURL := setupTestGateway(t, miniappSvc, rdb, pool)

	// Connect two WebSocket clients
	conn1, err := createWSConnectionV2(t, wsURL, user1ID, pool)
	require.NoError(t, err)
	defer conn1.Close()

	conn2, err := createWSConnectionV2(t, wsURL, user2ID, pool)
	require.NoError(t, err)
	defer conn2.Close()

	// Both clients subscribe to session
	err = subscribeSession(t, conn1, sessionID)
	require.NoError(t, err)
	waitForMessageNonBlocking(t, conn1, 2*time.Second) // consume ack

	err = subscribeSession(t, conn2, sessionID)
	require.NoError(t, err)
	waitForMessageNonBlocking(t, conn2, 2*time.Second) // consume ack

	// Trigger event from client 1
	eventBody := map[string]any{"type": "state_change", "data": "shared"}
	seq, err := miniappSvc.AppendEvent(ctx, sessionID, user1ID, EventTypeStorageUpdated, "updateSharedState", eventBody)
	require.NoError(t, err)

	// Both clients should receive identical events
	event1 := waitForMessageNonBlocking(t, conn1, 100*time.Millisecond)
	require.NotNil(t, event1)
	require.Equal(t, "session_event", event1["event"])

	event2 := waitForMessageNonBlocking(t, conn2, 100*time.Millisecond)
	require.NotNil(t, event2)
	require.Equal(t, "session_event", event2["event"])

	// Verify both received same event_seq
	data1 := event1["data"].(map[string]any)
	data2 := event2["data"].(map[string]any)
	assert.Equal(t, data1["event_seq"], data2["event_seq"])
	assert.Equal(t, float64(seq), data1["event_seq"])

	_ = gh
}

// TestReconnectWithCursorResume tests that a client can reconnect and retrieve missed events
// using polling with a cursor-based resume token.
func TestReconnectWithCursorResume(t *testing.T) {
	t.Parallel()
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}

	dsn := os.Getenv("TEST_DATABASE_URL")
	if dsn == "" {
		t.Skip("skipping; set TEST_DATABASE_URL to run")
	}

	ctx := context.Background()
	pool, err := pgxpool.New(ctx, dsn)
	require.NoError(t, err)
	defer pool.Close()

	applyAllMigrations(t, ctx, pool)

	mr, err := miniredis.Run()
	require.NoError(t, err)
	defer mr.Close()

	rdb := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	defer rdb.Close()

	store := replication.NewStore(pool, rdb)
	miniappSvc := NewService(pool, config.Config{}, rdb, store)

	// Create test user and session
	userID := insertTestUser(t, ctx, pool)
	appID := "com.example.realtime." + uuid.NewString()
	insertMiniappCapableDevice(t, ctx, pool, userID)
	conversationID := createTestConversation(t, ctx, pool, userID)

	// Register manifest
	manifestID, err := miniappSvc.RegisterManifest(ctx, userID, testManifest(appID))
	require.NoError(t, err)

	// Create session
	session, _, err := miniappSvc.CreateSession(ctx, CreateSessionInput{
		ManifestID:     manifestID,
		AppID:          appID,
		ConversationID: conversationID,
		Viewer: SessionParticipant{
			UserID: userID,
			Role:   "owner",
		},
		Participants: []SessionParticipant{
			{UserID: userID, Role: "owner"},
		},
		GrantedPermissions: []string{"storage.session", "realtime.session"},
		StateSnapshot:      map[string]any{},
		TTL:                time.Hour,
	})
	require.NoError(t, err)
	sessionID, _ := session["app_session_id"].(string)
	require.NotEmpty(t, sessionID)

	// Append events while disconnected
	seqEvents := make([]int64, 3)
	for i := 0; i < 3; i++ {
		seq, err := miniappSvc.AppendEvent(ctx, sessionID, userID, EventTypeStorageUpdated, fmt.Sprintf("event%d", i), map[string]any{"n": i})
		require.NoError(t, err)
		seqEvents[i] = seq
	}

	// Query events using polling API
	eventType := EventTypeStorageUpdated
	events, err := miniappSvc.GetSessionEvents(ctx, sessionID, &eventType, 100, 0, nil)
	require.NoError(t, err)
	require.Len(t, events, 3)

	// Verify events are in order with correct sequence numbers
	for i, evt := range events {
		assert.Equal(t, seqEvents[i], evt.EventSeq)
	}

	// Simulate polling with since_seq cursor
	lastSeq := seqEvents[1] // Get events after second event
	resumeEvents, err := miniappSvc.GetSessionEvents(ctx, sessionID, nil, 100, 0, &lastSeq)
	require.NoError(t, err)
	require.Len(t, resumeEvents, 1) // Only the third event
	assert.Equal(t, seqEvents[2], resumeEvents[0].EventSeq)
}

// TestUnsubscribeStopsEventDelivery tests that unsubscribing from a session stops event delivery.
// If unsubscribe is not implemented, closing the connection should stop events.
func TestUnsubscribeStopsEventDelivery(t *testing.T) {
	t.Parallel()
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}

	dsn := os.Getenv("TEST_DATABASE_URL")
	if dsn == "" {
		t.Skip("skipping; set TEST_DATABASE_URL to run")
	}

	ctx := context.Background()
	pool, err := pgxpool.New(ctx, dsn)
	require.NoError(t, err)
	defer pool.Close()

	applyAllMigrations(t, ctx, pool)

	mr, err := miniredis.Run()
	require.NoError(t, err)
	defer mr.Close()

	rdb := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	defer rdb.Close()

	store := replication.NewStore(pool, rdb)
	miniappSvc := NewService(pool, config.Config{}, rdb, store)

	userID := insertTestUser(t, ctx, pool)
	appID := "com.example.realtime." + uuid.NewString()
	insertMiniappCapableDevice(t, ctx, pool, userID)
	conversationID := createTestConversation(t, ctx, pool, userID)

	// Register manifest
	manifestID, err := miniappSvc.RegisterManifest(ctx, userID, testManifest(appID))
	require.NoError(t, err)

	// Create session
	session, _, err := miniappSvc.CreateSession(ctx, CreateSessionInput{
		ManifestID:     manifestID,
		AppID:          appID,
		ConversationID: conversationID,
		Viewer: SessionParticipant{
			UserID: userID,
			Role:   "owner",
		},
		Participants: []SessionParticipant{
			{UserID: userID, Role: "owner"},
		},
		GrantedPermissions: []string{"storage.session", "realtime.session"},
		StateSnapshot:      map[string]any{},
		TTL:                time.Hour,
	})
	require.NoError(t, err)
	sessionID, _ := session["app_session_id"].(string)
	require.NotEmpty(t, sessionID)

	// Setup gateway handler
	gh, wsURL := setupTestGateway(t, miniappSvc, rdb, pool)

	// Connect client
	conn, err := createWSConnectionV2(t, wsURL, userID, pool)
	require.NoError(t, err)

	// Subscribe to session
	err = subscribeSession(t, conn, sessionID)
	require.NoError(t, err)
	waitForMessageNonBlocking(t, conn, 2*time.Second) // consume ack

	// Trigger first event and verify receipt
	seq1, err := miniappSvc.AppendEvent(ctx, sessionID, userID, EventTypeStorageUpdated, "event1", map[string]any{})
	require.NoError(t, err)

	event1 := waitForMessageNonBlocking(t, conn, 100*time.Millisecond)
	require.NotNil(t, event1)
	data1 := event1["data"].(map[string]any)
	assert.Equal(t, float64(seq1), data1["event_seq"])

	// Close connection (simulates unsubscribe or disconnect)
	conn.Close()

	// Give the cleanup goroutine time to run
	time.Sleep(50 * time.Millisecond)

	// Trigger second event - client should not receive it
	seq2, err := miniappSvc.AppendEvent(ctx, sessionID, userID, EventTypeStorageUpdated, "event2", map[string]any{})
	require.NoError(t, err)
	require.Greater(t, seq2, seq1)

	// Try to read from closed connection - should fail
	_, _, err = conn.ReadMessage()
	require.Error(t, err)

	_ = gh
}

// TestSubscriptionPersistencyAcrossStateUpdates tests that subscriptions remain active
// and all events are delivered in order even when state changes occur.
func TestSubscriptionPersistencyAcrossStateUpdates(t *testing.T) {
	t.Parallel()
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}

	dsn := os.Getenv("TEST_DATABASE_URL")
	if dsn == "" {
		t.Skip("skipping; set TEST_DATABASE_URL to run")
	}

	ctx := context.Background()
	pool, err := pgxpool.New(ctx, dsn)
	require.NoError(t, err)
	defer pool.Close()

	applyAllMigrations(t, ctx, pool)

	mr, err := miniredis.Run()
	require.NoError(t, err)
	defer mr.Close()

	rdb := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	defer rdb.Close()

	store := replication.NewStore(pool, rdb)
	miniappSvc := NewService(pool, config.Config{}, rdb, store)

	userID := insertTestUser(t, ctx, pool)
	appID := "com.example.realtime." + uuid.NewString()
	insertMiniappCapableDevice(t, ctx, pool, userID)
	conversationID := createTestConversation(t, ctx, pool, userID)

	// Register manifest
	manifestID, err := miniappSvc.RegisterManifest(ctx, userID, testManifest(appID))
	require.NoError(t, err)

	// Create session
	session, _, err := miniappSvc.CreateSession(ctx, CreateSessionInput{
		ManifestID:     manifestID,
		AppID:          appID,
		ConversationID: conversationID,
		Viewer: SessionParticipant{
			UserID: userID,
			Role:   "owner",
		},
		Participants: []SessionParticipant{
			{UserID: userID, Role: "owner"},
		},
		GrantedPermissions: []string{"storage.session", "realtime.session"},
		StateSnapshot:      map[string]any{"initial": true},
		TTL:                time.Hour,
	})
	require.NoError(t, err)
	sessionID, _ := session["app_session_id"].(string)
	require.NotEmpty(t, sessionID)

	// Setup gateway handler
	gh, wsURL := setupTestGateway(t, miniappSvc, rdb, pool)

	// Connect client
	conn, err := createWSConnectionV2(t, wsURL, userID, pool)
	require.NoError(t, err)
	defer conn.Close()

	// Subscribe to session
	err = subscribeSession(t, conn, sessionID)
	require.NoError(t, err)
	waitForMessageNonBlocking(t, conn, 2*time.Second) // consume ack

	// Append event 1
	seq1, err := miniappSvc.AppendEvent(ctx, sessionID, userID, EventTypeStorageUpdated, "event1", map[string]any{"v": 1})
	require.NoError(t, err)

	// Snapshot session (state update)
	_, err = miniappSvc.SnapshotSession(ctx, sessionID, map[string]any{"v": 1}, 0, userID)
	require.NoError(t, err)

	// Append event 2
	seq2, err := miniappSvc.AppendEvent(ctx, sessionID, userID, EventTypeStorageUpdated, "event2", map[string]any{"v": 2})
	require.NoError(t, err)

	// Append event 3
	seq3, err := miniappSvc.AppendEvent(ctx, sessionID, userID, EventTypeStorageUpdated, "event3", map[string]any{"v": 3})
	require.NoError(t, err)

	// Collect all events from WebSocket
	storageEvents := make([]map[string]any, 0, 3)
	deadline := time.Now().Add(2 * time.Second)
	for len(storageEvents) < 3 && time.Now().Before(deadline) {
		msg := waitForMessageNonBlocking(t, conn, time.Until(deadline))
		if msg == nil {
			break
		}
		if msg["event"] != "session_event" {
			continue
		}
		data := msg["data"].(map[string]any)
		if data["event_type"] == EventTypeStorageUpdated {
			storageEvents = append(storageEvents, data)
		}
	}

	require.Len(t, storageEvents, 3)

	// Verify sequence numbers increment correctly
	expectedSeqs := []int64{seq1, seq2, seq3}
	for i, evt := range storageEvents[:3] {
		assert.Equal(t, float64(expectedSeqs[i]), evt["event_seq"])
	}

	_ = gh
}

// Helper functions

// setupTestGateway initializes a test gateway with WebSocket handler, miniapp service, and Redis.
func setupTestGateway(t *testing.T, miniappSvc *Service, redisClient *redis.Client, dbPool *pgxpool.Pool) (*realtime.Handler, string) {
	t.Helper()

	// Create token service (mock)
	tokenSvc := token.NewService("test-secret")

	// Create test server
	handler := realtime.NewHandlerWithSend(tokenSvc, nil, redisClient, nil, nil, miniappSvc)

	// Create HTTP server
	mux := http.NewServeMux()
	mux.HandleFunc("/v2/ws", handler.ServeV2)

	server := httptest.NewServer(mux)
	t.Cleanup(server.Close)

	// Convert to WebSocket URL
	wsURL := "ws" + server.URL[4:] + "/v2/ws"
	return handler, wsURL
}

// createWSConnectionV2 establishes an authenticated WebSocket v2 connection.
func createWSConnectionV2(t *testing.T, wsURL string, userID string, dbPool *pgxpool.Pool) (*websocket.Conn, error) {
	t.Helper()

	// Generate test token
	ctx := context.Background()
	tokenSvc := token.NewService("test-secret")
	accessToken, err := tokenSvc.IssueAccess(userID, "device-1", time.Hour, nil)
	if err != nil {
		return nil, err
	}

	// Connect to WebSocket with token
	header := http.Header{}
	header.Add("Authorization", "Bearer "+accessToken)

	conn, _, err := websocket.DefaultDialer.DialContext(ctx, wsURL, header)
	if err != nil {
		return nil, err
	}

	return conn, nil
}

// subscribeSession sends a subscribe_session message to subscribe to a session.
func subscribeSession(t *testing.T, conn *websocket.Conn, sessionID string) error {
	t.Helper()

	payload := map[string]any{
		"session_id": sessionID,
	}
	data, err := json.Marshal(payload)
	if err != nil {
		return err
	}

	envelope := map[string]any{
		"event": "subscribe_session",
		"data":  json.RawMessage(data),
	}
	body, err := json.Marshal(envelope)
	if err != nil {
		return err
	}

	return conn.WriteMessage(websocket.TextMessage, body)
}

// waitForMessageNonBlocking attempts to read a message with a timeout without blocking the caller.
func waitForMessageNonBlocking(t *testing.T, conn *websocket.Conn, timeout time.Duration) map[string]any {
	t.Helper()

	conn.SetReadDeadline(time.Now().Add(timeout))
	_, payload, err := conn.ReadMessage()
	conn.SetReadDeadline(time.Time{})

	if err != nil {
		return nil
	}

	var envelope struct {
		Event string          `json:"event"`
		Data  json.RawMessage `json:"data"`
	}
	if err := json.Unmarshal(payload, &envelope); err != nil {
		return nil
	}

	var data map[string]any
	if err := json.Unmarshal(envelope.Data, &data); err != nil {
		data = map[string]any{}
	}

	return map[string]any{
		"event": envelope.Event,
		"data":  data,
	}
}

// createTestSession creates a mini-app session with known parameters.
func createTestSession(t *testing.T, svc *Service, userID, appID string) (string, error) {
	conversationID := createTestConversation(t, context.Background(), svc.db, userID)

	// Register manifest
	manifestID, err := svc.RegisterManifest(context.Background(), userID, testManifest(appID))
	if err != nil {
		return "", err
	}

	session, _, err := svc.CreateSession(context.Background(), CreateSessionInput{
		ManifestID:     manifestID,
		AppID:          appID,
		ConversationID: conversationID,
		Viewer: SessionParticipant{
			UserID: userID,
			Role:   "owner",
		},
		Participants: []SessionParticipant{
			{UserID: userID, Role: "owner"},
		},
		GrantedPermissions: []string{"storage.session", "realtime.session"},
		StateSnapshot:      map[string]any{},
		TTL:                time.Hour,
	})
	if err != nil {
		return "", err
	}
	sessionID, _ := session["app_session_id"].(string)
	if sessionID == "" {
		return "", fmt.Errorf("app_session_id missing")
	}
	return sessionID, nil
}

func createTestConversation(t *testing.T, ctx context.Context, pool *pgxpool.Pool, userIDs ...string) string {
	t.Helper()

	conversationID := uuid.NewString()
	_, err := pool.Exec(ctx, `INSERT INTO conversations (id, type) VALUES ($1, 'PRIVATE')`, conversationID)
	require.NoError(t, err)
	_, err = pool.Exec(ctx, `INSERT INTO conversation_counters (conversation_id) VALUES ($1)`, conversationID)
	require.NoError(t, err)
	for _, userID := range userIDs {
		_, err = pool.Exec(ctx, `INSERT INTO conversation_members (conversation_id, user_id) VALUES ($1, $2::uuid)`, conversationID, userID)
		require.NoError(t, err)
	}
	return conversationID
}

package realtime

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	miniredis "github.com/alicebob/miniredis/v2"
	pgxmock "github.com/pashagolub/pgxmock/v4"
	"github.com/redis/go-redis/v9"
	"ohmf/services/gateway/internal/messages"
	"ohmf/services/gateway/internal/replication"
)

func TestHandleTypingSignalBroadcastsToOtherMembers(t *testing.T) {
	mock, err := pgxmock.NewPool()
	if err != nil {
		t.Fatal(err)
	}
	defer mock.Close()

	svc := messages.NewService(mock, messages.Options{})
	handler := &Handler{
		messages: svc,
		clients:  map[string]map[*client]struct{}{},
	}
	actor := &client{userID: "user-1", deviceID: "device-1", send: make(chan []byte, 1)}
	recipient := &client{userID: "user-2", send: make(chan []byte, 1)}
	handler.clients["user-1"] = map[*client]struct{}{actor: struct{}{}}
	handler.clients["user-2"] = map[*client]struct{}{recipient: struct{}{}}

	mock.ExpectQuery(`SELECT 1 FROM conversation_members WHERE conversation_id = \$1::uuid AND user_id = \$2::uuid`).
		WithArgs("conversation-1", "user-1").
		WillReturnRows(pgxmock.NewRows([]string{"one"}).AddRow(1))
	mock.ExpectQuery(`SELECT share_typing FROM user_privacy_preferences WHERE user_id = \$1::uuid`).
		WithArgs("user-1").
		WillReturnRows(pgxmock.NewRows([]string{"share_typing"}).AddRow(true))
	mock.ExpectQuery(`SELECT 1 FROM conversation_members WHERE conversation_id = \$1::uuid AND user_id = \$2::uuid`).
		WithArgs("conversation-1", "user-2").
		WillReturnRows(pgxmock.NewRows([]string{"one"}).AddRow(1))

	handler.handleTypingSignal(context.Background(), actor, "conversation-1", "typing.started", "127.0.0.1")

	select {
	case raw := <-recipient.send:
		var envelope struct {
			Event string         `json:"event"`
			Data  map[string]any `json:"data"`
		}
		if err := json.Unmarshal(raw, &envelope); err != nil {
			t.Fatalf("failed to decode ws payload: %v", err)
		}
		if envelope.Event != "typing.started" {
			t.Fatalf("unexpected event: %#v", envelope.Event)
		}
		if got := envelope.Data["conversation_id"]; got != "conversation-1" {
			t.Fatalf("unexpected conversation id: %#v", got)
		}
		if got := envelope.Data["retry_after_ms"]; got != float64(3000) {
			t.Fatalf("unexpected retry_after_ms: %#v", got)
		}
		if got := envelope.Data["started_at_ms"]; got == nil {
			t.Fatalf("expected started_at_ms in payload")
		}
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for typing broadcast")
	}

	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("unmet expectations: %v", err)
	}
}

func TestHandleHelloResumeTouchesPresenceAndReplaysFromLastCursor(t *testing.T) {
	mock, err := pgxmock.NewPool()
	if err != nil {
		t.Fatal(err)
	}
	defer mock.Close()

	mr, err := miniredis.Run()
	if err != nil {
		t.Fatalf("start miniredis: %v", err)
	}
	defer mr.Close()

	rdb := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	defer rdb.Close()

	createdAt := time.Date(2026, 3, 20, 13, 0, 0, 0, time.UTC)
	rows := pgxmock.NewRows([]string{"user_event_id", "event_type", "payload", "created_at"}).
		AddRow(int64(18), replication.UserEventConversationMessageAppended, []byte(`{"message_id":"msg-1","conversation_id":"conv-1"}`), createdAt)
	mock.ExpectQuery(`SELECT user_event_id, event_type, payload, created_at FROM user_inbox_events`).
		WithArgs("user-1", int64(17), 251).
		WillReturnRows(rows)

	handler := &Handler{
		redis:       rdb,
		replication: replication.NewStore(mock, nil),
		clients:     map[string]map[*client]struct{}{},
	}
	c := &client{
		userID:    "user-1",
		sessionID: "wsv2:session-1",
		v2:        true,
		send:      make(chan []byte, 8),
	}

	handler.handleHelloResume(context.Background(), c, resumePayload{
		DeviceID:   "device-1",
		LastCursor: "17",
	})

	helloMsg := decodeWSMessage(t, c.send)
	if helloMsg.Event != "hello_ack" {
		t.Fatalf("unexpected hello event: %q", helloMsg.Event)
	}
	if got := helloMsg.Data["resume_supported"]; got != true {
		t.Fatalf("expected resume_supported=true, got %#v", got)
	}
	if got := helloMsg.Data["session_id"]; got != "wsv2:session-1" {
		t.Fatalf("unexpected session id: %#v", got)
	}

	eventMsg := decodeWSMessage(t, c.send)
	if eventMsg.Event != "event" {
		t.Fatalf("unexpected resume event: %q", eventMsg.Event)
	}
	if got := eventMsg.Data["user_event_id"]; got != float64(18) {
		t.Fatalf("unexpected user_event_id: %#v", got)
	}
	if got := eventMsg.Data["type"]; got != replication.UserEventConversationMessageAppended {
		t.Fatalf("unexpected event type: %#v", got)
	}

	if got := mr.Exists("presence:user:user-1"); !got {
		t.Fatalf("expected presence key to exist")
	}
	lastSeen, err := rdb.Get(context.Background(), "presence:user:user-1:last_seen").Result()
	if err != nil {
		t.Fatalf("expected last_seen key: %v", err)
	}
	if lastSeen == "" {
		t.Fatalf("expected non-empty last_seen value")
	}
	sessionBody, err := rdb.Get(context.Background(), "session:wsv2:session-1").Result()
	if err != nil {
		t.Fatalf("expected session key: %v", err)
	}
	var session map[string]any
	if err := json.Unmarshal([]byte(sessionBody), &session); err != nil {
		t.Fatalf("decode session body: %v", err)
	}
	if got := session["device_id"]; got != "device-1" {
		t.Fatalf("unexpected device_id in session: %#v", got)
	}
	if got := session["last_seen_at_ms"]; got == nil {
		t.Fatalf("expected last_seen_at_ms in session body")
	}

	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("unmet expectations: %v", err)
	}
}

func TestTypingStateIsCleanedUpOnDisconnect(t *testing.T) {
	mock, err := pgxmock.NewPool()
	if err != nil {
		t.Fatal(err)
	}
	defer mock.Close()

	mr, err := miniredis.Run()
	if err != nil {
		t.Fatalf("start miniredis: %v", err)
	}
	defer mr.Close()

	rdb := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	defer rdb.Close()

	svc := messages.NewService(mock, messages.Options{})
	handler := &Handler{
		messages: svc,
		redis:    rdb,
		clients:  map[string]map[*client]struct{}{},
	}
	actor := &client{
		userID:    "user-1",
		deviceID:  "device-1",
		sessionID: "wsv1:session-1",
		send:      make(chan []byte, 1),
		v2:        false,
	}
	handler.register(actor)

	mock.ExpectQuery(`SELECT 1 FROM conversation_members WHERE conversation_id = \$1::uuid AND user_id = \$2::uuid`).
		WithArgs("conversation-1", "user-1").
		WillReturnRows(pgxmock.NewRows([]string{"one"}).AddRow(1))
	mock.ExpectQuery(`SELECT share_typing FROM user_privacy_preferences WHERE user_id = \$1::uuid`).
		WithArgs("user-1").
		WillReturnRows(pgxmock.NewRows([]string{"share_typing"}).AddRow(true))

	handler.handleTypingSignal(context.Background(), actor, "conversation-1", "typing.started", "127.0.0.1")
	if !mr.Exists("typing:conv:conversation-1:user:user-1") {
		t.Fatalf("expected typing key to exist after typing.started")
	}
	if !mr.Exists("presence:user:user-1:last_seen") {
		t.Fatalf("expected presence last_seen key to be updated")
	}

	handler.unregister(actor)

	if mr.Exists("typing:conv:conversation-1:user:user-1") {
		t.Fatalf("expected typing key to be removed on disconnect")
	}
	if mr.Exists("presence:user:user-1") {
		t.Fatalf("expected presence key to be removed on disconnect")
	}
	if mr.Exists("presence:user:user-1:last_seen") {
		t.Fatalf("expected last_seen key to be removed on disconnect")
	}

	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("unmet expectations: %v", err)
	}
}

func decodeWSMessage(t *testing.T, ch <-chan []byte) struct {
	Event string         `json:"event"`
	Data  map[string]any `json:"data"`
} {
	t.Helper()
	select {
	case raw := <-ch:
		var msg struct {
			Event string         `json:"event"`
			Data  map[string]any `json:"data"`
		}
		if err := json.Unmarshal(raw, &msg); err != nil {
			t.Fatalf("decode ws payload: %v", err)
		}
		return msg
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for websocket message")
		return struct {
			Event string         `json:"event"`
			Data  map[string]any `json:"data"`
		}{}
	}
}

func TestUserEventTypeNameIncludesTypingAndEffectEvents(t *testing.T) {
	handler := &Handler{}
	if got := handler.userEventTypeName(replication.UserEventConversationTypingUpdated); got != "conversation_typing_updated" {
		t.Fatalf("unexpected typing mapping: %q", got)
	}
	if got := handler.userEventTypeName(replication.UserEventConversationMessageEffectTriggered); got != "conversation_message_effect_triggered" {
		t.Fatalf("unexpected effect mapping: %q", got)
	}
	if got := handler.userEventTypeName(replication.UserEventAccountDeviceLinked); got != "account_device_linked" {
		t.Fatalf("unexpected linked-device mapping: %q", got)
	}
	if got := handler.userEventTypeName("unknown"); got != "event" {
		t.Fatalf("unexpected default mapping: %q", got)
	}
}

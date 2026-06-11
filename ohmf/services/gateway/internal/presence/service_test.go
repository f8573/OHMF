package presence

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	miniredis "github.com/alicebob/miniredis/v2"
	pgxmock "github.com/pashagolub/pgxmock/v4"
	"github.com/redis/go-redis/v9"
)

func TestGetUserPresenceReturnsSessions(t *testing.T) {
	mr, err := miniredis.Run()
	if err != nil {
		t.Fatal(err)
	}
	defer mr.Close()

	rdb := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	defer rdb.Close()

	now := time.Now().UTC()
	sessionBody, _ := json.Marshal(map[string]any{
		"device_id":       "device-1",
		"session_id":      "wsv2:1",
		"version":         "v2",
		"last_seen_at_ms": now.UnixMilli(),
	})
	if err := rdb.Set(context.Background(), "presence:user:user-1", "online", time.Minute).Err(); err != nil {
		t.Fatal(err)
	}
	if err := rdb.Set(context.Background(), "presence:user:user-1:last_seen", now.UnixMilli(), time.Minute).Err(); err != nil {
		t.Fatal(err)
	}
	if err := rdb.SAdd(context.Background(), "user_sessions:user-1", "wsv2:1").Err(); err != nil {
		t.Fatal(err)
	}
	if err := rdb.Set(context.Background(), "session:wsv2:1", sessionBody, time.Minute).Err(); err != nil {
		t.Fatal(err)
	}

	svc := NewService(nil, rdb)
	item, err := svc.GetUserPresence(context.Background(), "user-1")
	if err != nil {
		t.Fatalf("GetUserPresence failed: %v", err)
	}
	if !item.Online {
		t.Fatalf("expected online presence")
	}
	if item.SessionCount != 1 {
		t.Fatalf("expected 1 session, got %d", item.SessionCount)
	}
	if len(item.Sessions) != 1 || item.Sessions[0].DeviceID != "device-1" {
		t.Fatalf("unexpected sessions: %#v", item.Sessions)
	}
}

func TestGetConversationPresenceRequiresMembership(t *testing.T) {
	mock, err := pgxmock.NewPool()
	if err != nil {
		t.Fatal(err)
	}
	defer mock.Close()

	mock.ExpectQuery(`SELECT EXISTS\(`).
		WithArgs("conversation-1", "user-1").
		WillReturnRows(pgxmock.NewRows([]string{"exists"}).AddRow(false))

	svc := NewService(mock, nil)
	if _, err := svc.GetConversationPresence(context.Background(), "user-1", "conversation-1"); err == nil {
		t.Fatal("expected membership failure")
	}
}

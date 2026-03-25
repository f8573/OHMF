package auth

import (
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	miniredis "github.com/alicebob/miniredis/v2"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/redis/go-redis/v9"
	"ohmf/services/gateway/internal/config"
	"ohmf/services/gateway/internal/middleware"
	"ohmf/services/gateway/internal/otp"
	"ohmf/services/gateway/internal/replication"
	"ohmf/services/gateway/internal/testutil"
	"ohmf/services/gateway/internal/token"
)

func TestStartPairingRequiresAuth(t *testing.T) {
	req := httptest.NewRequest(http.MethodPost, "/v1/auth/pairing/start", bytes.NewBufferString(`{}`))
	rec := httptest.NewRecorder()

	(&Handler{}).StartPairing(rec, req)

	if rec.Code != http.StatusUnauthorized {
		t.Fatalf("expected 401, got %d", rec.Code)
	}

	var payload map[string]any
	if err := json.Unmarshal(rec.Body.Bytes(), &payload); err != nil {
		t.Fatalf("decode unauthorized body: %v", err)
	}
	if payload["code"] != "unauthorized" {
		t.Fatalf("expected unauthorized code, got %#v", payload["code"])
	}
}

func TestStartPairingReturnsArtifactForAuthenticatedUser(t *testing.T) {
	ctx, pool := testutil.OpenAndMigrateGatewayPool(t)
	defer pool.Close()

	userID := testutil.InsertTestUser(t, ctx, pool)
	deviceID := testutil.InsertTestDevice(t, ctx, pool, userID, "WEB", "Primary web")
	handler := newTestHandler(pool, nil)

	req := httptest.NewRequest(http.MethodPost, "/v1/auth/pairing/start", bytes.NewBufferString(`{}`))
	req = req.WithContext(middleware.WithDeviceID(middleware.WithUserID(req.Context(), userID), deviceID))
	rec := httptest.NewRecorder()

	handler.StartPairing(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d: %s", rec.Code, rec.Body.String())
	}

	var payload struct {
		PairingSessionID string `json:"pairing_session_id"`
		PairingCode      string `json:"pairing_code"`
		ExpiresAt        string `json:"expires_at"`
	}
	if err := json.Unmarshal(rec.Body.Bytes(), &payload); err != nil {
		t.Fatalf("decode response: %v", err)
	}
	if payload.PairingSessionID == "" || payload.PairingCode == "" || payload.ExpiresAt == "" {
		t.Fatalf("expected pairing artifact fields, got %#v", payload)
	}

	var storedCode string
	var requestedBy string
	var status string
	if err := pool.QueryRow(ctx, `
		SELECT pairing_code, COALESCE(requested_by_device_id::text, ''), status
		FROM device_pairing_sessions
		WHERE id = $1::uuid
	`, payload.PairingSessionID).Scan(&storedCode, &requestedBy, &status); err != nil {
		t.Fatalf("load pairing session: %v", err)
	}
	if storedCode != payload.PairingCode {
		t.Fatalf("expected stored pairing code %q, got %q", payload.PairingCode, storedCode)
	}
	if requestedBy != deviceID {
		t.Fatalf("expected requested_by_device_id %q, got %q", deviceID, requestedBy)
	}
	if status != "PENDING" {
		t.Fatalf("expected pending status, got %q", status)
	}
}

func TestCompletePairingRejectsInvalidCode(t *testing.T) {
	_, pool := testutil.OpenAndMigrateGatewayPool(t)
	defer pool.Close()

	handler := newTestHandler(pool, nil)
	req := httptest.NewRequest(http.MethodPost, "/v1/auth/pairing/complete", bytes.NewBufferString(`{"pairing_code":"BAD123","device":{"platform":"WEB"}}`))
	rec := httptest.NewRecorder()

	handler.CompletePairing(rec, req)

	if rec.Code != http.StatusUnauthorized {
		t.Fatalf("expected 401, got %d", rec.Code)
	}

	var payload map[string]any
	if err := json.Unmarshal(rec.Body.Bytes(), &payload); err != nil {
		t.Fatalf("decode response: %v", err)
	}
	if payload["code"] != "invalid_pairing_code" {
		t.Fatalf("expected invalid_pairing_code, got %#v", payload["code"])
	}
}

func TestCompletePairingRejectsExpiredCode(t *testing.T) {
	ctx, pool := testutil.OpenAndMigrateGatewayPool(t)
	defer pool.Close()

	userID := testutil.InsertTestUser(t, ctx, pool)
	expiredCode := "EXPIRED1"
	if _, err := pool.Exec(ctx, `
		INSERT INTO device_pairing_sessions (user_id, pairing_code, expires_at, status)
		VALUES ($1::uuid, $2, now() - interval '1 minute', 'PENDING')
	`, userID, expiredCode); err != nil {
		t.Fatalf("insert expired pairing session: %v", err)
	}

	handler := newTestHandler(pool, nil)
	req := httptest.NewRequest(http.MethodPost, "/v1/auth/pairing/complete", bytes.NewBufferString(`{"pairing_code":"EXPIRED1","device":{"platform":"WEB","device_name":"Expired web"}}`))
	rec := httptest.NewRecorder()

	handler.CompletePairing(rec, req)

	if rec.Code != http.StatusGone {
		t.Fatalf("expected 410, got %d: %s", rec.Code, rec.Body.String())
	}

	var payload map[string]any
	if err := json.Unmarshal(rec.Body.Bytes(), &payload); err != nil {
		t.Fatalf("decode response: %v", err)
	}
	if payload["code"] != "pairing_expired" {
		t.Fatalf("expected pairing_expired, got %#v", payload["code"])
	}
}

func TestCompletePairingCreatesDeviceIssuesTokensAndPublishesLinkedDeviceEvent(t *testing.T) {
	ctx, pool := testutil.OpenAndMigrateGatewayPool(t)
	defer pool.Close()

	mr, err := miniredis.Run()
	if err != nil {
		t.Fatalf("start miniredis: %v", err)
	}
	defer mr.Close()

	rdb := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	defer rdb.Close()

	userID := testutil.InsertTestUser(t, ctx, pool)
	requestedByDeviceID := testutil.InsertTestDevice(t, ctx, pool, userID, "WEB", "Primary web")
	pairingCode := "PAIR1234"
	if _, err := pool.Exec(ctx, `
		INSERT INTO device_pairing_sessions (user_id, requested_by_device_id, pairing_code, expires_at, status)
		VALUES ($1::uuid, $2::uuid, $3, now() + interval '10 minute', 'PENDING')
	`, userID, requestedByDeviceID, pairingCode); err != nil {
		t.Fatalf("insert pairing session: %v", err)
	}

	store := replication.NewStore(pool, rdb)
	pubsub := rdb.Subscribe(ctx, store.ChannelForUser(userID))
	defer pubsub.Close()
	if _, err := pubsub.ReceiveTimeout(ctx, time.Second); err != nil {
		t.Fatalf("subscribe user channel: %v", err)
	}

	handler := newTestHandler(pool, rdb)
	req := httptest.NewRequest(http.MethodPost, "/v1/auth/pairing/complete", bytes.NewBufferString(`{
		"pairing_code":"PAIR1234",
		"device":{
			"platform":"WEB",
			"device_name":"Secondary web",
			"capabilities":["WEB_PUSH_V1","MINI_APPS"]
		}
	}`))
	rec := httptest.NewRecorder()

	handler.CompletePairing(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d: %s", rec.Code, rec.Body.String())
	}

	var payload struct {
		UserID       string `json:"user_id"`
		DeviceID     string `json:"device_id"`
		AccessToken  string `json:"access_token"`
		RefreshToken string `json:"refresh_token"`
	}
	if err := json.Unmarshal(rec.Body.Bytes(), &payload); err != nil {
		t.Fatalf("decode response: %v", err)
	}
	if payload.UserID != userID {
		t.Fatalf("expected user_id %q, got %q", userID, payload.UserID)
	}
	if payload.DeviceID == "" || payload.AccessToken == "" || payload.RefreshToken == "" {
		t.Fatalf("expected issued tokens and device_id, got %#v", payload)
	}

	var sessionStatus string
	var pairedDeviceID string
	if err := pool.QueryRow(ctx, `
		SELECT status, COALESCE(paired_device_id::text, '')
		FROM device_pairing_sessions
		WHERE pairing_code = $1
	`, pairingCode).Scan(&sessionStatus, &pairedDeviceID); err != nil {
		t.Fatalf("load pairing session after complete: %v", err)
	}
	if sessionStatus != "COMPLETED" {
		t.Fatalf("expected completed status, got %q", sessionStatus)
	}
	if pairedDeviceID != payload.DeviceID {
		t.Fatalf("expected paired_device_id %q, got %q", payload.DeviceID, pairedDeviceID)
	}

	var refreshTokenCount int
	if err := pool.QueryRow(ctx, `
		SELECT count(1)
		FROM refresh_tokens
		WHERE user_id = $1::uuid AND device_id = $2::uuid AND revoked_at IS NULL
	`, userID, payload.DeviceID).Scan(&refreshTokenCount); err != nil {
		t.Fatalf("count refresh tokens: %v", err)
	}
	if refreshTokenCount != 1 {
		t.Fatalf("expected 1 active refresh token for paired device, got %d", refreshTokenCount)
	}

	select {
	case published := <-pubsub.Channel():
		var evt replication.Event
		if err := json.Unmarshal([]byte(published.Payload), &evt); err != nil {
			t.Fatalf("decode user event: %v", err)
		}
		if evt.Type != replication.UserEventAccountDeviceLinked {
			t.Fatalf("expected user event %q, got %q", replication.UserEventAccountDeviceLinked, evt.Type)
		}
		if evt.Payload["paired_device_id"] != payload.DeviceID {
			t.Fatalf("expected paired_device_id %#v, got %#v", payload.DeviceID, evt.Payload["paired_device_id"])
		}
		if evt.Payload["requested_by_device_id"] != requestedByDeviceID {
			t.Fatalf("expected requested_by_device_id %#v, got %#v", requestedByDeviceID, evt.Payload["requested_by_device_id"])
		}
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for linked-device event publish")
	}
}

func newTestHandler(pool *pgxpool.Pool, rdb *redis.Client) *Handler {
	cfg := config.Config{
		AccessTTL:  15 * time.Minute,
		RefreshTTL: 24 * time.Hour,
	}
	return NewHandler(pool, rdb, token.NewService("test-secret"), otp.DevProvider{}, cfg.AccessTTL, cfg.RefreshTTL, cfg, nil, nil)
}

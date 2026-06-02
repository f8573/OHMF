package auth

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"os"
	"testing"
	"time"

	miniredis "github.com/alicebob/miniredis/v2"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/redis/go-redis/v9"
	"ohmf/services/gateway/internal/config"
	"ohmf/services/gateway/internal/otp"
	"ohmf/services/gateway/internal/testutil"
	"ohmf/services/gateway/internal/token"
)

type stubOTPProvider struct {
	name string
}

func (p stubOTPProvider) SendCode(context.Context, string, string) error { return nil }
func (p stubOTPProvider) Name() string                                   { return p.name }

func TestGenerateOTPCodeUsesFixedDevCode(t *testing.T) {
	svc := &Service{otp: otp.DevProvider{}}
	code, err := svc.generateOTPCode()
	if err != nil {
		t.Fatalf("generateOTPCode returned error: %v", err)
	}
	if code != "123456" {
		t.Fatalf("expected dev OTP 123456, got %q", code)
	}
}

func TestGenerateOTPCodeUsesRandomForNonDevProvider(t *testing.T) {
	svc := &Service{otp: stubOTPProvider{name: "twilio_sms"}}
	code, err := svc.generateOTPCode()
	if err != nil {
		t.Fatalf("generateOTPCode returned error: %v", err)
	}
	if len(code) != 6 {
		t.Fatalf("expected 6-digit OTP, got %q", code)
	}
	for _, ch := range code {
		if ch < '0' || ch > '9' {
			t.Fatalf("expected numeric OTP, got %q", code)
		}
	}
}

func TestNormalizeRemoteAddrStripsPortAndBrackets(t *testing.T) {
	cases := map[string]string{
		"203.0.113.10:443":       "203.0.113.10",
		"[2001:db8::1]:8080":      "2001:db8::1",
		"2001:db8::1":             "2001:db8::1",
		"   198.51.100.4:12345  ": "198.51.100.4",
	}
	for input, want := range cases {
		if got := normalizeRemoteAddr(input); got != want {
			t.Fatalf("normalizeRemoteAddr(%q) = %q, want %q", input, got, want)
		}
	}
}

func TestAllowRateUsesRedisCounters(t *testing.T) {
	mr, err := miniredis.Run()
	if err != nil {
		t.Fatalf("start miniredis: %v", err)
	}
	defer mr.Close()

	rdb := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	defer rdb.Close()

	h := &Handler{redis: rdb}
	ctx := context.Background()

	allowed, err := h.allowRate(ctx, "otp:test:bucket", 1, time.Minute)
	if err != nil {
		t.Fatalf("first allowRate call: %v", err)
	}
	if !allowed {
		t.Fatalf("expected first rate-limit check to pass")
	}

	allowed, err = h.allowRate(ctx, "otp:test:bucket", 1, time.Minute)
	if err != nil {
		t.Fatalf("second allowRate call: %v", err)
	}
	if allowed {
		t.Fatalf("expected second rate-limit check to fail")
	}
}

func TestRefreshRotatesToken(t *testing.T) {
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

	// apply baseline migrations (idempotent)
	mig := testutil.ReadGatewayMigration(t, "000001_init.up.sql")
	if _, err := pool.Exec(ctx, string(mig)); err != nil {
		t.Fatalf("apply migration: %v", err)
	}

	// create user
	var userID string
	if err := pool.QueryRow(ctx, `INSERT INTO users (primary_phone_e164) VALUES ($1) ON CONFLICT DO NOTHING RETURNING id::text`, "+10000000000").Scan(&userID); err != nil {
		// if returning fails because row existed, select it
		if err == sql.ErrNoRows {
			if err := pool.QueryRow(ctx, `SELECT id::text FROM users WHERE primary_phone_e164 = $1`, "+10000000000").Scan(&userID); err != nil {
				t.Fatalf("select user: %v", err)
			}
		} else {
			t.Fatalf("insert user: %v", err)
		}
	}

	// create device
	var deviceID string
	if err := pool.QueryRow(ctx, `INSERT INTO devices (user_id, platform, device_name, created_at) VALUES ($1, 'TEST', 'ci-device', now()) RETURNING id::text`, userID).Scan(&deviceID); err != nil {
		t.Fatalf("insert device: %v", err)
	}

	// insert a refresh token for "oldtoken"
	old := "oldtoken"
	h := sha256.Sum256([]byte(old))
	oldHash := hex.EncodeToString(h[:])
	if _, err := pool.Exec(ctx, `INSERT INTO refresh_tokens (user_id, device_id, token_hash, expires_at) VALUES ($1, $2::uuid, $3, now() + interval '1 hour')`, userID, deviceID, oldHash); err != nil {
		t.Fatalf("insert refresh token: %v", err)
	}

	tokSvc := token.NewService("test-secret")
	svc := NewService(pool, nil, tokSvc, otp.DevProvider{}, 15*time.Minute, 30*24*time.Hour, config.Config{})

	out, err := svc.Refresh(ctx, RefreshRequest{RefreshToken: old})
	if err != nil {
		t.Fatalf("refresh failed: %v", err)
	}
	if out["access_token"] == nil || out["refresh_token"] == nil {
		t.Fatalf("expected access and refresh tokens; got %#v", out)
	}

	// verify old token was revoked
	var revoked sql.NullTime
	if err := pool.QueryRow(ctx, `SELECT revoked_at FROM refresh_tokens WHERE token_hash = $1`, oldHash).Scan(&revoked); err != nil {
		t.Fatalf("query old token: %v", err)
	}
	if !revoked.Valid {
		t.Fatalf("expected old token revoked_at to be set")
	}

	// call Logout to revoke all for device
	if err := svc.Logout(ctx, userID, LogoutRequest{DeviceID: deviceID}); err != nil {
		t.Fatalf("logout failed: %v", err)
	}

	// ensure any remaining tokens for user are revoked
	var cnt int
	if err := pool.QueryRow(ctx, `SELECT count(1) FROM refresh_tokens WHERE user_id = $1 AND revoked_at IS NULL`, userID).Scan(&cnt); err != nil {
		t.Fatalf("count tokens: %v", err)
	}
	if cnt != 0 {
		t.Fatalf("expected 0 active tokens after logout; got %d", cnt)
	}
}


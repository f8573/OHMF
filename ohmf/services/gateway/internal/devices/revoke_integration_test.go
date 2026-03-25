package devices

import (
	"context"
	"testing"

	"github.com/jackc/pgx/v5/pgxpool"
	"ohmf/services/gateway/internal/testutil"
)

func TestRevokeDeviceDeletesOnlyTargetAndLeavesCurrentDeviceActive(t *testing.T) {
	ctx, pool := testutil.OpenAndMigrateGatewayPool(t)
	defer pool.Close()

	userID := testutil.InsertTestUser(t, ctx, pool)
	otherUserID := testutil.InsertTestUser(t, ctx, pool)
	currentDeviceID := testutil.InsertTestDevice(t, ctx, pool, userID, "WEB", "Current web")
	linkedDeviceID := testutil.InsertTestDevice(t, ctx, pool, userID, "IOS", "Linked iPhone")
	otherDeviceID := testutil.InsertTestDevice(t, ctx, pool, otherUserID, "WEB", "Other web")

	insertActiveRefreshToken(t, ctx, pool, userID, currentDeviceID, "current-token")
	insertActiveRefreshToken(t, ctx, pool, userID, linkedDeviceID, "linked-token")

	svc := NewService(pool, nil, nil, 0)
	if err := svc.RevokeDevice(ctx, userID, linkedDeviceID); err != nil {
		t.Fatalf("revoke linked device: %v", err)
	}

	var remainingForUser []string
	rows, err := pool.Query(ctx, `SELECT id::text FROM devices WHERE user_id = $1::uuid ORDER BY id::text`, userID)
	if err != nil {
		t.Fatalf("list remaining devices: %v", err)
	}
	defer rows.Close()
	for rows.Next() {
		var deviceID string
		if err := rows.Scan(&deviceID); err != nil {
			t.Fatalf("scan device: %v", err)
		}
		remainingForUser = append(remainingForUser, deviceID)
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("iterate devices: %v", err)
	}
	if len(remainingForUser) != 1 || remainingForUser[0] != currentDeviceID {
		t.Fatalf("expected only current device to remain, got %#v", remainingForUser)
	}

	var otherCount int
	if err := pool.QueryRow(ctx, `SELECT count(1) FROM devices WHERE id = $1::uuid AND user_id = $2::uuid`, otherDeviceID, otherUserID).Scan(&otherCount); err != nil {
		t.Fatalf("count other user's device: %v", err)
	}
	if otherCount != 1 {
		t.Fatalf("expected other user's device to remain, got count=%d", otherCount)
	}

	var currentRefreshCount int
	if err := pool.QueryRow(ctx, `
		SELECT count(1)
		FROM refresh_tokens
		WHERE user_id = $1::uuid AND device_id = $2::uuid AND revoked_at IS NULL
	`, userID, currentDeviceID).Scan(&currentRefreshCount); err != nil {
		t.Fatalf("count current device refresh tokens: %v", err)
	}
	if currentRefreshCount != 1 {
		t.Fatalf("expected current device refresh token to remain active, got %d", currentRefreshCount)
	}
}

func insertActiveRefreshToken(t *testing.T, ctx context.Context, pool *pgxpool.Pool, userID, deviceID, tokenHash string) {
	t.Helper()

	if _, err := pool.Exec(ctx, `
		INSERT INTO refresh_tokens (user_id, device_id, token_hash, expires_at)
		VALUES ($1::uuid, $2::uuid, $3, now() + interval '1 day')
	`, userID, deviceID, tokenHash); err != nil {
		t.Fatalf("insert refresh token for %q: %v", deviceID, err)
	}
}

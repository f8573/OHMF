package miniapp

import (
	"context"
	"testing"

	"github.com/jackc/pgx/v5/pgxpool"
	"ohmf/services/gateway/internal/testutil"
)

func applyAllMigrations(t *testing.T, ctx context.Context, pool *pgxpool.Pool) {
	t.Helper()
	testutil.ResetAndMigrateGateway(t, ctx, pool)
}

func insertTestUser(t *testing.T, ctx context.Context, pool *pgxpool.Pool) string {
	t.Helper()
	return testutil.InsertTestUser(t, ctx, pool)
}

func insertMiniappCapableDevice(t *testing.T, ctx context.Context, pool *pgxpool.Pool, userID string) {
	t.Helper()
	testutil.InsertTestDevice(t, ctx, pool, userID, "WEB", "OHMF Web", "MINI_APPS")
}

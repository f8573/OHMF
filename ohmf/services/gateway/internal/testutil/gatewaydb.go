package testutil

import (
	"context"
	"os"
	"path/filepath"
	"sort"
	"testing"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
)

func OpenAndMigrateGatewayPool(t *testing.T) (context.Context, *pgxpool.Pool) {
	t.Helper()

	dsn := os.Getenv("TEST_DATABASE_URL")
	if dsn == "" {
		t.Skip("skipping DB integration test; set TEST_DATABASE_URL to run")
	}

	ctx := context.Background()
	pool, err := pgxpool.New(ctx, dsn)
	if err != nil {
		t.Fatalf("connect: %v", err)
	}

	ResetAndMigrateGateway(t, ctx, pool)
	return ctx, pool
}

func ResetAndMigrateGateway(t *testing.T, ctx context.Context, pool *pgxpool.Pool) {
	t.Helper()

	if _, err := pool.Exec(ctx, `DROP SCHEMA IF EXISTS public CASCADE; CREATE SCHEMA public;`); err != nil {
		t.Fatalf("reset public schema: %v", err)
	}

	patterns := []string{
		filepath.Join("..", "..", "migrations", "*.up.sql"),
		filepath.Join("..", "migrations", "*.up.sql"),
		filepath.Join("migrations", "*.up.sql"),
	}

	var paths []string
	for _, pattern := range patterns {
		matches, err := filepath.Glob(pattern)
		if err != nil {
			t.Fatalf("glob migrations %q: %v", pattern, err)
		}
		if len(matches) > 0 {
			paths = matches
			break
		}
	}
	if len(paths) == 0 {
		t.Fatal("no gateway migrations found")
	}

	sort.Strings(paths)
	for _, path := range paths {
		body, err := os.ReadFile(path)
		if err != nil {
			t.Fatalf("read migration %q: %v", path, err)
		}
		if _, err := pool.Exec(ctx, string(body)); err != nil {
			t.Fatalf("apply migration %q: %v", path, err)
		}
	}
}

func InsertTestUser(t *testing.T, ctx context.Context, pool *pgxpool.Pool) string {
	t.Helper()

	var userID string
	phone := "+test-" + uuid.NewString()
	if err := pool.QueryRow(ctx, `INSERT INTO users (primary_phone_e164) VALUES ($1) RETURNING id::text`, phone).Scan(&userID); err != nil {
		t.Fatalf("insert user %q: %v", phone, err)
	}
	return userID
}

func InsertTestDevice(t *testing.T, ctx context.Context, pool *pgxpool.Pool, userID, platform, name string, capabilities ...string) string {
	t.Helper()

	var deviceID string
	if err := pool.QueryRow(ctx, `
		INSERT INTO devices (user_id, platform, device_name, capabilities, last_seen_at)
		VALUES ($1::uuid, $2, $3, $4::text[], now())
		RETURNING id::text
	`, userID, platform, name, capabilities).Scan(&deviceID); err != nil {
		t.Fatalf("insert device %q: %v", name, err)
	}
	return deviceID
}

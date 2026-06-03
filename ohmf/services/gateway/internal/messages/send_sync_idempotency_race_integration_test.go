package messages

import (
	"context"
	"os"
	"sync"
	"testing"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
)

func TestSendSyncConcurrentSameIdempotencyKeyCreatesOneMessage(t *testing.T) {
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

	senderID := insertTestUser(t, ctx, pool)
	recipientID := insertTestUser(t, ctx, pool)
	conversationID := uuid.NewString()
	if _, err := pool.Exec(ctx, `
		INSERT INTO conversations (id, type)
		VALUES ($1::uuid, 'DM')
	`, conversationID); err != nil {
		t.Fatalf("insert conversation: %v", err)
	}
	if _, err := pool.Exec(ctx, `
		INSERT INTO conversation_members (conversation_id, user_id)
		VALUES ($1::uuid, $2::uuid), ($1::uuid, $3::uuid)
	`, conversationID, senderID, recipientID); err != nil {
		t.Fatalf("insert members: %v", err)
	}
	if _, err := pool.Exec(ctx, `
		INSERT INTO conversation_counters (conversation_id, next_server_order)
		VALUES ($1::uuid, 1)
	`, conversationID); err != nil {
		t.Fatalf("insert counter: %v", err)
	}

	svc := NewService(pool, Options{})
	idemKey := "idem-" + uuid.NewString()
	content := map[string]any{"text": "race-check"}

	start := make(chan struct{})
	results := make(chan Message, 2)
	errs := make(chan error, 2)
	var wg sync.WaitGroup
	for i := 0; i < 2; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			msg, err := svc.sendSync(ctx, senderID, "", conversationID, idemKey, "text", content, "")
			if err != nil {
				errs <- err
				return
			}
			results <- msg
		}()
	}
	close(start)
	wg.Wait()
	close(results)
	close(errs)

	for err := range errs {
		if err != nil {
			t.Fatalf("sendSync returned error: %v", err)
		}
	}

	got := make([]Message, 0, 2)
	for msg := range results {
		got = append(got, msg)
	}
	if len(got) != 2 {
		t.Fatalf("expected 2 successful callers, got %d", len(got))
	}
	if got[0].MessageID != got[1].MessageID {
		t.Fatalf("expected same canonical message_id, got %q and %q", got[0].MessageID, got[1].MessageID)
	}
	if got[0].ServerOrder != got[1].ServerOrder {
		t.Fatalf("expected same canonical server_order, got %d and %d", got[0].ServerOrder, got[1].ServerOrder)
	}

	var messageCount int
	if err := pool.QueryRow(ctx, `
		SELECT COUNT(1)
		FROM messages
		WHERE conversation_id = $1::uuid
	`, conversationID).Scan(&messageCount); err != nil {
		t.Fatalf("count messages: %v", err)
	}
	if messageCount != 1 {
		t.Fatalf("expected exactly 1 persisted message, got %d", messageCount)
	}

	var nextServerOrder int64
	if err := pool.QueryRow(ctx, `
		SELECT next_server_order
		FROM conversation_counters
		WHERE conversation_id = $1::uuid
	`, conversationID).Scan(&nextServerOrder); err != nil {
		t.Fatalf("load next_server_order: %v", err)
	}
	if nextServerOrder != 2 {
		t.Fatalf("expected next_server_order=2 after one persisted send, got %d", nextServerOrder)
	}
}

func TestSendToPhoneSyncConcurrentSameIdempotencyKeyCreatesOneMessage(t *testing.T) {
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

	senderID := insertTestUser(t, ctx, pool)
	svc := NewService(pool, Options{})
	idemKey := "idem-phone-" + uuid.NewString()
	content := map[string]any{"text": "race-check-phone"}

	start := make(chan struct{})
	results := make(chan Message, 2)
	errs := make(chan error, 2)
	var wg sync.WaitGroup
	for i := 0; i < 2; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			msg, err := svc.sendToPhoneSync(ctx, senderID, "", "+15550123000", idemKey, "text", content, "")
			if err != nil {
				errs <- err
				return
			}
			results <- msg
		}()
	}
	close(start)
	wg.Wait()
	close(results)
	close(errs)

	for err := range errs {
		if err != nil {
			t.Fatalf("sendToPhoneSync returned error: %v", err)
		}
	}

	got := make([]Message, 0, 2)
	for msg := range results {
		got = append(got, msg)
	}
	if len(got) != 2 {
		t.Fatalf("expected 2 successful callers, got %d", len(got))
	}
	if got[0].MessageID != got[1].MessageID {
		t.Fatalf("expected same canonical message_id, got %q and %q", got[0].MessageID, got[1].MessageID)
	}
	if got[0].ServerOrder != got[1].ServerOrder {
		t.Fatalf("expected same canonical server_order, got %d and %d", got[0].ServerOrder, got[1].ServerOrder)
	}

	var messageCount int
	if err := pool.QueryRow(ctx, `
		SELECT COUNT(1)
		FROM messages
		WHERE conversation_id = $1::uuid
	`, got[0].ConversationID).Scan(&messageCount); err != nil {
		t.Fatalf("count messages: %v", err)
	}
	if messageCount != 1 {
		t.Fatalf("expected exactly 1 persisted message, got %d", messageCount)
	}

	var nextServerOrder int64
	if err := pool.QueryRow(ctx, `
		SELECT next_server_order
		FROM conversation_counters
		WHERE conversation_id = $1::uuid
	`, got[0].ConversationID).Scan(&nextServerOrder); err != nil {
		t.Fatalf("load next_server_order: %v", err)
	}
	if nextServerOrder != 2 {
		t.Fatalf("expected next_server_order=2 after one persisted send, got %d", nextServerOrder)
	}
}

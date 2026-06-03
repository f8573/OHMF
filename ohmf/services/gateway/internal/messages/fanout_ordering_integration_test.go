package messages

import (
	"context"
	"fmt"
	"os"
	"sync"
	"testing"

	miniredis "github.com/alicebob/miniredis/v2"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/redis/go-redis/v9"
	"ohmf/services/gateway/internal/replication"
)

func TestConcurrentFanoutWorkersPreserveConversationOrderInUserInbox(t *testing.T) {
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

	mr, err := miniredis.Run()
	if err != nil {
		t.Fatalf("start miniredis: %v", err)
	}
	defer mr.Close()

	rdb := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	defer rdb.Close()

	store := replication.NewStore(pool, rdb)
	svc := NewService(pool, Options{Redis: rdb, Replication: store})

	senderID := insertTestUser(t, ctx, pool)
	recipientID := insertTestUser(t, ctx, pool)
	conversationID := uuid.NewString()

	if _, err := pool.Exec(ctx, `INSERT INTO conversations (id, type) VALUES ($1, 'PRIVATE')`, conversationID); err != nil {
		t.Fatalf("insert conversation: %v", err)
	}
	if _, err := pool.Exec(ctx, `INSERT INTO conversation_counters (conversation_id) VALUES ($1)`, conversationID); err != nil {
		t.Fatalf("insert counter: %v", err)
	}
	if _, err := pool.Exec(ctx, `INSERT INTO conversation_members (conversation_id, user_id) VALUES ($1, $2::uuid), ($1, $3::uuid)`, conversationID, senderID, recipientID); err != nil {
		t.Fatalf("insert members: %v", err)
	}

	const messageCount = 40
	for i := 1; i <= messageCount; i++ {
		_, err := svc.Send(ctx, senderID, "", conversationID, uuid.NewString(), "text", map[string]any{"text": fmt.Sprintf("m-%03d", i)}, "", fmt.Sprintf("trace-fanout-order-%03d", i), "127.0.0.1")
		if err != nil {
			t.Fatalf("send message %d: %v", i, err)
		}
	}

	const workers = 4
	var wg sync.WaitGroup
	errCh := make(chan error, workers)

	for i := 0; i < workers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				processed, err := store.ProcessBatch(ctx, 3)
				if err != nil {
					errCh <- err
					return
				}
				if processed == 0 {
					return
				}
			}
		}()
	}

	wg.Wait()
	close(errCh)
	for err := range errCh {
		if err != nil {
			t.Fatalf("concurrent ProcessBatch failed: %v", err)
		}
	}

	resp, err := store.ListEvents(ctx, recipientID, 0, 1000)
	if err != nil {
		t.Fatalf("list recipient events: %v", err)
	}

	orders := make([]int64, 0, messageCount)
	for _, evt := range resp.Events {
		if evt.Type != replication.UserEventConversationMessageAppended {
			continue
		}
		cid, _ := evt.Payload["conversation_id"].(string)
		if cid != conversationID {
			continue
		}
		val, ok := evt.Payload["server_order"]
		if !ok {
			t.Fatalf("missing server_order in payload for user_event_id=%d", evt.UserEventID)
		}
		order, ok := val.(float64)
		if !ok {
			t.Fatalf("server_order has unexpected type %T for user_event_id=%d", val, evt.UserEventID)
		}
		orders = append(orders, int64(order))
	}

	if len(orders) != messageCount {
		t.Fatalf("expected %d message_appended events for recipient, got %d", messageCount, len(orders))
	}

	for i := 1; i < len(orders); i++ {
		if orders[i] < orders[i-1] {
			t.Fatalf("non-monotonic server_order in client-visible inbox stream: index=%d prev=%d curr=%d all=%v", i, orders[i-1], orders[i], orders)
		}
	}
}

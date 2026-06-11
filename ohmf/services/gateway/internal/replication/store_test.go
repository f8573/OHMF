package replication

import (
	"context"
	"testing"
	"time"

	pgxmock "github.com/pashagolub/pgxmock/v4"
)

func TestAppendTypingEventWritesDomainEvent(t *testing.T) {
	mock, err := pgxmock.NewPool()
	if err != nil {
		t.Fatal(err)
	}
	defer mock.Close()

	store := NewStore(mock, nil)
	now := time.Date(2026, 3, 20, 12, 30, 0, 0, time.UTC)
	mock.ExpectExec(`INSERT INTO domain_events \(conversation_id, actor_user_id, event_type, payload\)`).
		WithArgs("conversation-1", "user-1", DomainEventTypingStarted, pgxmock.AnyArg()).
		WillReturnResult(pgxmock.NewResult("INSERT", 1))
	mock.ExpectExec(`SELECT pg_notify\(\$1, \$2\)`).
		WithArgs("ohmf_domain_events", "conversation-1").
		WillReturnResult(pgxmock.NewResult("SELECT", 1))

	if err := store.AppendTypingEvent(context.Background(), "conversation-1", "user-1", "device-1", "typing_started", now); err != nil {
		t.Fatalf("AppendTypingEvent failed: %v", err)
	}

	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("unmet expectations: %v", err)
	}
}

func TestAppendMessageEffectEventWritesDomainEvent(t *testing.T) {
	mock, err := pgxmock.NewPool()
	if err != nil {
		t.Fatal(err)
	}
	defer mock.Close()

	store := NewStore(mock, nil)
	now := time.Date(2026, 3, 20, 12, 45, 0, 0, time.UTC)
	mock.ExpectExec(`INSERT INTO domain_events \(conversation_id, actor_user_id, event_type, payload\)`).
		WithArgs("conversation-1", "user-1", DomainEventMessageEffectTriggered, pgxmock.AnyArg()).
		WillReturnResult(pgxmock.NewResult("INSERT", 1))
	mock.ExpectExec(`SELECT pg_notify\(\$1, \$2\)`).
		WithArgs("ohmf_domain_events", "conversation-1").
		WillReturnResult(pgxmock.NewResult("SELECT", 1))

	if err := store.AppendMessageEffectEvent(context.Background(), mock, "conversation-1", "user-1", "message-1", "bubble_confetti", now); err != nil {
		t.Fatalf("AppendMessageEffectEvent failed: %v", err)
	}

	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("unmet expectations: %v", err)
	}
}

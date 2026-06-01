package messages

import (
	"context"
	"errors"
	"testing"

	"github.com/jackc/pgx/v5"
	pgxmock "github.com/pashagolub/pgxmock/v4"
)

func TestSendValidationParitySyncAndAsync(t *testing.T) {
	type mode struct {
		name     string
		useAsync bool
	}
	modes := []mode{
		{name: "sync", useAsync: false},
		{name: "async", useAsync: true},
	}

	for _, m := range modes {
		t.Run(m.name+"/blocked_recipient", func(t *testing.T) {
			mock, err := pgxmock.NewPool()
			if err != nil {
				t.Fatal(err)
			}
			defer mock.Close()

			svc := &Service{db: mock, useKafkaSend: m.useAsync}
			if m.useAsync {
				svc.async = &AsyncPipeline{}
			} else {
				mock.ExpectBegin()
			}
			expectMembershipOK(mock)
			if !m.useAsync {
				expectIdempotencyClaim(mock, "/v1/messages", "idem-1")
			}
			expectBlockedByRecipient(mock)
			if !m.useAsync {
				mock.ExpectRollback()
			}

			_, err = svc.Send(context.Background(), "user-1", "device-1", "conversation-1", "idem-1", "text", map[string]any{"text": "hi"}, "", "", "")
			if !errors.Is(err, ErrConversationBlocked) {
				t.Fatalf("expected ErrConversationBlocked, got %v", err)
			}
			if err := mock.ExpectationsWereMet(); err != nil {
				t.Fatalf("unmet expectations: %v", err)
			}
		})

		t.Run(m.name+"/encryption_policy", func(t *testing.T) {
			mock, err := pgxmock.NewPool()
			if err != nil {
				t.Fatal(err)
			}
			defer mock.Close()

			svc := &Service{db: mock, useKafkaSend: m.useAsync}
			if m.useAsync {
				svc.async = &AsyncPipeline{}
			} else {
				mock.ExpectBegin()
			}
			expectMembershipOK(mock)
			if !m.useAsync {
				expectIdempotencyClaim(mock, "/v1/messages", "idem-1")
			}
			expectUnblocked(mock)
			mock.ExpectQuery(`SELECT type, COALESCE\(encryption_state, 'PLAINTEXT'\), COALESCE\(is_mls_encrypted, false\) FROM conversations WHERE id = \$1::uuid`).
				WithArgs("conversation-1").
				WillReturnRows(pgxmock.NewRows([]string{"type", "encryption_state", "is_mls_encrypted"}).AddRow("DM", "ENCRYPTED", false))
			mock.ExpectQuery(`SELECT EXISTS\(`).
				WithArgs("device-1", "user-1").
				WillReturnRows(pgxmock.NewRows([]string{"exists"}).AddRow(true))
			if !m.useAsync {
				mock.ExpectRollback()
			}

			_, err = svc.Send(context.Background(), "user-1", "device-1", "conversation-1", "idem-1", "text", map[string]any{"text": "hi"}, "", "", "")
			if !errors.Is(err, ErrEncryptedMessageRequired) {
				t.Fatalf("expected ErrEncryptedMessageRequired, got %v", err)
			}
			if err := mock.ExpectationsWereMet(); err != nil {
				t.Fatalf("unmet expectations: %v", err)
			}
		})

		t.Run(m.name+"/sender_device_ownership", func(t *testing.T) {
			mock, err := pgxmock.NewPool()
			if err != nil {
				t.Fatal(err)
			}
			defer mock.Close()

			svc := &Service{db: mock, useKafkaSend: m.useAsync}
			if m.useAsync {
				svc.async = &AsyncPipeline{}
			} else {
				mock.ExpectBegin()
			}
			expectMembershipOK(mock)
			if !m.useAsync {
				expectIdempotencyClaim(mock, "/v1/messages", "idem-1")
			}
			expectUnblocked(mock)
			mock.ExpectQuery(`SELECT type, COALESCE\(encryption_state, 'PLAINTEXT'\), COALESCE\(is_mls_encrypted, false\) FROM conversations WHERE id = \$1::uuid`).
				WithArgs("conversation-1").
				WillReturnRows(pgxmock.NewRows([]string{"type", "encryption_state", "is_mls_encrypted"}).AddRow("GROUP", "ENCRYPTED", false))
			mock.ExpectQuery(`SELECT EXISTS\(`).
				WithArgs("device-1", "user-1").
				WillReturnRows(pgxmock.NewRows([]string{"exists"}).AddRow(false))
			if !m.useAsync {
				mock.ExpectRollback()
			}

			_, err = svc.Send(context.Background(), "user-1", "device-1", "conversation-1", "idem-1", "encrypted", map[string]any{"ciphertext": "x"}, "", "", "")
			if !errors.Is(err, ErrSenderDeviceInvalid) {
				t.Fatalf("expected ErrSenderDeviceInvalid, got %v", err)
			}
			if err := mock.ExpectationsWereMet(); err != nil {
				t.Fatalf("unmet expectations: %v", err)
			}
		})

		t.Run(m.name+"/reply_target", func(t *testing.T) {
			mock, err := pgxmock.NewPool()
			if err != nil {
				t.Fatal(err)
			}
			defer mock.Close()

			svc := &Service{db: mock, useKafkaSend: m.useAsync}
			if m.useAsync {
				svc.async = &AsyncPipeline{}
			} else {
				mock.ExpectBegin()
			}
			expectMembershipOK(mock)
			if !m.useAsync {
				expectIdempotencyClaim(mock, "/v1/messages", "idem-1")
			}
			expectUnblocked(mock)
			mock.ExpectQuery(`SELECT type, COALESCE\(encryption_state, 'PLAINTEXT'\), COALESCE\(is_mls_encrypted, false\) FROM conversations WHERE id = \$1::uuid`).
				WithArgs("conversation-1").
				WillReturnRows(pgxmock.NewRows([]string{"type", "encryption_state", "is_mls_encrypted"}).AddRow("GROUP", "PLAINTEXT", false))
			mock.ExpectQuery(`SELECT EXISTS\(`).
				WithArgs("reply-404", "conversation-1").
				WillReturnRows(pgxmock.NewRows([]string{"exists"}).AddRow(false))
			if !m.useAsync {
				mock.ExpectRollback()
			}

			_, err = svc.Send(context.Background(), "user-1", "device-1", "conversation-1", "idem-1", "text", map[string]any{"text": "hi", "reply_to_message_id": "reply-404"}, "", "", "")
			if err == nil || err.Error() != "reply_target_not_found" {
				t.Fatalf("expected reply_target_not_found, got %v", err)
			}
			if err := mock.ExpectationsWereMet(); err != nil {
				t.Fatalf("unmet expectations: %v", err)
			}
		})
	}
}

func expectMembershipOK(mock pgxmock.PgxPoolIface) {
	mock.ExpectQuery(`SELECT 1\s+FROM conversation_members\s+WHERE conversation_id = \$1::uuid AND user_id = \$2::uuid`).
		WithArgs("conversation-1", "user-1").
		WillReturnRows(pgxmock.NewRows([]string{"one"}).AddRow(1))
}

func expectUnblocked(mock pgxmock.PgxPoolIface) {
	mock.ExpectQuery(`SELECT user_id::text FROM conversation_members WHERE conversation_id = \$1::uuid AND user_id <> \$2::uuid`).
		WithArgs("conversation-1", "user-1").
		WillReturnRows(pgxmock.NewRows([]string{"user_id"}).AddRow("user-2"))
	mock.ExpectQuery(`SELECT 1 FROM user_blocks WHERE blocker_user_id = \$1::uuid AND blocked_user_id = \$2::uuid`).
		WithArgs("user-2", "user-1").
		WillReturnError(pgx.ErrNoRows)
	mock.ExpectQuery(`SELECT 1 FROM user_blocks WHERE blocker_user_id = \$1::uuid AND blocked_user_id = \$2::uuid`).
		WithArgs("user-1", "user-2").
		WillReturnError(pgx.ErrNoRows)
}

func expectBlockedByRecipient(mock pgxmock.PgxPoolIface) {
	mock.ExpectQuery(`SELECT user_id::text FROM conversation_members WHERE conversation_id = \$1::uuid AND user_id <> \$2::uuid`).
		WithArgs("conversation-1", "user-1").
		WillReturnRows(pgxmock.NewRows([]string{"user_id"}).AddRow("user-2"))
	mock.ExpectQuery(`SELECT 1 FROM user_blocks WHERE blocker_user_id = \$1::uuid AND blocked_user_id = \$2::uuid`).
		WithArgs("user-2", "user-1").
		WillReturnRows(pgxmock.NewRows([]string{"one"}).AddRow(1))
}

func expectIdempotencyClaim(mock pgxmock.PgxPoolIface, endpoint, idemKey string) {
	mock.ExpectQuery(`INSERT INTO idempotency_keys \(actor_user_id, endpoint, key, status_code, expires_at\)`).
		WithArgs("user-1", endpoint, idemKey).
		WillReturnRows(pgxmock.NewRows([]string{"?column?"}).AddRow(1))
}

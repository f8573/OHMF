package messages

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	pgxmock "github.com/pashagolub/pgxmock/v4"
	"ohmf/services/gateway/internal/e2ee"
	"ohmf/services/gateway/internal/replication"
)

func TestValidateEncryptedConversationEnvelopeRejectsEpochMismatch(t *testing.T) {
	mock, err := pgxmock.NewPool()
	if err != nil {
		t.Fatal(err)
	}
	defer mock.Close()

	svc := &Service{db: mock}
	mock.ExpectQuery(`SELECT type,\s+COALESCE\(encryption_state, 'PLAINTEXT'\),\s+COALESCE\(is_mls_encrypted, false\),\s+COALESCE\(encryption_epoch, 0\),\s+COALESCE\(mls_epoch, 0\)`).
		WithArgs("conversation-1").
		WillReturnRows(pgxmock.NewRows([]string{"type", "encryption_state", "is_mls_encrypted", "encryption_epoch", "mls_epoch"}).AddRow("GROUP", "ENCRYPTED", false, int64(7), int64(0)))

	err = svc.validateEncryptedConversationEnvelope(context.Background(), mock, "conversation-1", &EncryptedMessageMetadata{
		ConversationEpoch: 6,
		Recipients: []RecipientKeyInfo{
			{UserID: "user-1", DeviceID: "device-1"},
		},
	})
	if !errors.Is(err, ErrEncryptedConversationStateChanged) {
		t.Fatalf("expected ErrEncryptedConversationStateChanged, got %v", err)
	}

	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("unmet expectations: %v", err)
	}
}

func TestValidateEncryptedConversationEnvelopeRejectsRecipientMismatch(t *testing.T) {
	mock, err := pgxmock.NewPool()
	if err != nil {
		t.Fatal(err)
	}
	defer mock.Close()

	svc := &Service{db: mock}
	mock.ExpectQuery(`SELECT type,\s+COALESCE\(encryption_state, 'PLAINTEXT'\),\s+COALESCE\(is_mls_encrypted, false\),\s+COALESCE\(encryption_epoch, 0\),\s+COALESCE\(mls_epoch, 0\)`).
		WithArgs("conversation-1").
		WillReturnRows(pgxmock.NewRows([]string{"type", "encryption_state", "is_mls_encrypted", "encryption_epoch", "mls_epoch"}).AddRow("GROUP", "ENCRYPTED", false, int64(3), int64(0)))
	mock.ExpectQuery(`SELECT user_id::text FROM conversation_members WHERE conversation_id = \$1::uuid`).
		WithArgs("conversation-1").
		WillReturnRows(pgxmock.NewRows([]string{"user_id"}).AddRow("user-1").AddRow("user-2"))
	mock.ExpectQuery(`SELECT cm.user_id::text, d.id::text, COALESCE\(dik.agreement_identity_public_key, ''\)`).
		WithArgs("conversation-1").
		WillReturnRows(pgxmock.NewRows([]string{"user_id", "device_id", "agreement_identity_public_key"}).
			AddRow("user-1", "device-1", "pub-1").
			AddRow("user-2", "device-2", "pub-2"))

	err = svc.validateEncryptedConversationEnvelope(context.Background(), mock, "conversation-1", &EncryptedMessageMetadata{
		Scheme:            SignalEncryptionScheme,
		ConversationEpoch: 3,
		Recipients: []RecipientKeyInfo{
			{UserID: "user-1", DeviceID: "device-1"},
		},
	})
	if !errors.Is(err, ErrEncryptedConversationStateChanged) {
		t.Fatalf("expected ErrEncryptedConversationStateChanged, got %v", err)
	}

	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("unmet expectations: %v", err)
	}
}

func TestValidateEncryptedConversationEnvelopeAcceptsExactRecipientSet(t *testing.T) {
	mock, err := pgxmock.NewPool()
	if err != nil {
		t.Fatal(err)
	}
	defer mock.Close()

	svc := &Service{db: mock}
	mock.ExpectQuery(`SELECT type,\s+COALESCE\(encryption_state, 'PLAINTEXT'\),\s+COALESCE\(is_mls_encrypted, false\),\s+COALESCE\(encryption_epoch, 0\),\s+COALESCE\(mls_epoch, 0\)`).
		WithArgs("conversation-1").
		WillReturnRows(pgxmock.NewRows([]string{"type", "encryption_state", "is_mls_encrypted", "encryption_epoch", "mls_epoch"}).AddRow("GROUP", "ENCRYPTED", false, int64(5), int64(0)))
	mock.ExpectQuery(`SELECT user_id::text FROM conversation_members WHERE conversation_id = \$1::uuid`).
		WithArgs("conversation-1").
		WillReturnRows(pgxmock.NewRows([]string{"user_id"}).AddRow("user-1").AddRow("user-2"))
	mock.ExpectQuery(`SELECT cm.user_id::text, d.id::text, COALESCE\(dik.agreement_identity_public_key, ''\)`).
		WithArgs("conversation-1").
		WillReturnRows(pgxmock.NewRows([]string{"user_id", "device_id", "agreement_identity_public_key"}).
			AddRow("user-1", "device-1", "pub-1").
			AddRow("user-2", "device-2", "pub-2"))

	err = svc.validateEncryptedConversationEnvelope(context.Background(), mock, "conversation-1", &EncryptedMessageMetadata{
		Scheme:            SignalEncryptionScheme,
		ConversationEpoch: 5,
		Recipients: []RecipientKeyInfo{
			{UserID: "user-1", DeviceID: "device-1"},
			{UserID: "user-2", DeviceID: "device-2"},
		},
	})
	if err != nil {
		t.Fatalf("validateEncryptedConversationEnvelope failed: %v", err)
	}

	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("unmet expectations: %v", err)
	}
}

func TestValidateEncryptedConversationEnvelopeAcceptsMLSBoxes(t *testing.T) {
	mock, err := pgxmock.NewPool()
	if err != nil {
		t.Fatal(err)
	}
	defer mock.Close()

	svc := &Service{db: mock}
	treeHash := e2ee.BuildMLSTree("conversation-mls", 4, []e2ee.TreeLeaf{
		{UserID: "user-1", DeviceID: "device-1", PublicKey: []byte("pub-1")},
		{UserID: "user-2", DeviceID: "device-2", PublicKey: []byte("pub-2")},
	}).ComputeTreeHash()

	mock.ExpectQuery(`SELECT type,\s+COALESCE\(encryption_state, 'PLAINTEXT'\),\s+COALESCE\(is_mls_encrypted, false\),\s+COALESCE\(encryption_epoch, 0\),\s+COALESCE\(mls_epoch, 0\)`).
		WithArgs("conversation-mls").
		WillReturnRows(pgxmock.NewRows([]string{"type", "encryption_state", "is_mls_encrypted", "encryption_epoch", "mls_epoch"}).AddRow("GROUP", "ENCRYPTED", true, int64(4), int64(4)))
	mock.ExpectQuery(`SELECT user_id::text FROM conversation_members WHERE conversation_id = \$1::uuid`).
		WithArgs("conversation-mls").
		WillReturnRows(pgxmock.NewRows([]string{"user_id"}).AddRow("user-1").AddRow("user-2"))
	mock.ExpectQuery(`SELECT cm.user_id::text, d.id::text, COALESCE\(dik.agreement_identity_public_key, ''\)`).
		WithArgs("conversation-mls").
		WillReturnRows(pgxmock.NewRows([]string{"user_id", "device_id", "agreement_identity_public_key"}).
			AddRow("user-1", "device-1", "pub-1").
			AddRow("user-2", "device-2", "pub-2"))
	mock.ExpectExec(`INSERT INTO group_epochs`).
		WithArgs("conversation-mls", int64(4), []byte("digest-1")).
		WillReturnResult(pgxmock.NewResult("INSERT", 1))
	mock.ExpectExec(`INSERT INTO group_sessions`).
		WithArgs("conversation-mls", "user-1", "device-1", int64(4), pgxmock.AnyArg()).
		WillReturnResult(pgxmock.NewResult("INSERT", 1))
	mock.ExpectExec(`INSERT INTO group_sessions`).
		WithArgs("conversation-mls", "user-2", "device-2", int64(4), pgxmock.AnyArg()).
		WillReturnResult(pgxmock.NewResult("INSERT", 1))

	err = svc.validateEncryptedConversationEnvelope(context.Background(), mock, "conversation-mls", &EncryptedMessageMetadata{
		Scheme:            MLSEncryptionScheme,
		ConversationEpoch: 4,
		MLSEpoch:          4,
		MLSTreeHash:       treeHash,
		EpochSecretDigest: "digest-1",
		EpochSecretBoxes: []RecipientKeyInfo{
			{UserID: "user-1", DeviceID: "device-1", WrappedKey: "wk-1", WrapNonce: "wn-1"},
			{UserID: "user-2", DeviceID: "device-2", WrappedKey: "wk-2", WrapNonce: "wn-2"},
		},
	})
	if err != nil {
		t.Fatalf("validateEncryptedConversationEnvelope failed: %v", err)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("unmet expectations: %v", err)
	}
}

func TestTriggerEffectPersistsAndAppendsDomainEvent(t *testing.T) {
	mock, err := pgxmock.NewPool()
	if err != nil {
		t.Fatal(err)
	}
	defer mock.Close()

	svc := &Service{
		db:          mock,
		replication: replication.NewStore(mock, nil),
	}

	mock.ExpectBegin()
	mock.ExpectQuery(`SELECT conversation_id::text FROM messages WHERE id = \$1::uuid`).
		WithArgs("message-1").
		WillReturnRows(pgxmock.NewRows([]string{"conversation_id"}).AddRow("conversation-1"))
	mock.ExpectQuery(`SELECT 1 FROM conversation_members WHERE conversation_id = \$1::uuid AND user_id = \$2::uuid`).
		WithArgs("conversation-1", "user-1").
		WillReturnRows(pgxmock.NewRows([]string{"one"}).AddRow(1))
	mock.ExpectQuery(`SELECT COALESCE\(allow_message_effects, TRUE\) FROM conversations WHERE id = \$1::uuid`).
		WithArgs("conversation-1").
		WillReturnRows(pgxmock.NewRows([]string{"allow_message_effects"}).AddRow(true))
	mock.ExpectExec(`INSERT INTO message_effects\(message_id, conversation_id, triggered_by_user_id, effect_type\)`).
		WithArgs("message-1", "conversation-1", "user-1", "bubble_confetti").
		WillReturnResult(pgxmock.NewResult("INSERT", 1))
	mock.ExpectExec(`INSERT INTO domain_events \(conversation_id, actor_user_id, event_type, payload\)`).
		WithArgs("conversation-1", "user-1", "message_effect_triggered", pgxmock.AnyArg()).
		WillReturnResult(pgxmock.NewResult("INSERT", 1))
	mock.ExpectCommit()

	if err := svc.TriggerEffect(context.Background(), "user-1", "message-1", "bubble_confetti"); err != nil {
		t.Fatalf("TriggerEffect failed: %v", err)
	}

	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("unmet expectations: %v", err)
	}
}

func TestMarkReadPersistsReceiptsAndDomainEvent(t *testing.T) {
	mock, err := pgxmock.NewPool()
	if err != nil {
		t.Fatal(err)
	}
	defer mock.Close()

	svc := &Service{
		db:          mock,
		replication: replication.NewStore(mock, nil),
	}

	mock.ExpectBegin()
	mock.ExpectExec(`UPDATE conversation_members`).
		WithArgs("conversation-1", "user-1", int64(7)).
		WillReturnResult(pgxmock.NewResult("UPDATE", 1))
	mock.ExpectExec(`INSERT INTO message_read_receipts\(message_id, reader_user_id, read_at\)`).
		WithArgs("user-1", pgxmock.AnyArg(), "conversation-1", int64(7)).
		WillReturnResult(pgxmock.NewResult("INSERT", 3))
	mock.ExpectExec(`UPDATE messages`).
		WithArgs("conversation-1", "user-1", int64(7)).
		WillReturnResult(pgxmock.NewResult("UPDATE", 2))
	mock.ExpectExec(`UPDATE conversations SET updated_at = now\(\) WHERE id = \$1::uuid`).
		WithArgs("conversation-1").
		WillReturnResult(pgxmock.NewResult("UPDATE", 1))
	mock.ExpectExec(`INSERT INTO domain_events \(conversation_id, actor_user_id, event_type, payload\)`).
		WithArgs("conversation-1", "user-1", "read_checkpoint_advanced", pgxmock.AnyArg()).
		WillReturnResult(pgxmock.NewResult("INSERT", 1))
	mock.ExpectCommit()

	if err := svc.MarkRead(context.Background(), "user-1", "conversation-1", 7); err != nil {
		t.Fatalf("MarkRead failed: %v", err)
	}

	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("unmet expectations: %v", err)
	}
}

func TestGetConversationReadStatusIncludesDeliveryAndReadTimes(t *testing.T) {
	mock, err := pgxmock.NewPool()
	if err != nil {
		t.Fatal(err)
	}
	defer mock.Close()

	svc := &Service{db: mock}
	now := time.Date(2026, 3, 20, 12, 0, 0, 0, time.UTC)
	later := now.Add(2 * time.Minute)

	mock.ExpectQuery(`SELECT 1 FROM conversation_members WHERE conversation_id = \$1::uuid AND user_id = \$2::uuid`).
		WithArgs("conversation-1", "user-1").
		WillReturnRows(pgxmock.NewRows([]string{"one"}).AddRow(1))
	rows := pgxmock.NewRows([]string{
		"id",
		"primary_phone_e164",
		"last_read_server_order",
		"last_delivered_server_order",
		"read_at",
		"delivery_at",
	}).
		AddRow("user-1", "+15550001111", int64(7), int64(9), now, later).
		AddRow("user-2", "+15550002222", int64(3), int64(4), nil, nil)
	mock.ExpectQuery(`SELECT u.id::text, u.primary_phone_e164, cm.last_read_server_order, cm.last_delivered_server_order, cm.read_at, cm.delivery_at FROM conversation_members cm JOIN users u ON u.id = cm.user_id WHERE cm.conversation_id = \$1::uuid ORDER BY cm.read_at DESC NULLS LAST, cm.delivery_at DESC NULLS LAST`).
		WithArgs("conversation-1").
		WillReturnRows(rows)

	status, err := svc.GetConversationReadStatus(context.Background(), "user-1", "conversation-1")
	if err != nil {
		t.Fatalf("GetConversationReadStatus failed: %v", err)
	}

	members, ok := status["members"].([]map[string]any)
	if !ok {
		t.Fatalf("unexpected members shape: %#v", status["members"])
	}
	if len(members) != 2 {
		t.Fatalf("expected 2 members, got %d", len(members))
	}
	if members[0]["last_read_server_order"] != int64(7) {
		t.Fatalf("unexpected status payload: %#v", members[0])
	}
	if members[0]["delivery_at"] == "" {
		t.Fatalf("expected delivery_at in response")
	}

	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("unmet expectations: %v", err)
	}
}

func TestTriggerEffectRejectsUnsupportedType(t *testing.T) {
	svc := &Service{}
	err := svc.TriggerEffect(context.Background(), "user-1", "message-1", "bad_effect")
	if !errors.Is(err, ErrInvalidMessageEffectType) {
		t.Fatalf("expected ErrInvalidMessageEffectType, got %v", err)
	}
}

func TestEffectTypeValidationHelper(t *testing.T) {
	if !isSupportedMessageEffectType("bubble_confetti") {
		t.Fatalf("expected effect helper to accept bubble_confetti")
	}
}

func TestSetMessagePinnedPersistsPinEffect(t *testing.T) {
	mock, err := pgxmock.NewPool()
	if err != nil {
		t.Fatal(err)
	}
	defer mock.Close()

	svc := &Service{db: mock}

	mock.ExpectBegin()
	mock.ExpectQuery(`SELECT conversation_id::text, deleted_at, visibility_state, expires_at FROM messages WHERE id = \$1::uuid`).
		WithArgs("message-1").
		WillReturnRows(pgxmock.NewRows([]string{"conversation_id", "deleted_at", "visibility_state", "expires_at"}).AddRow("conversation-1", nil, "", nil))
	mock.ExpectQuery(`SELECT 1 FROM conversation_members WHERE conversation_id = \$1::uuid AND user_id = \$2::uuid`).
		WithArgs("conversation-1", "user-1").
		WillReturnRows(pgxmock.NewRows([]string{"one"}).AddRow(1))
	mock.ExpectQuery(`SELECT effect_type FROM message_effects`).
		WithArgs("message-1", "conversation-1", "PINNED", "UNPINNED").
		WillReturnError(pgx.ErrNoRows)
	mock.ExpectExec(`INSERT INTO message_effects\(message_id, conversation_id, triggered_by_user_id, effect_type\)`).
		WithArgs("message-1", "conversation-1", "user-1", "PINNED").
		WillReturnResult(pgxmock.NewResult("INSERT", 1))
	mock.ExpectExec(`UPDATE conversations SET updated_at = now\(\) WHERE id = \$1::uuid`).
		WithArgs("conversation-1").
		WillReturnResult(pgxmock.NewResult("UPDATE", 1))
	mock.ExpectCommit()

	if err := svc.SetMessagePinned(context.Background(), "user-1", "message-1", true); err != nil {
		t.Fatalf("SetMessagePinned failed: %v", err)
	}

	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("unmet expectations: %v", err)
	}
}

func TestForwardMessageCopiesSourceMetadata(t *testing.T) {
	mock, err := pgxmock.NewPool()
	if err != nil {
		t.Fatal(err)
	}
	defer mock.Close()

	svc := &Service{db: mock}
	createdAt := time.Date(2026, 3, 20, 15, 4, 5, 0, time.UTC)
	forwardedAt := createdAt.Add(time.Minute)

	mock.ExpectQuery(`SELECT conversation_id::text FROM messages WHERE id = \$1::uuid`).
		WithArgs("message-src").
		WillReturnRows(pgxmock.NewRows([]string{"conversation_id"}).AddRow("conversation-source"))
	mock.ExpectQuery(`SELECT 1 FROM conversation_members WHERE conversation_id = \$1::uuid AND user_id = \$2::uuid`).
		WithArgs("conversation-source", "user-1").
		WillReturnRows(pgxmock.NewRows([]string{"one"}).AddRow(1))
	mock.ExpectQuery(`SELECT\s+m.id::text,\s*m.conversation_id::text,\s*m.sender_user_id::text`).
		WithArgs("user-1", "message-src").
		WillReturnRows(pgxmock.NewRows([]string{
			"id",
			"conversation_id",
			"sender_user_id",
			"sender_device_id",
			"reply_to_message_id",
			"content_type",
			"content",
			"client_generated_id",
			"transport",
			"server_order",
			"delivery_status",
			"created_at",
			"delivered_at",
			"read_at",
			"edited_at",
			"deleted_at",
			"expires_at",
			"visibility_state",
			"reply_count",
			"reactions",
		}).AddRow(
			"message-src",
			"conversation-source",
			"user-2",
			"device-2",
			"",
			"text",
			[]byte(`{"text":"hello"}`),
			"client-src",
			"OTT",
			int64(9),
			"",
			createdAt,
			nil,
			nil,
			nil,
			nil,
			nil,
			"",
			int64(0),
			[]byte(`{}`),
		))

	mock.ExpectBegin()
	mock.ExpectQuery(`SELECT 1 FROM conversation_members WHERE conversation_id = \$1::uuid AND user_id = \$2::uuid`).
		WithArgs("conversation-target", "user-1").
		WillReturnRows(pgxmock.NewRows([]string{"one"}).AddRow(1))
	mock.ExpectQuery(`SELECT user_id::text FROM conversation_members WHERE conversation_id = \$1::uuid AND user_id <> \$2::uuid`).
		WithArgs("conversation-target", "user-1").
		WillReturnRows(pgxmock.NewRows([]string{"user_id"}))
	mock.ExpectQuery(`SELECT response_payload FROM idempotency_keys WHERE actor_user_id = \$1::uuid AND endpoint = \$2 AND key = \$3 AND expires_at > now\(\)`).
		WithArgs("user-1", "/v1/messages/forward", "idem-forward-1").
		WillReturnError(pgx.ErrNoRows)
	mock.ExpectQuery(`SELECT type, COALESCE\(encryption_state, 'PLAINTEXT'\), COALESCE\(is_mls_encrypted, false\) FROM conversations WHERE id = \$1::uuid`).
		WithArgs("conversation-target").
		WillReturnRows(pgxmock.NewRows([]string{"type", "encryption_state", "is_mls_encrypted"}).AddRow("GROUP", "PLAINTEXT", false))
	mock.ExpectQuery(`UPDATE conversation_counters`).
		WithArgs("conversation-target").
		WillReturnRows(pgxmock.NewRows([]string{"next_server_order"}).AddRow(int64(22)))
	mock.ExpectQuery(`SELECT COALESCE\(retention_seconds, 0\), expires_at FROM conversations WHERE id = \$1::uuid`).
		WithArgs("conversation-target").
		WillReturnRows(pgxmock.NewRows([]string{"retention_seconds", "expires_at"}).AddRow(int64(0), nil))
	mock.ExpectQuery(`SELECT transport_policy FROM conversations WHERE id = \$1::uuid`).
		WithArgs("conversation-target").
		WillReturnRows(pgxmock.NewRows([]string{"transport_policy"}).AddRow("AUTO"))
	mock.ExpectQuery(`SELECT COUNT\(1\) FROM conversation_members WHERE conversation_id = \$1::uuid AND user_id <> \$2::uuid`).
		WithArgs("conversation-target", "user-1").
		WillReturnRows(pgxmock.NewRows([]string{"count"}).AddRow(1))
	mock.ExpectQuery(`INSERT INTO messages \(conversation_id, sender_user_id, sender_device_id, reply_to_message_id, content_type, content, client_generated_id, transport, server_order, expires_at, is_encrypted, encryption_scheme\)`).
		WithArgs("conversation-target", "user-1", "device-1", "", "text", pgxmock.AnyArg(), "client-forwarded", "OTT", int64(22), pgxmock.AnyArg(), false, "").
		WillReturnRows(pgxmock.NewRows([]string{"id", "created_at"}).AddRow("message-forwarded", forwardedAt))
	mock.ExpectExec(`UPDATE conversations SET last_message_id = \$2::uuid, updated_at = now\(\) WHERE id = \$1::uuid`).
		WithArgs("conversation-target", "message-forwarded").
		WillReturnResult(pgxmock.NewResult("UPDATE", 1))
	mock.ExpectExec(`INSERT INTO idempotency_keys \(actor_user_id, endpoint, key, response_payload, status_code, expires_at\)`).
		WithArgs("user-1", "/v1/messages/forward", "idem-forward-1", pgxmock.AnyArg()).
		WillReturnResult(pgxmock.NewResult("INSERT", 1))
	mock.ExpectQuery(`SELECT user_id::text FROM conversation_members WHERE conversation_id = \$1::uuid AND user_id <> \$2::uuid`).
		WithArgs("conversation-target", "user-1").
		WillReturnRows(pgxmock.NewRows([]string{"user_id"}).AddRow("user-3"))
	mock.ExpectCommit()

	result, err := svc.ForwardMessage(context.Background(), "user-1", "device-1", "message-src", "conversation-target", "idem-forward-1", "client-forwarded", "")
	if err != nil {
		t.Fatalf("ForwardMessage failed: %v", err)
	}

	forwardedFrom, ok := result.Message.Content["forwarded_from"].(map[string]any)
	if !ok {
		t.Fatalf("missing forwarded_from metadata: %#v", result.Message.Content)
	}
	if forwardedFrom["message_id"] != "message-src" {
		t.Fatalf("unexpected forwarded_from payload: %#v", forwardedFrom)
	}
	if result.Message.Source != "FORWARDED" {
		t.Fatalf("expected forwarded source marker, got %q", result.Message.Source)
	}

	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("unmet expectations: %v", err)
	}
}

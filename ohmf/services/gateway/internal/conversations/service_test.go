package conversations

import (
	"context"
	"testing"
	"time"

	pgxmock "github.com/pashagolub/pgxmock/v4"
	"ohmf/services/gateway/internal/devicekeys"
)

func TestNormalizeCreateEncryptionStateForGroupDefaultsToEncrypted(t *testing.T) {
	if got := normalizeCreateEncryptionState("GROUP", "ENCRYPTED"); got != "ENCRYPTED" {
		t.Fatalf("expected ENCRYPTED, got %q", got)
	}
	if got := normalizeCreateEncryptionState("GROUP", "PENDING_E2EE"); got != "ENCRYPTED" {
		t.Fatalf("expected ENCRYPTED, got %q", got)
	}
	if got := normalizeCreateEncryptionState("GROUP", "PLAINTEXT"); got != "ENCRYPTED" {
		t.Fatalf("expected ENCRYPTED, got %q", got)
	}
	if got := normalizeCreateEncryptionState("DM", "ENCRYPTED"); got != "ENCRYPTED" {
		t.Fatalf("expected ENCRYPTED, got %q", got)
	}
}

func TestEncryptionReadyForConversationQueryReportsBlockedMembers(t *testing.T) {
	mock, err := pgxmock.NewPool()
	if err != nil {
		t.Fatal(err)
	}
	defer mock.Close()

	svc := NewService(mock, nil, nil)

	mock.ExpectQuery(`SELECT user_id::text FROM conversation_members WHERE conversation_id = \$1::uuid ORDER BY joined_at ASC`).
		WithArgs("conversation-1").
		WillReturnRows(pgxmock.NewRows([]string{"user_id"}).AddRow("user-1").AddRow("user-2"))
	mock.ExpectQuery(`SELECT EXISTS\(`).
		WithArgs("user-1", devicekeys.BundleVersionSignalV1, []string{devicekeys.RequiredDeviceCapability}).
		WillReturnRows(pgxmock.NewRows([]string{"exists"}).AddRow(true))
	mock.ExpectQuery(`SELECT EXISTS\(`).
		WithArgs("user-2", devicekeys.BundleVersionSignalV1, []string{devicekeys.RequiredDeviceCapability}).
		WillReturnRows(pgxmock.NewRows([]string{"exists"}).AddRow(false))

	ready, blocked, err := svc.encryptionReadyForConversationQuery(context.Background(), mock, "conversation-1")
	if err != nil {
		t.Fatalf("encryptionReadyForConversationQuery failed: %v", err)
	}
	if ready {
		t.Fatalf("expected readiness to be false")
	}
	if len(blocked) != 1 || blocked[0] != "user-2" {
		t.Fatalf("unexpected blocked members: %#v", blocked)
	}

	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("unmet expectations: %v", err)
	}
}

func TestUpdateEffectPolicyOwnerCanToggleEffects(t *testing.T) {
	mock, err := pgxmock.NewPool()
	if err != nil {
		t.Fatal(err)
	}
	defer mock.Close()

	svc := NewService(mock, nil, nil)

	mock.ExpectBegin()
	mock.ExpectQuery(`SELECT cm.role, c.type FROM conversation_members cm JOIN conversations c ON c.id = cm.conversation_id WHERE cm.conversation_id = \$1::uuid AND cm.user_id = \$2::uuid`).
		WithArgs("conversation-1", "owner-1").
		WillReturnRows(pgxmock.NewRows([]string{"role", "type"}).AddRow("OWNER", "GROUP"))
	mock.ExpectExec(`UPDATE conversations`).
		WithArgs("conversation-1", false).
		WillReturnResult(pgxmock.NewResult("UPDATE", 1))
	mock.ExpectCommit()

	if err := svc.UpdateEffectPolicy(context.Background(), "owner-1", "conversation-1", false); err != nil {
		t.Fatalf("UpdateEffectPolicy failed: %v", err)
	}

	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("unmet expectations: %v", err)
	}
}

func TestUpdateEffectPolicyRejectsNonOwner(t *testing.T) {
	mock, err := pgxmock.NewPool()
	if err != nil {
		t.Fatal(err)
	}
	defer mock.Close()

	svc := NewService(mock, nil, nil)

	mock.ExpectBegin()
	mock.ExpectQuery(`SELECT cm.role, c.type FROM conversation_members cm JOIN conversations c ON c.id = cm.conversation_id WHERE cm.conversation_id = \$1::uuid AND cm.user_id = \$2::uuid`).
		WithArgs("conversation-1", "member-1").
		WillReturnRows(pgxmock.NewRows([]string{"role", "type"}).AddRow("MEMBER", "GROUP"))
	mock.ExpectRollback()

	err = svc.UpdateEffectPolicy(context.Background(), "member-1", "conversation-1", true)
	if err == nil || err.Error() != "forbidden" {
		t.Fatalf("expected forbidden error, got %v", err)
	}

	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("unmet expectations: %v", err)
	}
}

func TestUpdateEffectPolicyAllowsAdmin(t *testing.T) {
	mock, err := pgxmock.NewPool()
	if err != nil {
		t.Fatal(err)
	}
	defer mock.Close()

	svc := NewService(mock, nil, nil)

	mock.ExpectBegin()
	mock.ExpectQuery(`SELECT cm.role, c.type FROM conversation_members cm JOIN conversations c ON c.id = cm.conversation_id WHERE cm.conversation_id = \$1::uuid AND cm.user_id = \$2::uuid`).
		WithArgs("conversation-1", "admin-1").
		WillReturnRows(pgxmock.NewRows([]string{"role", "type"}).AddRow("ADMIN", "GROUP"))
	mock.ExpectExec(`UPDATE conversations`).
		WithArgs("conversation-1", true).
		WillReturnResult(pgxmock.NewResult("UPDATE", 1))
	mock.ExpectCommit()

	if err := svc.UpdateEffectPolicy(context.Background(), "admin-1", "conversation-1", true); err != nil {
		t.Fatalf("UpdateEffectPolicy failed: %v", err)
	}

	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("unmet expectations: %v", err)
	}
}

func TestUpdateMemberRoleRejectsDemotingLastOwner(t *testing.T) {
	mock, err := pgxmock.NewPool()
	if err != nil {
		t.Fatal(err)
	}
	defer mock.Close()

	svc := NewService(mock, nil, nil)

	mock.ExpectBegin()
	mock.ExpectQuery(`SELECT cm.role, c.type FROM conversation_members cm JOIN conversations c ON c.id = cm.conversation_id WHERE cm.conversation_id = \$1::uuid AND cm.user_id = \$2::uuid`).
		WithArgs("conversation-1", "owner-1").
		WillReturnRows(pgxmock.NewRows([]string{"role", "type"}).AddRow("OWNER", "GROUP"))
	mock.ExpectQuery(`SELECT role FROM conversation_members WHERE conversation_id = \$1::uuid AND user_id = \$2::uuid`).
		WithArgs("conversation-1", "owner-2").
		WillReturnRows(pgxmock.NewRows([]string{"role"}).AddRow("OWNER"))
	mock.ExpectQuery(`SELECT COUNT\(1\) FROM conversation_members WHERE conversation_id = \$1::uuid AND role = 'OWNER'`).
		WithArgs("conversation-1").
		WillReturnRows(pgxmock.NewRows([]string{"count"}).AddRow(1))
	mock.ExpectRollback()

	_, err = svc.UpdateMemberRole(context.Background(), "owner-1", "conversation-1", "owner-2", "ADMIN")
	if err == nil || err.Error() != "last_owner_required" {
		t.Fatalf("expected last_owner_required, got %v", err)
	}

	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("unmet expectations: %v", err)
	}
}

func TestCreateInviteCreatesActiveCode(t *testing.T) {
	mock, err := pgxmock.NewPool()
	if err != nil {
		t.Fatal(err)
	}
	defer mock.Close()

	svc := NewService(mock, nil, nil)
	now := time.Date(2026, 3, 20, 12, 0, 0, 0, time.UTC)
	expires := now.Add(time.Hour)

	mock.ExpectBegin()
	mock.ExpectQuery(`SELECT cm.role, c.type FROM conversation_members cm JOIN conversations c ON c.id = cm.conversation_id WHERE cm.conversation_id = \$1::uuid AND cm.user_id = \$2::uuid`).
		WithArgs("conversation-1", "admin-1").
		WillReturnRows(pgxmock.NewRows([]string{"role", "type"}).AddRow("ADMIN", "GROUP"))
	mock.ExpectQuery(`INSERT INTO conversation_invites`).
		WithArgs("conversation-1", pgxmock.AnyArg(), "admin-1", pgxmock.AnyArg(), 3).
		WillReturnRows(pgxmock.NewRows([]string{"id", "created_at", "expires_at"}).AddRow("invite-1", now, expires))
	mock.ExpectCommit()

	invite, err := svc.CreateInvite(context.Background(), "admin-1", "conversation-1", 3, 3600)
	if err != nil {
		t.Fatalf("CreateInvite failed: %v", err)
	}
	if invite.InviteID != "invite-1" {
		t.Fatalf("unexpected invite id: %#v", invite)
	}
	if invite.Code == "" {
		t.Fatalf("expected invite code to be generated")
	}
	if invite.MaxUses != 3 {
		t.Fatalf("expected max uses 3, got %d", invite.MaxUses)
	}

	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("unmet expectations: %v", err)
	}
}

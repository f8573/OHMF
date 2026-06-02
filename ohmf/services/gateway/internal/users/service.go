package users

import (
	"context"
	cryptoRand "crypto/rand"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"math/big"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"ohmf/services/gateway/internal/replication"
	"ohmf/services/gateway/internal/securityaudit"
)

// Service handles all user-related business logic
type Service struct {
	db          DB
	replication *replication.Store
}

type auditExecutor interface {
	Exec(ctx context.Context, sql string, args ...any) (pgconn.CommandTag, error)
	QueryRow(ctx context.Context, sql string, args ...any) pgx.Row
}

// NewService creates a user service
func NewService(db DB, replicationStore *replication.Store) *Service {
	return &Service{
		db:          db,
		replication: replicationStore,
	}
}

type DB interface {
	BeginTx(ctx context.Context, txOptions pgx.TxOptions) (pgx.Tx, error)
	Exec(ctx context.Context, sql string, args ...any) (pgconn.CommandTag, error)
	Query(ctx context.Context, sql string, args ...any) (pgx.Rows, error)
	QueryRow(ctx context.Context, sql string, args ...any) pgx.Row
}

// Profile represents user profile information
type Profile struct {
	UserID      string `json:"user_id"`
	DisplayName string `json:"display_name"`
	AvatarURL   string `json:"avatar_url"`
	PhoneE164   string `json:"phone_e164,omitempty"`
}

// GetProfile fetches a user's profile information
func (s *Service) GetProfile(ctx context.Context, userID string) (Profile, error) {
	var profile Profile
	if err := s.db.QueryRow(ctx, `
		SELECT id::text, COALESCE(display_name, ''), COALESCE(avatar_url, ''), COALESCE(primary_phone_e164, '')
		FROM users
		WHERE id = $1::uuid
	`, userID).Scan(&profile.UserID, &profile.DisplayName, &profile.AvatarURL, &profile.PhoneE164); err != nil {
		return Profile{}, err
	}
	return profile, nil
}

// UpdateProfile updates user profile display name and avatar
func (s *Service) UpdateProfile(ctx context.Context, userID string, displayName, avatarURL *string) (Profile, error) {
	var displayNameArg any
	if displayName != nil {
		displayNameArg = strings.TrimSpace(*displayName)
	}
	var avatarURLArg any
	if avatarURL != nil {
		avatarURLArg = strings.TrimSpace(*avatarURL)
	}
	if _, err := s.db.Exec(ctx, `
		UPDATE users
		SET display_name = CASE WHEN $2::bool THEN NULLIF($3::text, '') ELSE display_name END,
		    avatar_url = CASE WHEN $4::bool THEN NULLIF($5::text, '') ELSE avatar_url END,
		    updated_at = now()
		WHERE id = $1::uuid
	`, userID, displayName != nil, displayNameArg, avatarURL != nil, avatarURLArg); err != nil {
		return Profile{}, err
	}
	return s.GetProfile(ctx, userID)
}

// ResolveProfiles retrieves multiple user profiles with deduplication and validation
func (s *Service) ResolveProfiles(ctx context.Context, userIDs []string) ([]Profile, error) {
	if len(userIDs) == 0 {
		return []Profile{}, nil
	}
	rows, err := s.db.Query(ctx, `
		SELECT id::text, COALESCE(display_name, ''), COALESCE(avatar_url, ''), COALESCE(primary_phone_e164, '')
		FROM users
		WHERE id = ANY($1::uuid[])
	`, s.dedupeAndValidateUUIDs(userIDs))
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := make([]Profile, 0, len(userIDs))
	for rows.Next() {
		var profile Profile
		if err := rows.Scan(&profile.UserID, &profile.DisplayName, &profile.AvatarURL, &profile.PhoneE164); err != nil {
			return nil, err
		}
		out = append(out, profile)
	}
	return out, rows.Err()
}

// dedupeAndValidateUUIDs deduplicates and validates UUIDs
func (s *Service) dedupeAndValidateUUIDs(items []string) []string {
	seen := map[string]struct{}{}
	out := make([]string, 0, len(items))
	for _, item := range items {
		if item == "" {
			continue
		}
		if _, err := uuid.Parse(item); err != nil {
			continue
		}
		if _, ok := seen[item]; ok {
			continue
		}
		seen[item] = struct{}{}
		out = append(out, item)
	}
	return out
}

// ExportAccount exports user account data including conversations, messages,
// devices, sessions, and security/compliance metadata.
func (s *Service) ExportAccount(ctx context.Context, userID string) (map[string]any, error) {
	user, err := s.exportAccountUser(ctx, userID)
	if err != nil {
		return nil, err
	}

	devices, err := s.exportAccountDevices(ctx, userID)
	if err != nil {
		return nil, err
	}

	security, err := s.exportAccountSecurity(ctx, userID)
	if err != nil {
		return nil, err
	}

	conversations, err := s.exportAccountConversations(ctx, userID)
	if err != nil {
		return nil, err
	}

	messages, err := s.exportAccountMessages(ctx, userID)
	if err != nil {
		return nil, err
	}

	return map[string]any{
		"export_version": 1,
		"exported_at":    time.Now().UTC().Format(time.RFC3339Nano),
		"user":           user,
		"devices":        devices,
		"security":       security,
		"conversations":  conversations,
		"messages":       messages,
	}, nil
}

// DeleteAccount records a deletion request, revokes live credentials, and
// applies a grace-period marker for later purge jobs.
func (s *Service) DeleteAccount(ctx context.Context, userID string) error {
	tx, err := s.db.BeginTx(ctx, pgx.TxOptions{})
	if err != nil {
		return err
	}
	defer tx.Rollback(ctx)

	var primaryPhone string
	if err := tx.QueryRow(ctx, `
		SELECT primary_phone_e164
		FROM users
		WHERE id = $1::uuid
		FOR UPDATE
	`, userID).Scan(&primaryPhone); err != nil {
		return err
	}

	if _, err := tx.Exec(ctx, `
		INSERT INTO account_deletion_audit (
			user_id,
			requested_by_user_id,
			requested_at,
			effective_at,
			status,
			reason,
			created_at,
			updated_at
		)
		VALUES ($1::uuid, $1::uuid, now(), now() + INTERVAL '30 days', 'PENDING', 'self_service_request', now(), now())
	`, userID); err != nil {
		return err
	}
	if err := appendSecurityAuditEvent(ctx, tx, userID, userID, "account_delete_requested", map[string]any{
		"grace_period_days": 30,
	}); err != nil {
		return err
	}

	// Revoke all active sessions and remove local device state.
	if _, err := tx.Exec(ctx, `UPDATE refresh_tokens SET revoked_at = now() WHERE user_id = $1::uuid AND revoked_at IS NULL`, userID); err != nil {
		return err
	}
	if _, err := tx.Exec(ctx, `DELETE FROM devices WHERE user_id = $1::uuid`, userID); err != nil {
		return err
	}

	if _, err := tx.Exec(ctx, `DELETE FROM account_recovery_codes WHERE user_id = $1::uuid`, userID); err != nil {
		return err
	}
	if _, err := tx.Exec(ctx, `DELETE FROM two_factor_methods WHERE user_id = $1::uuid`, userID); err != nil {
		return err
	}
	if _, err := tx.Exec(ctx, `DELETE FROM idempotency_keys WHERE actor_user_id = $1::uuid`, userID); err != nil {
		return err
	}
	if _, err := tx.Exec(ctx, `DELETE FROM user_blocks WHERE blocker_user_id = $1::uuid OR blocked_user_id = $1::uuid`, userID); err != nil {
		return err
	}
	if _, err := tx.Exec(ctx, `DELETE FROM phone_verification_challenges WHERE phone_e164 = $1`, primaryPhone); err != nil {
		return err
	}

	// Remove discovery index entries for the phone.
	if primaryPhone != "" {
		if _, err := tx.Exec(ctx, `DELETE FROM external_contacts WHERE phone_e164 = $1`, primaryPhone); err != nil {
			return err
		}
	}

	deletedPhone := fmt.Sprintf("deleted:%s", userID)
	if _, err := tx.Exec(ctx, `
		UPDATE users
		SET primary_phone_e164 = $2,
		    phone_verified_at = NULL,
		    display_name = NULL,
		    avatar_url = NULL,
		    deletion_state = 'PENDING',
		    deletion_requested_at = now(),
		    deletion_effective_at = now() + INTERVAL '30 days',
		    deletion_completed_at = NULL,
		    deletion_reason = 'self_service_request',
		    updated_at = now()
		WHERE id = $1::uuid
	`, userID, deletedPhone); err != nil {
		return err
	}

	if err := tx.Commit(ctx); err != nil {
		return err
	}
	return nil
}

func (s *Service) CreateExportArtifact(ctx context.Context, userID string) (map[string]any, error) {
	payload, err := s.ExportAccount(ctx, userID)
	if err != nil {
		return nil, err
	}
	raw, err := json.Marshal(payload)
	if err != nil {
		return nil, err
	}
	downloadToken, err := generateRandomCode(32)
	if err != nil {
		return nil, err
	}
	var exportID string
	var expiresAt time.Time
	if err := s.db.QueryRow(ctx, `
		INSERT INTO account_exports (user_id, export_blob, download_token, size_bytes)
		VALUES ($1::uuid, $2::jsonb, $3, $4)
		RETURNING id::text, expires_at
	`, userID, string(raw), downloadToken, len(raw)).Scan(&exportID, &expiresAt); err != nil {
		return nil, err
	}
	if err := appendSecurityAuditEvent(ctx, s.db, userID, userID, "account_export_created", map[string]any{
		"export_id":  exportID,
		"size_bytes": len(raw),
	}); err != nil {
		return nil, err
	}
	return map[string]any{
		"export_id":      exportID,
		"status":         "READY",
		"format":         "application/json",
		"download_token": downloadToken,
		"size_bytes":     len(raw),
		"expires_at":     expiresAt.UTC().Format(time.RFC3339Nano),
	}, nil
}

func (s *Service) ListExportArtifacts(ctx context.Context, userID string) ([]map[string]any, error) {
	rows, err := s.db.Query(ctx, `
		SELECT id::text, status, format, download_token, size_bytes, created_at, expires_at, downloaded_at
		FROM account_exports
		WHERE user_id = $1::uuid
		ORDER BY created_at DESC
	`, userID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	items := make([]map[string]any, 0, 4)
	for rows.Next() {
		var id, status, format, downloadToken string
		var sizeBytes int
		var createdAt, expiresAt time.Time
		var downloadedAt sql.NullTime
		if err := rows.Scan(&id, &status, &format, &downloadToken, &sizeBytes, &createdAt, &expiresAt, &downloadedAt); err != nil {
			return nil, err
		}
		item := map[string]any{
			"export_id":      id,
			"status":         status,
			"format":         format,
			"download_token": downloadToken,
			"size_bytes":     sizeBytes,
			"created_at":     createdAt.UTC().Format(time.RFC3339Nano),
			"expires_at":     expiresAt.UTC().Format(time.RFC3339Nano),
		}
		if downloadedAt.Valid {
			item["downloaded_at"] = downloadedAt.Time.UTC().Format(time.RFC3339Nano)
		}
		items = append(items, item)
	}
	return items, rows.Err()
}

func (s *Service) GetExportArtifact(ctx context.Context, userID, exportID string) (map[string]any, error) {
	tx, err := s.db.BeginTx(ctx, pgx.TxOptions{})
	if err != nil {
		return nil, err
	}
	defer tx.Rollback(ctx)

	var status, format, downloadToken string
	var sizeBytes int
	var createdAt, expiresAt time.Time
	var downloadedAt sql.NullTime
	var raw []byte
	if err := tx.QueryRow(ctx, `
		SELECT status, format, download_token, size_bytes, created_at, expires_at, downloaded_at, export_blob
		FROM account_exports
		WHERE id = $1::uuid AND user_id = $2::uuid
		FOR UPDATE
	`, exportID, userID).Scan(&status, &format, &downloadToken, &sizeBytes, &createdAt, &expiresAt, &downloadedAt, &raw); err != nil {
		return nil, err
	}
	if time.Now().UTC().After(expiresAt.UTC()) {
		return nil, errors.New("export_expired")
	}
	if _, err := tx.Exec(ctx, `
		UPDATE account_exports
		SET downloaded_at = now()
		WHERE id = $1::uuid
	`, exportID); err != nil {
		return nil, err
	}
	if err := appendSecurityAuditEvent(ctx, tx, userID, userID, "account_export_downloaded", map[string]any{
		"export_id": exportID,
	}); err != nil {
		return nil, err
	}
	if err := tx.Commit(ctx); err != nil {
		return nil, err
	}
	var blob map[string]any
	if err := json.Unmarshal(raw, &blob); err != nil {
		return nil, err
	}
	return map[string]any{
		"export_id":      exportID,
		"status":         status,
		"format":         format,
		"download_token": downloadToken,
		"size_bytes":     sizeBytes,
		"created_at":     createdAt.UTC().Format(time.RFC3339Nano),
		"expires_at":     expiresAt.UTC().Format(time.RFC3339Nano),
		"downloaded_at":  time.Now().UTC().Format(time.RFC3339Nano),
		"payload":        blob,
	}, nil
}

func (s *Service) FinalizeDeletion(ctx context.Context, userID string) error {
	tx, err := s.db.BeginTx(ctx, pgx.TxOptions{})
	if err != nil {
		return err
	}
	defer tx.Rollback(ctx)

	var deletionState string
	var effectiveAt sql.NullTime
	if err := tx.QueryRow(ctx, `
		SELECT COALESCE(deletion_state, 'ACTIVE'), deletion_effective_at
		FROM users
		WHERE id = $1::uuid
		FOR UPDATE
	`, userID).Scan(&deletionState, &effectiveAt); err != nil {
		return err
	}
	if !strings.EqualFold(deletionState, "PENDING") {
		return errors.New("deletion_not_pending")
	}
	if !effectiveAt.Valid || effectiveAt.Time.After(time.Now().UTC()) {
		return errors.New("deletion_not_effective")
	}

	if _, err := tx.Exec(ctx, `DELETE FROM account_exports WHERE user_id = $1::uuid`, userID); err != nil {
		return err
	}
	if _, err := tx.Exec(ctx, `DELETE FROM device_pairing_sessions WHERE user_id = $1::uuid`, userID); err != nil {
		return err
	}
	if _, err := tx.Exec(ctx, `
		UPDATE account_deletion_audit
		SET status = 'COMPLETED',
		    completed_at = now(),
		    updated_at = now()
		WHERE user_id = $1::uuid AND status = 'PENDING'
	`, userID); err != nil {
		return err
	}
	if _, err := tx.Exec(ctx, `
		UPDATE users
		SET deletion_state = 'COMPLETED',
		    deletion_completed_at = now(),
		    updated_at = now()
		WHERE id = $1::uuid
	`, userID); err != nil {
		return err
	}
	if err := appendSecurityAuditEvent(ctx, tx, userID, userID, "account_delete_finalized", map[string]any{}); err != nil {
		return err
	}
	return tx.Commit(ctx)
}

func appendSecurityAuditEvent(ctx context.Context, exec auditExecutor, actorUserID, targetUserID, eventType string, payload map[string]any) error {
	return securityaudit.Append(ctx, exec, actorUserID, targetUserID, eventType, payload)
}

func (s *Service) exportAccountUser(ctx context.Context, userID string) (map[string]any, error) {
	var phoneVerifiedAt sql.NullTime
	var deletionRequestedAt sql.NullTime
	var deletionEffectiveAt sql.NullTime
	var deletionCompletedAt sql.NullTime
	var createdAt time.Time
	var updatedAt time.Time
	var primaryPhone, displayName, avatarURL, deletionState, deletionReason string
	if err := s.db.QueryRow(ctx, `
		SELECT
			id::text,
			COALESCE(primary_phone_e164, ''),
			COALESCE(display_name, ''),
			COALESCE(avatar_url, ''),
			phone_verified_at,
			created_at,
			updated_at,
			COALESCE(deletion_state, 'ACTIVE'),
			deletion_requested_at,
			deletion_effective_at,
			deletion_completed_at,
			COALESCE(deletion_reason, '')
		FROM users
		WHERE id = $1::uuid
	`, userID).Scan(
		&userID,
		&primaryPhone,
		&displayName,
		&avatarURL,
		&phoneVerifiedAt,
		&createdAt,
		&updatedAt,
		&deletionState,
		&deletionRequestedAt,
		&deletionEffectiveAt,
		&deletionCompletedAt,
		&deletionReason,
	); err != nil {
		return nil, err
	}

	user := map[string]any{
		"user_id":            userID,
		"primary_phone_e164": primaryPhone,
		"display_name":       displayName,
		"avatar_url":         avatarURL,
		"created_at":         createdAt.UTC().Format(time.RFC3339Nano),
		"updated_at":         updatedAt.UTC().Format(time.RFC3339Nano),
		"deletion_state":     deletionState,
		"deletion_reason":    deletionReason,
	}
	if phoneVerifiedAt.Valid {
		user["phone_verified_at"] = phoneVerifiedAt.Time.UTC().Format(time.RFC3339Nano)
	}
	if deletionRequestedAt.Valid {
		user["deletion_requested_at"] = deletionRequestedAt.Time.UTC().Format(time.RFC3339Nano)
	}
	if deletionEffectiveAt.Valid {
		user["deletion_effective_at"] = deletionEffectiveAt.Time.UTC().Format(time.RFC3339Nano)
	}
	if deletionCompletedAt.Valid {
		user["deletion_completed_at"] = deletionCompletedAt.Time.UTC().Format(time.RFC3339Nano)
	}
	user["is_pending_deletion"] = strings.EqualFold(deletionState, "PENDING")
	return user, nil
}

func (s *Service) exportAccountDevices(ctx context.Context, userID string) ([]map[string]any, error) {
	rows, err := s.db.Query(ctx, `
		SELECT
			id::text,
			platform,
			COALESCE(device_name, ''),
			COALESCE(client_version, ''),
			COALESCE(capabilities, ARRAY[]::text[]),
			COALESCE(sms_role_state, ''),
			COALESCE(push_token, ''),
			COALESCE(push_provider, ''),
			COALESCE(public_key, ''),
			COALESCE(last_seen_at, created_at),
			created_at,
			updated_at
		FROM devices
		WHERE user_id = $1::uuid
		ORDER BY updated_at DESC, created_at DESC
	`, userID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	devices := make([]map[string]any, 0)
	for rows.Next() {
		var id, platform, deviceName, clientVersion, smsRoleState, pushToken, pushProvider, publicKey string
		var capabilities []string
		var lastSeenAt, createdAt, updatedAt time.Time
		if err := rows.Scan(&id, &platform, &deviceName, &clientVersion, &capabilities, &smsRoleState, &pushToken, &pushProvider, &publicKey, &lastSeenAt, &createdAt, &updatedAt); err != nil {
			return nil, err
		}
		devices = append(devices, map[string]any{
			"device_id":      id,
			"platform":       platform,
			"device_name":    deviceName,
			"client_version": clientVersion,
			"capabilities":   capabilities,
			"sms_role_state": smsRoleState,
			"push_token":     pushToken,
			"push_provider":  pushProvider,
			"public_key":     publicKey,
			"last_seen_at":   lastSeenAt.UTC().Format(time.RFC3339Nano),
			"created_at":     createdAt.UTC().Format(time.RFC3339Nano),
			"updated_at":     updatedAt.UTC().Format(time.RFC3339Nano),
		})
	}
	return devices, rows.Err()
}

func (s *Service) exportAccountSecurity(ctx context.Context, userID string) (map[string]any, error) {
	refreshTokens, err := s.exportAccountRefreshTokens(ctx, userID)
	if err != nil {
		return nil, err
	}
	twoFactorMethods, err := s.exportAccountTwoFactorMethods(ctx, userID)
	if err != nil {
		return nil, err
	}
	recoveryCodes, err := s.exportAccountRecoveryCodes(ctx, userID)
	if err != nil {
		return nil, err
	}
	deletionRequests, err := s.exportAccountDeletionRequests(ctx, userID)
	if err != nil {
		return nil, err
	}

	return map[string]any{
		"refresh_tokens":     refreshTokens,
		"two_factor_methods": twoFactorMethods,
		"recovery_codes":     recoveryCodes,
		"deletion_requests":  deletionRequests,
	}, nil
}

func (s *Service) exportAccountRefreshTokens(ctx context.Context, userID string) ([]map[string]any, error) {
	rows, err := s.db.Query(ctx, `
		SELECT
			id::text,
			COALESCE(device_id::text, ''),
			expires_at,
			revoked_at,
			created_at
		FROM refresh_tokens
		WHERE user_id = $1::uuid
		ORDER BY created_at DESC
	`, userID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	out := make([]map[string]any, 0)
	for rows.Next() {
		var id, deviceID string
		var expiresAt time.Time
		var revokedAt sql.NullTime
		var createdAt time.Time
		if err := rows.Scan(&id, &deviceID, &expiresAt, &revokedAt, &createdAt); err != nil {
			return nil, err
		}
		status := "ACTIVE"
		if !revokedAt.Valid && time.Now().UTC().After(expiresAt.UTC()) {
			status = "EXPIRED"
		}
		if revokedAt.Valid {
			status = "REVOKED"
		}
		item := map[string]any{
			"token_id":   id,
			"device_id":  deviceID,
			"status":     status,
			"expires_at": expiresAt.UTC().Format(time.RFC3339Nano),
			"created_at": createdAt.UTC().Format(time.RFC3339Nano),
			"revoked":    revokedAt.Valid,
		}
		if revokedAt.Valid {
			item["revoked_at"] = revokedAt.Time.UTC().Format(time.RFC3339Nano)
		}
		out = append(out, item)
	}
	return out, rows.Err()
}

func (s *Service) exportAccountTwoFactorMethods(ctx context.Context, userID string) ([]map[string]any, error) {
	rows, err := s.db.Query(ctx, `
		SELECT id::text, COALESCE(method_type, ''), COALESCE(identifier, ''), enabled, created_at
		FROM two_factor_methods
		WHERE user_id = $1::uuid
		ORDER BY created_at DESC
	`, userID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	out := make([]map[string]any, 0)
	for rows.Next() {
		var id, methodType, identifier string
		var enabled bool
		var createdAt time.Time
		if err := rows.Scan(&id, &methodType, &identifier, &enabled, &createdAt); err != nil {
			return nil, err
		}
		item := map[string]any{
			"method_id":   id,
			"method_type": methodType,
			"enabled":     enabled,
			"created_at":  createdAt.UTC().Format(time.RFC3339Nano),
		}
		if strings.EqualFold(methodType, "sms") {
			item["identifier"] = identifier
		} else if identifier != "" {
			item["identifier_redacted"] = true
		}
		out = append(out, item)
	}
	return out, rows.Err()
}

func (s *Service) exportAccountRecoveryCodes(ctx context.Context, userID string) ([]map[string]any, error) {
	rows, err := s.db.Query(ctx, `
		SELECT id::text, code, used, used_at, created_at, expires_at
		FROM account_recovery_codes
		WHERE user_id = $1::uuid
		ORDER BY created_at DESC
	`, userID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	out := make([]map[string]any, 0)
	for rows.Next() {
		var id, code string
		var used bool
		var usedAt sql.NullTime
		var createdAt time.Time
		var expiresAt sql.NullTime
		if err := rows.Scan(&id, &code, &used, &usedAt, &createdAt, &expiresAt); err != nil {
			return nil, err
		}
		item := map[string]any{
			"recovery_code_id": id,
			"code":             code,
			"used":             used,
			"created_at":       createdAt.UTC().Format(time.RFC3339Nano),
		}
		if usedAt.Valid {
			item["used_at"] = usedAt.Time.UTC().Format(time.RFC3339Nano)
		}
		if expiresAt.Valid {
			item["expires_at"] = expiresAt.Time.UTC().Format(time.RFC3339Nano)
		}
		if used {
			item["status"] = "USED"
		} else if expiresAt.Valid && time.Now().UTC().After(expiresAt.Time.UTC()) {
			item["status"] = "EXPIRED"
		} else {
			item["status"] = "ACTIVE"
		}
		out = append(out, item)
	}
	return out, rows.Err()
}

func (s *Service) exportAccountDeletionRequests(ctx context.Context, userID string) ([]map[string]any, error) {
	rows, err := s.db.Query(ctx, `
		SELECT id::text, requested_at, effective_at, completed_at, status, COALESCE(reason, '')
		FROM account_deletion_audit
		WHERE user_id = $1::uuid
		ORDER BY requested_at DESC
	`, userID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	out := make([]map[string]any, 0)
	for rows.Next() {
		var id, status, reason string
		var requestedAt, effectiveAt time.Time
		var completedAt sql.NullTime
		if err := rows.Scan(&id, &requestedAt, &effectiveAt, &completedAt, &status, &reason); err != nil {
			return nil, err
		}
		item := map[string]any{
			"request_id":   id,
			"status":       status,
			"reason":       reason,
			"requested_at": requestedAt.UTC().Format(time.RFC3339Nano),
			"effective_at": effectiveAt.UTC().Format(time.RFC3339Nano),
		}
		if completedAt.Valid {
			item["completed_at"] = completedAt.Time.UTC().Format(time.RFC3339Nano)
		}
		out = append(out, item)
	}
	return out, rows.Err()
}

func (s *Service) exportAccountConversations(ctx context.Context, userID string) ([]map[string]any, error) {
	rows, err := s.db.Query(ctx, `
		SELECT
			c.id::text,
			c.type,
			COALESCE(c.title, ''),
			COALESCE(c.avatar_url, ''),
			COALESCE(c.description, ''),
			COALESCE(c.created_by_user_id::text, ''),
			COALESCE(c.encryption_state, 'PLAINTEXT'),
			COALESCE(c.encryption_epoch, 0),
			COALESCE(c.allow_message_effects, TRUE),
			COALESCE(c.theme, ''),
			COALESCE(c.retention_seconds, 0),
			c.expires_at,
			COALESCE(c.settings_version, 1),
			COALESCE(c.settings_updated_at, c.updated_at),
			COALESCE(ucs.updated_at, c.updated_at),
			COALESCE(ucs.last_message_preview, ''),
			COALESCE(ucs.unread_count, 0),
			COALESCE(ucs.nickname, ''),
			COALESCE(cm.role, 'MEMBER'),
			COALESCE(ucs.is_closed, false),
			COALESCE(ucs.is_archived, false),
			COALESCE(ucs.is_pinned, false),
			ucs.muted_until,
			cm.joined_at,
			cm.last_read_server_order,
			cm.last_delivered_server_order,
			cm.read_at,
			cm.delivery_at,
			EXISTS (
				SELECT 1
				FROM conversation_members others
				JOIN user_blocks ub
				  ON ub.blocker_user_id = cm.user_id
				 AND ub.blocked_user_id = others.user_id
				WHERE others.conversation_id = c.id
				  AND others.user_id <> cm.user_id
			),
			EXISTS (
				SELECT 1
				FROM conversation_members others
				JOIN user_blocks ub
				  ON ub.blocker_user_id = others.user_id
				 AND ub.blocked_user_id = cm.user_id
				WHERE others.conversation_id = c.id
				  AND others.user_id <> cm.user_id
			),
			COALESCE(participants_meta.participants, '[]'::jsonb),
			COALESCE(external_meta.external_phones, '[]'::jsonb)
		FROM conversations c
		JOIN conversation_members cm ON cm.conversation_id = c.id
		LEFT JOIN user_conversation_state ucs
		  ON ucs.conversation_id = c.id
		 AND ucs.user_id = cm.user_id
		LEFT JOIN LATERAL (
			SELECT jsonb_agg(cm2.user_id::text ORDER BY cm2.joined_at) AS participants
			FROM conversation_members cm2
			WHERE cm2.conversation_id = c.id
		) participants_meta ON TRUE
		LEFT JOIN LATERAL (
			SELECT jsonb_agg(ec.phone_e164 ORDER BY ec.phone_e164) AS external_phones
			FROM conversation_external_members cem
			JOIN external_contacts ec ON ec.id = cem.external_contact_id
			WHERE cem.conversation_id = c.id
		) external_meta ON TRUE
		WHERE cm.user_id = $1::uuid
		ORDER BY COALESCE(ucs.updated_at, c.updated_at) DESC, c.updated_at DESC
	`, userID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	out := make([]map[string]any, 0)
	for rows.Next() {
		var (
			conversationID           string
			convType                 string
			title                    string
			avatarURL                string
			description              string
			creatorUserID            string
			encryptionState          string
			encryptionEpoch          int64
			allowEffects             bool
			theme                    string
			retentionSecs            int64
			expiresAt                sql.NullTime
			settingsVersion          int64
			settingsUpdatedAt        time.Time
			updatedAt                time.Time
			lastMessagePreview       string
			unreadCount              int64
			nickname                 string
			viewerRole               string
			closed                   bool
			archived                 bool
			pinned                   bool
			mutedUntil               sql.NullTime
			joinedAt                 time.Time
			lastReadServerOrder      int64
			lastDeliveredServerOrder int64
			readAt                   sql.NullTime
			deliveryAt               sql.NullTime
			blockedByViewer          bool
			blockedByOther           bool
			participantsRaw          []byte
			externalPhonesRaw        []byte
		)
		if err := rows.Scan(
			&conversationID,
			&convType,
			&title,
			&avatarURL,
			&description,
			&creatorUserID,
			&encryptionState,
			&encryptionEpoch,
			&allowEffects,
			&theme,
			&retentionSecs,
			&expiresAt,
			&settingsVersion,
			&settingsUpdatedAt,
			&updatedAt,
			&lastMessagePreview,
			&unreadCount,
			&nickname,
			&viewerRole,
			&closed,
			&archived,
			&pinned,
			&mutedUntil,
			&joinedAt,
			&lastReadServerOrder,
			&lastDeliveredServerOrder,
			&readAt,
			&deliveryAt,
			&blockedByViewer,
			&blockedByOther,
			&participantsRaw,
			&externalPhonesRaw,
		); err != nil {
			return nil, err
		}

		item := map[string]any{
			"conversation_id":             conversationID,
			"type":                        convType,
			"title":                       title,
			"avatar_url":                  avatarURL,
			"description":                 description,
			"creator_user_id":             creatorUserID,
			"encryption_state":            encryptionState,
			"encryption_epoch":            encryptionEpoch,
			"allow_message_effects":       allowEffects,
			"theme":                       theme,
			"retention_seconds":           retentionSecs,
			"settings_version":            settingsVersion,
			"settings_updated_at":         settingsUpdatedAt.UTC().Format(time.RFC3339Nano),
			"updated_at":                  updatedAt.UTC().Format(time.RFC3339Nano),
			"last_message_preview":        lastMessagePreview,
			"unread_count":                unreadCount,
			"nickname":                    nickname,
			"viewer_role":                 viewerRole,
			"closed":                      closed,
			"archived":                    archived,
			"pinned":                      pinned,
			"joined_at":                   joinedAt.UTC().Format(time.RFC3339Nano),
			"last_read_server_order":      lastReadServerOrder,
			"last_delivered_server_order": lastDeliveredServerOrder,
			"blocked_by_viewer":           blockedByViewer,
			"blocked_by_other":            blockedByOther,
			"blocked":                     blockedByViewer || blockedByOther,
		}
		if expiresAt.Valid {
			item["expires_at"] = expiresAt.Time.UTC().Format(time.RFC3339Nano)
		}
		if mutedUntil.Valid {
			item["muted_until"] = mutedUntil.Time.UTC().Format(time.RFC3339Nano)
		}
		if readAt.Valid {
			item["read_at"] = readAt.Time.UTC().Format(time.RFC3339Nano)
		}
		if deliveryAt.Valid {
			item["delivery_at"] = deliveryAt.Time.UTC().Format(time.RFC3339Nano)
		}
		if len(participantsRaw) > 0 {
			var participants []string
			if err := json.Unmarshal(participantsRaw, &participants); err == nil {
				item["participants"] = participants
			}
		}
		if len(externalPhonesRaw) > 0 {
			var phones []string
			if err := json.Unmarshal(externalPhonesRaw, &phones); err == nil {
				item["external_phones"] = phones
			}
		}
		out = append(out, item)
	}
	return out, rows.Err()
}

func (s *Service) exportAccountMessages(ctx context.Context, userID string) ([]map[string]any, error) {
	rows, err := s.db.Query(ctx, `
		SELECT
			m.id::text,
			m.conversation_id::text,
			m.sender_user_id::text,
			COALESCE(m.sender_device_id::text, ''),
			m.content_type,
			CASE
				WHEN m.deleted_at IS NOT NULL OR m.visibility_state = 'SOFT_DELETED' THEN '{}'::jsonb
				ELSE COALESCE(m.content, '{}'::jsonb)
			END AS content,
			COALESCE(m.client_generated_id, ''),
			m.transport,
			m.server_order,
			m.created_at,
			m.edited_at,
			m.deleted_at,
			m.visibility_state,
			COALESCE(attachments_meta.attachments, '[]'::jsonb),
			COALESCE(reaction_meta.reactions, '[]'::jsonb),
			COALESCE(read_meta.read_receipts, '[]'::jsonb),
			COALESCE(effect_meta.effects, '[]'::jsonb)
		FROM messages m
		JOIN conversation_members cm
		  ON cm.conversation_id = m.conversation_id
		 AND cm.user_id = $1::uuid
		LEFT JOIN LATERAL (
			SELECT jsonb_agg(jsonb_build_object(
				'attachment_id', a.attachment_id::text,
				'object_key', COALESCE(a.object_key, ''),
				'thumbnail_key', COALESCE(a.thumbnail_key, ''),
				'mime_type', a.mime_type,
				'size_bytes', COALESCE(a.size_bytes, 0),
				'created_at', a.created_at,
				'deleted_at', a.deleted_at,
				'redacted_at', a.redacted_at
			) ORDER BY a.created_at ASC) AS attachments
			FROM attachments a
			WHERE a.message_id = m.id
		) attachments_meta ON TRUE
		LEFT JOIN LATERAL (
			SELECT jsonb_agg(jsonb_build_object(
				'user_id', mr.user_id::text,
				'emoji', mr.emoji,
				'created_at', mr.created_at
			) ORDER BY mr.created_at ASC) AS reactions
			FROM message_reactions mr
			WHERE mr.message_id = m.id
		) reaction_meta ON TRUE
		LEFT JOIN LATERAL (
			SELECT jsonb_agg(jsonb_build_object(
				'reader_user_id', mrr.reader_user_id::text,
				'read_at', mrr.read_at
			) ORDER BY mrr.read_at ASC) AS read_receipts
			FROM message_read_receipts mrr
			WHERE mrr.message_id = m.id
		) read_meta ON TRUE
		LEFT JOIN LATERAL (
			SELECT jsonb_agg(jsonb_build_object(
				'triggered_by_user_id', me.triggered_by_user_id::text,
				'effect_type', me.effect_type,
				'triggered_at', me.triggered_at
			) ORDER BY me.triggered_at ASC) AS effects
			FROM message_effects me
			WHERE me.message_id = m.id
		) effect_meta ON TRUE
		ORDER BY m.conversation_id ASC, m.server_order ASC
	`, userID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	out := make([]map[string]any, 0)
	for rows.Next() {
		var (
			messageID         string
			conversationID    string
			senderUserID      string
			senderDeviceID    string
			contentType       string
			contentRaw        []byte
			clientGeneratedID string
			transport         string
			serverOrder       int64
			createdAt         time.Time
			editedAt          sql.NullTime
			deletedAt         sql.NullTime
			visibilityState   string
			attachmentsRaw    []byte
			reactionsRaw      []byte
			readReceiptsRaw   []byte
			effectsRaw        []byte
		)
		if err := rows.Scan(
			&messageID,
			&conversationID,
			&senderUserID,
			&senderDeviceID,
			&contentType,
			&contentRaw,
			&clientGeneratedID,
			&transport,
			&serverOrder,
			&createdAt,
			&editedAt,
			&deletedAt,
			&visibilityState,
			&attachmentsRaw,
			&reactionsRaw,
			&readReceiptsRaw,
			&effectsRaw,
		); err != nil {
			return nil, err
		}

		content := map[string]any{}
		if len(contentRaw) > 0 {
			_ = json.Unmarshal(contentRaw, &content)
		}
		item := map[string]any{
			"message_id":          messageID,
			"conversation_id":     conversationID,
			"sender_user_id":      senderUserID,
			"sender_device_id":    senderDeviceID,
			"content_type":        contentType,
			"content":             content,
			"client_generated_id": clientGeneratedID,
			"transport":           transport,
			"server_order":        serverOrder,
			"created_at":          createdAt.UTC().Format(time.RFC3339Nano),
			"visibility_state":    visibilityState,
		}
		if editedAt.Valid {
			item["edited_at"] = editedAt.Time.UTC().Format(time.RFC3339Nano)
		}
		if deletedAt.Valid {
			item["deleted_at"] = deletedAt.Time.UTC().Format(time.RFC3339Nano)
		}
		if len(attachmentsRaw) > 0 {
			var attachments []map[string]any
			if err := json.Unmarshal(attachmentsRaw, &attachments); err == nil {
				item["attachments"] = attachments
			}
		}
		if len(reactionsRaw) > 0 {
			var reactions []map[string]any
			if err := json.Unmarshal(reactionsRaw, &reactions); err == nil {
				item["reactions"] = reactions
			}
		}
		if len(readReceiptsRaw) > 0 {
			var receipts []map[string]any
			if err := json.Unmarshal(readReceiptsRaw, &receipts); err == nil {
				item["read_receipts"] = receipts
			}
		}
		if len(effectsRaw) > 0 {
			var effects []map[string]any
			if err := json.Unmarshal(effectsRaw, &effects); err == nil {
				item["effects"] = effects
			}
		}
		out = append(out, item)
	}
	return out, rows.Err()
}

// BlockUser creates a block relationship between two users
func (s *Service) BlockUser(ctx context.Context, actorID, targetID string) error {
	if _, err := s.db.Exec(ctx, `
		INSERT INTO user_blocks (blocker_user_id, blocked_user_id, created_at)
		VALUES ($1::uuid, $2::uuid, now())
		ON CONFLICT DO NOTHING
	`, actorID, targetID); err != nil {
		return err
	}
	return s.emitBlockStateUpdates(ctx, actorID, targetID)
}

// UnblockUser removes a block relationship between two users
func (s *Service) UnblockUser(ctx context.Context, actorID, targetID string) error {
	if _, err := s.db.Exec(ctx, `DELETE FROM user_blocks WHERE blocker_user_id = $1::uuid AND blocked_user_id = $2::uuid`, actorID, targetID); err != nil {
		return err
	}
	return s.emitBlockStateUpdates(ctx, actorID, targetID)
}

// HasBlocked checks if one user has blocked another
func (s *Service) HasBlocked(ctx context.Context, blockerID, blockedID string) (bool, error) {
	var exists bool
	if err := s.db.QueryRow(ctx, `
		SELECT EXISTS(SELECT 1 FROM user_blocks WHERE blocker_user_id = $1::uuid AND blocked_user_id = $2::uuid)
	`, blockerID, blockedID).Scan(&exists); err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return false, nil
		}
		return false, err
	}
	return exists, nil
}

// emitBlockStateUpdates notifies about block state changes in shared conversations
func (s *Service) emitBlockStateUpdates(ctx context.Context, actorID, targetID string) error {
	if s.replication == nil {
		return nil
	}

	// Check current block states
	actorBlocksTarget, err := s.HasBlocked(ctx, actorID, targetID)
	if err != nil {
		return err
	}
	targetBlocksActor, err := s.HasBlocked(ctx, targetID, actorID)
	if err != nil {
		return err
	}

	// Get shared conversations
	rows, err := s.db.Query(ctx, `
		SELECT DISTINCT c.id::text
		FROM conversations c
		JOIN conversation_members cm1 ON cm1.conversation_id = c.id AND cm1.user_id = $1::uuid
		JOIN conversation_members cm2 ON cm2.conversation_id = c.id AND cm2.user_id = $2::uuid
	`, actorID, targetID)
	if err != nil {
		return err
	}
	defer rows.Close()

	// Emit events for each shared conversation
	for rows.Next() {
		var conversationID string
		if err := rows.Scan(&conversationID); err != nil {
			return err
		}

		// Emit for actor
		s.replication.EmitUserEvent(ctx, actorID, conversationID, replication.UserEventConversationStateUpdated, map[string]any{
			"conversation_id":   conversationID,
			"blocked_by_viewer": actorBlocksTarget,
			"blocked_by_other":  targetBlocksActor,
			"blocked":           actorBlocksTarget || targetBlocksActor,
			"updated_at":        time.Now().UTC().Format(time.RFC3339Nano),
		})

		// Emit for target
		s.replication.EmitUserEvent(ctx, targetID, conversationID, replication.UserEventConversationStateUpdated, map[string]any{
			"conversation_id":   conversationID,
			"blocked_by_viewer": targetBlocksActor,
			"blocked_by_other":  actorBlocksTarget,
			"blocked":           actorBlocksTarget || targetBlocksActor,
			"updated_at":        time.Now().UTC().Format(time.RFC3339Nano),
		})
	}

	return rows.Err()
}

// ListBlockedUsers retrieves users blocked by the current user
func (s *Service) ListBlockedUsers(ctx context.Context, userID string) ([]Profile, error) {
	rows, err := s.db.Query(ctx, `
		SELECT u.id::text, COALESCE(u.display_name, ''), COALESCE(u.avatar_url, ''), COALESCE(u.primary_phone_e164, '')
		FROM users u
		INNER JOIN user_blocks ub ON ub.blocked_user_id = u.id
		WHERE ub.blocker_user_id = $1::uuid
	`, userID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var blocked []Profile
	for rows.Next() {
		var p Profile
		if err := rows.Scan(&p.UserID, &p.DisplayName, &p.AvatarURL, &p.PhoneE164); err != nil {
			return nil, err
		}
		blocked = append(blocked, p)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}

	return blocked, nil
}

// GenerateRecoveryCodes creates recovery codes for a user
func (s *Service) GenerateRecoveryCodes(ctx context.Context, userID string) ([]string, error) {
	tx, err := s.db.BeginTx(ctx, pgx.TxOptions{})
	if err != nil {
		return nil, err
	}
	defer tx.Rollback(ctx)

	codes := make([]string, 0, 10)
	seen := make(map[string]struct{}, 10)
	for len(codes) < 10 {
		code, err := generateRandomCode(8)
		if err != nil {
			return nil, err
		}
		if _, exists := seen[code]; exists {
			continue
		}
		seen[code] = struct{}{}
		codes = append(codes, code)
	}

	// Delete any existing unused codes and replace them atomically.
	if _, err := tx.Exec(ctx, `
		DELETE FROM account_recovery_codes
		WHERE user_id = $1::uuid AND used = FALSE
	`, userID); err != nil {
		return nil, err
	}

	// Insert new codes with 90-day expiry
	expiresAt := time.Now().AddDate(0, 0, 90)
	for _, code := range codes {
		if _, err := tx.Exec(ctx, `
			INSERT INTO account_recovery_codes (user_id, code, used, created_at, expires_at)
			VALUES ($1::uuid, $2, FALSE, now(), $3)
		`, userID, code, expiresAt); err != nil {
			return nil, err
		}
	}

	if err := tx.Commit(ctx); err != nil {
		return nil, err
	}

	return codes, nil
}

// ValidateRecoveryCode checks if a recovery code is valid and marks it as used
func (s *Service) ValidateRecoveryCode(ctx context.Context, userID, code string) (bool, error) {
	tx, err := s.db.BeginTx(ctx, pgx.TxOptions{})
	if err != nil {
		return false, err
	}
	defer tx.Rollback(ctx)

	var id int64
	var used bool
	var expiresAt *time.Time

	err = tx.QueryRow(ctx, `
		SELECT id, used, expires_at FROM account_recovery_codes
		WHERE user_id = $1::uuid AND code = $2
		FOR UPDATE
	`, userID, code).Scan(&id, &used, &expiresAt)

	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return false, nil
		}
		return false, err
	}

	// Check if already used
	if used {
		return false, nil
	}

	// Check if expired
	if expiresAt != nil && time.Now().After(*expiresAt) {
		return false, nil
	}

	// Mark as used
	if _, err := tx.Exec(ctx, `
		UPDATE account_recovery_codes SET used = TRUE, used_at = now() WHERE id = $1
	`, id); err != nil {
		return false, err
	}

	if err := tx.Commit(ctx); err != nil {
		return false, err
	}

	return true, nil
}

// Enable2FA sets up 2FA for a user (SMS or TOTP)
func (s *Service) Enable2FA(ctx context.Context, userID, methodType string) (string, error) {
	// For TOTP, we'd need to generate a secret and return a provisioning URL
	// For SMS, we'd return a verification URL
	if methodType != "sms" && methodType != "totp" {
		return "", errors.New("invalid 2fa method type")
	}

	tx, err := s.db.BeginTx(ctx, pgx.TxOptions{})
	if err != nil {
		return "", err
	}
	defer tx.Rollback(ctx)

	_, err = tx.Exec(ctx, `
		DELETE FROM two_factor_methods
		WHERE user_id = $1::uuid AND method_type = $2
	`, userID, methodType)
	if err != nil {
		return "", err
	}

	// For TOTP, generate a shared secret. For SMS, keep the user's primary phone as the identifier.
	var identifier string
	if methodType == "totp" {
		identifier, err = generateRandomCode(32)
		if err != nil {
			return "", err
		}
	} else {
		_ = tx.QueryRow(ctx, `SELECT COALESCE(primary_phone_e164, '') FROM users WHERE id = $1::uuid`, userID).Scan(&identifier)
	}

	if _, err := tx.Exec(ctx, `
		INSERT INTO two_factor_methods (user_id, method_type, identifier, enabled, created_at)
		VALUES ($1::uuid, $2, $3, FALSE, now())
	`, userID, methodType, identifier); err != nil {
		return "", err
	}

	if err := tx.Commit(ctx); err != nil {
		return "", err
	}

	return identifier, nil
}

// Verify2FA verifies and enables 2FA for a user
func (s *Service) Verify2FA(ctx context.Context, userID, methodType, code string) (bool, error) {
	// In a real implementation, this would verify against TOTP or SMS
	// For now, we'll just enable the method if the code is provided
	if code == "" {
		return false, errors.New("code required")
	}

	result, err := s.db.Exec(ctx, `
		UPDATE two_factor_methods
		SET enabled = TRUE
		WHERE user_id = $1::uuid AND method_type = $2
	`, userID, methodType)

	if err != nil {
		return false, err
	}

	return result.RowsAffected() > 0, nil
}

// List2FAMethods retrieves active 2FA methods for a user
func (s *Service) List2FAMethods(ctx context.Context, userID string) ([]map[string]any, error) {
	rows, err := s.db.Query(ctx, `
		SELECT id, method_type, enabled, created_at
		FROM two_factor_methods
		WHERE user_id = $1::uuid AND enabled = TRUE
	`, userID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var methods []map[string]any
	for rows.Next() {
		var id int64
		var methodType string
		var enabled bool
		var createdAt time.Time

		if err := rows.Scan(&id, &methodType, &enabled, &createdAt); err != nil {
			return nil, err
		}

		methods = append(methods, map[string]any{
			"id":          id,
			"method_type": methodType,
			"enabled":     enabled,
			"created_at":  createdAt,
		})
	}

	return methods, rows.Err()
}

// generateRandomCode generates a random alphanumeric code
func generateRandomCode(length int) (string, error) {
	const chars = "ABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789"
	if length <= 0 {
		return "", nil
	}
	out := make([]byte, length)
	max := big.NewInt(int64(len(chars)))
	for i := range out {
		n, err := cryptoRand.Int(cryptoRand.Reader, max)
		if err != nil {
			return "", err
		}
		out[i] = chars[n.Int64()]
	}
	return string(out), nil
}

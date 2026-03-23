package devicekeys

import (
	"context"
	"crypto/ed25519"
	"crypto/rand"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"golang.org/x/crypto/curve25519"
	"ohmf/services/gateway/internal/e2ee"
	"ohmf/services/gateway/internal/securityaudit"
)

type Service struct {
	pool *pgxpool.Pool
}

type KeyBackup struct {
	BackupName         string         `json:"backup_name"`
	SourceDeviceID     string         `json:"source_device_id,omitempty"`
	EncryptedBlob      string         `json:"encrypted_blob,omitempty"`
	WrappingAlg        string         `json:"wrapping_alg"`
	WrappedKey         string         `json:"wrapped_key,omitempty"`
	RecoveryData       map[string]any `json:"recovery_data,omitempty"`
	AttestationType    string         `json:"attestation_type,omitempty"`
	AttestationPayload map[string]any `json:"attestation_payload,omitempty"`
	BackupHash         string         `json:"backup_hash,omitempty"`
	CreatedAt          string         `json:"created_at,omitempty"`
	UpdatedAt          string         `json:"updated_at,omitempty"`
	LastRestoredAt     string         `json:"last_restored_at,omitempty"`
}

type UpsertBackupRequest struct {
	SourceDeviceID     string         `json:"source_device_id,omitempty"`
	EncryptedBlob      string         `json:"encrypted_blob"`
	WrappingAlg        string         `json:"wrapping_alg,omitempty"`
	WrappedKey         string         `json:"wrapped_key,omitempty"`
	RecoveryData       map[string]any `json:"recovery_data,omitempty"`
	AttestationType    string         `json:"attestation_type,omitempty"`
	AttestationPayload map[string]any `json:"attestation_payload,omitempty"`
	BackupHash         string         `json:"backup_hash,omitempty"`
}

var ErrBackupNotFound = errors.New("backup_not_found")

func NewService(pool *pgxpool.Pool) *Service {
	return &Service{pool: pool}
}

// DB returns the underlying database pool for handlers
func (s *Service) DB() *pgxpool.Pool {
	return s.pool
}

// GenerateAndPublishDefaultBundle creates and publishes initial E2EE keys for a newly registered device
func (s *Service) GenerateAndPublishDefaultBundle(ctx context.Context, userID, deviceID string) error {
	// Generate X25519 identity keypair (ECDH)
	_, identityPub, err := generateX25519Keypair()
	if err != nil {
		return fmt.Errorf("failed to generate identity keys: %w", err)
	}

	// Generate Ed25519 signing keypair
	signingPub, signingPriv, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		return fmt.Errorf("failed to generate signing keys: %w", err)
	}

	// Generate signed prekey (short-lived identity key + signature)
	_, signedPrekeyPub, err := generateX25519Keypair()
	if err != nil {
		return fmt.Errorf("failed to generate signed prekey: %w", err)
	}
	// Sign the prekey public key with the Ed25519 signing key
	signedPrekeySignatureBinary := ed25519.Sign(signingPriv, signedPrekeyPub[:])
	signedPrekeySignature := base64.StdEncoding.EncodeToString(signedPrekeySignatureBinary)

	// Encode keys to base64
	identityPubB64 := base64.StdEncoding.EncodeToString(identityPub[:])
	signingPubB64 := base64.StdEncoding.EncodeToString(signingPub[:])
	signedPrekeyPubB64 := base64.StdEncoding.EncodeToString(signedPrekeyPub[:])

	// Compute fingerprint for trust verification
	fingerprint, err := e2ee.ComputeFingerprint(signingPubB64)
	if err != nil {
		return fmt.Errorf("failed to compute fingerprint: %w", err)
	}

	// Store in database as a transaction
	tx, err := s.pool.BeginTx(ctx, pgx.TxOptions{})
	if err != nil {
		return err
	}
	defer tx.Rollback(ctx)

	// Insert device identity keys
	_, err = tx.Exec(ctx, `
		INSERT INTO device_identity_keys (
			user_id, device_id, key_version,
			identity_key_alg, identity_public_key,
			agreement_identity_public_key,
			signing_key_alg, signing_public_key,
			signed_prekey_id, signed_prekey_public_key, signed_prekey_signature,
			fingerprint, bundle_version,
			published_at, updated_at
		)
		VALUES (
			$1::uuid, $2::uuid, '1',
			'X25519', $3,
			$4,
			'Ed25519', $5,
			'0', $6, $7,
			$8, 'OHMF_LEGACY_V0',
			NOW(), NOW()
		)
		ON CONFLICT (user_id, device_id) DO UPDATE SET
			identity_public_key = EXCLUDED.identity_public_key,
			agreement_identity_public_key = EXCLUDED.agreement_identity_public_key,
			signing_public_key = EXCLUDED.signing_public_key,
			signed_prekey_id = EXCLUDED.signed_prekey_id,
			signed_prekey_public_key = EXCLUDED.signed_prekey_public_key,
			signed_prekey_signature = EXCLUDED.signed_prekey_signature,
			fingerprint = EXCLUDED.fingerprint,
			updated_at = NOW()
	`, userID, deviceID, identityPubB64, identityPubB64, signingPubB64, signedPrekeyPubB64, signedPrekeySignature, fingerprint)
	if err != nil {
		return fmt.Errorf("failed to insert identity keys: %w", err)
	}

	// Generate and insert 5 one-time prekeys
	for i := 0; i < 5; i++ {
		_, otpPub, err := generateX25519Keypair()
		if err != nil {
			return fmt.Errorf("failed to generate one-time prekey %d: %w", i, err)
		}
		// Use a numeric prekey ID - for now just use a simple counter
		// In production, this should use sequences or UUIDs
		_, err = tx.Exec(ctx, `
			INSERT INTO device_one_time_prekeys (
				device_id, prekey_id, public_key, created_at
			)
			VALUES (
				$1::uuid, (EXTRACT(EPOCH FROM now())::bigint * 100000 + $2), $3, NOW()
			)
		`, deviceID, i,
			base64.StdEncoding.EncodeToString(otpPub[:]))
		if err != nil {
			return fmt.Errorf("failed to insert one-time prekey %d: %w", i, err)
		}
	}

	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("failed to commit key generation transaction: %w", err)
	}

	return nil
}

// Helper function to generate X25519 keypair
func generateX25519Keypair() ([32]byte, [32]byte, error) {
	var privateKey [32]byte
	_, err := rand.Read(privateKey[:])
	if err != nil {
		return [32]byte{}, [32]byte{}, err
	}
	publicKey, err := curve25519.X25519(privateKey[:], curve25519.Basepoint[:])
	if err != nil {
		return [32]byte{}, [32]byte{}, err
	}
	var pubKeyArray [32]byte
	copy(pubKeyArray[:], publicKey)
	return privateKey, pubKeyArray, nil
}


func (s *Service) PublishBundle(ctx context.Context, actorUserID, deviceID string, req PublishRequest) (Bundle, error) {
	return (&Handler{DB: s.pool}).PublishBundle(ctx, actorUserID, deviceID, req)
}

func (s *Service) AddOneTimePrekeys(ctx context.Context, actorUserID, deviceID string, prekeys []OneTimePrekey) (Bundle, error) {
	return (&Handler{DB: s.pool}).AddOneTimePrekeys(ctx, actorUserID, deviceID, prekeys)
}

func (s *Service) ListBundlesForUser(ctx context.Context, userID string) ([]Bundle, error) {
	return (&Handler{DB: s.pool}).ListBundlesForUser(ctx, userID)
}

func (s *Service) ClaimBundles(ctx context.Context, userID string) ([]Bundle, error) {
	return (&Handler{DB: s.pool}).ClaimBundles(ctx, userID)
}

func (s *Service) UpsertBackup(ctx context.Context, actorUserID, backupName string, req UpsertBackupRequest) (KeyBackup, error) {
	backupName = strings.TrimSpace(backupName)
	if backupName == "" {
		backupName = "default"
	}
	req.EncryptedBlob = strings.TrimSpace(req.EncryptedBlob)
	if req.EncryptedBlob == "" {
		return KeyBackup{}, errors.New("encrypted_blob_required")
	}
	if req.WrappingAlg == "" {
		req.WrappingAlg = "X25519_AES256GCM"
	}
	if req.RecoveryData == nil {
		req.RecoveryData = map[string]any{}
	}
	if req.AttestationPayload == nil {
		req.AttestationPayload = map[string]any{}
	}
	if strings.TrimSpace(req.SourceDeviceID) != "" {
		var exists bool
		if err := s.pool.QueryRow(ctx, `
			SELECT EXISTS(
				SELECT 1 FROM devices
				WHERE id = $1::uuid AND user_id = $2::uuid
			)
		`, req.SourceDeviceID, actorUserID).Scan(&exists); err != nil {
			return KeyBackup{}, err
		}
		if !exists {
			return KeyBackup{}, ErrDeviceNotOwned
		}
	}
	_, err := s.pool.Exec(ctx, `
		INSERT INTO device_key_backups (
			user_id,
			source_device_id,
			backup_name,
			encrypted_blob,
			wrapping_alg,
			wrapped_key,
			recovery_data,
			attestation_type,
			attestation_payload,
			backup_hash,
			updated_at,
			deleted_at
		)
		VALUES (
			$1::uuid,
			NULLIF($2, '')::uuid,
			$3,
			$4,
			$5,
			NULLIF($6, ''),
			$7::jsonb,
			NULLIF($8, ''),
			$9::jsonb,
			NULLIF($10, ''),
			now(),
			NULL
		)
		ON CONFLICT (user_id, backup_name) WHERE deleted_at IS NULL
		DO UPDATE SET
			source_device_id = EXCLUDED.source_device_id,
			encrypted_blob = EXCLUDED.encrypted_blob,
			wrapping_alg = EXCLUDED.wrapping_alg,
			wrapped_key = EXCLUDED.wrapped_key,
			recovery_data = EXCLUDED.recovery_data,
			attestation_type = EXCLUDED.attestation_type,
			attestation_payload = EXCLUDED.attestation_payload,
			backup_hash = EXCLUDED.backup_hash,
			updated_at = now(),
			deleted_at = NULL
	`, actorUserID, req.SourceDeviceID, backupName, req.EncryptedBlob, req.WrappingAlg, req.WrappedKey, mustJSON(req.RecoveryData), req.AttestationType, mustJSON(req.AttestationPayload), req.BackupHash)
	if err != nil {
		return KeyBackup{}, err
	}
	_ = securityaudit.Append(ctx, s.pool, actorUserID, actorUserID, "device_key_backup_upserted", map[string]any{
		"backup_name":      backupName,
		"source_device_id": req.SourceDeviceID,
		"backup_hash":      req.BackupHash,
	})
	return s.GetBackup(ctx, actorUserID, backupName, false)
}

func (s *Service) ListBackups(ctx context.Context, actorUserID string) ([]KeyBackup, error) {
	rows, err := s.pool.Query(ctx, `
		SELECT
			backup_name,
			COALESCE(source_device_id::text, ''),
			wrapping_alg,
			COALESCE(attestation_type, ''),
			COALESCE(backup_hash, ''),
			created_at,
			updated_at,
			last_restored_at
		FROM device_key_backups
		WHERE user_id = $1::uuid
		  AND deleted_at IS NULL
		ORDER BY updated_at DESC
	`, actorUserID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	items := make([]KeyBackup, 0, 2)
	for rows.Next() {
		var item KeyBackup
		var createdAt time.Time
		var updatedAt time.Time
		var lastRestoredAt *time.Time
		if err := rows.Scan(&item.BackupName, &item.SourceDeviceID, &item.WrappingAlg, &item.AttestationType, &item.BackupHash, &createdAt, &updatedAt, &lastRestoredAt); err != nil {
			return nil, err
		}
		item.CreatedAt = createdAt.UTC().Format(time.RFC3339Nano)
		item.UpdatedAt = updatedAt.UTC().Format(time.RFC3339Nano)
		if lastRestoredAt != nil {
			item.LastRestoredAt = lastRestoredAt.UTC().Format(time.RFC3339Nano)
		}
		items = append(items, item)
	}
	return items, rows.Err()
}

func (s *Service) GetBackup(ctx context.Context, actorUserID, backupName string, markRestored bool) (KeyBackup, error) {
	backupName = strings.TrimSpace(backupName)
	if backupName == "" {
		backupName = "default"
	}
	if markRestored {
		if _, err := s.pool.Exec(ctx, `
			UPDATE device_key_backups
			SET last_restored_at = now(),
			    updated_at = now()
			WHERE user_id = $1::uuid
			  AND backup_name = $2
			  AND deleted_at IS NULL
		`, actorUserID, backupName); err != nil {
			return KeyBackup{}, err
		}
		_ = securityaudit.Append(ctx, s.pool, actorUserID, actorUserID, "device_key_backup_restored", map[string]any{
			"backup_name": backupName,
		})
	}
	var item KeyBackup
	var recoveryRaw []byte
	var attestationRaw []byte
	var createdAt time.Time
	var updatedAt time.Time
	var lastRestoredAt *time.Time
	err := s.pool.QueryRow(ctx, `
		SELECT
			backup_name,
			COALESCE(source_device_id::text, ''),
			encrypted_blob,
			wrapping_alg,
			COALESCE(wrapped_key, ''),
			recovery_data,
			COALESCE(attestation_type, ''),
			attestation_payload,
			COALESCE(backup_hash, ''),
			created_at,
			updated_at,
			last_restored_at
		FROM device_key_backups
		WHERE user_id = $1::uuid
		  AND backup_name = $2
		  AND deleted_at IS NULL
	`, actorUserID, backupName).Scan(
		&item.BackupName,
		&item.SourceDeviceID,
		&item.EncryptedBlob,
		&item.WrappingAlg,
		&item.WrappedKey,
		&recoveryRaw,
		&item.AttestationType,
		&attestationRaw,
		&item.BackupHash,
		&createdAt,
		&updatedAt,
		&lastRestoredAt,
	)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return KeyBackup{}, ErrBackupNotFound
		}
		return KeyBackup{}, err
	}
	_ = decodeJSONMap(recoveryRaw, &item.RecoveryData)
	_ = decodeJSONMap(attestationRaw, &item.AttestationPayload)
	item.CreatedAt = createdAt.UTC().Format(time.RFC3339Nano)
	item.UpdatedAt = updatedAt.UTC().Format(time.RFC3339Nano)
	if lastRestoredAt != nil {
		item.LastRestoredAt = lastRestoredAt.UTC().Format(time.RFC3339Nano)
	}
	return item, nil
}

func (s *Service) DeleteBackup(ctx context.Context, actorUserID, backupName string) error {
	backupName = strings.TrimSpace(backupName)
	if backupName == "" {
		backupName = "default"
	}
	tag, err := s.pool.Exec(ctx, `
		UPDATE device_key_backups
		SET deleted_at = now(),
		    updated_at = now(),
		    encrypted_blob = '',
		    wrapped_key = NULL,
		    recovery_data = '{}'::jsonb,
		    attestation_payload = '{}'::jsonb
		WHERE user_id = $1::uuid
		  AND backup_name = $2
		  AND deleted_at IS NULL
	`, actorUserID, backupName)
	if err != nil {
		return err
	}
	if tag.RowsAffected() == 0 {
		return ErrBackupNotFound
	}
	_ = securityaudit.Append(ctx, s.pool, actorUserID, actorUserID, "device_key_backup_deleted", map[string]any{
		"backup_name": backupName,
	})
	return nil
}

func mustJSON(value map[string]any) string {
	if value == nil {
		value = map[string]any{}
	}
	raw, _ := json.Marshal(value)
	return string(raw)
}

func decodeJSONMap(raw []byte, target *map[string]any) error {
	if len(raw) == 0 {
		*target = map[string]any{}
		return nil
	}
	return json.Unmarshal(raw, target)
}

// E2EE Device Key Management Methods

// ClaimOneTimePrekey atomically claims the next available one-time prekey for a device
func (s *Service) ClaimOneTimePrekey(ctx context.Context, userID string, deviceID string) (OneTimePrekey, error) {
	var prekey OneTimePrekey

	// Find the first unclaimed prekey and mark it as consumed
	err := s.pool.QueryRow(ctx, `
		UPDATE device_one_time_prekeys
		SET consumed_at = now()
		WHERE device_id = $1::uuid
		  AND consumed_at IS NULL
		ORDER BY prekey_id ASC
		LIMIT 1
		RETURNING prekey_id, public_key
	`, deviceID).Scan(&prekey.PrekeyID, &prekey.PublicKey)

	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return OneTimePrekey{}, errors.New("no_available_prekeys")
		}
		return OneTimePrekey{}, err
	}

	return prekey, nil
}

// CountAvailableOTPrekeyPool counts available (unclaimed) one-time prekeys
func (s *Service) CountAvailableOTPrekeyPool(ctx context.Context, deviceID string) (int64, error) {
	var count int64
	err := s.pool.QueryRow(ctx, `
		SELECT COUNT(*)
		FROM device_one_time_prekeys
		WHERE device_id = $1::uuid
		  AND consumed_at IS NULL
	`, deviceID).Scan(&count)

	if err != nil {
		return 0, err
	}

	return count, nil
}

// ReplenishOTPrekeyPool generates new one-time prekeys if needed
// Ensures at least targetCount available prekeys exist for a device
func (s *Service) ReplenishOTPrekeyPool(ctx context.Context, userID string, deviceID string, targetCount int64) (int64, error) {
	// Count current available prekeys
	available, err := s.CountAvailableOTPrekeyPool(ctx, deviceID)
	if err != nil {
		return 0, err
	}

	// If we already have enough, return
	if available >= targetCount {
		return available, nil
	}

	// Calculate how many to generate
	toGenerate := targetCount - available

	// Get the max prekey_id to continue sequence
	var maxPrekeyID int64
	err = s.pool.QueryRow(ctx, `
		SELECT COALESCE(MAX(prekey_id), 0)
		FROM device_one_time_prekeys
		WHERE device_id = $1::uuid
	`, deviceID).Scan(&maxPrekeyID)

	if err != nil {
		return 0, err
	}

	// Generate new prekeys in a transaction
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return 0, err
	}
	defer tx.Rollback(ctx)

	// Insert new prekeys
	for i := int64(0); i < toGenerate; i++ {
		newPrekeyID := maxPrekeyID + 1 + i
		// In real implementation, this would be generated client-side
		// For now, we just create entries and clients fill in public keys via AddPrekeys endpoint
		// This is a placeholder showing the structure
		_, err := tx.Exec(ctx, `
			INSERT INTO device_one_time_prekeys (device_id, prekey_id, public_key, created_at)
			VALUES ($1::uuid, $2, '', now())
			ON CONFLICT DO NOTHING
		`, deviceID, newPrekeyID)

		if err != nil {
			return 0, err
		}
	}

	if err = tx.Commit(ctx); err != nil {
		return 0, err
	}

	return available + toGenerate, nil
}

// RotateSignedPrekeyAndLog handles signed prekey rotation
// This is typically called via admin action or scheduled job
// Note: Actual key rotation happens client-side; this marks the old key as rotated
func (s *Service) RotateSignedPrekeyAndLog(ctx context.Context, userID string, deviceID string) error {
	// Update the device_identity_keys table to mark rotation event
	// The actual new signed prekey is uploaded by the client via PublishBundle
	tag, err := s.pool.Exec(ctx, `
		UPDATE device_identity_keys
		SET updated_at = now()
		WHERE device_id = $1::uuid
		  AND user_id = $2::uuid
	`, deviceID, userID)

	if err != nil {
		return err
	}

	if tag.RowsAffected() == 0 {
		return errors.New("device_not_found")
	}

	// Log the rotation event for audit
	_ = securityaudit.Append(ctx, s.pool, userID, userID, "device_signed_prekey_rotated", map[string]any{
		"device_id": deviceID,
	})

	return nil
}

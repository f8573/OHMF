package devices

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"io"
	"strings"
	"time"

	"crypto/aes"
	"crypto/cipher"
	"crypto/rand"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"ohmf/services/gateway/internal/deviceattestation"
	"ohmf/services/gateway/internal/securityaudit"
	"ohmf/services/gateway/internal/sqlutil"
)

// Service contains business logic for device management
type Service struct {
	db              *pgxpool.Pool
	subscriptionKey []byte
	attestation     *deviceattestation.Verifier
	challengeTTL    time.Duration
}

type Device struct {
	ID                   string    `json:"id"`
	UserID               string    `json:"user_id"`
	Platform             string    `json:"platform"`
	DeviceName           string    `json:"device_name"`
	ClientVersion        string    `json:"client_version"`
	Capabilities         []string  `json:"capabilities"`
	SMSRoleState         string    `json:"sms_role_state"`
	PushToken            string    `json:"push_token"`
	PushProvider         string    `json:"push_provider"`
	PushSubscription     string    `json:"push_subscription"`
	PublicKey            string    `json:"public_key"`
	LastSeenAt           time.Time `json:"last_seen_at"`
	HasPushSubscription  bool      `json:"has_push_subscription"`
	AttestationType      string    `json:"attestation_type,omitempty"`
	AttestationState     string    `json:"attestation_state,omitempty"`
	AttestedAt           string    `json:"attested_at,omitempty"`
	AttestationExpiresAt string    `json:"attestation_expires_at,omitempty"`
	AttestationLastError string    `json:"attestation_last_error,omitempty"`
}

type DeviceActivity struct {
	ID              int64          `json:"id"`
	EventType       string         `json:"event_type"`
	CreatedAt       string         `json:"created_at"`
	DeviceID        string         `json:"device_id,omitempty"`
	ContactUserID   string         `json:"contact_user_id,omitempty"`
	ContactDeviceID string         `json:"contact_device_id,omitempty"`
	Summary         string         `json:"summary"`
	Payload         map[string]any `json:"payload,omitempty"`
}

var ErrDeviceNotFound = errors.New("device not found")
var ErrAttestationDisabled = errors.New("device_attestation_disabled")
var ErrAttestationChallengeNotFound = errors.New("device_attestation_challenge_not_found")

func normalizeCapabilities(platform string, caps []string) []string {
	if len(caps) == 0 {
		return []string{}
	}
	normalized := make([]string, 0, len(caps))
	seen := make(map[string]bool)
	for _, cap := range caps {
		if cap != "" && !seen[cap] {
			normalized = append(normalized, cap)
			seen[cap] = true
		}
	}
	return normalized
}

type AttestationChallenge struct {
	Nonce     string `json:"nonce"`
	ExpiresAt string `json:"expires_at"`
}

type AttestationStatement struct {
	AttestationType string `json:"attestation_type"`
	Payload         string `json:"payload"`
	Signature       string `json:"signature"`
}

// NewService creates a device service with encrypted subscription support
func NewService(db *pgxpool.Pool, subscriptionKey []byte, verifier *deviceattestation.Verifier, challengeTTL time.Duration) *Service {
	if challengeTTL <= 0 {
		challengeTTL = 10 * time.Minute
	}
	return &Service{
		db:              db,
		subscriptionKey: subscriptionKey,
		attestation:     verifier,
		challengeTTL:    challengeTTL,
	}
}

// RegisterDevice creates a new device for a user
func (s *Service) RegisterDevice(ctx context.Context, userID string, d Device) (string, error) {
	d.Capabilities = normalizeCapabilities(d.Platform, d.Capabilities)
	encryptedSubscription, err := s.encryptSubscription(d.PushSubscription)
	if err != nil {
		return "", err
	}
	var id string
	err = s.db.QueryRow(ctx, `
		INSERT INTO devices (
			user_id,
			platform,
			device_name,
			client_version,
			capabilities,
			sms_role_state,
			push_token,
			push_provider,
			push_subscription,
			push_subscription_updated_at,
			public_key,
			last_seen_at
		)
		VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,CASE WHEN NULLIF($9, '') IS NULL THEN NULL ELSE now() END,$10,now())
		RETURNING id::text
	`, userID, d.Platform, d.DeviceName, d.ClientVersion, d.Capabilities, d.SMSRoleState, sqlutil.Nullable(d.PushToken), sqlutil.Nullable(d.PushProvider), sqlutil.Nullable(encryptedSubscription), sqlutil.Nullable(d.PublicKey)).Scan(&id)
	if err != nil {
		return "", err
	}
	return id, nil
}

// UpdateDevice updates device information for a user
func (s *Service) UpdateDevice(ctx context.Context, userID, deviceID string, d Device) (Device, error) {
	encryptedSubscription, err := s.encryptSubscription(d.PushSubscription)
	if err != nil {
		return Device{}, err
	}
	tag, err := s.db.Exec(ctx, `
		UPDATE devices
		SET device_name = COALESCE(NULLIF($3, ''), device_name),
		    client_version = COALESCE(NULLIF($4, ''), client_version),
		    platform = COALESCE(NULLIF($5, ''), platform),
		    capabilities = CASE WHEN $6::bool THEN $7 ELSE capabilities END,
		    push_token = CASE WHEN $8::bool THEN NULLIF($9, '') ELSE push_token END,
		    push_provider = CASE WHEN $10::bool THEN NULLIF($11, '') ELSE push_provider END,
		    push_subscription = CASE WHEN $12::bool THEN NULLIF($13, '') ELSE push_subscription END,
		    push_subscription_updated_at = CASE WHEN $12::bool THEN now() ELSE push_subscription_updated_at END,
		    public_key = CASE WHEN $14::bool THEN NULLIF($15, '') ELSE public_key END,
		    attestation_state = CASE WHEN $14::bool OR $16::bool THEN 'UNVERIFIED' ELSE attestation_state END,
		    attestation_type = CASE WHEN $14::bool OR $16::bool THEN NULL ELSE attestation_type END,
		    attestation_payload = CASE WHEN $14::bool OR $16::bool THEN '{}'::jsonb ELSE attestation_payload END,
		    attested_at = CASE WHEN $14::bool OR $16::bool THEN NULL ELSE attested_at END,
		    attestation_expires_at = CASE WHEN $14::bool OR $16::bool THEN NULL ELSE attestation_expires_at END,
		    attestation_public_key_hash = CASE WHEN $14::bool OR $16::bool THEN NULL ELSE attestation_public_key_hash END,
		    attestation_last_error = CASE WHEN $14::bool OR $16::bool THEN NULL ELSE attestation_last_error END,
		    last_seen_at = now(),
		    updated_at = now()
		WHERE id = $1::uuid AND user_id = $2::uuid
	`, deviceID, userID,
		d.DeviceName,
		d.ClientVersion,
		d.Platform,
		len(d.Capabilities) > 0, normalizeCapabilities(d.Platform, d.Capabilities),
		d.PushToken != "", d.PushToken,
		d.PushProvider != "", d.PushProvider,
		d.PushSubscription != "", encryptedSubscription,
		d.PublicKey != "", d.PublicKey,
		d.Platform != "",
	)
	if err != nil {
		return Device{}, err
	}
	if tag.RowsAffected() == 0 {
		return Device{}, ErrDeviceNotFound
	}
	return s.GetDevice(ctx, userID, deviceID)
}

// GetDevice retrieves a single device
func (s *Service) GetDevice(ctx context.Context, userID, deviceID string) (Device, error) {
	var d Device
	var caps []string
	var encryptedSubscription string
	err := s.db.QueryRow(ctx, `
		SELECT
			id::text,
			user_id::text,
			platform,
			COALESCE(device_name, ''),
			COALESCE(client_version, ''),
			COALESCE(capabilities, ARRAY[]::text[]),
			COALESCE(sms_role_state, ''),
			COALESCE(push_token, ''),
			COALESCE(push_provider, ''),
			COALESCE(push_subscription, ''),
			COALESCE(public_key, ''),
			COALESCE(last_seen_at, now()),
			COALESCE(attestation_type, ''),
			COALESCE(attestation_state, 'UNVERIFIED'),
			attested_at,
			attestation_expires_at,
			COALESCE(attestation_last_error, '')
		FROM devices
		WHERE user_id = $1 AND id = $2::uuid
	`, userID, deviceID).Scan(&d.ID, &d.UserID, &d.Platform, &d.DeviceName, &d.ClientVersion, &caps, &d.SMSRoleState, &d.PushToken, &d.PushProvider, &encryptedSubscription, &d.PublicKey, &d.LastSeenAt, &d.AttestationType, &d.AttestationState, nullableTimeString(&d.AttestedAt), nullableTimeString(&d.AttestationExpiresAt), &d.AttestationLastError)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return Device{}, ErrDeviceNotFound
		}
		return Device{}, err
	}
	d.Capabilities = caps
	d.HasPushSubscription = encryptedSubscription != ""
	return d, nil
}

// ListDevices retrieves all devices for a user
func (s *Service) ListDevices(ctx context.Context, userID string) ([]Device, error) {
	rows, err := s.db.Query(ctx, `
		SELECT
			id::text,
			user_id::text,
			platform,
			COALESCE(device_name, ''),
			COALESCE(client_version, ''),
			COALESCE(capabilities, ARRAY[]::text[]),
			COALESCE(sms_role_state, ''),
			COALESCE(push_token, ''),
			COALESCE(push_provider, ''),
			COALESCE(push_subscription, ''),
			COALESCE(public_key, ''),
			COALESCE(last_seen_at, now()),
			COALESCE(attestation_type, ''),
			COALESCE(attestation_state, 'UNVERIFIED'),
			attested_at,
			attestation_expires_at,
			COALESCE(attestation_last_error, '')
		FROM devices
		WHERE user_id = $1
	`, userID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []Device
	for rows.Next() {
		var d Device
		var caps []string
		var encryptedSubscription string
		if err := rows.Scan(&d.ID, &d.UserID, &d.Platform, &d.DeviceName, &d.ClientVersion, &caps, &d.SMSRoleState, &d.PushToken, &d.PushProvider, &encryptedSubscription, &d.PublicKey, &d.LastSeenAt, &d.AttestationType, &d.AttestationState, nullableTimeString(&d.AttestedAt), nullableTimeString(&d.AttestationExpiresAt), &d.AttestationLastError); err != nil {
			return nil, err
		}
		d.Capabilities = caps
		d.HasPushSubscription = encryptedSubscription != ""
		out = append(out, d)
	}
	return out, rows.Err()
}

func (s *Service) CreateAttestationChallenge(ctx context.Context, userID, deviceID string) (AttestationChallenge, error) {
	if s.attestation == nil || !s.attestation.Enabled() {
		return AttestationChallenge{}, ErrAttestationDisabled
	}
	device, err := s.GetDevice(ctx, userID, deviceID)
	if err != nil {
		return AttestationChallenge{}, err
	}
	nonce := uuid.NewString()
	expiresAt := time.Now().UTC().Add(s.challengeTTL)
	if _, err := s.db.Exec(ctx, `
		INSERT INTO device_attestation_challenges (device_id, user_id, platform, nonce, public_key_hash, expires_at, consumed_at, created_at)
		VALUES ($1::uuid, $2::uuid, $3, $4, NULLIF($5, ''), $6, NULL, now())
		ON CONFLICT (device_id)
		DO UPDATE SET
			user_id = EXCLUDED.user_id,
			platform = EXCLUDED.platform,
			nonce = EXCLUDED.nonce,
			public_key_hash = EXCLUDED.public_key_hash,
			expires_at = EXCLUDED.expires_at,
			consumed_at = NULL,
			created_at = now()
	`, deviceID, userID, device.Platform, nonce, deviceattestation.ComputePublicKeyHash(device.PublicKey), expiresAt); err != nil {
		return AttestationChallenge{}, err
	}
	_ = securityaudit.Append(ctx, s.db, userID, userID, "device_attestation_challenge_created", map[string]any{
		"device_id":  deviceID,
		"platform":   device.Platform,
		"expires_at": expiresAt.Format(time.RFC3339Nano),
	})
	return AttestationChallenge{Nonce: nonce, ExpiresAt: expiresAt.Format(time.RFC3339Nano)}, nil
}

func (s *Service) VerifyAttestation(ctx context.Context, userID, deviceID string, statement AttestationStatement) (Device, error) {
	if s.attestation == nil || !s.attestation.Enabled() {
		return Device{}, ErrAttestationDisabled
	}
	tx, err := s.db.BeginTx(ctx, pgx.TxOptions{})
	if err != nil {
		return Device{}, err
	}
	defer tx.Rollback(ctx)

	var platform string
	var publicKey string
	var nonce string
	var publicKeyHash string
	var expiresAt time.Time
	err = tx.QueryRow(ctx, `
		SELECT d.platform,
		       COALESCE(d.public_key, ''),
		       c.nonce,
		       COALESCE(c.public_key_hash, ''),
		       c.expires_at
		FROM devices d
		JOIN device_attestation_challenges c ON c.device_id = d.id
		WHERE d.id = $1::uuid
		  AND d.user_id = $2::uuid
		  AND c.consumed_at IS NULL
		FOR UPDATE
	`, deviceID, userID).Scan(&platform, &publicKey, &nonce, &publicKeyHash, &expiresAt)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return Device{}, ErrAttestationChallengeNotFound
		}
		return Device{}, err
	}
	if !expiresAt.After(time.Now().UTC()) {
		return Device{}, ErrAttestationChallengeNotFound
	}
	payload, attestationExpiresAt, verifyErr := s.attestation.Verify(deviceattestation.Statement(statement), deviceattestation.Expected{
		Platform:            platform,
		DeviceID:            deviceID,
		Nonce:               nonce,
		DevicePublicKeyHash: publicKeyHash,
	})
	if verifyErr != nil {
		_, _ = tx.Exec(ctx, `
			UPDATE devices
			SET attestation_state = 'FAILED',
			    attestation_last_error = $3,
			    updated_at = now()
			WHERE id = $1::uuid
			  AND user_id = $2::uuid
		`, deviceID, userID, verifyErr.Error())
		return Device{}, verifyErr
	}
	payloadJSON, err := json.Marshal(payload)
	if err != nil {
		return Device{}, err
	}
	if _, err := tx.Exec(ctx, `
		UPDATE devices
		SET attestation_type = $3,
		    attestation_state = 'VERIFIED',
		    attestation_payload = $4::jsonb,
		    attested_at = now(),
		    attestation_expires_at = $5,
		    attestation_public_key_hash = NULLIF($6, ''),
		    attestation_last_error = NULL,
		    updated_at = now()
		WHERE id = $1::uuid
		  AND user_id = $2::uuid
	`, deviceID, userID, statement.AttestationType, string(payloadJSON), attestationExpiresAt, deviceattestation.ComputePublicKeyHash(publicKey)); err != nil {
		return Device{}, err
	}
	if _, err := tx.Exec(ctx, `
		UPDATE device_attestation_challenges
		SET consumed_at = now()
		WHERE device_id = $1::uuid
	`, deviceID); err != nil {
		return Device{}, err
	}
	if err := securityaudit.Append(ctx, tx, userID, userID, "device_attestation_verified", map[string]any{
		"device_id":              deviceID,
		"platform":               platform,
		"attestation_type":       statement.AttestationType,
		"attestation_expires_at": attestationExpiresAt.Format(time.RFC3339Nano),
	}); err != nil {
		return Device{}, err
	}
	if err := tx.Commit(ctx); err != nil {
		return Device{}, err
	}
	return s.GetDevice(ctx, userID, deviceID)
}

func (s *Service) ListRecentActivity(ctx context.Context, userID string, limit int) ([]DeviceActivity, error) {
	if limit <= 0 || limit > 100 {
		limit = 25
	}
	rows, err := s.db.Query(ctx, `
		SELECT id, event_type, payload, created_at
		FROM security_audit_events
		WHERE target_user_id = $1::uuid
		  AND event_type = ANY($2::text[])
		ORDER BY created_at DESC, id DESC
		LIMIT $3
	`, userID, []string{
		"device_pairing_started",
		"device_pairing_completed",
		"device_revoked",
		"device_attestation_challenge_created",
		"device_attestation_verified",
		"device_trust_verified",
		"device_trust_revoked",
	}, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	items := make([]DeviceActivity, 0, limit)
	for rows.Next() {
		var item DeviceActivity
		var payloadRaw []byte
		var createdAt time.Time
		if err := rows.Scan(&item.ID, &item.EventType, &payloadRaw, &createdAt); err != nil {
			return nil, err
		}
		item.CreatedAt = createdAt.UTC().Format(time.RFC3339Nano)
		if len(payloadRaw) > 0 {
			item.Payload = map[string]any{}
			if err := json.Unmarshal(payloadRaw, &item.Payload); err != nil {
				item.Payload = map[string]any{}
			}
		}
		item.DeviceID = strings.TrimSpace(stringValue(item.Payload, "device_id"))
		item.ContactUserID = strings.TrimSpace(stringValue(item.Payload, "contact_user_id"))
		item.ContactDeviceID = strings.TrimSpace(stringValue(item.Payload, "contact_device_id"))
		item.Summary = summarizeDeviceActivity(item)
		items = append(items, item)
	}
	return items, rows.Err()
}

func stringValue(payload map[string]any, key string) string {
	if payload == nil {
		return ""
	}
	value, _ := payload[key].(string)
	return value
}

func summarizeDeviceActivity(item DeviceActivity) string {
	switch item.EventType {
	case "device_pairing_started":
		return "Started a new pairing code."
	case "device_pairing_completed":
		if item.DeviceID != "" {
			return "Linked a new device."
		}
		return "Completed device pairing."
	case "device_revoked":
		return "Revoked a linked device."
	case "device_attestation_challenge_created":
		return "Requested device attestation."
	case "device_attestation_verified":
		return "Verified device attestation."
	case "device_trust_verified":
		return "Verified a contact device."
	case "device_trust_revoked":
		return "Revoked trust for a contact device."
	default:
		return strings.ReplaceAll(item.EventType, "_", " ")
	}
}

func nullableTimeString(target *string) any {
	return newNullableTimeScanner(target)
}

type nullableTimeScanner struct {
	target *string
}

func newNullableTimeScanner(target *string) *nullableTimeScanner {
	return &nullableTimeScanner{target: target}
}

func (s *nullableTimeScanner) Scan(src any) error {
	if s.target == nil {
		return nil
	}
	switch v := src.(type) {
	case nil:
		*s.target = ""
	case time.Time:
		*s.target = v.UTC().Format(time.RFC3339Nano)
	case []byte:
		*s.target = string(v)
	case string:
		*s.target = v
	}
	return nil
}

// RevokeDevice deletes a device, returning ErrDeviceNotFound if device doesn't exist
func (s *Service) RevokeDevice(ctx context.Context, userID, deviceID string) error {
	device, err := s.GetDevice(ctx, userID, deviceID)
	if err != nil {
		return err
	}
	tag, err := s.db.Exec(ctx, `DELETE FROM devices WHERE user_id = $1 AND id = $2::uuid`, userID, deviceID)
	if err != nil {
		return err
	}
	if tag.RowsAffected() == 0 {
		return ErrDeviceNotFound
	}
	_ = securityaudit.Append(ctx, s.db, userID, userID, "device_revoked", map[string]any{
		"device_id":   deviceID,
		"platform":    device.Platform,
		"device_name": device.DeviceName,
	})
	return nil
}

// ListWebPushSubscriptions retrieves all web push subscriptions for a user
func (s *Service) ListWebPushSubscriptions(ctx context.Context, userID string) ([]string, error) {
	rows, err := s.db.Query(ctx, `
		SELECT COALESCE(push_subscription, '')
		FROM devices
		WHERE user_id = $1::uuid
		  AND UPPER(COALESCE(push_provider, '')) = 'WEBPUSH'
		  AND NULLIF(push_subscription, '') IS NOT NULL
	`, userID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := make([]string, 0, 2)
	for rows.Next() {
		var encrypted string
		if err := rows.Scan(&encrypted); err != nil {
			return nil, err
		}
		decrypted, err := s.decryptValue(encrypted)
		if err != nil || decrypted == "" {
			continue
		}
		out = append(out, decrypted)
	}
	return out, rows.Err()
}

func (s *Service) ListPushTokensForProvider(ctx context.Context, userID, providerType string) ([]string, error) {
	providerType = normalizeProviderType(providerType)
	if providerType == "" {
		return nil, errors.New("invalid_push_provider")
	}
	rows, err := s.db.Query(ctx, `
		SELECT COALESCE(push_token, '')
		FROM device_push_tokens
		WHERE user_id = $1::uuid AND provider_type = $2 AND push_token IS NOT NULL
		ORDER BY registered_at DESC
		LIMIT 100
	`, userID, providerType)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := make([]string, 0, 4)
	for rows.Next() {
		var encrypted string
		if err := rows.Scan(&encrypted); err != nil {
			return nil, err
		}
		decrypted, err := s.decryptValue(encrypted)
		if err != nil || decrypted == "" {
			continue
		}
		out = append(out, decrypted)
	}
	return out, rows.Err()
}

func (s *Service) ListPushTokensForProviderAndDevices(ctx context.Context, userID, providerType string, deviceIDs []string) ([]string, error) {
	providerType = normalizeProviderType(providerType)
	if providerType == "" {
		return nil, errors.New("invalid_push_provider")
	}
	if len(deviceIDs) == 0 {
		return []string{}, nil
	}
	rows, err := s.db.Query(ctx, `
		SELECT COALESCE(push_token, '')
		FROM device_push_tokens
		WHERE user_id = $1::uuid
		  AND provider_type = $2
		  AND device_id = ANY($3::uuid[])
		  AND push_token IS NOT NULL
		ORDER BY registered_at DESC
		LIMIT 100
	`, userID, providerType, deviceIDs)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := make([]string, 0, len(deviceIDs))
	for rows.Next() {
		var token string
		if err := rows.Scan(&token); err != nil {
			return nil, err
		}
		if token != "" {
			out = append(out, token)
		}
	}
	return out, rows.Err()
}

func (s *Service) ListWebPushSubscriptionsForDevices(ctx context.Context, userID string, deviceIDs []string) ([]string, error) {
	if len(deviceIDs) == 0 {
		return []string{}, nil
	}
	rows, err := s.db.Query(ctx, `
		SELECT COALESCE(push_subscription, '')
		FROM devices
		WHERE user_id = $1::uuid
		  AND id = ANY($2::uuid[])
		  AND UPPER(COALESCE(push_provider, '')) = 'WEBPUSH'
		  AND NULLIF(push_subscription, '') IS NOT NULL
	`, userID, deviceIDs)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := make([]string, 0, len(deviceIDs))
	for rows.Next() {
		var encrypted string
		if err := rows.Scan(&encrypted); err != nil {
			return nil, err
		}
		decrypted, err := s.decryptValue(encrypted)
		if err != nil || decrypted == "" {
			continue
		}
		out = append(out, decrypted)
	}
	return out, rows.Err()
}

// encryptSubscription encrypts a web push subscription using AES-GCM
func (s *Service) encryptSubscription(raw string) (string, error) {
	return s.encryptValue(raw)
}

func (s *Service) encryptValue(raw string) (string, error) {
	if raw == "" {
		return "", nil
	}
	block, err := aes.NewCipher(s.subscriptionKey)
	if err != nil {
		return "", err
	}
	gcm, err := cipher.NewGCM(block)
	if err != nil {
		return "", err
	}
	nonce := make([]byte, gcm.NonceSize())
	if _, err := io.ReadFull(rand.Reader, nonce); err != nil {
		return "", err
	}
	ciphertext := gcm.Seal(nil, nonce, []byte(raw), nil)
	payload := append(nonce, ciphertext...)
	return base64.RawStdEncoding.EncodeToString(payload), nil
}

// decryptSubscription decrypts a web push subscription using AES-GCM
func (s *Service) decryptSubscription(encrypted string) (string, error) {
	return s.decryptValue(encrypted)
}

func (s *Service) decryptValue(encrypted string) (string, error) {
	if encrypted == "" {
		return "", nil
	}
	raw, err := base64.RawStdEncoding.DecodeString(encrypted)
	if err != nil {
		return "", err
	}
	block, err := aes.NewCipher(s.subscriptionKey)
	if err != nil {
		return "", err
	}
	gcm, err := cipher.NewGCM(block)
	if err != nil {
		return "", err
	}
	if len(raw) < gcm.NonceSize() {
		return "", errors.New("invalid_push_subscription")
	}
	nonce := raw[:gcm.NonceSize()]
	ciphertext := raw[gcm.NonceSize():]
	plain, err := gcm.Open(nil, nonce, ciphertext, nil)
	if err != nil {
		return "", err
	}
	return string(plain), nil
}

// RegisterPushToken registers a push notification token for a device
func (s *Service) RegisterPushToken(ctx context.Context, userID, deviceID, providerType, token string) error {
	providerType = normalizeProviderType(providerType)
	if providerType == "" {
		return errors.New("invalid_push_provider")
	}
	// Verify the device belongs to the user
	var existingUserID string
	err := s.db.QueryRow(ctx, `
		SELECT user_id::text FROM devices WHERE id = $1::uuid
	`, deviceID).Scan(&existingUserID)
	if err != nil {
		if err == pgx.ErrNoRows {
			return ErrDeviceNotFound
		}
		return err
	}
	if existingUserID != userID {
		return ErrDeviceNotFound
	}

	encryptedToken, err := s.encryptValue(token)
	if err != nil {
		return err
	}

	// Insert or update the push token
	_, err = s.db.Exec(ctx, `
		INSERT INTO device_push_tokens (device_id, user_id, provider_type, push_token, registered_at, last_verified_at)
		VALUES ($1::uuid, $2::uuid, $3, $4, now(), now())
		ON CONFLICT (device_id, provider_type) DO UPDATE
		SET push_token = EXCLUDED.push_token, last_verified_at = now()
	`, deviceID, userID, providerType, encryptedToken)
	return err
}

func (s *Service) RemovePushToken(ctx context.Context, userID, providerType, token string) error {
	providerType = normalizeProviderType(providerType)
	if providerType == "" {
		return errors.New("invalid_push_provider")
	}
	rows, err := s.db.Query(ctx, `
		SELECT id, push_token
		FROM device_push_tokens
		WHERE user_id = $1::uuid AND provider_type = $2 AND push_token IS NOT NULL
	`, userID, providerType)
	if err != nil {
		return err
	}
	defer rows.Close()

	for rows.Next() {
		var id int64
		var encrypted string
		if err := rows.Scan(&id, &encrypted); err != nil {
			continue
		}
		decrypted, err := s.decryptValue(encrypted)
		if err != nil || decrypted != token {
			continue
		}
		_, err = s.db.Exec(ctx, `DELETE FROM device_push_tokens WHERE id = $1`, id)
		return err
	}
	return rows.Err()
}

func normalizeProviderType(providerType string) string {
	providerType = strings.TrimSpace(strings.ToLower(providerType))
	switch providerType {
	case "fcm", "apns":
		return providerType
	default:
		return ""
	}
}

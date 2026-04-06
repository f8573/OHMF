package e2ee

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"
	"ohmf/services/gateway/internal/securityaudit"
)

var ErrTrustDeviceNotFound = errors.New("trust_device_not_found")
var ErrTrustFingerprintMismatch = errors.New("trust_fingerprint_mismatch")

type TrustStateView struct {
	ContactUserID       string     `json:"contact_user_id"`
	ContactDeviceID     string     `json:"contact_device_id"`
	TrustState          string     `json:"trust_state"`
	EffectiveTrustState string     `json:"effective_trust_state"`
	RecordedFingerprint string     `json:"recorded_fingerprint,omitempty"`
	CurrentFingerprint  string     `json:"current_fingerprint,omitempty"`
	TrustEstablishedAt  *time.Time `json:"trust_established_at,omitempty"`
	VerifiedAt          *time.Time `json:"verified_at,omitempty"`
	Warning             string     `json:"warning,omitempty"`
}

func normalizeStoredTrustState(value string) string {
	switch strings.ToUpper(strings.TrimSpace(value)) {
	case "VERIFIED":
		return "VERIFIED"
	case "BLOCKED", "REVOKED":
		return "REVOKED"
	case "TOFU", "UNVERIFIED", "", "UNKNOWN":
		return "UNVERIFIED"
	default:
		return "UNVERIFIED"
	}
}

func deriveEffectiveTrustState(storedState string, recordedFingerprint string, currentFingerprint string) (string, string) {
	normalizedStored := normalizeStoredTrustState(storedState)
	recorded := strings.TrimSpace(recordedFingerprint)
	current := strings.TrimSpace(currentFingerprint)
	if recorded != "" && current != "" && !strings.EqualFold(recorded, current) {
		return "MISMATCH", "Current fingerprint does not match the previously recorded trust state."
	}
	if normalizedStored == "REVOKED" {
		return "REVOKED", "Verification was revoked for this device."
	}
	if normalizedStored == "VERIFIED" {
		return "VERIFIED", ""
	}
	return "UNVERIFIED", ""
}

func (sm *SessionManager) loadCurrentTrustMaterial(
	ctx context.Context,
	contactUserID string,
	contactDeviceID string,
) (string, string, error) {
	query := `
		SELECT signing_public_key, COALESCE(fingerprint, '')
		FROM device_identity_keys
		WHERE user_id = $1::uuid AND device_id = $2::uuid
		LIMIT 1
	`

	var signingPublicKey string
	var fingerprint string
	if err := sm.db.QueryRow(ctx, query, contactUserID, contactDeviceID).Scan(&signingPublicKey, &fingerprint); err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return "", "", ErrTrustDeviceNotFound
		}
		return "", "", fmt.Errorf("load current trust material: %w", err)
	}
	if fingerprint == "" {
		computed, err := ComputeFingerprint(signingPublicKey)
		if err != nil {
			return "", "", fmt.Errorf("compute current fingerprint: %w", err)
		}
		fingerprint = computed
	}
	return fingerprint, signingPublicKey, nil
}

func (sm *SessionManager) GetTrustStateView(
	ctx context.Context,
	userID string,
	contactUserID string,
	contactDeviceID string,
) (*TrustStateView, error) {
	currentFingerprint, _, err := sm.loadCurrentTrustMaterial(ctx, contactUserID, contactDeviceID)
	if err != nil {
		return nil, err
	}

	query := `
		SELECT trust_state, fingerprint, trust_established_at, verified_at
		FROM device_key_trust
		WHERE user_id = $1::uuid AND contact_user_id = $2::uuid AND contact_device_id = $3::uuid
	`

	var storedState string
	var recordedFingerprint string
	var trustEstablishedAt sql.NullTime
	var verifiedAt sql.NullTime
	err = sm.db.QueryRow(ctx, query, userID, contactUserID, contactDeviceID).Scan(
		&storedState,
		&recordedFingerprint,
		&trustEstablishedAt,
		&verifiedAt,
	)
	if err != nil && !errors.Is(err, pgx.ErrNoRows) {
		return nil, fmt.Errorf("query trust state: %w", err)
	}

	view := &TrustStateView{
		ContactUserID:       contactUserID,
		ContactDeviceID:     contactDeviceID,
		TrustState:          normalizeStoredTrustState(storedState),
		RecordedFingerprint: recordedFingerprint,
		CurrentFingerprint:  currentFingerprint,
	}
	if errors.Is(err, pgx.ErrNoRows) {
		view.TrustState = "UNVERIFIED"
		view.EffectiveTrustState = "UNVERIFIED"
		return view, nil
	}
	if trustEstablishedAt.Valid {
		ts := trustEstablishedAt.Time.UTC()
		view.TrustEstablishedAt = &ts
	}
	if verifiedAt.Valid {
		ts := verifiedAt.Time.UTC()
		view.VerifiedAt = &ts
	}
	view.EffectiveTrustState, view.Warning = deriveEffectiveTrustState(storedState, recordedFingerprint, currentFingerprint)
	return view, nil
}

func (sm *SessionManager) VerifyTrustState(
	ctx context.Context,
	userID string,
	contactUserID string,
	contactDeviceID string,
	expectedFingerprint string,
) (*TrustStateView, error) {
	currentFingerprint, signingPublicKey, err := sm.loadCurrentTrustMaterial(ctx, contactUserID, contactDeviceID)
	if err != nil {
		return nil, err
	}
	if strings.TrimSpace(expectedFingerprint) == "" || !strings.EqualFold(strings.TrimSpace(expectedFingerprint), currentFingerprint) {
		return nil, ErrTrustFingerprintMismatch
	}
	return sm.writeTrustState(ctx, userID, contactUserID, contactDeviceID, "VERIFIED", currentFingerprint, signingPublicKey)
}

func (sm *SessionManager) writeTrustState(
	ctx context.Context,
	userID string,
	contactUserID string,
	contactDeviceID string,
	trustState string,
	currentFingerprint string,
	signingPublicKey string,
) (*TrustStateView, error) {
	verified := normalizeStoredTrustState(trustState) == "VERIFIED"
	query := `
		INSERT INTO device_key_trust (
			user_id,
			contact_user_id,
			contact_device_id,
			trust_state,
			fingerprint,
			trusted_device_public_key,
			trust_established_at,
			verified_at
		)
		VALUES ($1::uuid, $2::uuid, $3::uuid, $4, $5, $6, NOW(), CASE WHEN $7 THEN NOW() ELSE NULL END)
		ON CONFLICT (user_id, contact_user_id, contact_device_id)
		DO UPDATE SET
			trust_state = EXCLUDED.trust_state,
			fingerprint = EXCLUDED.fingerprint,
			trusted_device_public_key = EXCLUDED.trusted_device_public_key,
			trust_established_at = COALESCE(device_key_trust.trust_established_at, EXCLUDED.trust_established_at),
			verified_at = CASE WHEN $7 THEN NOW() ELSE NULL END
	`
	if _, err := sm.db.Exec(ctx, query, userID, contactUserID, contactDeviceID, trustState, currentFingerprint, signingPublicKey, verified); err != nil {
		return nil, fmt.Errorf("write trust state: %w", err)
	}
	eventType := "device_trust_revoked"
	if verified {
		eventType = "device_trust_verified"
	}
	_ = securityaudit.Append(ctx, sm.db, userID, userID, eventType, map[string]any{
		"contact_user_id":   contactUserID,
		"contact_device_id": contactDeviceID,
		"fingerprint":       currentFingerprint,
		"trust_state":       normalizeStoredTrustState(trustState),
	})
	return sm.GetTrustStateView(ctx, userID, contactUserID, contactDeviceID)
}

func (sm *SessionManager) RevokeTrustState(
	ctx context.Context,
	userID string,
	contactUserID string,
	contactDeviceID string,
) (*TrustStateView, error) {
	currentFingerprint, signingPublicKey, err := sm.loadCurrentTrustMaterial(ctx, contactUserID, contactDeviceID)
	if err != nil {
		return nil, err
	}
	return sm.writeTrustState(ctx, userID, contactUserID, contactDeviceID, "BLOCKED", currentFingerprint, signingPublicKey)
}

var trustStateFetcher = func(ctx context.Context, sm *SessionManager, userID string, contactUserID string, contactDeviceID string) (*TrustStateView, error) {
	return sm.GetTrustStateView(ctx, userID, contactUserID, contactDeviceID)
}

var trustStateVerifier = func(ctx context.Context, sm *SessionManager, userID string, contactUserID string, contactDeviceID string, fingerprint string) (*TrustStateView, error) {
	return sm.VerifyTrustState(ctx, userID, contactUserID, contactDeviceID, fingerprint)
}

var trustStateRevoker = func(ctx context.Context, sm *SessionManager, userID string, contactUserID string, contactDeviceID string) (*TrustStateView, error) {
	return sm.RevokeTrustState(ctx, userID, contactUserID, contactDeviceID)
}

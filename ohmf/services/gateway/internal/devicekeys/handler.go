package devicekeys

import (
	"context"
	"crypto/ed25519"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/go-chi/chi/v5"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"net/http"
	"ohmf/services/gateway/internal/httpx"
	"ohmf/services/gateway/internal/middleware"
	"strings"
	"time"
)

func NewHandler(db *pgxpool.Pool) *Handler { return &Handler{DB: db} }

type Handler struct {
	DB *pgxpool.Pool
}

// removed: trivial constructor wrapper with type mismatch fixed
func (h *Handler) Publish(w http.ResponseWriter, r *http.Request) {
	actor, ok := middleware.UserIDFromContext(r.Context())
	if !ok {
		httpx.WriteError(w, r, http.StatusUnauthorized, "unauthorized", "missing auth", nil)
		return
	}
	deviceID := chi.URLParam(r, "deviceID")
	var req PublishRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		httpx.WriteError(w, r, http.StatusBadRequest, "invalid_request", "invalid body", nil)
		return
	}
	bundle, err := h.PublishBundle(r.Context(), actor, deviceID, req)
	if err != nil {
		if errors.Is(err, ErrDeviceNotOwned) {
			httpx.WriteError(w, r, http.StatusForbidden, "forbidden", "device not owned by actor", nil)
			return
		}
		if errors.Is(err, ErrDeviceCapabilityRequired) {
			httpx.WriteError(w, r, http.StatusConflict, "device_capability_required", "device must support E2EE_OTT_V2", nil)
			return
		}
		if errors.Is(err, ErrInvalidBundle) || errors.Is(err, ErrEncryptedPrekeysInsufficient) {
			httpx.WriteError(w, r, http.StatusBadRequest, "invalid_request", err.Error(), nil)
			return
		}
		httpx.WriteError(w, r, http.StatusInternalServerError, "publish_failed", err.Error(), nil)
		return
	}
	httpx.WriteJSON(w, http.StatusOK, bundle)
}

func (h *Handler) AddPrekeys(w http.ResponseWriter, r *http.Request) {
	actor, ok := middleware.UserIDFromContext(r.Context())
	if !ok {
		httpx.WriteError(w, r, http.StatusUnauthorized, "unauthorized", "missing auth", nil)
		return
	}
	deviceID := chi.URLParam(r, "deviceID")
	var body struct {
		OneTimePrekeys []OneTimePrekey `json:"one_time_prekeys"`
	}
	if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
		httpx.WriteError(w, r, http.StatusBadRequest, "invalid_request", "invalid body", nil)
		return
	}
	bundle, err := h.AddOneTimePrekeys(r.Context(), actor, deviceID, body.OneTimePrekeys)
	if err != nil {
		if errors.Is(err, ErrDeviceNotOwned) {
			httpx.WriteError(w, r, http.StatusForbidden, "forbidden", "device not owned by actor", nil)
			return
		}
		if errors.Is(err, ErrDeviceCapabilityRequired) {
			httpx.WriteError(w, r, http.StatusConflict, "device_capability_required", "device must support E2EE_OTT_V2", nil)
			return
		}
		if errors.Is(err, ErrInvalidBundle) || errors.Is(err, ErrEncryptedPrekeysRequired) {
			httpx.WriteError(w, r, http.StatusBadRequest, "invalid_request", err.Error(), nil)
			return
		}
		httpx.WriteError(w, r, http.StatusInternalServerError, "prekey_add_failed", err.Error(), nil)
		return
	}
	httpx.WriteJSON(w, http.StatusOK, bundle)
}

func (h *Handler) ListForUser(w http.ResponseWriter, r *http.Request) {
	if _, ok := middleware.UserIDFromContext(r.Context()); !ok {
		httpx.WriteError(w, r, http.StatusUnauthorized, "unauthorized", "missing auth", nil)
		return
	}
	userID := chi.URLParam(r, "userID")
	items, err := h.ListBundlesForUser(r.Context(), userID)
	if err != nil {
		httpx.WriteError(w, r, http.StatusInternalServerError, "list_failed", err.Error(), nil)
		return
	}
	httpx.WriteJSON(w, http.StatusOK, map[string]any{"items": items})
}

func (h *Handler) ClaimForUser(w http.ResponseWriter, r *http.Request) {
	if _, ok := middleware.UserIDFromContext(r.Context()); !ok {
		httpx.WriteError(w, r, http.StatusUnauthorized, "unauthorized", "missing auth", nil)
		return
	}
	userID := chi.URLParam(r, "userID")
	items, err := h.ClaimBundles(r.Context(), userID)
	if err != nil {
		httpx.WriteError(w, r, http.StatusInternalServerError, "claim_failed", err.Error(), nil)
		return
	}
	httpx.WriteJSON(w, http.StatusOK, map[string]any{"items": items})
}

func (h *Handler) UpsertBackup(w http.ResponseWriter, r *http.Request) {
	actor, ok := middleware.UserIDFromContext(r.Context())
	if !ok {
		httpx.WriteError(w, r, http.StatusUnauthorized, "unauthorized", "missing auth", nil)
		return
	}
	backupName := chi.URLParam(r, "name")
	var req UpsertBackupRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		httpx.WriteError(w, r, http.StatusBadRequest, "invalid_request", "invalid body", nil)
		return
	}
	item, err := (&Service{pool: h.DB}).UpsertBackup(r.Context(), actor, backupName, req)
	if err != nil {
		switch {
		case errors.Is(err, ErrDeviceNotOwned):
			httpx.WriteError(w, r, http.StatusForbidden, "forbidden", "device not owned by actor", nil)
			return
		case err.Error() == "encrypted_blob_required":
			httpx.WriteError(w, r, http.StatusBadRequest, "invalid_request", "encrypted_blob required", nil)
			return
		default:
			httpx.WriteError(w, r, http.StatusInternalServerError, "backup_failed", err.Error(), nil)
			return
		}
	}
	httpx.WriteJSON(w, http.StatusOK, item)
}

func (h *Handler) ListBackups(w http.ResponseWriter, r *http.Request) {
	actor, ok := middleware.UserIDFromContext(r.Context())
	if !ok {
		httpx.WriteError(w, r, http.StatusUnauthorized, "unauthorized", "missing auth", nil)
		return
	}
	items, err := (&Service{pool: h.DB}).ListBackups(r.Context(), actor)
	if err != nil {
		httpx.WriteError(w, r, http.StatusInternalServerError, "list_failed", err.Error(), nil)
		return
	}
	httpx.WriteJSON(w, http.StatusOK, map[string]any{"items": items})
}

func (h *Handler) GetBackup(w http.ResponseWriter, r *http.Request) {
	actor, ok := middleware.UserIDFromContext(r.Context())
	if !ok {
		httpx.WriteError(w, r, http.StatusUnauthorized, "unauthorized", "missing auth", nil)
		return
	}
	item, err := (&Service{pool: h.DB}).GetBackup(r.Context(), actor, chi.URLParam(r, "name"), false)
	if err != nil {
		if errors.Is(err, ErrBackupNotFound) {
			httpx.WriteError(w, r, http.StatusNotFound, "not_found", "backup not found", nil)
			return
		}
		httpx.WriteError(w, r, http.StatusInternalServerError, "get_failed", err.Error(), nil)
		return
	}
	httpx.WriteJSON(w, http.StatusOK, item)
}

func (h *Handler) RestoreBackup(w http.ResponseWriter, r *http.Request) {
	actor, ok := middleware.UserIDFromContext(r.Context())
	if !ok {
		httpx.WriteError(w, r, http.StatusUnauthorized, "unauthorized", "missing auth", nil)
		return
	}
	item, err := (&Service{pool: h.DB}).GetBackup(r.Context(), actor, chi.URLParam(r, "name"), true)
	if err != nil {
		if errors.Is(err, ErrBackupNotFound) {
			httpx.WriteError(w, r, http.StatusNotFound, "not_found", "backup not found", nil)
			return
		}
		httpx.WriteError(w, r, http.StatusInternalServerError, "restore_failed", err.Error(), nil)
		return
	}
	httpx.WriteJSON(w, http.StatusOK, item)
}

func (h *Handler) DeleteBackup(w http.ResponseWriter, r *http.Request) {
	actor, ok := middleware.UserIDFromContext(r.Context())
	if !ok {
		httpx.WriteError(w, r, http.StatusUnauthorized, "unauthorized", "missing auth", nil)
		return
	}
	if err := (&Service{pool: h.DB}).DeleteBackup(r.Context(), actor, chi.URLParam(r, "name")); err != nil {
		if errors.Is(err, ErrBackupNotFound) {
			httpx.WriteError(w, r, http.StatusNotFound, "not_found", "backup not found", nil)
			return
		}
		httpx.WriteError(w, r, http.StatusInternalServerError, "delete_failed", err.Error(), nil)
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

var (
	ErrDeviceNotOwned               = errors.New("device_not_owned")
	ErrDeviceCapabilityRequired     = errors.New("device_e2ee_capability_required")
	ErrInvalidBundle                = errors.New("invalid_bundle")
	ErrEncryptedPrekeysRequired     = errors.New("encrypted_prekeys_required")
	ErrEncryptedPrekeysInsufficient = errors.New("encrypted_prekeys_insufficient")
)

const (
	BundleVersionSignalV1    = "OHMF_SIGNAL_V1"
	LegacyBundleVersion      = "OHMF_LEGACY_V0"
	RequiredDeviceCapability = "E2EE_OTT_V2"
	minInitialOneTimePrekeys = 100
	x25519PublicKeySize      = 32
	ed25519PublicKeySize     = 32
	ed25519SignatureSize     = 64
)

type OneTimePrekey struct {
	PrekeyID  int64  `json:"prekey_id"`
	PublicKey string `json:"public_key"`
}

type SignedPrekey struct {
	PrekeyID  int64  `json:"prekey_id"`
	PublicKey string `json:"public_key"`
	Signature string `json:"signature"`
}

type Bundle struct {
	DeviceID                   string         `json:"device_id"`
	UserID                     string         `json:"user_id"`
	BundleVersion              string         `json:"bundle_version"`
	IdentityKeyAlg             string         `json:"identity_key_alg"`
	IdentityPublicKey          string         `json:"identity_public_key"`
	AgreementIdentityPublicKey string         `json:"agreement_identity_public_key"`
	SigningKeyAlg              string         `json:"signing_key_alg"`
	SigningPublicKey           string         `json:"signing_public_key"`
	SignedPrekeyID             int64          `json:"signed_prekey_id"`
	SignedPrekeyPublicKey      string         `json:"signed_prekey_public_key"`
	SignedPrekeySignature      string         `json:"signed_prekey_signature"`
	SignedPrekey               SignedPrekey   `json:"signed_prekey"`
	KeyVersion                 int            `json:"key_version"`
	TrustLevel                 string         `json:"trust_level"`
	Fingerprint                string         `json:"fingerprint"`
	PublishedAt                string         `json:"published_at,omitempty"`
	UpdatedAt                  string         `json:"updated_at,omitempty"`
	AvailableOneTimePrekeys    int64          `json:"available_one_time_prekeys,omitempty"`
	ClaimedOneTimePrekey       *OneTimePrekey `json:"claimed_one_time_prekey,omitempty"`
}

type PublishRequest struct {
	BundleVersion              string          `json:"bundle_version"`
	IdentityKeyAlg             string          `json:"identity_key_alg"`
	IdentityPublicKey          string          `json:"identity_public_key"`
	AgreementIdentityPublicKey string          `json:"agreement_identity_public_key"`
	SigningKeyAlg              string          `json:"signing_key_alg"`
	SigningPublicKey           string          `json:"signing_public_key"`
	SignedPrekeyID             int64           `json:"signed_prekey_id"`
	SignedPrekeyPublicKey      string          `json:"signed_prekey_public_key"`
	SignedPrekeySignature      string          `json:"signed_prekey_signature"`
	SignedPrekey               SignedPrekey    `json:"signed_prekey"`
	KeyVersion                 int             `json:"key_version"`
	TrustLevel                 string          `json:"trust_level"`
	OneTimePrekeys             []OneTimePrekey `json:"one_time_prekeys,omitempty"`
}

func (h *Handler) PublishBundle(ctx context.Context, actorUserID, deviceID string, req PublishRequest) (Bundle, error) {
	if err := h.ensureDeviceOwnership(ctx, actorUserID, deviceID); err != nil {
		return Bundle{}, err
	}
	if err := h.ensureDeviceCapability(ctx, actorUserID, deviceID, RequiredDeviceCapability); err != nil {
		return Bundle{}, err
	}
	req = normalizePublishRequest(req)
	var priorFingerprint string
	_ = h.DB.QueryRow(ctx, `SELECT COALESCE(fingerprint, '') FROM device_identity_keys WHERE device_id = $1::uuid`, deviceID).Scan(&priorFingerprint)
	if err := validatePublishRequest(req, priorFingerprint == ""); err != nil {
		return Bundle{}, err
	}
	fingerprint, err := computeFingerprint(req.SigningPublicKey)
	if err != nil {
		return Bundle{}, fmt.Errorf("%w: %v", ErrInvalidBundle, err)
	}
	if req.AgreementIdentityPublicKey == "" {
		req.AgreementIdentityPublicKey = req.IdentityPublicKey
	}
	if req.IdentityPublicKey == "" {
		req.IdentityPublicKey = req.AgreementIdentityPublicKey
	}
	if req.KeyVersion <= 0 {
		req.KeyVersion = 1
	}
	if req.TrustLevel == "" {
		req.TrustLevel = "TRUSTED_SELF"
	}

	tx, err := h.DB.BeginTx(ctx, pgx.TxOptions{})
	if err != nil {
		return Bundle{}, err
	}
	defer tx.Rollback(ctx)

	if _, err := tx.Exec(ctx, `
		INSERT INTO device_identity_keys (
			device_id,
			user_id,
			bundle_version,
			identity_key_alg,
			identity_public_key,
			agreement_identity_public_key,
			signing_key_alg,
			signing_public_key,
			signed_prekey_id,
			signed_prekey_public_key,
			signed_prekey_signature,
			fingerprint,
			key_version,
			trust_level,
			published_at,
			updated_at
		)
		VALUES ($1::uuid, $2::uuid, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, now(), now())
		ON CONFLICT (device_id)
		DO UPDATE SET
			user_id = EXCLUDED.user_id,
			bundle_version = EXCLUDED.bundle_version,
			identity_key_alg = EXCLUDED.identity_key_alg,
			identity_public_key = EXCLUDED.identity_public_key,
			agreement_identity_public_key = EXCLUDED.agreement_identity_public_key,
			signing_key_alg = EXCLUDED.signing_key_alg,
			signing_public_key = EXCLUDED.signing_public_key,
			signed_prekey_id = EXCLUDED.signed_prekey_id,
			signed_prekey_public_key = EXCLUDED.signed_prekey_public_key,
			signed_prekey_signature = EXCLUDED.signed_prekey_signature,
			fingerprint = EXCLUDED.fingerprint,
			key_version = EXCLUDED.key_version,
			trust_level = EXCLUDED.trust_level,
			updated_at = now()
	`, deviceID, actorUserID, req.BundleVersion, req.IdentityKeyAlg, req.IdentityPublicKey, req.AgreementIdentityPublicKey, req.SigningKeyAlg, req.SigningPublicKey, req.SignedPrekeyID, req.SignedPrekeyPublicKey, req.SignedPrekeySignature, fingerprint, req.KeyVersion, req.TrustLevel); err != nil {
		return Bundle{}, err
	}

	if len(req.OneTimePrekeys) > 0 {
		if err := h.insertPrekeysTx(ctx, tx, deviceID, req.OneTimePrekeys); err != nil {
			return Bundle{}, err
		}
	}
	if priorFingerprint != "" && priorFingerprint != fingerprint {
		if _, err := tx.Exec(ctx, `
			UPDATE conversations c
			SET encryption_epoch = encryption_epoch + 1,
			    updated_at = now()
			WHERE c.type = 'DM'
			  AND c.encryption_state = 'ENCRYPTED'
			  AND EXISTS (
			    SELECT 1
			    FROM conversation_members cm
			    WHERE cm.conversation_id = c.id
			      AND cm.user_id = $1::uuid
			  )
		`, actorUserID); err != nil {
			return Bundle{}, err
		}
	}

	if err := tx.Commit(ctx); err != nil {
		return Bundle{}, err
	}
	return h.loadBundle(ctx, actorUserID, deviceID)
}

func (h *Handler) AddOneTimePrekeys(ctx context.Context, actorUserID, deviceID string, prekeys []OneTimePrekey) (Bundle, error) {
	if err := h.ensureDeviceOwnership(ctx, actorUserID, deviceID); err != nil {
		return Bundle{}, err
	}
	if err := h.ensureDeviceCapability(ctx, actorUserID, deviceID, RequiredDeviceCapability); err != nil {
		return Bundle{}, err
	}
	if len(prekeys) == 0 {
		return Bundle{}, ErrEncryptedPrekeysRequired
	}
	tx, err := h.DB.BeginTx(ctx, pgx.TxOptions{})
	if err != nil {
		return Bundle{}, err
	}
	defer tx.Rollback(ctx)

	if err := h.insertPrekeysTx(ctx, tx, deviceID, prekeys); err != nil {
		return Bundle{}, err
	}
	if err := tx.Commit(ctx); err != nil {
		return Bundle{}, err
	}
	return h.loadBundle(ctx, actorUserID, deviceID)
}

func (h *Handler) ListBundlesForUser(ctx context.Context, userID string) ([]Bundle, error) {
	rows, err := h.DB.Query(ctx, `
		SELECT
			dik.device_id::text,
			dik.user_id::text,
			dik.bundle_version,
			dik.identity_key_alg,
			dik.identity_public_key,
			dik.agreement_identity_public_key,
			dik.signing_key_alg,
			dik.signing_public_key,
			dik.signed_prekey_id,
			dik.signed_prekey_public_key,
			dik.signed_prekey_signature,
			COALESCE(dik.fingerprint, ''),
			dik.key_version,
			dik.trust_level,
			dik.published_at,
			dik.updated_at,
			COALESCE((
				SELECT COUNT(1)
				FROM device_one_time_prekeys dotp
				WHERE dotp.device_id = dik.device_id
				  AND dotp.consumed_at IS NULL
			), 0) AS available_one_time_prekeys
		FROM device_identity_keys dik
		WHERE dik.user_id = $1::uuid
		ORDER BY dik.updated_at DESC
	`, userID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	out := make([]Bundle, 0, 4)
	for rows.Next() {
		var bundle Bundle
		var publishedAt time.Time
		var updatedAt time.Time
		if err := rows.Scan(
			&bundle.DeviceID,
			&bundle.UserID,
			&bundle.BundleVersion,
			&bundle.IdentityKeyAlg,
			&bundle.IdentityPublicKey,
			&bundle.AgreementIdentityPublicKey,
			&bundle.SigningKeyAlg,
			&bundle.SigningPublicKey,
			&bundle.SignedPrekeyID,
			&bundle.SignedPrekeyPublicKey,
			&bundle.SignedPrekeySignature,
			&bundle.Fingerprint,
			&bundle.KeyVersion,
			&bundle.TrustLevel,
			&publishedAt,
			&updatedAt,
			&bundle.AvailableOneTimePrekeys,
		); err != nil {
			return nil, err
		}
		bundle.PublishedAt = publishedAt.UTC().Format(time.RFC3339Nano)
		bundle.UpdatedAt = updatedAt.UTC().Format(time.RFC3339Nano)
		if bundle.IdentityPublicKey == "" {
			bundle.IdentityPublicKey = bundle.AgreementIdentityPublicKey
		}
		if bundle.BundleVersion == "" {
			bundle.BundleVersion = LegacyBundleVersion
		}
		bundle.SignedPrekey = SignedPrekey{
			PrekeyID:  bundle.SignedPrekeyID,
			PublicKey: bundle.SignedPrekeyPublicKey,
			Signature: bundle.SignedPrekeySignature,
		}
		out = append(out, bundle)
	}
	return out, rows.Err()
}

func (h *Handler) ClaimBundles(ctx context.Context, userID string) ([]Bundle, error) {
	tx, err := h.DB.BeginTx(ctx, pgx.TxOptions{})
	if err != nil {
		return nil, fmt.Errorf("begin_tx_failed: %w", err)
	}
	defer tx.Rollback(ctx)

	rows, err := tx.Query(ctx, `
		SELECT
			dik.device_id::text,
			dik.user_id::text,
			dik.bundle_version,
			dik.identity_key_alg,
			dik.identity_public_key,
			dik.agreement_identity_public_key,
			dik.signing_key_alg,
			dik.signing_public_key,
			dik.signed_prekey_id,
			dik.signed_prekey_public_key,
			dik.signed_prekey_signature,
			COALESCE(dik.fingerprint, ''),
			dik.key_version,
			dik.trust_level,
			dik.published_at,
			dik.updated_at
		FROM device_identity_keys dik
		WHERE dik.user_id = $1::uuid
		ORDER BY dik.updated_at DESC
		FOR UPDATE
	`, userID)
	if err != nil {
		return nil, fmt.Errorf("bundle_query_failed: %w", err)
	}
	defer rows.Close()

	out := make([]Bundle, 0, 4)
	for rows.Next() {
		var bundle Bundle
		var publishedAt time.Time
		var updatedAt time.Time
		if err := rows.Scan(
			&bundle.DeviceID,
			&bundle.UserID,
			&bundle.BundleVersion,
			&bundle.IdentityKeyAlg,
			&bundle.IdentityPublicKey,
			&bundle.AgreementIdentityPublicKey,
			&bundle.SigningKeyAlg,
			&bundle.SigningPublicKey,
			&bundle.SignedPrekeyID,
			&bundle.SignedPrekeyPublicKey,
			&bundle.SignedPrekeySignature,
			&bundle.Fingerprint,
			&bundle.KeyVersion,
			&bundle.TrustLevel,
			&publishedAt,
			&updatedAt,
		); err != nil {
			return nil, fmt.Errorf("bundle_scan_failed: %w", err)
		}
		bundle.PublishedAt = publishedAt.UTC().Format(time.RFC3339Nano)
		bundle.UpdatedAt = updatedAt.UTC().Format(time.RFC3339Nano)
		if bundle.IdentityPublicKey == "" {
			bundle.IdentityPublicKey = bundle.AgreementIdentityPublicKey
		}
		if bundle.BundleVersion == "" {
			bundle.BundleVersion = LegacyBundleVersion
		}
		bundle.SignedPrekey = SignedPrekey{
			PrekeyID:  bundle.SignedPrekeyID,
			PublicKey: bundle.SignedPrekeyPublicKey,
			Signature: bundle.SignedPrekeySignature,
		}
		out = append(out, bundle)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("rows_iteration_failed: %w", err)
	}
	rows.Close()

	for index := range out {
		var claimed OneTimePrekey
		var consumedAt time.Time
		err := tx.QueryRow(ctx, `
			UPDATE device_one_time_prekeys
			SET consumed_at = now()
			WHERE (device_id, prekey_id) = (
				SELECT device_id, prekey_id
				FROM device_one_time_prekeys
				WHERE device_id = $1::uuid
				  AND consumed_at IS NULL
				ORDER BY created_at ASC
				LIMIT 1
				FOR UPDATE SKIP LOCKED
			)
			RETURNING prekey_id, public_key, consumed_at
		`, out[index].DeviceID).Scan(&claimed.PrekeyID, &claimed.PublicKey, &consumedAt)
		if err == nil {
			out[index].ClaimedOneTimePrekey = &claimed
			continue
		}
		if err != pgx.ErrNoRows {
			return nil, fmt.Errorf("prekey_claim_failed: %w", err)
		}
	}

	if err := tx.Commit(ctx); err != nil {
		return nil, fmt.Errorf("commit_failed: %w", err)
	}
	return out, nil
}

func (h *Handler) ensureDeviceOwnership(ctx context.Context, actorUserID, deviceID string) error {
	var exists bool
	if err := h.DB.QueryRow(ctx, `SELECT EXISTS(SELECT 1 FROM devices WHERE id = $1::uuid AND user_id = $2::uuid)`, deviceID, actorUserID).Scan(&exists); err != nil {
		return err
	}
	if !exists {
		return ErrDeviceNotOwned
	}
	return nil
}

func (h *Handler) ensureDeviceCapability(ctx context.Context, actorUserID, deviceID, capability string) error {
	var exists bool
	if err := h.DB.QueryRow(ctx, `
		SELECT EXISTS(
			SELECT 1
			FROM devices
			WHERE id = $1::uuid
			  AND user_id = $2::uuid
			  AND capabilities @> $3::text[]
		)
	`, deviceID, actorUserID, []string{strings.ToUpper(strings.TrimSpace(capability))}).Scan(&exists); err != nil {
		return err
	}
	if !exists {
		return ErrDeviceCapabilityRequired
	}
	return nil
}

func (h *Handler) insertPrekeysTx(ctx context.Context, tx pgx.Tx, deviceID string, prekeys []OneTimePrekey) error {
	for _, prekey := range prekeys {
		if prekey.PrekeyID <= 0 || prekey.PublicKey == "" {
			return fmt.Errorf("%w: invalid one-time prekey", ErrInvalidBundle)
		}
		if _, err := decodeSizedBase64(prekey.PublicKey, x25519PublicKeySize); err != nil {
			return fmt.Errorf("%w: invalid one-time prekey: %v", ErrInvalidBundle, err)
		}
		if _, err := tx.Exec(ctx, `
			INSERT INTO device_one_time_prekeys (device_id, prekey_id, public_key, consumed_at, created_at)
			VALUES ($1::uuid, $2, $3, NULL, now())
			ON CONFLICT (device_id, prekey_id)
			DO UPDATE SET public_key = EXCLUDED.public_key, consumed_at = NULL
		`, deviceID, prekey.PrekeyID, prekey.PublicKey); err != nil {
			return err
		}
	}
	return nil
}

func (h *Handler) loadBundle(ctx context.Context, actorUserID, deviceID string) (Bundle, error) {
	if err := h.ensureDeviceOwnership(ctx, actorUserID, deviceID); err != nil {
		return Bundle{}, err
	}
	var bundle Bundle
	var publishedAt time.Time
	var updatedAt time.Time
	err := h.DB.QueryRow(ctx, `
		SELECT
			dik.device_id::text,
			dik.user_id::text,
			dik.bundle_version,
			dik.identity_key_alg,
			dik.identity_public_key,
			dik.agreement_identity_public_key,
			dik.signing_key_alg,
			dik.signing_public_key,
			dik.signed_prekey_id,
			dik.signed_prekey_public_key,
			dik.signed_prekey_signature,
			COALESCE(dik.fingerprint, ''),
			dik.key_version,
			dik.trust_level,
			dik.published_at,
			dik.updated_at,
			COALESCE((
				SELECT COUNT(1)
				FROM device_one_time_prekeys dotp
				WHERE dotp.device_id = dik.device_id
				  AND dotp.consumed_at IS NULL
			), 0) AS available_one_time_prekeys
		FROM device_identity_keys dik
		WHERE dik.device_id = $1::uuid
	`, deviceID).Scan(
		&bundle.DeviceID,
		&bundle.UserID,
		&bundle.BundleVersion,
		&bundle.IdentityKeyAlg,
		&bundle.IdentityPublicKey,
		&bundle.AgreementIdentityPublicKey,
		&bundle.SigningKeyAlg,
		&bundle.SigningPublicKey,
		&bundle.SignedPrekeyID,
		&bundle.SignedPrekeyPublicKey,
		&bundle.SignedPrekeySignature,
		&bundle.Fingerprint,
		&bundle.KeyVersion,
		&bundle.TrustLevel,
		&publishedAt,
		&updatedAt,
		&bundle.AvailableOneTimePrekeys,
	)
	if err != nil {
		return Bundle{}, err
	}
	bundle.PublishedAt = publishedAt.UTC().Format(time.RFC3339Nano)
	bundle.UpdatedAt = updatedAt.UTC().Format(time.RFC3339Nano)
	if bundle.IdentityPublicKey == "" {
		bundle.IdentityPublicKey = bundle.AgreementIdentityPublicKey
	}
	if bundle.BundleVersion == "" {
		bundle.BundleVersion = LegacyBundleVersion
	}
	bundle.SignedPrekey = SignedPrekey{
		PrekeyID:  bundle.SignedPrekeyID,
		PublicKey: bundle.SignedPrekeyPublicKey,
		Signature: bundle.SignedPrekeySignature,
	}
	return bundle, nil
}

func normalizePublishRequest(req PublishRequest) PublishRequest {
	if req.BundleVersion == "" {
		req.BundleVersion = BundleVersionSignalV1
	}
	if req.SignedPrekey.PrekeyID > 0 {
		req.SignedPrekeyID = req.SignedPrekey.PrekeyID
	}
	if req.SignedPrekey.PublicKey != "" {
		req.SignedPrekeyPublicKey = req.SignedPrekey.PublicKey
	}
	if req.SignedPrekey.Signature != "" {
		req.SignedPrekeySignature = req.SignedPrekey.Signature
	}
	if req.IdentityKeyAlg == "" {
		req.IdentityKeyAlg = "X25519"
	}
	if req.SigningKeyAlg == "" {
		req.SigningKeyAlg = "Ed25519"
	}
	if req.KeyVersion <= 0 {
		req.KeyVersion = 1
	}
	if req.TrustLevel == "" {
		req.TrustLevel = "TRUSTED_SELF"
	}
	return req
}

func validatePublishRequest(req PublishRequest, initialPublish bool) error {
	if strings.TrimSpace(req.BundleVersion) != BundleVersionSignalV1 {
		return fmt.Errorf("%w: bundle_version must be %s", ErrInvalidBundle, BundleVersionSignalV1)
	}
	if strings.ToUpper(strings.TrimSpace(req.IdentityKeyAlg)) != "X25519" {
		return fmt.Errorf("%w: identity_key_alg must be X25519", ErrInvalidBundle)
	}
	if strings.ToUpper(strings.TrimSpace(req.SigningKeyAlg)) != "ED25519" {
		return fmt.Errorf("%w: signing_key_alg must be Ed25519", ErrInvalidBundle)
	}
	if req.AgreementIdentityPublicKey == "" {
		return fmt.Errorf("%w: agreement_identity_public_key required", ErrInvalidBundle)
	}
	if req.SigningPublicKey == "" {
		return fmt.Errorf("%w: signing_public_key required", ErrInvalidBundle)
	}
	if req.SignedPrekeyID <= 0 || req.SignedPrekeyPublicKey == "" || req.SignedPrekeySignature == "" {
		return fmt.Errorf("%w: signed_prekey required", ErrInvalidBundle)
	}
	if _, err := decodeSizedBase64(req.AgreementIdentityPublicKey, x25519PublicKeySize); err != nil {
		return fmt.Errorf("%w: agreement_identity_public_key: %v", ErrInvalidBundle, err)
	}
	if _, err := decodeSizedBase64(req.SigningPublicKey, ed25519PublicKeySize); err != nil {
		return fmt.Errorf("%w: signing_public_key: %v", ErrInvalidBundle, err)
	}
	if _, err := decodeSizedBase64(req.SignedPrekeyPublicKey, x25519PublicKeySize); err != nil {
		return fmt.Errorf("%w: signed_prekey.public_key: %v", ErrInvalidBundle, err)
	}
	signingPublicKey, err := decodeSizedBase64(req.SigningPublicKey, ed25519PublicKeySize)
	if err != nil {
		return fmt.Errorf("%w: signing_public_key: %v", ErrInvalidBundle, err)
	}
	signature, err := decodeSizedBase64(req.SignedPrekeySignature, ed25519SignatureSize)
	if err != nil {
		return fmt.Errorf("%w: signed_prekey.signature: %v", ErrInvalidBundle, err)
	}
	payload := signedPrekeyPayload(req.BundleVersion, req.SignedPrekeyID, req.SignedPrekeyPublicKey)
	if !ed25519.Verify(ed25519.PublicKey(signingPublicKey), []byte(payload), signature) {
		return fmt.Errorf("%w: signed_prekey signature invalid", ErrInvalidBundle)
	}
	if initialPublish && len(req.OneTimePrekeys) < minInitialOneTimePrekeys {
		return fmt.Errorf("%w: initial publish requires at least %d one-time prekeys", ErrEncryptedPrekeysInsufficient, minInitialOneTimePrekeys)
	}
	for _, prekey := range req.OneTimePrekeys {
		if prekey.PrekeyID <= 0 || prekey.PublicKey == "" {
			return fmt.Errorf("%w: invalid one-time prekey", ErrInvalidBundle)
		}
		if _, err := decodeSizedBase64(prekey.PublicKey, x25519PublicKeySize); err != nil {
			return fmt.Errorf("%w: invalid one-time prekey: %v", ErrInvalidBundle, err)
		}
	}
	return nil
}

func decodeSizedBase64(value string, size int) ([]byte, error) {
	decoded, err := base64.StdEncoding.DecodeString(strings.TrimSpace(value))
	if err != nil {
		decoded, err = base64.RawStdEncoding.DecodeString(strings.TrimSpace(value))
		if err != nil {
			return nil, err
		}
	}
	if len(decoded) != size {
		return nil, fmt.Errorf("expected %d bytes", size)
	}
	return decoded, nil
}

func computeFingerprint(signingPublicKey string) (string, error) {
	decoded, err := decodeSizedBase64(signingPublicKey, ed25519PublicKeySize)
	if err != nil {
		return "", err
	}
	sum := sha256.Sum256(decoded)
	return hex.EncodeToString(sum[:]), nil
}

func signedPrekeyPayload(bundleVersion string, prekeyID int64, publicKey string) string {
	return strings.Join([]string{strings.TrimSpace(bundleVersion), "signed_prekey", fmt.Sprintf("%d", prekeyID), strings.TrimSpace(publicKey)}, "|")
}

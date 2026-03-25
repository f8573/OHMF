package e2ee

import (
	"encoding/json"
	"errors"
	"net/http"
	"time"

	"github.com/go-chi/chi/v5"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"ohmf/services/gateway/internal/httpx"
	"ohmf/services/gateway/internal/middleware"
)

// Handler handles E2EE HTTP endpoints
type Handler struct {
	pool *pgxpool.Pool
	sm   *SessionManager
}

// NewHandler creates a new E2EE handler
func NewHandler(pool *pgxpool.Pool) *Handler {
	return &Handler{
		pool: pool,
		sm:   &SessionManager{db: pool}, // removed: NewSessionManager wrapper
	}
}

// DeviceKeyResponse represents a device key in API response
type DeviceKeyResponse struct {
	DeviceID          string  `json:"device_id"`
	UserID            string  `json:"user_id"`
	BundleVersion     string  `json:"bundle_version"`
	IdentityKeyAlg    string  `json:"identity_key_alg"`
	IdentityPublicKey string  `json:"identity_public_key"`
	SigningKeyAlg     string  `json:"signing_key_alg"`
	SigningPublicKey  string  `json:"signing_public_key"`
	Fingerprint       string  `json:"fingerprint"`
	PublishedAt       *string `json:"published_at,omitempty"`
}

// BundleResponse represents a full Signal protocol key bundle
type BundleResponse struct {
	DeviceID                   string            `json:"device_id"`
	UserID                     string            `json:"user_id"`
	BundleVersion              string            `json:"bundle_version"`
	IdentityKeyAlg             string            `json:"identity_key_alg"`
	IdentityPublicKey          string            `json:"identity_public_key"`
	AgreementIdentityPublicKey string            `json:"agreement_identity_public_key"`
	SigningKeyAlg              string            `json:"signing_key_alg"`
	SigningPublicKey           string            `json:"signing_public_key"`
	SignedPrekeyID             int64             `json:"signed_prekey_id"`
	SignedPrekeyPublicKey      string            `json:"signed_prekey_public_key"`
	SignedPrekeySignature      string            `json:"signed_prekey_signature"`
	Fingerprint                string            `json:"fingerprint"`
	ClaimedOneTimePrekey       *ClaimOTPResponse `json:"claimed_one_time_prekey,omitempty"`
}

// ClaimOTPResponse represents the response when claiming an OTP
type ClaimOTPResponse struct {
	PrekeyID  int64  `json:"prekey_id"`
	PublicKey string `json:"public_key"`
}

// removed: duplicate inline struct - VerifyFingerprintRequest already at line 69

// VerifyFingerprintRequest represents the request to verify a device fingerprint
type VerifyFingerprintRequest struct {
	ContactUserID   string `json:"contact_user_id"`
	ContactDeviceID string `json:"contact_device_id"`
	Fingerprint     string `json:"fingerprint"`
}

type RevokeFingerprintRequest struct {
	ContactUserID   string `json:"contact_user_id"`
	ContactDeviceID string `json:"contact_device_id"`
}

// VerifyFingerprintResponse represents the response from fingerprint verification
type VerifyFingerprintResponse struct {
	Verified   bool   `json:"verified"`
	TrustState string `json:"trust_state"`
	Message    string `json:"message,omitempty"`
}

// ListDeviceKeys handles GET /v1/e2ee/keys
// Returns the public keys for all devices of the authenticated user
func (h *Handler) ListDeviceKeys(w http.ResponseWriter, r *http.Request) {
	userID, ok := middleware.UserIDFromContext(r.Context())
	if !ok {
		httpx.WriteError(w, r, http.StatusUnauthorized, "unauthorized", "user context required", nil)
		return
	}

	query := `
		SELECT device_id, user_id, key_version, identity_key_alg, identity_public_key,
		       signing_key_alg, signing_public_key, published_at
		FROM device_identity_keys
		WHERE user_id = $1::uuid
		ORDER BY published_at DESC
	`

	rows, err := h.pool.Query(r.Context(), query, userID)
	if err != nil {
		httpx.WriteError(w, r, http.StatusInternalServerError, "database_error", "failed to query device keys", nil)
		return
	}
	defer rows.Close()

	var keys []DeviceKeyResponse
	for rows.Next() {
		var deviceID, userID, identityKeyAlg, identityPublicKey, signingKeyAlg, signingPublicKey string
		var keyVersion int
		var publishedAt *time.Time

		if err := rows.Scan(&deviceID, &userID, &keyVersion, &identityKeyAlg, &identityPublicKey,
			&signingKeyAlg, &signingPublicKey, &publishedAt); err != nil {
			httpx.WriteError(w, r, http.StatusInternalServerError, "database_error", "failed to scan device key", nil)
			return
		}

		// Compute fingerprint
		fingerprint, err := ComputeFingerprint(signingPublicKey)
		if err != nil {
			httpx.WriteError(w, r, http.StatusInternalServerError, "crypto_error", "failed to compute fingerprint", nil)
			return
		}

		var publishedAtStr *string
		if publishedAt != nil {
			s := publishedAt.String()
			publishedAtStr = &s
		}

		keys = append(keys, DeviceKeyResponse{
			DeviceID:          deviceID,
			UserID:            userID,
			BundleVersion:     "OHMF_SIGNAL_V1",
			IdentityKeyAlg:    identityKeyAlg,
			IdentityPublicKey: identityPublicKey,
			SigningKeyAlg:     signingKeyAlg,
			SigningPublicKey:  signingPublicKey,
			Fingerprint:       fingerprint,
			PublishedAt:       publishedAtStr,
		})
	}

	httpx.WriteJSON(w, http.StatusOK, keys)
}

// GetDeviceKeyBundle handles GET /v1/e2ee/keys/{user_id}/{device_id}
// Returns the full key bundle for X3DH key exchange including a claimed one-time prekey
func (h *Handler) GetDeviceKeyBundle(w http.ResponseWriter, r *http.Request) {
	targetUserID := chi.URLParam(r, "user_id")
	targetDeviceID := chi.URLParam(r, "device_id")

	if targetUserID == "" {
		targetUserID = chi.URLParam(r, "userID") // Support alternate naming convention
	}
	if targetDeviceID == "" {
		targetDeviceID = chi.URLParam(r, "deviceID") // Support alternate naming convention
	}

	if targetUserID == "" || targetDeviceID == "" {
		httpx.WriteError(w, r, http.StatusBadRequest, "missing_param", "user_id and device_id path parameters required", nil)
		return
	}

	// Query device bundle for specific device
	query := `
		SELECT device_id, user_id, signed_prekey_id, identity_key_alg, identity_public_key,
		       signed_prekey_public_key, signed_prekey_signature, signing_key_alg, signing_public_key
		FROM device_identity_keys
		WHERE user_id = $1::uuid AND device_id = $2::uuid
		LIMIT 1
	`

	var deviceID, userID, identityKeyAlg, identityPublicKey, signedPrekeyPublicKey, signedPrekeySignature, signingKeyAlg, signingPublicKey string
	var signedPrekeyID int64

	err := h.pool.QueryRow(r.Context(), query, targetUserID, targetDeviceID).Scan(
		&deviceID, &userID, &signedPrekeyID, &identityKeyAlg, &identityPublicKey,
		&signedPrekeyPublicKey, &signedPrekeySignature, &signingKeyAlg, &signingPublicKey,
	)

	if err != nil {
		if err == pgx.ErrNoRows {
			httpx.WriteError(w, r, http.StatusNotFound, "not_found", "device key bundle not found", nil)
			return
		}
		httpx.WriteError(w, r, http.StatusInternalServerError, "database_error", "failed to query device bundle", nil)
		return
	}

	// Compute fingerprint
	fingerprint, err := ComputeFingerprint(signingPublicKey)
	if err != nil {
		httpx.WriteError(w, r, http.StatusInternalServerError, "crypto_error", "failed to compute fingerprint", nil)
		return
	}

	// Try to claim an available one-time prekey
	otpQuery := `
		UPDATE device_one_time_prekeys
		SET consumed_at = NOW()
		WHERE device_id = $1::uuid AND consumed_at IS NULL
		RETURNING prekey_id, public_key
		LIMIT 1
	`

	var otpPreKeyID int64
	var otpPublicKey string
	var claimedOTP *ClaimOTPResponse

	err = h.pool.QueryRow(r.Context(), otpQuery, deviceID).Scan(&otpPreKeyID, &otpPublicKey)
	if err == nil {
		claimedOTP = &ClaimOTPResponse{
			PrekeyID:  otpPreKeyID,
			PublicKey: otpPublicKey,
		}
	} else if err != pgx.ErrNoRows {
		httpx.WriteError(w, r, http.StatusInternalServerError, "database_error", "failed to claim prekey", nil)
		return
	}
	// removed: inline struct definition - use ClaimOTPResponse type

	bundle := BundleResponse{
		DeviceID:                   deviceID,
		UserID:                     userID,
		BundleVersion:              "OHMF_SIGNAL_V1",
		IdentityKeyAlg:             identityKeyAlg,
		IdentityPublicKey:          identityPublicKey,
		AgreementIdentityPublicKey: identityPublicKey, // For X3DH
		SigningKeyAlg:              signingKeyAlg,
		SigningPublicKey:           signingPublicKey,
		SignedPrekeyID:             signedPrekeyID,
		SignedPrekeyPublicKey:      signedPrekeyPublicKey,
		SignedPrekeySignature:      signedPrekeySignature,
		Fingerprint:                fingerprint,
		ClaimedOneTimePrekey:       claimedOTP,
	}

	httpx.WriteJSON(w, http.StatusOK, bundle)
}

// ClaimOneTimePrekey handles POST /v1/e2ee/claim-prekey
// Atomically claims the next available one-time prekey for a device
func (h *Handler) ClaimOneTimePrekey(w http.ResponseWriter, r *http.Request) {
	// Verify user is authenticated
	if _, ok := middleware.UserIDFromContext(r.Context()); !ok {
		httpx.WriteError(w, r, http.StatusUnauthorized, "unauthorized", "user context required", nil)
		return
	}

	// Parse request body to get target device
	var req struct {
		UserID   string `json:"user_id"`
		DeviceID string `json:"device_id"`
	}

	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		httpx.WriteError(w, r, http.StatusBadRequest, "invalid_json", "failed to parse request body", nil)
		return
	}

	if req.UserID == "" || req.DeviceID == "" {
		httpx.WriteError(w, r, http.StatusBadRequest, "missing_field", "user_id and device_id required", nil)
		return
	}

	// Verify device exists and belongs to target user
	verifyQuery := `
		SELECT user_id FROM devices WHERE id = $1::uuid
	`
	var deviceUserID string
	err := h.pool.QueryRow(r.Context(), verifyQuery, req.DeviceID).Scan(&deviceUserID)
	if err != nil {
		if err == pgx.ErrNoRows {
			httpx.WriteError(w, r, http.StatusNotFound, "not_found", "device not found", nil)
			return
		}
		httpx.WriteError(w, r, http.StatusInternalServerError, "database_error", "failed to verify device", nil)
		return
	}

	if deviceUserID != req.UserID {
		httpx.WriteError(w, r, http.StatusForbidden, "forbidden", "device does not belong to specified user", nil)
		return
	}

	// Atomically claim next available prekey
	claimQuery := `
		UPDATE device_one_time_prekeys
		SET consumed_at = NOW()
		WHERE device_id = $1::uuid AND consumed_at IS NULL
		RETURNING prekey_id, public_key
		LIMIT 1
	`

	var prekeyID int64
	var publicKey string

	err = h.pool.QueryRow(r.Context(), claimQuery, req.DeviceID).Scan(&prekeyID, &publicKey)
	if err != nil {
		if err == pgx.ErrNoRows {
			httpx.WriteError(w, r, http.StatusNotFound, "prekey_exhausted", "no available one-time prekeys for device", nil)
			return
		}
		httpx.WriteError(w, r, http.StatusInternalServerError, "database_error", "failed to claim prekey", nil)
		return
	}

	response := ClaimOTPResponse{
		PrekeyID:  prekeyID,
		PublicKey: publicKey,
	}

	httpx.WriteJSON(w, http.StatusOK, response)
}

// VerifyDeviceFingerprint handles POST /v1/e2ee/verify
// Verifies and records TOFU (Trust on First Use) trust for device fingerprints
func (h *Handler) VerifyDeviceFingerprint(w http.ResponseWriter, r *http.Request) {
	userID, ok := middleware.UserIDFromContext(r.Context())
	if !ok {
		httpx.WriteError(w, r, http.StatusUnauthorized, "unauthorized", "user context required", nil)
		return
	}

	var req VerifyFingerprintRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		httpx.WriteError(w, r, http.StatusBadRequest, "invalid_json", "failed to parse request body", nil)
		return
	}

	if req.ContactUserID == "" || req.ContactDeviceID == "" || req.Fingerprint == "" {
		httpx.WriteError(w, r, http.StatusBadRequest, "missing_field", "contact_user_id, contact_device_id, and fingerprint required", nil)
		return
	}

	view, err := trustStateVerifier(r.Context(), h.sm, userID, req.ContactUserID, req.ContactDeviceID, req.Fingerprint)
	if err != nil {
		switch {
		case errors.Is(err, ErrTrustDeviceNotFound):
			httpx.WriteError(w, r, http.StatusNotFound, "not_found", "contact device not found", nil)
		case errors.Is(err, ErrTrustFingerprintMismatch):
			httpx.WriteError(w, r, http.StatusConflict, "fingerprint_mismatch", "fingerprint does not match the current published device fingerprint", nil)
		default:
			httpx.WriteError(w, r, http.StatusInternalServerError, "database_error", "failed to update trust state", nil)
		}
		return
	}

	httpx.WriteJSON(w, http.StatusOK, view)
}

// GetTrustState handles GET /v1/e2ee/trust/{contact_user_id}/{device_id}
// Returns the current trust state for a device
func (h *Handler) GetTrustState(w http.ResponseWriter, r *http.Request) {
	userID, ok := middleware.UserIDFromContext(r.Context())
	if !ok {
		httpx.WriteError(w, r, http.StatusUnauthorized, "unauthorized", "user context required", nil)
		return
	}

	contactUserID := chi.URLParam(r, "contact_user_id")
	contactDeviceID := chi.URLParam(r, "device_id")
	if contactUserID == "" {
		contactUserID = r.URL.Query().Get("contact_user_id")
	}
	if contactDeviceID == "" {
		contactDeviceID = r.URL.Query().Get("contact_device_id")
	}

	if contactUserID == "" || contactDeviceID == "" {
		httpx.WriteError(w, r, http.StatusBadRequest, "missing_param", "contact_user_id and contact_device_id are required", nil)
		return
	}

	view, err := trustStateFetcher(r.Context(), h.sm, userID, contactUserID, contactDeviceID)
	if err != nil {
		if errors.Is(err, ErrTrustDeviceNotFound) {
			httpx.WriteError(w, r, http.StatusNotFound, "not_found", "contact device not found", nil)
			return
		}
		httpx.WriteError(w, r, http.StatusInternalServerError, "database_error", "failed to query trust state", nil)
		return
	}
	httpx.WriteJSON(w, http.StatusOK, view)
}

func (h *Handler) RevokeDeviceFingerprint(w http.ResponseWriter, r *http.Request) {
	userID, ok := middleware.UserIDFromContext(r.Context())
	if !ok {
		httpx.WriteError(w, r, http.StatusUnauthorized, "unauthorized", "user context required", nil)
		return
	}

	var req RevokeFingerprintRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		httpx.WriteError(w, r, http.StatusBadRequest, "invalid_json", "failed to parse request body", nil)
		return
	}

	if req.ContactUserID == "" || req.ContactDeviceID == "" {
		httpx.WriteError(w, r, http.StatusBadRequest, "missing_field", "contact_user_id and contact_device_id required", nil)
		return
	}

	view, err := trustStateRevoker(r.Context(), h.sm, userID, req.ContactUserID, req.ContactDeviceID)
	if err != nil {
		if errors.Is(err, ErrTrustDeviceNotFound) {
			httpx.WriteError(w, r, http.StatusNotFound, "not_found", "contact device not found", nil)
			return
		}
		httpx.WriteError(w, r, http.StatusInternalServerError, "database_error", "failed to revoke trust state", nil)
		return
	}

	httpx.WriteJSON(w, http.StatusOK, view)
}

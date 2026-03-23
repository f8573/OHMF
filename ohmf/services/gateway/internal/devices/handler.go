package devices

import (
	"encoding/json"
	"errors"
	"net/http"

	"github.com/go-chi/chi/v5"
	"ohmf/services/gateway/internal/devicekeys"
	"ohmf/services/gateway/internal/httpx"
	"ohmf/services/gateway/internal/middleware"
)

// Handler handles HTTP requests for device management
type Handler struct {
	svc     *Service
	keysSvc *devicekeys.Service
}

// NewHandler creates a handler for device operations
func NewHandler(svc *Service, keysSvc *devicekeys.Service) *Handler {
	return &Handler{svc: svc, keysSvc: keysSvc}
}

// Register registers a new device for the user
func (h *Handler) Register(w http.ResponseWriter, r *http.Request) {
	userID, ok := middleware.UserIDFromContext(r.Context())
	if !ok {
		httpx.WriteError(w, r, http.StatusUnauthorized, "unauthorized", "missing auth", nil)
		return
	}
	var d Device
	if err := json.NewDecoder(r.Body).Decode(&d); err != nil {
		httpx.WriteError(w, r, http.StatusBadRequest, "invalid_json", "invalid request body", nil)
		return
	}
	id, err := h.svc.RegisterDevice(r.Context(), userID, d)
	if err != nil {
		httpx.WriteError(w, r, http.StatusInternalServerError, "register_failed", err.Error(), nil)
		return
	}

	// Auto-generate and publish E2EE key bundle for the new device
	if h.keysSvc != nil {
		if err := h.keysSvc.GenerateAndPublishDefaultBundle(r.Context(), userID, id); err != nil {
			// Log the error but don't fail the device registration
			// The device is already created, but key generation failed
			httpx.WriteError(w, r, http.StatusInternalServerError, "key_generation_failed", err.Error(), nil)
			return
		}
	}

	httpx.WriteJSON(w, http.StatusOK, map[string]any{"device_id": id})
}

// Update updates device information
func (h *Handler) Update(w http.ResponseWriter, r *http.Request) {
	userID, ok := middleware.UserIDFromContext(r.Context())
	if !ok {
		httpx.WriteError(w, r, http.StatusUnauthorized, "unauthorized", "missing auth", nil)
		return
	}
	deviceID := chi.URLParam(r, "id")
	if deviceID == "" {
		httpx.WriteError(w, r, http.StatusBadRequest, "invalid_request", "missing device id", nil)
		return
	}
	var d Device
	if err := json.NewDecoder(r.Body).Decode(&d); err != nil {
		httpx.WriteError(w, r, http.StatusBadRequest, "invalid_json", "invalid request body", nil)
		return
	}
	device, err := h.svc.UpdateDevice(r.Context(), userID, deviceID, d)
	if err != nil {
		status := http.StatusInternalServerError
		code := "update_failed"
		if err == ErrDeviceNotFound {
			status = http.StatusNotFound
			code = "device_not_found"
		}
		httpx.WriteError(w, r, status, code, err.Error(), nil)
		return
	}
	httpx.WriteJSON(w, http.StatusOK, device)
}

// List retrieves all devices for the user
func (h *Handler) List(w http.ResponseWriter, r *http.Request) {
	userID, ok := middleware.UserIDFromContext(r.Context())
	if !ok {
		httpx.WriteError(w, r, http.StatusUnauthorized, "unauthorized", "missing auth", nil)
		return
	}
	ds, err := h.svc.ListDevices(r.Context(), userID)
	if err != nil {
		httpx.WriteError(w, r, http.StatusInternalServerError, "list_failed", err.Error(), nil)
		return
	}
	httpx.WriteJSON(w, http.StatusOK, map[string]any{"devices": ds})
}

// Revoke revokes a device
func (h *Handler) Revoke(w http.ResponseWriter, r *http.Request) {
	userID, ok := middleware.UserIDFromContext(r.Context())
	if !ok {
		httpx.WriteError(w, r, http.StatusUnauthorized, "unauthorized", "missing auth", nil)
		return
	}
	id := chi.URLParam(r, "id")
	if id == "" {
		httpx.WriteError(w, r, http.StatusBadRequest, "invalid_request", "missing device id", nil)
		return
	}
	if err := h.svc.RevokeDevice(r.Context(), userID, id); err != nil {
		if errors.Is(err, ErrDeviceNotFound) {
			httpx.WriteError(w, r, http.StatusNotFound, "not_found", "device not found", nil)
			return
		}
		httpx.WriteError(w, r, http.StatusInternalServerError, "revoke_failed", err.Error(), nil)
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

// RegisterPushToken registers a push notification token for a device
func (h *Handler) RegisterPushToken(w http.ResponseWriter, r *http.Request) {
	userID, ok := middleware.UserIDFromContext(r.Context())
	if !ok {
		httpx.WriteError(w, r, http.StatusUnauthorized, "unauthorized", "missing auth", nil)
		return
	}
	deviceID := chi.URLParam(r, "id")
	if deviceID == "" {
		httpx.WriteError(w, r, http.StatusBadRequest, "invalid_request", "missing device id", nil)
		return
	}

	var req struct {
		ProviderType string `json:"provider_type"` // 'fcm' or 'apns'
		Token        string `json:"token"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		httpx.WriteError(w, r, http.StatusBadRequest, "invalid_json", "invalid request body", nil)
		return
	}

	if req.ProviderType == "" || req.Token == "" {
		httpx.WriteError(w, r, http.StatusBadRequest, "missing_fields", "provider_type and token required", nil)
		return
	}

	if err := h.svc.RegisterPushToken(r.Context(), userID, deviceID, req.ProviderType, req.Token); err != nil {
		if err == ErrDeviceNotFound {
			httpx.WriteError(w, r, http.StatusNotFound, "device_not_found", "device not found", nil)
			return
		}
		httpx.WriteError(w, r, http.StatusInternalServerError, "register_failed", err.Error(), nil)
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

func (h *Handler) CreateAttestationChallenge(w http.ResponseWriter, r *http.Request) {
	userID, ok := middleware.UserIDFromContext(r.Context())
	if !ok {
		httpx.WriteError(w, r, http.StatusUnauthorized, "unauthorized", "missing auth", nil)
		return
	}
	deviceID := chi.URLParam(r, "id")
	if deviceID == "" {
		httpx.WriteError(w, r, http.StatusBadRequest, "invalid_request", "missing device id", nil)
		return
	}
	challenge, err := h.svc.CreateAttestationChallenge(r.Context(), userID, deviceID)
	if err != nil {
		switch {
		case errors.Is(err, ErrDeviceNotFound):
			httpx.WriteError(w, r, http.StatusNotFound, "device_not_found", "device not found", nil)
		case errors.Is(err, ErrAttestationDisabled):
			httpx.WriteError(w, r, http.StatusNotImplemented, "attestation_disabled", err.Error(), nil)
		default:
			httpx.WriteError(w, r, http.StatusInternalServerError, "challenge_failed", err.Error(), nil)
		}
		return
	}
	httpx.WriteJSON(w, http.StatusOK, challenge)
}

func (h *Handler) VerifyAttestation(w http.ResponseWriter, r *http.Request) {
	userID, ok := middleware.UserIDFromContext(r.Context())
	if !ok {
		httpx.WriteError(w, r, http.StatusUnauthorized, "unauthorized", "missing auth", nil)
		return
	}
	deviceID := chi.URLParam(r, "id")
	if deviceID == "" {
		httpx.WriteError(w, r, http.StatusBadRequest, "invalid_request", "missing device id", nil)
		return
	}
	var req AttestationStatement
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		httpx.WriteError(w, r, http.StatusBadRequest, "invalid_json", "invalid request body", nil)
		return
	}
	device, err := h.svc.VerifyAttestation(r.Context(), userID, deviceID, req)
	if err != nil {
		switch {
		case errors.Is(err, ErrDeviceNotFound):
			httpx.WriteError(w, r, http.StatusNotFound, "device_not_found", "device not found", nil)
		case errors.Is(err, ErrAttestationDisabled):
			httpx.WriteError(w, r, http.StatusNotImplemented, "attestation_disabled", err.Error(), nil)
		case errors.Is(err, ErrAttestationChallengeNotFound):
			httpx.WriteError(w, r, http.StatusGone, "challenge_not_found", err.Error(), nil)
		default:
			httpx.WriteError(w, r, http.StatusUnauthorized, "attestation_invalid", err.Error(), nil)
		}
		return
	}
	httpx.WriteJSON(w, http.StatusOK, device)
}

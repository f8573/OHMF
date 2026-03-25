package e2ee

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"ohmf/services/gateway/internal/middleware"
)

func withTestUser(req *http.Request) *http.Request {
	return req.WithContext(middleware.WithUserID(req.Context(), "user-1"))
}

func swapTrustHandlers(
	t *testing.T,
	fetcher func(context.Context, *SessionManager, string, string, string) (*TrustStateView, error),
	verifier func(context.Context, *SessionManager, string, string, string, string) (*TrustStateView, error),
	revoker func(context.Context, *SessionManager, string, string, string) (*TrustStateView, error),
) {
	t.Helper()
	originalFetcher := trustStateFetcher
	originalVerifier := trustStateVerifier
	originalRevoker := trustStateRevoker
	trustStateFetcher = fetcher
	trustStateVerifier = verifier
	trustStateRevoker = revoker
	t.Cleanup(func() {
		trustStateFetcher = originalFetcher
		trustStateVerifier = originalVerifier
		trustStateRevoker = originalRevoker
	})
}

func TestGetTrustStateUsesQueryContract(t *testing.T) {
	swapTrustHandlers(t,
		func(_ context.Context, _ *SessionManager, userID string, contactUserID string, contactDeviceID string) (*TrustStateView, error) {
			if userID != "user-1" || contactUserID != "contact-1" || contactDeviceID != "device-1" {
				t.Fatalf("unexpected lookup args: %s %s %s", userID, contactUserID, contactDeviceID)
			}
			return &TrustStateView{
				ContactUserID:       contactUserID,
				ContactDeviceID:     contactDeviceID,
				TrustState:          "VERIFIED",
				EffectiveTrustState: "VERIFIED",
				CurrentFingerprint:  "fingerprint-1",
			}, nil
		},
		nil,
		nil,
	)

	handler := &Handler{sm: &SessionManager{}}
	req := withTestUser(httptest.NewRequest("GET", "/v1/e2ee/session/trust-state?contact_user_id=contact-1&contact_device_id=device-1", nil))
	w := httptest.NewRecorder()

	handler.GetTrustState(w, req)

	if w.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", w.Code)
	}
	var payload TrustStateView
	if err := json.Unmarshal(w.Body.Bytes(), &payload); err != nil {
		t.Fatalf("decode response: %v", err)
	}
	if payload.EffectiveTrustState != "VERIFIED" {
		t.Fatalf("expected VERIFIED, got %s", payload.EffectiveTrustState)
	}
}

func TestVerifyDeviceFingerprintRejectsForgedUpdates(t *testing.T) {
	swapTrustHandlers(t,
		nil,
		func(context.Context, *SessionManager, string, string, string, string) (*TrustStateView, error) {
			return nil, ErrTrustFingerprintMismatch
		},
		nil,
	)

	handler := &Handler{sm: &SessionManager{}}
	req := withTestUser(httptest.NewRequest("POST", "/v1/e2ee/session/verify", strings.NewReader(`{"contact_user_id":"contact-1","contact_device_id":"device-1","fingerprint":"forged"}`)))
	req.Header.Set("Content-Type", "application/json")
	w := httptest.NewRecorder()

	handler.VerifyDeviceFingerprint(w, req)

	if w.Code != http.StatusConflict {
		t.Fatalf("expected 409, got %d", w.Code)
	}
	if !strings.Contains(w.Body.String(), "fingerprint_mismatch") {
		t.Fatalf("expected fingerprint_mismatch error, got %s", w.Body.String())
	}
}

func TestVerifyDeviceFingerprintReturnsVerifiedState(t *testing.T) {
	swapTrustHandlers(t,
		nil,
		func(_ context.Context, _ *SessionManager, _ string, contactUserID string, contactDeviceID string, fingerprint string) (*TrustStateView, error) {
			if fingerprint != "fingerprint-1" {
				t.Fatalf("unexpected fingerprint %s", fingerprint)
			}
			return &TrustStateView{
				ContactUserID:       contactUserID,
				ContactDeviceID:     contactDeviceID,
				TrustState:          "VERIFIED",
				EffectiveTrustState: "VERIFIED",
				CurrentFingerprint:  fingerprint,
			}, nil
		},
		nil,
	)

	handler := &Handler{sm: &SessionManager{}}
	req := withTestUser(httptest.NewRequest("POST", "/v1/e2ee/session/verify", strings.NewReader(`{"contact_user_id":"contact-1","contact_device_id":"device-1","fingerprint":"fingerprint-1"}`)))
	req.Header.Set("Content-Type", "application/json")
	w := httptest.NewRecorder()

	handler.VerifyDeviceFingerprint(w, req)

	if w.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", w.Code)
	}
	if !strings.Contains(w.Body.String(), `"effective_trust_state":"VERIFIED"`) {
		t.Fatalf("expected verified payload, got %s", w.Body.String())
	}
}

func TestRevokeDeviceFingerprintRejectsMalformedRequest(t *testing.T) {
	handler := &Handler{sm: &SessionManager{}}
	req := withTestUser(httptest.NewRequest("POST", "/v1/e2ee/session/revoke", strings.NewReader(`{"contact_user_id":"contact-1"}`)))
	req.Header.Set("Content-Type", "application/json")
	w := httptest.NewRecorder()

	handler.RevokeDeviceFingerprint(w, req)

	if w.Code != http.StatusBadRequest {
		t.Fatalf("expected 400, got %d", w.Code)
	}
}

func TestRevokeDeviceFingerprintReturnsRevokedState(t *testing.T) {
	swapTrustHandlers(t,
		nil,
		nil,
		func(_ context.Context, _ *SessionManager, _ string, contactUserID string, contactDeviceID string) (*TrustStateView, error) {
			return &TrustStateView{
				ContactUserID:       contactUserID,
				ContactDeviceID:     contactDeviceID,
				TrustState:          "REVOKED",
				EffectiveTrustState: "REVOKED",
				CurrentFingerprint:  "fingerprint-1",
				Warning:             "Verification was revoked for this device.",
			}, nil
		},
	)

	handler := &Handler{sm: &SessionManager{}}
	req := withTestUser(httptest.NewRequest("POST", "/v1/e2ee/session/revoke", strings.NewReader(`{"contact_user_id":"contact-1","contact_device_id":"device-1"}`)))
	req.Header.Set("Content-Type", "application/json")
	w := httptest.NewRecorder()

	handler.RevokeDeviceFingerprint(w, req)

	if w.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", w.Code)
	}
	if !strings.Contains(w.Body.String(), `"effective_trust_state":"REVOKED"`) {
		t.Fatalf("expected revoked payload, got %s", w.Body.String())
	}
}

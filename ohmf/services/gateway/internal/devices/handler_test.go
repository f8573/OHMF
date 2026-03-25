package devices

import (
	"net/http"
	"net/http/httptest"
	"testing"
)

func TestRevokeRequiresAuth(t *testing.T) {
	req := httptest.NewRequest(http.MethodDelete, "/v1/devices/device-1", nil)
	rec := httptest.NewRecorder()

	(&Handler{}).Revoke(rec, req)

	if rec.Code != http.StatusUnauthorized {
		t.Fatalf("expected 401, got %d", rec.Code)
	}
}

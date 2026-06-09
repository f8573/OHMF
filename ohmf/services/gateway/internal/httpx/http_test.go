package httpx

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	chimiddleware "github.com/go-chi/chi/v5/middleware"
)

func TestWriteErrorUsesChiRequestID(t *testing.T) {
	req := httptest.NewRequest(http.MethodGet, "/v1/messages", nil)
	req = req.WithContext(context.WithValue(req.Context(), chimiddleware.RequestIDKey, "req-123"))
	rec := httptest.NewRecorder()

	WriteError(rec, req, http.StatusInternalServerError, "send_failed", "boom", nil)

	if got := rec.Header().Get("X-Request-ID"); got != "req-123" {
		t.Fatalf("expected X-Request-ID header, got %q", got)
	}
	var envelope ErrorEnvelope
	if err := json.Unmarshal(rec.Body.Bytes(), &envelope); err != nil {
		t.Fatalf("decode error envelope: %v", err)
	}
	if envelope.RequestID != "req-123" {
		t.Fatalf("expected request_id in body, got %q", envelope.RequestID)
	}
}

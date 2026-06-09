package httpx

import (
	"encoding/json"
	"net/http"

	chimiddleware "github.com/go-chi/chi/v5/middleware"
)

type ErrorEnvelope struct {
	Code      string         `json:"code"`
	Message   string         `json:"message"`
	RequestID string         `json:"request_id"`
	Details   map[string]any `json:"details,omitempty"`
}

func WriteJSON(w http.ResponseWriter, code int, payload any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	_ = json.NewEncoder(w).Encode(payload)
}

func RequestID(r *http.Request) string {
	if r == nil {
		return ""
	}
	if reqID := chimiddleware.GetReqID(r.Context()); reqID != "" {
		return reqID
	}
	return r.Header.Get("X-Request-Id")
}

func WriteError(w http.ResponseWriter, r *http.Request, status int, code, message string, details map[string]any) {
	reqID := RequestID(r)
	if reqID != "" {
		w.Header().Set("X-Request-ID", reqID)
	}
	WriteJSON(w, status, ErrorEnvelope{
		Code:      code,
		Message:   message,
		RequestID: reqID,
		Details:   details,
	})
}

package main

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
)

func TestAuthenticatedGetJSONRefreshesExpiredToken(t *testing.T) {
	mux := http.NewServeMux()
	mux.HandleFunc("/v1/auth/refresh", func(w http.ResponseWriter, r *http.Request) {
		var body map[string]any
		if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
			t.Fatalf("decode refresh request: %v", err)
		}
		if got := textField(body["refresh_token"]); got != "refresh-old" {
			t.Fatalf("expected original refresh token, got %q", got)
		}
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]any{
			"tokens": map[string]any{
				"access_token":  "access-new",
				"refresh_token": "refresh-new",
			},
		})
	})
	mux.HandleFunc("/v1/conversations/conversation-1/timeline", func(w http.ResponseWriter, r *http.Request) {
		switch r.Header.Get("Authorization") {
		case "Bearer access-old":
			http.Error(w, "invalid token", http.StatusUnauthorized)
		case "Bearer access-new":
			w.Header().Set("Content-Type", "application/json")
			_ = json.NewEncoder(w).Encode(map[string]any{"items": []any{}})
		default:
			t.Fatalf("unexpected authorization header %q", r.Header.Get("Authorization"))
		}
	})

	server := httptest.NewServer(mux)
	defer server.Close()

	device := &deviceClient{
		userID:       "user-1",
		deviceID:     "device-1",
		accessToken:  "access-old",
		refreshToken: "refresh-old",
		baseURL:      server.URL,
		httpClient:   server.Client(),
	}

	payload, err := device.authenticatedGetJSON(context.Background(), server.URL+"/v1/conversations/conversation-1/timeline?limit=500")
	if err != nil {
		t.Fatalf("authenticated get json: %v", err)
	}
	if _, ok := payload["items"]; !ok {
		t.Fatalf("expected timeline payload items, got %#v", payload)
	}
	if got := device.accessTokenValue(); got != "access-new" {
		t.Fatalf("expected refreshed access token, got %q", got)
	}
	if got := device.refreshTokenValue(); got != "refresh-new" {
		t.Fatalf("expected refreshed refresh token, got %q", got)
	}
}

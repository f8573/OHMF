package messages

import (
	"encoding/json"
	"errors"
	"net"
	"net/http"
	"ohmf/services/gateway/internal/observability"
	"strconv"
	"strings"
	"time"

	"github.com/go-chi/chi/v5"
	chimiddleware "github.com/go-chi/chi/v5/middleware"
	"ohmf/services/gateway/internal/httpx"
	"ohmf/services/gateway/internal/limit"
	"ohmf/services/gateway/internal/middleware"
)

type Handler struct {
	svc *Service
}

func isReservedContentType(contentType string) bool {
	switch contentType {
	case "app_card":
		return true
	default:
		return false
	}
}

func validateSendContent(contentType string, content map[string]any) error {
	switch strings.ToLower(strings.TrimSpace(contentType)) {
	case "text":
		_, err := messageLifecycleHintsFromContent(content)
		return err
	case "encrypted":
		if _, ok := content["ciphertext"].(string); !ok {
			return errors.New("encrypted messages require ciphertext")
		}
		if _, ok := content["nonce"].(string); !ok {
			return errors.New("encrypted messages require nonce")
		}
		encryption, ok := content["encryption"].(map[string]any)
		if !ok {
			return errors.New("encrypted messages require encryption metadata")
		}
		if scheme, _ := encryption["scheme"].(string); scheme != "OHMF_SIGNAL_V1" {
			return errors.New("encrypted messages require scheme OHMF_SIGNAL_V1")
		}
		if _, ok := encryption["sender_user_id"].(string); !ok {
			return errors.New("encrypted messages require sender_user_id")
		}
		if _, ok := encryption["sender_device_id"].(string); !ok {
			return errors.New("encrypted messages require sender_device_id")
		}
		if _, ok := encryption["sender_signature"].(string); !ok {
			return errors.New("encrypted messages require sender_signature")
		}
		recipients, ok := encryption["recipients"].([]any)
		if !ok || len(recipients) == 0 {
			return errors.New("encrypted messages require recipients")
		}
	case "attachment":
		if _, ok := content["attachment_id"].(string); !ok {
			return errors.New("attachment messages require attachment_id")
		}
	case "link_preview":
		if _, ok := content["url"].(string); !ok {
			return errors.New("link previews require url")
		}
	}
	return nil
}

type messageLifecycleHints struct {
	ExpiresOnRead     bool
	ExpiresAt         *time.Time
	HasExplicitExpiry bool
}

func messageLifecycleHintsFromContent(content map[string]any) (messageLifecycleHints, error) {
	var hints messageLifecycleHints
	text, ok := content["text"].(string)
	if !ok || strings.TrimSpace(text) == "" {
		return hints, errors.New("text messages require text")
	}
	if err := validateMessageSpans(content["spans"], text); err != nil {
		return hints, err
	}
	if err := validateMessageMentions(content["mentions"], text); err != nil {
		return hints, err
	}

	if v, ok := content["expires_on_read"].(bool); ok {
		hints.ExpiresOnRead = v
	}
	if raw, ok := content["expires_in_seconds"]; ok {
		secs, ok := messageInt64(raw)
		if !ok || secs <= 0 {
			return hints, errors.New("text messages require positive expires_in_seconds")
		}
		expiry := time.Now().UTC().Add(time.Duration(secs) * time.Second)
		hints.ExpiresAt = &expiry
		hints.HasExplicitExpiry = true
	}
	if raw, ok := content["expires_at"]; ok {
		expiry, err := messageTime(raw)
		if err != nil {
			return hints, errors.New("text messages require valid expires_at")
		}
		if !expiry.After(time.Now().UTC()) {
			return hints, errors.New("text messages require future expires_at")
		}
		if hints.ExpiresAt == nil || expiry.Before(*hints.ExpiresAt) {
			hints.ExpiresAt = &expiry
		}
		hints.HasExplicitExpiry = true
	}
	return hints, nil
}

func validateMessageSpans(raw any, text string) error {
	if raw == nil {
		return nil
	}
	spans, ok := raw.([]any)
	if !ok {
		return errors.New("content.spans must be an array")
	}
	for _, spanRaw := range spans {
		span, ok := spanRaw.(map[string]any)
		if !ok {
			return errors.New("content.spans entries must be objects")
		}
		start, ok := messageInt64(span["start"])
		if !ok {
			return errors.New("content.spans entries require start")
		}
		end, ok := messageInt64(span["end"])
		if !ok {
			return errors.New("content.spans entries require end")
		}
		if start < 0 || end <= start || end > int64(len(text)) {
			return errors.New("content.spans entry has invalid range")
		}
		style, _ := span["style"].(string)
		if style == "" {
			style, _ = span["type"].(string)
		}
		switch strings.ToLower(strings.TrimSpace(style)) {
		case "bold", "italic", "underline", "strikethrough", "spoiler", "link", "mention", "code", "quote":
		default:
			return errors.New("content.spans entry has unsupported style")
		}
		if strings.EqualFold(style, "link") {
			if _, ok := span["url"].(string); !ok {
				return errors.New("link spans require url")
			}
		}
		if strings.EqualFold(style, "mention") {
			if _, ok := span["user_id"].(string); !ok {
				return errors.New("mention spans require user_id")
			}
		}
	}
	return nil
}

func validateMessageMentions(raw any, text string) error {
	if raw == nil {
		return nil
	}
	mentions, ok := raw.([]any)
	if !ok {
		return errors.New("content.mentions must be an array")
	}
	for _, mentionRaw := range mentions {
		mention, ok := mentionRaw.(map[string]any)
		if !ok {
			return errors.New("content.mentions entries must be objects")
		}
		if _, ok := mention["user_id"].(string); !ok {
			if _, ok := mention["id"].(string); !ok {
				return errors.New("content.mentions entries require user_id")
			}
		}
		if display, ok := mention["display"].(string); ok && strings.TrimSpace(display) == "" {
			return errors.New("content.mentions display must not be empty")
		}
		if startRaw, ok := mention["start"]; ok {
			start, ok := messageInt64(startRaw)
			if !ok {
				return errors.New("content.mentions entries require start")
			}
			end, ok := messageInt64(mention["end"])
			if !ok {
				return errors.New("content.mentions entries require end")
			}
			if start < 0 || end <= start || end > int64(len(text)) {
				return errors.New("content.mentions entry has invalid range")
			}
		}
	}
	return nil
}

func messageInt64(raw any) (int64, bool) {
	switch v := raw.(type) {
	case int:
		return int64(v), true
	case int8:
		return int64(v), true
	case int16:
		return int64(v), true
	case int32:
		return int64(v), true
	case int64:
		return v, true
	case float32:
		if float64(int64(v)) != float64(v) {
			return 0, false
		}
		return int64(v), true
	case float64:
		if float64(int64(v)) != v {
			return 0, false
		}
		return int64(v), true
	case json.Number:
		i, err := v.Int64()
		return i, err == nil
	case string:
		i, err := strconv.ParseInt(strings.TrimSpace(v), 10, 64)
		return i, err == nil
	default:
		return 0, false
	}
}

func messageTime(raw any) (time.Time, error) {
	switch v := raw.(type) {
	case string:
		if parsed, err := time.Parse(time.RFC3339Nano, strings.TrimSpace(v)); err == nil {
			return parsed.UTC(), nil
		}
		if parsed, err := time.Parse(time.RFC3339, strings.TrimSpace(v)); err == nil {
			return parsed.UTC(), nil
		}
		return time.Time{}, errors.New("invalid time")
	case time.Time:
		return v.UTC(), nil
	default:
		return time.Time{}, errors.New("invalid time")
	}
}

func NewHandler(svc *Service) *Handler {
	return &Handler{svc: svc}
}

func (h *Handler) Send(w http.ResponseWriter, r *http.Request) {
	userID, ok := middleware.UserIDFromContext(r.Context())
	if !ok {
		httpx.WriteError(w, r, http.StatusUnauthorized, "unauthorized", "missing auth", nil)
		return
	}
	var req struct {
		ConversationID    string         `json:"conversation_id"`
		IdempotencyKey    string         `json:"idempotency_key"`
		ContentType       string         `json:"content_type"`
		Content           map[string]any `json:"content"`
		ClientGeneratedID string         `json:"client_generated_id,omitempty"`
		ReplyToMessageID  string         `json:"reply_to_message_id,omitempty"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		httpx.WriteError(w, r, http.StatusBadRequest, "invalid_request", "invalid body", nil)
		return
	}
	if req.IdempotencyKey == "" {
		httpx.WriteError(w, r, http.StatusBadRequest, "invalid_request", "idempotency_key required", nil)
		return
	}

	// Validate required fields per OpenAPI: conversation_id, content_type, content
	if req.ConversationID == "" {
		httpx.WriteError(w, r, http.StatusBadRequest, "invalid_request", "conversation_id required", nil)
		return
	}
	if req.ContentType == "" {
		httpx.WriteError(w, r, http.StatusBadRequest, "invalid_request", "content_type required", nil)
		return
	}
	if isReservedContentType(req.ContentType) {
		httpx.WriteError(w, r, http.StatusBadRequest, "invalid_request", "content_type is server-authored only", nil)
		return
	}
	if req.Content == nil {
		httpx.WriteError(w, r, http.StatusBadRequest, "invalid_request", "content required", nil)
		return
	}
	if strings.TrimSpace(req.ReplyToMessageID) != "" {
		req.Content["reply_to_message_id"] = strings.TrimSpace(req.ReplyToMessageID)
	}
	if err := validateSendContent(req.ContentType, req.Content); err != nil {
		httpx.WriteError(w, r, http.StatusBadRequest, "invalid_request", err.Error(), nil)
		return
	}
	deviceID, _ := middleware.DeviceIDFromContext(r.Context())
	traceID := buildTraceID(chimiddleware.GetReqID(r.Context()))
	result, err := h.svc.Send(r.Context(), userID, deviceID, req.ConversationID, req.IdempotencyKey, req.ContentType, req.Content, req.ClientGeneratedID, traceID, ipOnly(r.RemoteAddr))
	if err != nil {
		if errors.Is(err, ErrConversationAccess) {
			httpx.WriteError(w, r, http.StatusForbidden, "forbidden", err.Error(), nil)
			return
		}
		if errors.Is(err, ErrConversationBlocked) {
			httpx.WriteError(w, r, http.StatusForbidden, "blocked", "conversation is blocked", nil)
			return
		}
		if errors.Is(err, ErrEncryptedMessageRequired) || errors.Is(err, ErrEncryptedMessageInvalid) || errors.Is(err, ErrSenderDeviceRequired) || errors.Is(err, ErrSenderDeviceInvalid) {
			httpx.WriteError(w, r, http.StatusConflict, "invalid_encrypted_message", err.Error(), nil)
			return
		}
		if err.Error() == "reply_target_not_found" {
			httpx.WriteError(w, r, http.StatusConflict, "reply_target_not_found", "reply target is unavailable", nil)
			return
		}
		var rlErr RateLimitError
		if errors.As(err, &rlErr) {
			decision := limit.Decision{Allowed: false, RetryAfter: time.Duration(retryAfterOrDefault(rlErr.RetryAfter)) * time.Millisecond}
			limit.SetHeaders(w, scopeLimit(rlErr.Scope), decision)
			httpx.WriteError(w, r, http.StatusTooManyRequests, "rate_limited", err.Error(), map[string]any{
				"scope":          rlErr.Scope,
				"retry_after_ms": retryAfterOrDefault(rlErr.RetryAfter),
			})
			return
		}
		httpx.WriteError(w, r, http.StatusInternalServerError, "send_failed", err.Error(), nil)
		return
	}
	httpStatus := http.StatusCreated
	if result.Queued {
		httpStatus = http.StatusAccepted
	}
	// Normalize transport and status to match OpenAPI contract.
	transport := result.Message.Transport
	switch transport {
	case "OTT":
		transport = "OHMF"
	case "":
		transport = "OHMF"
	}

	messageStatus := result.Message.Status
	if messageStatus == "" {
		messageStatus = "SENT"
	}

	response := map[string]any{
		"message_id":   result.Message.MessageID,
		"server_order": result.Message.ServerOrder,
		"transport":    transport,
		"status":       messageStatus,
		"queued":       result.Queued,
	}
	if result.Queued {
		response["ack_timeout_ms"] = result.AckTimeoutMS
	}
	httpx.WriteJSON(w, httpStatus, response)
	// Emit structured event for observability
	observability.EmitEvent("message.created", map[string]any{
		"message_id":      result.Message.MessageID,
		"conversation_id": req.ConversationID,
		"sender_user_id":  userID,
		"transport":       result.Message.Transport,
		"server_order":    result.Message.ServerOrder,
	})
}

func (h *Handler) SendToPhone(w http.ResponseWriter, r *http.Request) {
	userID, ok := middleware.UserIDFromContext(r.Context())
	if !ok {
		httpx.WriteError(w, r, http.StatusUnauthorized, "unauthorized", "missing auth", nil)
		return
	}
	var req struct {
		PhoneE164         string         `json:"phone_e164"`
		IdempotencyKey    string         `json:"idempotency_key"`
		ContentType       string         `json:"content_type"`
		Content           map[string]any `json:"content"`
		ClientGeneratedID string         `json:"client_generated_id,omitempty"`
		ReplyToMessageID  string         `json:"reply_to_message_id,omitempty"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		httpx.WriteError(w, r, http.StatusBadRequest, "invalid_request", "invalid body", nil)
		return
	}
	if req.PhoneE164 == "" || req.IdempotencyKey == "" {
		httpx.WriteError(w, r, http.StatusBadRequest, "invalid_request", "phone_e164 and idempotency_key required", nil)
		return
	}

	// Validate required fields per OpenAPI: content_type, content
	if req.ContentType == "" {
		httpx.WriteError(w, r, http.StatusBadRequest, "invalid_request", "content_type required", nil)
		return
	}
	if isReservedContentType(req.ContentType) {
		httpx.WriteError(w, r, http.StatusBadRequest, "invalid_request", "content_type is server-authored only", nil)
		return
	}
	if req.Content == nil {
		httpx.WriteError(w, r, http.StatusBadRequest, "invalid_request", "content required", nil)
		return
	}
	if strings.TrimSpace(req.ReplyToMessageID) != "" {
		req.Content["reply_to_message_id"] = strings.TrimSpace(req.ReplyToMessageID)
	}
	if err := validateSendContent(req.ContentType, req.Content); err != nil {
		httpx.WriteError(w, r, http.StatusBadRequest, "invalid_request", err.Error(), nil)
		return
	}
	deviceID, _ := middleware.DeviceIDFromContext(r.Context())
	traceID := buildTraceID(chimiddleware.GetReqID(r.Context()))
	result, err := h.svc.SendToPhone(r.Context(), userID, deviceID, req.PhoneE164, req.IdempotencyKey, req.ContentType, req.Content, req.ClientGeneratedID, traceID, ipOnly(r.RemoteAddr))
	if err != nil {
		if errors.Is(err, ErrConversationBlocked) {
			httpx.WriteError(w, r, http.StatusForbidden, "blocked", "conversation is blocked", nil)
			return
		}
		if errors.Is(err, ErrEncryptedMessageInvalid) || errors.Is(err, ErrSenderDeviceRequired) || errors.Is(err, ErrSenderDeviceInvalid) {
			httpx.WriteError(w, r, http.StatusConflict, "invalid_encrypted_message", err.Error(), nil)
			return
		}
		if err.Error() == "reply_target_not_found" {
			httpx.WriteError(w, r, http.StatusConflict, "reply_target_not_found", "reply target is unavailable", nil)
			return
		}
		var rlErr RateLimitError
		if errors.As(err, &rlErr) {
			decision := limit.Decision{Allowed: false, RetryAfter: time.Duration(retryAfterOrDefault(rlErr.RetryAfter)) * time.Millisecond}
			limit.SetHeaders(w, scopeLimit(rlErr.Scope), decision)
			httpx.WriteError(w, r, http.StatusTooManyRequests, "rate_limited", err.Error(), map[string]any{
				"scope":          rlErr.Scope,
				"retry_after_ms": retryAfterOrDefault(rlErr.RetryAfter),
			})
			return
		}
		httpx.WriteError(w, r, http.StatusInternalServerError, "send_phone_failed", err.Error(), nil)
		return
	}
	httpStatus := http.StatusCreated
	if result.Queued {
		httpStatus = http.StatusAccepted
	}
	transport := result.Message.Transport
	if transport == "" {
		transport = "SMS"
	}
	// Map any internal transport names to API-facing enums
	if transport == "OTT" {
		transport = "OHMF"
	}

	messageStatus := result.Message.Status
	if messageStatus == "" {
		messageStatus = "SENT"
	}

	response := map[string]any{
		"message_id":      result.Message.MessageID,
		"conversation_id": result.Message.ConversationID,
		"server_order":    result.Message.ServerOrder,
		"transport":       transport,
		"status":          messageStatus,
		"queued":          result.Queued,
	}
	if result.Queued {
		response["ack_timeout_ms"] = result.AckTimeoutMS
	}
	httpx.WriteJSON(w, httpStatus, response)
}

func (h *Handler) List(w http.ResponseWriter, r *http.Request) {
	userID, ok := middleware.UserIDFromContext(r.Context())
	if !ok {
		httpx.WriteError(w, r, http.StatusUnauthorized, "unauthorized", "missing auth", nil)
		return
	}
	id := chi.URLParam(r, "id")
	items, err := h.svc.List(r.Context(), userID, id)
	if err != nil {
		if errors.Is(err, ErrConversationAccess) {
			httpx.WriteError(w, r, http.StatusForbidden, "forbidden", err.Error(), nil)
			return
		}
		httpx.WriteError(w, r, http.StatusInternalServerError, "list_failed", err.Error(), nil)
		return
	}
	httpx.WriteJSON(w, http.StatusOK, map[string]any{"items": items, "next_cursor": nil})
}

func (h *Handler) Timeline(w http.ResponseWriter, r *http.Request) {
	userID, ok := middleware.UserIDFromContext(r.Context())
	if !ok {
		httpx.WriteError(w, r, http.StatusUnauthorized, "unauthorized", "missing auth", nil)
		return
	}
	id := chi.URLParam(r, "id")
	// optional limit param
	q := r.URL.Query().Get("limit")
	limit := 100
	if q != "" {
		if v, err := strconv.Atoi(q); err == nil && v > 0 && v <= 500 {
			limit = v
		}
	}
	items, err := h.svc.ListUnified(r.Context(), userID, id, limit)
	if err != nil {
		if errors.Is(err, ErrConversationAccess) {
			httpx.WriteError(w, r, http.StatusForbidden, "forbidden", err.Error(), nil)
			return
		}
		httpx.WriteError(w, r, http.StatusInternalServerError, "list_failed", err.Error(), nil)
		return
	}
	httpx.WriteJSON(w, http.StatusOK, map[string]any{"items": items, "next_cursor": nil})
}

func (h *Handler) RecordDelivery(w http.ResponseWriter, r *http.Request) {
	// Intentionally allow authenticated services to post delivery updates
	_, ok := middleware.UserIDFromContext(r.Context())
	if !ok {
		httpx.WriteError(w, r, http.StatusUnauthorized, "unauthorized", "missing auth", nil)
		return
	}
	id := chi.URLParam(r, "id")
	if id == "" {
		httpx.WriteError(w, r, http.StatusBadRequest, "invalid_request", "message id required", nil)
		return
	}
	var body struct {
		RecipientUserID   string `json:"recipient_user_id,omitempty"`
		RecipientDeviceID string `json:"recipient_device_id,omitempty"`
		RecipientPhone    string `json:"recipient_phone_e164,omitempty"`
		Transport         string `json:"transport"`
		State             string `json:"state"`
		Provider          string `json:"provider,omitempty"`
		SubmittedAt       string `json:"submitted_at,omitempty"`
		FailureCode       string `json:"failure_code,omitempty"`
	}
	if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
		httpx.WriteError(w, r, http.StatusBadRequest, "invalid_request", "invalid body", nil)
		return
	}
	dr := DeliveryRecord{
		MessageID:         id,
		RecipientUserID:   body.RecipientUserID,
		RecipientDeviceID: body.RecipientDeviceID,
		RecipientPhone:    body.RecipientPhone,
		Transport:         body.Transport,
		State:             body.State,
		Provider:          body.Provider,
		SubmittedAt:       body.SubmittedAt,
		FailureCode:       body.FailureCode,
	}
	if err := h.svc.RecordDelivery(r.Context(), id, dr); err != nil {
		httpx.WriteError(w, r, http.StatusInternalServerError, "record_failed", err.Error(), nil)
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

func (h *Handler) ListDeliveries(w http.ResponseWriter, r *http.Request) {
	userID, ok := middleware.UserIDFromContext(r.Context())
	if !ok {
		httpx.WriteError(w, r, http.StatusUnauthorized, "unauthorized", "missing auth", nil)
		return
	}
	// ensure membership by reusing existing List authorization check
	// reuse List to validate access: List will return error if not a member
	id := chi.URLParam(r, "id")
	if id == "" {
		httpx.WriteError(w, r, http.StatusBadRequest, "invalid_request", "message id required", nil)
		return
	}
	// Find the conversation id for the message to enforce membership
	var convID string
	if err := h.svc.db.QueryRow(r.Context(), `SELECT conversation_id::text FROM messages WHERE id = $1`, id).Scan(&convID); err != nil {
		httpx.WriteError(w, r, http.StatusNotFound, "not_found", "message not found", nil)
		return
	}
	if ok, err := h.svc.hasMembership(r.Context(), h.svc.db, userID, convID); err != nil {
		httpx.WriteError(w, r, http.StatusInternalServerError, "check_failed", err.Error(), nil)
		return
	} else if !ok {
		httpx.WriteError(w, r, http.StatusForbidden, "forbidden", "not a member", nil)
		return
	}
	items, err := h.svc.ListDeliveries(r.Context(), id)
	if err != nil {
		httpx.WriteError(w, r, http.StatusInternalServerError, "list_failed", err.Error(), nil)
		return
	}
	httpx.WriteJSON(w, http.StatusOK, map[string]any{"items": items})
}

func (h *Handler) MarkRead(w http.ResponseWriter, r *http.Request) {
	userID, ok := middleware.UserIDFromContext(r.Context())
	if !ok {
		httpx.WriteError(w, r, http.StatusUnauthorized, "unauthorized", "missing auth", nil)
		return
	}
	id := chi.URLParam(r, "id")
	var req struct {
		ThroughServerOrder int64 `json:"through_server_order"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		httpx.WriteError(w, r, http.StatusBadRequest, "invalid_request", "invalid body", nil)
		return
	}
	if err := h.svc.MarkRead(r.Context(), userID, id, req.ThroughServerOrder); err != nil {
		if errors.Is(err, ErrConversationAccess) {
			httpx.WriteError(w, r, http.StatusForbidden, "forbidden", err.Error(), nil)
			return
		}
		httpx.WriteError(w, r, http.StatusInternalServerError, "mark_read_failed", err.Error(), nil)
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

func (h *Handler) Redact(w http.ResponseWriter, r *http.Request) {
	userID, ok := middleware.UserIDFromContext(r.Context())
	if !ok {
		httpx.WriteError(w, r, http.StatusUnauthorized, "unauthorized", "missing auth", nil)
		return
	}
	id := chi.URLParam(r, "id")
	if id == "" {
		httpx.WriteError(w, r, http.StatusBadRequest, "invalid_request", "message id required", nil)
		return
	}
	if err := h.svc.Redact(r.Context(), userID, id); err != nil {
		if err.Error() == "forbidden" {
			httpx.WriteError(w, r, http.StatusForbidden, "forbidden", "not allowed to redact", nil)
			return
		}
		if err.Error() == "message_not_found" {
			httpx.WriteError(w, r, http.StatusNotFound, "not_found", "message not found", nil)
			return
		}
		httpx.WriteError(w, r, http.StatusInternalServerError, "redact_failed", err.Error(), nil)
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

func (h *Handler) Edit(w http.ResponseWriter, r *http.Request) {
	userID, ok := middleware.UserIDFromContext(r.Context())
	if !ok {
		httpx.WriteError(w, r, http.StatusUnauthorized, "unauthorized", "missing auth", nil)
		return
	}
	id := chi.URLParam(r, "id")
	if id == "" {
		httpx.WriteError(w, r, http.StatusBadRequest, "invalid_request", "message id required", nil)
		return
	}
	var req struct {
		Content map[string]any `json:"content"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		httpx.WriteError(w, r, http.StatusBadRequest, "invalid_request", "invalid body", nil)
		return
	}
	if req.Content == nil {
		httpx.WriteError(w, r, http.StatusBadRequest, "invalid_request", "content required", nil)
		return
	}
	deviceID, _ := middleware.DeviceIDFromContext(r.Context())
	if err := h.svc.EditMessage(r.Context(), userID, deviceID, id, req.Content); err != nil {
		switch err.Error() {
		case "forbidden":
			httpx.WriteError(w, r, http.StatusForbidden, "forbidden", "not allowed to edit", nil)
			return
		case "message_not_found":
			httpx.WriteError(w, r, http.StatusNotFound, "not_found", "message not found", nil)
			return
		case "message_not_editable":
			httpx.WriteError(w, r, http.StatusConflict, "message_not_editable", "message cannot be edited", nil)
			return
		}
		var invalidEditErr *invalidEditContentError
		if errors.As(err, &invalidEditErr) {
			httpx.WriteError(w, r, http.StatusBadRequest, "invalid_request", invalidEditErr.Error(), nil)
			return
		}
		if errors.Is(err, ErrEncryptedMessageInvalid) || errors.Is(err, ErrSenderDeviceRequired) || errors.Is(err, ErrSenderDeviceInvalid) {
			httpx.WriteError(w, r, http.StatusConflict, "invalid_encrypted_message", err.Error(), nil)
			return
		}
		if errors.Is(err, ErrEncryptedEditDeviceMismatch) {
			httpx.WriteError(w, r, http.StatusConflict, "e2ee_edit_requires_origin_device", "encrypted messages must be edited from the originating device", nil)
			return
		}
		httpx.WriteError(w, r, http.StatusInternalServerError, "edit_failed", err.Error(), nil)
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

func (h *Handler) GetEditHistory(w http.ResponseWriter, r *http.Request) {
	userID, ok := middleware.UserIDFromContext(r.Context())
	if !ok {
		httpx.WriteError(w, r, http.StatusUnauthorized, "unauthorized", "missing auth", nil)
		return
	}
	id := chi.URLParam(r, "id")
	if id == "" {
		httpx.WriteError(w, r, http.StatusBadRequest, "invalid_request", "message id required", nil)
		return
	}
	edits, err := h.svc.GetMessageEditHistory(r.Context(), userID, id)
	if err != nil {
		if errors.Is(err, ErrConversationAccess) {
			httpx.WriteError(w, r, http.StatusForbidden, "forbidden", "not a member", nil)
			return
		}
		if err.Error() == "message_not_found" {
			httpx.WriteError(w, r, http.StatusNotFound, "not_found", "message not found", nil)
			return
		}
		httpx.WriteError(w, r, http.StatusInternalServerError, "list_failed", err.Error(), nil)
		return
	}
	httpx.WriteJSON(w, http.StatusOK, map[string]any{"edits": edits})
}

func (h *Handler) GetReactionHistory(w http.ResponseWriter, r *http.Request) {
	userID, ok := middleware.UserIDFromContext(r.Context())
	if !ok {
		httpx.WriteError(w, r, http.StatusUnauthorized, "unauthorized", "missing auth", nil)
		return
	}
	id := chi.URLParam(r, "id")
	if id == "" {
		httpx.WriteError(w, r, http.StatusBadRequest, "invalid_request", "message id required", nil)
		return
	}
	history, err := h.svc.GetMessageReactionHistory(r.Context(), userID, id)
	if err != nil {
		if errors.Is(err, ErrConversationAccess) {
			httpx.WriteError(w, r, http.StatusForbidden, "forbidden", "not a member", nil)
			return
		}
		if err.Error() == "message_not_found" {
			httpx.WriteError(w, r, http.StatusNotFound, "not_found", "message not found", nil)
			return
		}
		httpx.WriteError(w, r, http.StatusInternalServerError, "list_failed", err.Error(), nil)
		return
	}
	httpx.WriteJSON(w, http.StatusOK, map[string]any{"history": history})
}

func (h *Handler) Search(w http.ResponseWriter, r *http.Request) {
	userID, ok := middleware.UserIDFromContext(r.Context())
	if !ok {
		httpx.WriteError(w, r, http.StatusUnauthorized, "unauthorized", "missing auth", nil)
		return
	}
	conversationID := chi.URLParam(r, "id")
	if conversationID == "" {
		httpx.WriteError(w, r, http.StatusBadRequest, "invalid_request", "conversation id required", nil)
		return
	}

	q := r.URL.Query().Get("q")
	if q == "" {
		httpx.WriteError(w, r, http.StatusBadRequest, "invalid_request", "query parameter required", nil)
		return
	}

	resultLimit := 50
	if limitStr := r.URL.Query().Get("limit"); limitStr != "" {
		if v, err := strconv.Atoi(limitStr); err == nil && v > 0 && v <= 100 {
			resultLimit = v
		}
	}

	// Build search options with enhanced parameters
	opts := SearchOptions{
		SenderUserID: r.URL.Query().Get("sender_user_id"),
		ContentType:  r.URL.Query().Get("content_type"),
		SearchMode:   r.URL.Query().Get("search_mode"), // "standard" | "fuzzy" | "exact"
		MatchType:    r.URL.Query().Get("match_type"),  // "any" | "all"
		SortBy:       r.URL.Query().Get("sort_by"),     // "relevance" | "recency"
		ExactMatch:   r.URL.Query().Get("exact_match") == "true",
		IncludeEdits: r.URL.Query().Get("include_edits") == "true",
	}

	// Validate search_mode
	if opts.SearchMode != "" && opts.SearchMode != "standard" && opts.SearchMode != "fuzzy" && opts.SearchMode != "exact" {
		httpx.WriteError(w, r, http.StatusBadRequest, "invalid_request", "search_mode must be one of: standard, fuzzy, exact", nil)
		return
	}

	// Validate match_type
	if opts.MatchType != "" && opts.MatchType != "any" && opts.MatchType != "all" {
		httpx.WriteError(w, r, http.StatusBadRequest, "invalid_request", "match_type must be one of: any, all", nil)
		return
	}

	// Validate sort_by
	if opts.SortBy != "" && opts.SortBy != "relevance" && opts.SortBy != "recency" {
		httpx.WriteError(w, r, http.StatusBadRequest, "invalid_request", "sort_by must be one of: relevance, recency", nil)
		return
	}

	if raw := strings.TrimSpace(r.URL.Query().Get("after")); raw != "" {
		parsed, err := time.Parse(time.RFC3339Nano, raw)
		if err != nil {
			httpx.WriteError(w, r, http.StatusBadRequest, "invalid_request", "after must be RFC3339", nil)
			return
		}
		opts.After = &parsed
	}
	if raw := strings.TrimSpace(r.URL.Query().Get("before")); raw != "" {
		parsed, err := time.Parse(time.RFC3339Nano, raw)
		if err != nil {
			httpx.WriteError(w, r, http.StatusBadRequest, "invalid_request", "before must be RFC3339", nil)
			return
		}
		opts.Before = &parsed
	}

	items, err := h.svc.SearchMessages(r.Context(), userID, conversationID, q, resultLimit, opts)
	if err != nil {
		if errors.Is(err, ErrConversationAccess) {
			httpx.WriteError(w, r, http.StatusForbidden, "forbidden", "not a member", nil)
			return
		}
		var rlErr RateLimitError
		if errors.As(err, &rlErr) {
			decision := limit.Decision{Allowed: false, RetryAfter: time.Duration(retryAfterOrDefault(rlErr.RetryAfter)) * time.Millisecond}
			limit.SetHeaders(w, scopeLimit(rlErr.Scope), decision)
			httpx.WriteError(w, r, http.StatusTooManyRequests, "rate_limited", err.Error(), map[string]any{
				"scope": rlErr.Scope,
			})
			return
		}
		httpx.WriteError(w, r, http.StatusInternalServerError, "search_failed", err.Error(), nil)
		return
	}

	// Return results with search metadata
	httpx.WriteJSON(w, http.StatusOK, map[string]any{
		"items": items,
		"search_metrics": map[string]any{
			"query_normalized": q,
			"search_mode":      opts.SearchMode,
			"match_type":       opts.MatchType,
			"result_count":     len(items),
		},
	})
}

func (h *Handler) GetReadStatus(w http.ResponseWriter, r *http.Request) {
	userID, ok := middleware.UserIDFromContext(r.Context())
	if !ok {
		httpx.WriteError(w, r, http.StatusUnauthorized, "unauthorized", "missing auth", nil)
		return
	}

	conversationID := chi.URLParam(r, "id")
	if conversationID == "" {
		httpx.WriteError(w, r, http.StatusBadRequest, "invalid_request", "conversation id required", nil)
		return
	}

	status, err := h.svc.GetConversationReadStatus(r.Context(), userID, conversationID)
	if err != nil {
		if errors.Is(err, ErrConversationAccess) {
			httpx.WriteError(w, r, http.StatusForbidden, "forbidden", "not a member", nil)
			return
		}
		httpx.WriteError(w, r, http.StatusInternalServerError, "read_status_failed", err.Error(), nil)
		return
	}
	httpx.WriteJSON(w, http.StatusOK, status)
}

func (h *Handler) TriggerEffect(w http.ResponseWriter, r *http.Request) {
	userID, ok := middleware.UserIDFromContext(r.Context())
	if !ok {
		httpx.WriteError(w, r, http.StatusUnauthorized, "unauthorized", "missing auth", nil)
		return
	}

	messageID := chi.URLParam(r, "id")
	if messageID == "" {
		httpx.WriteError(w, r, http.StatusBadRequest, "invalid_request", "message id required", nil)
		return
	}

	var req struct {
		EffectType string `json:"effect_type"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		httpx.WriteError(w, r, http.StatusBadRequest, "invalid_request", "invalid body", nil)
		return
	}

	if req.EffectType == "" {
		httpx.WriteError(w, r, http.StatusBadRequest, "invalid_request", "effect_type required", nil)
		return
	}
	if !isSupportedMessageEffectType(req.EffectType) {
		httpx.WriteError(w, r, http.StatusBadRequest, "invalid_request", "unsupported effect_type", nil)
		return
	}

	if err := h.svc.TriggerEffect(r.Context(), userID, messageID, req.EffectType); err != nil {
		if errors.Is(err, ErrConversationAccess) {
			httpx.WriteError(w, r, http.StatusForbidden, "forbidden", "not a member", nil)
			return
		}
		if err.Error() == "effects_disabled" {
			httpx.WriteError(w, r, http.StatusConflict, "effects_disabled", "effects not enabled", nil)
			return
		}
		if err.Error() == "message_not_found" {
			httpx.WriteError(w, r, http.StatusNotFound, "not_found", "message not found", nil)
			return
		}
		if errors.Is(err, ErrInvalidMessageEffectType) {
			httpx.WriteError(w, r, http.StatusBadRequest, "invalid_request", err.Error(), nil)
			return
		}
		httpx.WriteError(w, r, http.StatusInternalServerError, "effect_failed", err.Error(), nil)
		return
	}

	w.WriteHeader(http.StatusNoContent)
}

func (h *Handler) Delete(w http.ResponseWriter, r *http.Request) {
	userID, ok := middleware.UserIDFromContext(r.Context())
	if !ok {
		httpx.WriteError(w, r, http.StatusUnauthorized, "unauthorized", "missing auth", nil)
		return
	}
	id := chi.URLParam(r, "id")
	if id == "" {
		httpx.WriteError(w, r, http.StatusBadRequest, "invalid_request", "message id required", nil)
		return
	}
	if err := h.svc.DeleteMessage(r.Context(), userID, id); err != nil {
		if err.Error() == "forbidden" {
			httpx.WriteError(w, r, http.StatusForbidden, "forbidden", "not allowed to delete", nil)
			return
		}
		if err.Error() == "message_not_found" {
			httpx.WriteError(w, r, http.StatusNotFound, "not_found", "message not found", nil)
			return
		}
		httpx.WriteError(w, r, http.StatusInternalServerError, "delete_failed", err.Error(), nil)
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

func (h *Handler) AddReaction(w http.ResponseWriter, r *http.Request) {
	userID, ok := middleware.UserIDFromContext(r.Context())
	if !ok {
		httpx.WriteError(w, r, http.StatusUnauthorized, "unauthorized", "missing auth", nil)
		return
	}
	id := chi.URLParam(r, "id")
	if id == "" {
		httpx.WriteError(w, r, http.StatusBadRequest, "invalid_request", "message id required", nil)
		return
	}
	var body struct {
		Emoji string `json:"emoji"`
	}
	if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
		httpx.WriteError(w, r, http.StatusBadRequest, "invalid_request", "invalid body", nil)
		return
	}
	if body.Emoji == "" {
		httpx.WriteError(w, r, http.StatusBadRequest, "invalid_request", "emoji required", nil)
		return
	}
	if err := h.svc.AddReaction(r.Context(), userID, id, body.Emoji); err != nil {
		if errors.Is(err, ErrConversationAccess) {
			httpx.WriteError(w, r, http.StatusForbidden, "forbidden", "not a member", nil)
			return
		}
		httpx.WriteError(w, r, http.StatusInternalServerError, "add_reaction_failed", err.Error(), nil)
		return
	}
	reactions, err := h.svc.ListReactions(r.Context(), id)
	if err != nil {
		httpx.WriteError(w, r, http.StatusInternalServerError, "list_reactions_failed", err.Error(), nil)
		return
	}
	httpx.WriteJSON(w, http.StatusOK, map[string]any{
		"message_id": id,
		"reactions":  reactions,
	})
}

func (h *Handler) RemoveReaction(w http.ResponseWriter, r *http.Request) {
	userID, ok := middleware.UserIDFromContext(r.Context())
	if !ok {
		httpx.WriteError(w, r, http.StatusUnauthorized, "unauthorized", "missing auth", nil)
		return
	}
	id := chi.URLParam(r, "id")
	if id == "" {
		httpx.WriteError(w, r, http.StatusBadRequest, "invalid_request", "message id required", nil)
		return
	}
	var body struct {
		Emoji string `json:"emoji"`
	}
	if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
		httpx.WriteError(w, r, http.StatusBadRequest, "invalid_request", "invalid body", nil)
		return
	}
	if body.Emoji == "" {
		httpx.WriteError(w, r, http.StatusBadRequest, "invalid_request", "emoji required", nil)
		return
	}
	if err := h.svc.RemoveReaction(r.Context(), userID, id, body.Emoji); err != nil {
		if errors.Is(err, ErrConversationAccess) {
			httpx.WriteError(w, r, http.StatusForbidden, "forbidden", "not a member", nil)
			return
		}
		httpx.WriteError(w, r, http.StatusInternalServerError, "remove_reaction_failed", err.Error(), nil)
		return
	}
	reactions, err := h.svc.ListReactions(r.Context(), id)
	if err != nil {
		httpx.WriteError(w, r, http.StatusInternalServerError, "list_reactions_failed", err.Error(), nil)
		return
	}
	httpx.WriteJSON(w, http.StatusOK, map[string]any{
		"message_id": id,
		"reactions":  reactions,
	})
}

func (h *Handler) ListReactionsAggregated(w http.ResponseWriter, r *http.Request) {
	userID, ok := middleware.UserIDFromContext(r.Context())
	if !ok {
		httpx.WriteError(w, r, http.StatusUnauthorized, "unauthorized", "missing auth", nil)
		return
	}
	id := chi.URLParam(r, "id")
	if id == "" {
		httpx.WriteError(w, r, http.StatusBadRequest, "invalid_request", "message id required", nil)
		return
	}

	var convID string
	if err := h.svc.db.QueryRow(r.Context(), `SELECT conversation_id::text FROM messages WHERE id = $1::uuid`, id).Scan(&convID); err != nil {
		httpx.WriteError(w, r, http.StatusNotFound, "not_found", "message not found", nil)
		return
	}
	if ok, err := h.svc.hasMembership(r.Context(), h.svc.db, userID, convID); err != nil {
		httpx.WriteError(w, r, http.StatusInternalServerError, "check_failed", err.Error(), nil)
		return
	} else if !ok {
		httpx.WriteError(w, r, http.StatusForbidden, "forbidden", "not a member", nil)
		return
	}

	reactions, err := h.svc.ListReactions(r.Context(), id)
	if err != nil {
		httpx.WriteError(w, r, http.StatusInternalServerError, "list_reactions_failed", err.Error(), nil)
		return
	}
	httpx.WriteJSON(w, http.StatusOK, map[string]any{
		"message_id": id,
		"reactions":  reactions,
	})
}

func (h *Handler) Pin(w http.ResponseWriter, r *http.Request) {
	userID, ok := middleware.UserIDFromContext(r.Context())
	if !ok {
		httpx.WriteError(w, r, http.StatusUnauthorized, "unauthorized", "missing auth", nil)
		return
	}
	id := chi.URLParam(r, "id")
	if id == "" {
		httpx.WriteError(w, r, http.StatusBadRequest, "invalid_request", "message id required", nil)
		return
	}
	if err := h.svc.SetMessagePinned(r.Context(), userID, id, true); err != nil {
		switch err.Error() {
		case "message_not_found":
			httpx.WriteError(w, r, http.StatusNotFound, "not_found", "message not found", nil)
			return
		case "message_not_pinnable":
			httpx.WriteError(w, r, http.StatusConflict, "message_not_pinnable", "message cannot be pinned", nil)
			return
		case ErrConversationAccess.Error():
			httpx.WriteError(w, r, http.StatusForbidden, "forbidden", "not a member", nil)
			return
		}
		httpx.WriteError(w, r, http.StatusInternalServerError, "pin_failed", err.Error(), nil)
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

func (h *Handler) Unpin(w http.ResponseWriter, r *http.Request) {
	userID, ok := middleware.UserIDFromContext(r.Context())
	if !ok {
		httpx.WriteError(w, r, http.StatusUnauthorized, "unauthorized", "missing auth", nil)
		return
	}
	id := chi.URLParam(r, "id")
	if id == "" {
		httpx.WriteError(w, r, http.StatusBadRequest, "invalid_request", "message id required", nil)
		return
	}
	if err := h.svc.SetMessagePinned(r.Context(), userID, id, false); err != nil {
		switch err.Error() {
		case "message_not_found":
			httpx.WriteError(w, r, http.StatusNotFound, "not_found", "message not found", nil)
			return
		case "message_not_pinnable":
			httpx.WriteError(w, r, http.StatusConflict, "message_not_pinnable", "message cannot be unpinned", nil)
			return
		case ErrConversationAccess.Error():
			httpx.WriteError(w, r, http.StatusForbidden, "forbidden", "not a member", nil)
			return
		}
		httpx.WriteError(w, r, http.StatusInternalServerError, "unpin_failed", err.Error(), nil)
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

func (h *Handler) ListPinned(w http.ResponseWriter, r *http.Request) {
	userID, ok := middleware.UserIDFromContext(r.Context())
	if !ok {
		httpx.WriteError(w, r, http.StatusUnauthorized, "unauthorized", "missing auth", nil)
		return
	}
	conversationID := chi.URLParam(r, "id")
	if conversationID == "" {
		httpx.WriteError(w, r, http.StatusBadRequest, "invalid_request", "conversation id required", nil)
		return
	}
	items, err := h.svc.ListPinnedMessages(r.Context(), userID, conversationID)
	if err != nil {
		if errors.Is(err, ErrConversationAccess) {
			httpx.WriteError(w, r, http.StatusForbidden, "forbidden", "not a member", nil)
			return
		}
		httpx.WriteError(w, r, http.StatusInternalServerError, "list_failed", err.Error(), nil)
		return
	}
	httpx.WriteJSON(w, http.StatusOK, map[string]any{"items": items})
}

func (h *Handler) GetReplies(w http.ResponseWriter, r *http.Request) {
	userID, ok := middleware.UserIDFromContext(r.Context())
	if !ok {
		httpx.WriteError(w, r, http.StatusUnauthorized, "unauthorized", "missing auth", nil)
		return
	}
	messageID := chi.URLParam(r, "id")
	if messageID == "" {
		httpx.WriteError(w, r, http.StatusBadRequest, "invalid_request", "message id required", nil)
		return
	}
	items, err := h.svc.ListReplies(r.Context(), userID, messageID)
	if err != nil {
		switch {
		case errors.Is(err, ErrConversationAccess):
			httpx.WriteError(w, r, http.StatusForbidden, "forbidden", "not a member", nil)
			return
		case err.Error() == "message_not_found":
			httpx.WriteError(w, r, http.StatusNotFound, "not_found", "message not found", nil)
			return
		default:
			httpx.WriteError(w, r, http.StatusInternalServerError, "list_failed", err.Error(), nil)
			return
		}
	}
	httpx.WriteJSON(w, http.StatusOK, map[string]any{"items": items})
}

func (h *Handler) Forward(w http.ResponseWriter, r *http.Request) {
	userID, ok := middleware.UserIDFromContext(r.Context())
	if !ok {
		httpx.WriteError(w, r, http.StatusUnauthorized, "unauthorized", "missing auth", nil)
		return
	}
	sourceMessageID := chi.URLParam(r, "id")
	if sourceMessageID == "" {
		httpx.WriteError(w, r, http.StatusBadRequest, "invalid_request", "message id required", nil)
		return
	}
	var req struct {
		ConversationID    string `json:"conversation_id"`
		IdempotencyKey    string `json:"idempotency_key"`
		ClientGeneratedID string `json:"client_generated_id,omitempty"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		httpx.WriteError(w, r, http.StatusBadRequest, "invalid_request", "invalid body", nil)
		return
	}
	if req.ConversationID == "" {
		httpx.WriteError(w, r, http.StatusBadRequest, "invalid_request", "conversation_id required", nil)
		return
	}
	if req.IdempotencyKey == "" {
		httpx.WriteError(w, r, http.StatusBadRequest, "invalid_request", "idempotency_key required", nil)
		return
	}

	deviceID, _ := middleware.DeviceIDFromContext(r.Context())
	result, err := h.svc.ForwardMessage(r.Context(), userID, deviceID, sourceMessageID, req.ConversationID, req.IdempotencyKey, req.ClientGeneratedID, ipOnly(r.RemoteAddr))
	if err != nil {
		switch {
		case errors.Is(err, ErrConversationAccess):
			httpx.WriteError(w, r, http.StatusForbidden, "forbidden", err.Error(), nil)
			return
		case errors.Is(err, ErrConversationBlocked):
			httpx.WriteError(w, r, http.StatusForbidden, "blocked", "conversation is blocked", nil)
			return
		case errors.Is(err, ErrEncryptedMessageRequired), errors.Is(err, ErrEncryptedMessageInvalid), errors.Is(err, ErrSenderDeviceRequired), errors.Is(err, ErrSenderDeviceInvalid):
			httpx.WriteError(w, r, http.StatusConflict, "invalid_encrypted_message", err.Error(), nil)
			return
		case err.Error() == "message_not_found":
			httpx.WriteError(w, r, http.StatusNotFound, "not_found", "message not found", nil)
			return
		case err.Error() == "message_not_forwardable":
			httpx.WriteError(w, r, http.StatusConflict, "message_not_forwardable", "message cannot be forwarded", nil)
			return
		}
		var rlErr RateLimitError
		if errors.As(err, &rlErr) {
			decision := limit.Decision{Allowed: false, RetryAfter: time.Duration(retryAfterOrDefault(rlErr.RetryAfter)) * time.Millisecond}
			limit.SetHeaders(w, scopeLimit(rlErr.Scope), decision)
			httpx.WriteError(w, r, http.StatusTooManyRequests, "rate_limited", err.Error(), map[string]any{
				"scope":          rlErr.Scope,
				"retry_after_ms": retryAfterOrDefault(rlErr.RetryAfter),
			})
			return
		}
		httpx.WriteError(w, r, http.StatusInternalServerError, "forward_failed", err.Error(), nil)
		return
	}

	transport := result.Message.Transport
	switch transport {
	case "OTT":
		transport = "OHMF"
	case "":
		transport = "OHMF"
	}
	status := result.Message.Status
	if status == "" {
		status = "SENT"
	}

	response := map[string]any{
		"message_id":      result.Message.MessageID,
		"conversation_id": result.Message.ConversationID,
		"server_order":    result.Message.ServerOrder,
		"transport":       transport,
		"status":          status,
		"queued":          result.Queued,
	}
	if result.Message.Source != "" {
		response["source"] = result.Message.Source
	}
	if result.Queued {
		response["ack_timeout_ms"] = result.AckTimeoutMS
	}
	httpx.WriteJSON(w, http.StatusCreated, response)
}

func retryAfterOrDefault(v time.Duration) int64 {
	if v <= 0 {
		return 1000
	}
	return v.Milliseconds()
}

func scopeLimit(scope string) int64 {
	switch scope {
	case "user":
		return 60
	case "conversation":
		return 500
	case "ip":
		return 240
	default:
		return 0
	}
}

func ipOnly(remoteAddr string) string {
	host, _, err := net.SplitHostPort(remoteAddr)
	if err != nil {
		return remoteAddr
	}
	return host
}

package middleware

import "testing"

func TestValidateDataSendMessageRequest(t *testing.T) {
	valid := map[string]any{
		"conversation_id": "cnv_123",
		"idempotency_key": "idem_123",
		"content_type":    "text",
		"content": map[string]any{
			"text": "hello",
		},
	}
	if err := ValidateData("send-message-request", valid); err != nil {
		t.Fatalf("expected valid payload, got error: %v", err)
	}

	invalid := map[string]any{
		"conversation_id": "cnv_123",
		"idempotency_key": "idem_123",
		"content_type":    "text",
		"content": map[string]any{
			"text": "hello",
		},
		"event_id": "evt_123",
	}
	if err := ValidateData("send-message-request", invalid); err == nil {
		t.Fatal("expected additional property to fail validation")
	}
}

func TestValidateDataSendPhoneMessageRequest(t *testing.T) {
	valid := map[string]any{
		"phone_e164":      "+15555550123",
		"idempotency_key": "idem_123",
		"content_type":    "text",
		"content": map[string]any{
			"text": "hello",
		},
	}
	if err := ValidateData("send-phone-message-request", valid); err != nil {
		t.Fatalf("expected valid payload, got error: %v", err)
	}

	invalid := map[string]any{
		"idempotency_key": "idem_123",
		"content_type":    "text",
		"content": map[string]any{
			"text": "hello",
		},
	}
	if err := ValidateData("send-phone-message-request", invalid); err == nil {
		t.Fatal("expected missing phone_e164 to fail validation")
	}
}

func TestValidateDataWSSubscribe(t *testing.T) {
	valid := map[string]any{
		"conversation_ids": []any{"conv_123"},
	}
	if err := ValidateData("ws-subscribe", valid); err != nil {
		t.Fatalf("expected valid ws subscribe payload, got error: %v", err)
	}

	invalid := map[string]any{
		"conversation_ids": "conv_123",
	}
	if err := ValidateData("ws-subscribe", invalid); err == nil {
		t.Fatal("expected non-array conversation_ids to fail validation")
	}
}

func TestValidateDataWSSendMessage(t *testing.T) {
	valid := map[string]any{
		"conversation_id": "cnv_123",
		"idempotency_key": "idem_ws_123",
		"content_type":    "text",
		"content": map[string]any{
			"text": "hello from websocket",
		},
	}
	if err := ValidateData("ws-send_message", valid); err != nil {
		t.Fatalf("expected valid ws send_message payload, got error: %v", err)
	}

	invalid := map[string]any{
		"conversation_id": "cnv_123",
		"idempotency_key": "idem_ws_123",
		"content_type":    "text",
	}
	if err := ValidateData("ws-send_message", invalid); err == nil {
		t.Fatal("expected missing content to fail validation")
	}
}

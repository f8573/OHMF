package messages

import "testing"

func TestValidateSendContentRejectsMalformedEncryptedEnvelope(t *testing.T) {
	err := validateSendContent("encrypted", map[string]any{
		"ciphertext": "abc",
		"encryption": map[string]any{
			"scheme":           SignalEncryptionScheme,
			"sender_user_id":   "user",
			"sender_device_id": "device",
			"sender_signature": "sig",
			"recipients":       []any{},
		},
	})
	if err == nil {
		t.Fatal("expected missing nonce / recipients to be rejected")
	}
}

func TestValidateSendContentAcceptsSignalEnvelope(t *testing.T) {
	err := validateSendContent("encrypted", map[string]any{
		"ciphertext": "cipher",
		"nonce":      "nonce",
		"encryption": map[string]any{
			"scheme":           SignalEncryptionScheme,
			"sender_user_id":   "user",
			"sender_device_id": "device",
			"sender_signature": "sig",
			"recipients": []any{
				map[string]any{
					"user_id":     "u1",
					"device_id":   "d1",
					"wrapped_key": "wk",
					"wrap_nonce":  "wn",
				},
			},
		},
	})
	if err != nil {
		t.Fatalf("expected valid signal envelope, got %v", err)
	}
}

func TestValidateSendContentAcceptsMLSEnvelope(t *testing.T) {
	err := validateSendContent("encrypted", map[string]any{
		"ciphertext": "cipher",
		"nonce":      "nonce",
		"encryption": map[string]any{
			"scheme":              MLSEncryptionScheme,
			"sender_user_id":      "user",
			"sender_device_id":    "device",
			"sender_signature":    "sig",
			"tree_hash":           "tree-hash",
			"epoch_secret_digest": "digest",
		},
	})
	if err != nil {
		t.Fatalf("expected valid MLS envelope, got %v", err)
	}
}

func TestValidateSendContentRejectsMalformedRichText(t *testing.T) {
	err := validateSendContent("text", map[string]any{
		"text": "hello",
		"spans": []any{
			map[string]any{"start": int64(0), "end": int64(9), "style": "bold"},
		},
	})
	if err == nil {
		t.Fatal("expected invalid span range to be rejected")
	}
}

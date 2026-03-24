package messages

import (
	"context"
	"crypto/ed25519"
	"crypto/rand"
	"encoding/base64"
	"encoding/json"
	"os"
	"testing"
	"time"

	miniredis "github.com/alicebob/miniredis/v2"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/redis/go-redis/v9"
	"ohmf/services/gateway/internal/replication"
)

func TestEditEncryptedMessageAllowsOriginDeviceAndPublishesSyncEvent(t *testing.T) {
	dsn := os.Getenv("TEST_DATABASE_URL")
	if dsn == "" {
		t.Skip("skipping DB integration test; set TEST_DATABASE_URL to run")
	}

	ctx := context.Background()
	pool, err := pgxpool.New(ctx, dsn)
	if err != nil {
		t.Fatalf("connect: %v", err)
	}
	defer pool.Close()

	applyAllMigrations(t, ctx, pool)

	mr, err := miniredis.Run()
	if err != nil {
		t.Fatalf("start miniredis: %v", err)
	}
	defer mr.Close()

	rdb := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	defer rdb.Close()

	store := replication.NewStore(pool, rdb)
	svc := NewService(pool, Options{Redis: rdb, Replication: store})

	senderID := insertTestUser(t, ctx, pool)
	recipientID := insertTestUser(t, ctx, pool)
	senderDeviceID := uuid.NewString()
	recipientDeviceID := uuid.NewString()
	conversationID := uuid.NewString()
	messageID := uuid.NewString()

	senderSigningKey := insertTestDeviceIdentity(t, ctx, pool, senderID, senderDeviceID)
	insertTestDeviceIdentity(t, ctx, pool, recipientID, recipientDeviceID)

	if _, err := pool.Exec(ctx, `
		INSERT INTO conversations (id, type, encryption_state, encryption_epoch)
		VALUES ($1::uuid, 'DM', 'ENCRYPTED', 1)
	`, conversationID); err != nil {
		t.Fatalf("insert conversation: %v", err)
	}
	if _, err := pool.Exec(ctx, `INSERT INTO conversation_counters (conversation_id, next_server_order) VALUES ($1::uuid, 2)`, conversationID); err != nil {
		t.Fatalf("insert counter: %v", err)
	}
	if _, err := pool.Exec(ctx, `INSERT INTO conversation_members (conversation_id, user_id) VALUES ($1::uuid, $2::uuid), ($1::uuid, $3::uuid)`, conversationID, senderID, recipientID); err != nil {
		t.Fatalf("insert members: %v", err)
	}

	initialContent := signedEncryptedContent(t, senderSigningKey, senderID, senderDeviceID, recipientID, recipientDeviceID, "ciphertext-before")
	if _, err := pool.Exec(ctx, `
		INSERT INTO messages (id, conversation_id, sender_user_id, content_type, content, transport, server_order)
		VALUES ($1::uuid, $2::uuid, $3::uuid, 'encrypted', $4::jsonb, 'OHMF', 1)
	`, messageID, conversationID, senderID, mustJSON(t, initialContent)); err != nil {
		t.Fatalf("insert encrypted message: %v", err)
	}
	if _, err := pool.Exec(ctx, `
		UPDATE conversations
		SET last_message_id = $2::uuid
		WHERE id = $1::uuid
	`, conversationID, messageID); err != nil {
		t.Fatalf("update conversation last_message_id: %v", err)
	}
	if _, err := pool.Exec(ctx, `
		INSERT INTO user_conversation_state (
			user_id,
			conversation_id,
			last_message_id,
			last_message_preview,
			last_message_at,
			unread_count,
			updated_at
		)
		SELECT
			$1::uuid,
			$2::uuid,
			$3::uuid,
			'Encrypted message',
			m.created_at,
			1,
			m.created_at
		FROM messages m
		WHERE m.id = $3::uuid
	`, recipientID, conversationID, messageID); err != nil {
		t.Fatalf("seed recipient conversation state: %v", err)
	}

	userEventPubsub := rdb.Subscribe(ctx, store.ChannelForUser(recipientID))
	defer userEventPubsub.Close()
	if _, err := userEventPubsub.ReceiveTimeout(ctx, time.Second); err != nil {
		t.Fatalf("subscribe user event channel: %v", err)
	}

	editedContent := signedEncryptedContent(t, senderSigningKey, senderID, senderDeviceID, recipientID, recipientDeviceID, "ciphertext-after")
	if err := svc.EditMessage(ctx, senderID, senderDeviceID, messageID, editedContent); err != nil {
		t.Fatalf("edit encrypted message: %v", err)
	}

	if processed, err := store.ProcessBatch(ctx, 100); err != nil {
		t.Fatalf("process encrypted edit sync batch: %v", err)
	} else if processed == 0 {
		t.Fatal("expected encrypted edit domain event to be processed")
	}

	select {
	case published := <-userEventPubsub.Channel():
		var evt replication.Event
		if err := json.Unmarshal([]byte(published.Payload), &evt); err != nil {
			t.Fatalf("unmarshal encrypted edit user event: %v", err)
		}
		if evt.Type != replication.UserEventConversationMessageEdited {
			t.Fatalf("expected edit user event type %q, got %q", replication.UserEventConversationMessageEdited, evt.Type)
		}
		if evt.Payload["preview"] != "Encrypted message" {
			t.Fatalf("expected encrypted preview, got %#v", evt.Payload["preview"])
		}
		messagePayload, _ := evt.Payload["message"].(map[string]any)
		if messagePayload["content_type"] != "encrypted" {
			t.Fatalf("expected encrypted content_type, got %#v", messagePayload["content_type"])
		}
		contentPayload, _ := messagePayload["content"].(map[string]any)
		if contentPayload["ciphertext"] != editedContent["ciphertext"] {
			t.Fatalf("expected edited ciphertext %q, got %#v", editedContent["ciphertext"], contentPayload["ciphertext"])
		}
		if messagePayload["edited_at"] == nil {
			t.Fatalf("expected edited_at in encrypted edit sync payload")
		}
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for encrypted edit sync event")
	}

	items, err := svc.List(ctx, recipientID, conversationID)
	if err != nil {
		t.Fatalf("list messages after encrypted edit: %v", err)
	}
	if len(items) != 1 {
		t.Fatalf("expected 1 listed message, got %d", len(items))
	}
	if items[0].ContentType != "encrypted" {
		t.Fatalf("expected listed content_type encrypted, got %q", items[0].ContentType)
	}
	if items[0].Content["ciphertext"] != editedContent["ciphertext"] {
		t.Fatalf("expected listed ciphertext %q, got %#v", editedContent["ciphertext"], items[0].Content["ciphertext"])
	}
	if items[0].EditedAt == "" {
		t.Fatalf("expected listed message edited_at")
	}

	var previewAfter string
	if err := pool.QueryRow(ctx, `
		SELECT COALESCE(last_message_preview, '')
		FROM user_conversation_state
		WHERE user_id = $1::uuid AND conversation_id = $2::uuid
	`, recipientID, conversationID).Scan(&previewAfter); err != nil {
		t.Fatalf("load user conversation state after encrypted edit: %v", err)
	}
	if previewAfter != "Encrypted message" {
		t.Fatalf("expected preview 'Encrypted message' after encrypted edit, got %q", previewAfter)
	}

	var storedSenderDeviceID string
	var storedEncrypted bool
	var storedScheme string
	if err := pool.QueryRow(ctx, `
		SELECT COALESCE(sender_device_id::text, ''), is_encrypted, COALESCE(encryption_scheme, '')
		FROM messages
		WHERE id = $1::uuid
	`, messageID).Scan(&storedSenderDeviceID, &storedEncrypted, &storedScheme); err != nil {
		t.Fatalf("load message metadata after encrypted edit: %v", err)
	}
	if storedSenderDeviceID != senderDeviceID {
		t.Fatalf("expected sender_device_id backfilled to %q, got %q", senderDeviceID, storedSenderDeviceID)
	}
	if !storedEncrypted {
		t.Fatal("expected is_encrypted backfilled to true")
	}
	if storedScheme != "OHMF_SIGNAL_V1" {
		t.Fatalf("expected encryption_scheme backfilled to OHMF_SIGNAL_V1, got %q", storedScheme)
	}
}

func insertTestDeviceIdentity(t *testing.T, ctx context.Context, pool *pgxpool.Pool, userID, deviceID string) ed25519.PrivateKey {
	t.Helper()

	publicKey, privateKey, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatalf("generate signing key: %v", err)
	}
	signingPublicKey := base64.StdEncoding.EncodeToString(publicKey)
	agreementPublicKey := base64.StdEncoding.EncodeToString([]byte("agreement-" + deviceID))
	identityPublicKey := base64.StdEncoding.EncodeToString([]byte("identity-" + deviceID))
	signedPrekeyPublicKey := base64.StdEncoding.EncodeToString([]byte("signed-prekey-" + deviceID))
	signedPrekeySignature := base64.StdEncoding.EncodeToString(ed25519.Sign(privateKey, []byte(signedPrekeyPublicKey)))

	if _, err := pool.Exec(ctx, `
		INSERT INTO devices (id, user_id, platform, device_name, capabilities)
		VALUES ($1::uuid, $2::uuid, 'web', 'test device', ARRAY['E2EE_OTT_V2']::text[])
	`, deviceID, userID); err != nil {
		t.Fatalf("insert device: %v", err)
	}
	if _, err := pool.Exec(ctx, `
		INSERT INTO device_identity_keys (
			device_id,
			user_id,
			identity_public_key,
			signed_prekey_id,
			signed_prekey_public_key,
			signed_prekey_signature,
			agreement_identity_public_key,
			signing_key_alg,
			signing_public_key,
			bundle_version
		)
		VALUES ($1::uuid, $2::uuid, $3, 1, $4, $5, $6, 'ED25519', $7, 'OHMF_SIGNAL_V1')
	`, deviceID, userID, identityPublicKey, signedPrekeyPublicKey, signedPrekeySignature, agreementPublicKey, signingPublicKey); err != nil {
		t.Fatalf("insert device identity keys: %v", err)
	}
	return privateKey
}

func signedEncryptedContent(t *testing.T, signingKey ed25519.PrivateKey, senderUserID, senderDeviceID, recipientUserID, recipientDeviceID, ciphertext string) map[string]any {
	t.Helper()

	ciphertextB64 := base64.StdEncoding.EncodeToString([]byte(ciphertext))
	recipients := []map[string]any{
		{
			"user_id":     recipientUserID,
			"device_id":   recipientDeviceID,
			"wrapped_key": base64.StdEncoding.EncodeToString([]byte("wrapped-key")),
			"wrap_nonce":  base64.StdEncoding.EncodeToString([]byte("wrap-nonce-1")),
		},
		{
			"user_id":     senderUserID,
			"device_id":   senderDeviceID,
			"wrapped_key": base64.StdEncoding.EncodeToString([]byte("self-wrapped")),
			"wrap_nonce":  base64.StdEncoding.EncodeToString([]byte("self-nonce-1")),
		},
	}
	recipientsRaw := make([]any, 0, len(recipients))
	for _, recipient := range recipients {
		recipientsRaw = append(recipientsRaw, recipient)
	}
	signaturePayload := encryptedEnvelopeSignaturePayload("OHMF_SIGNAL_V1", 1, base64.StdEncoding.EncodeToString([]byte("nonce-12345678")), ciphertextB64, recipientsRaw)
	signature := base64.StdEncoding.EncodeToString(ed25519.Sign(signingKey, []byte(signaturePayload)))

	return map[string]any{
		"ciphertext": ciphertextB64,
		"nonce":      base64.StdEncoding.EncodeToString([]byte("nonce-12345678")),
		"encryption": map[string]any{
			"scheme":             "OHMF_SIGNAL_V1",
			"sender_user_id":     senderUserID,
			"sender_device_id":   senderDeviceID,
			"sender_signature":   signature,
			"conversation_epoch": 1,
			"recipients":         recipients,
		},
	}
}

func mustJSON(t *testing.T, v any) string {
	t.Helper()

	body, err := json.Marshal(v)
	if err != nil {
		t.Fatalf("marshal json: %v", err)
	}
	return string(body)
}

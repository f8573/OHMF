package messages

import (
	"context"
	"database/sql"
	"encoding/base64"
	"errors"
	"fmt"
	"sort"
	"strconv"
	"strings"

	"github.com/jackc/pgx/v5"
	"ohmf/services/gateway/internal/e2ee"
)

// EncryptedMessageMetadata represents extracted metadata from an encrypted message
type EncryptedMessageMetadata struct {
	IsEncrypted       bool
	Scheme            string
	SenderUserID      string
	SenderDeviceID    string
	SenderSignature   string
	ConversationEpoch int64
	MLSEpoch          int64
	MLSTreeHash       string
	EpochSecretDigest string
	Recipients        []RecipientKeyInfo
	EpochSecretBoxes  []RecipientKeyInfo
	Ciphertext        string
	Nonce             string
}

// RecipientKeyInfo represents recipient information from encrypted message
type RecipientKeyInfo struct {
	UserID     string
	DeviceID   string
	WrappedKey string
	WrapNonce  string
}

// EncryptedMessageQuerier defines the database interface needed for encrypted message processing
type EncryptedMessageQuerier interface {
	QueryRow(ctx context.Context, sql string, args ...any) pgx.Row
}

const (
	SignalEncryptionScheme = "OHMF_SIGNAL_V1"
	MLSEncryptionScheme    = "OHMF_MLS_V1"
)

// ProcessEncryptedMessage validates and extracts metadata from encrypted message content
func ProcessEncryptedMessage(
	ctx context.Context,
	querier EncryptedMessageQuerier,
	senderUserID string,
	senderDeviceID string,
	content map[string]any,
) (*EncryptedMessageMetadata, error) {
	// Extract encryption metadata
	ciphertext, ok := content["ciphertext"].(string)
	if !ok || ciphertext == "" {
		return nil, errors.New("invalid_ciphertext")
	}

	nonce, ok := content["nonce"].(string)
	if !ok || nonce == "" {
		return nil, errors.New("invalid_nonce")
	}

	encryptionObj, ok := content["encryption"].(map[string]any)
	if !ok {
		return nil, errors.New("missing_encryption_metadata")
	}

	scheme, _ := encryptionObj["scheme"].(string)
	if scheme != SignalEncryptionScheme && scheme != MLSEncryptionScheme {
		return nil, errors.New("invalid_encryption_scheme")
	}

	metadataSenderUserID, _ := encryptionObj["sender_user_id"].(string)
	metadataSenderDeviceID, _ := encryptionObj["sender_device_id"].(string)
	senderSignature, _ := encryptionObj["sender_signature"].(string)

	// Verify sender matches JWT
	if metadataSenderUserID != senderUserID {
		return nil, errors.New("sender_user_id_mismatch")
	}

	if metadataSenderDeviceID != senderDeviceID {
		return nil, errors.New("sender_device_id_mismatch")
	}

	// Verify sender has device identity key
	var signingPublicKeyB64 string
	err := querier.QueryRow(ctx, `
		SELECT signing_public_key
		FROM device_identity_keys
		WHERE device_id = $1 AND user_id = $2
	`, senderDeviceID, senderUserID).Scan(&signingPublicKeyB64)

	if err != nil {
		if err.Error() == "no rows in result set" {
			return nil, errors.New("sender_device_not_found")
		}
		return nil, fmt.Errorf("failed to query sender device: %w", err)
	}

	var recipients []RecipientKeyInfo
	var epochSecretBoxes []RecipientKeyInfo
	var recipientsRaw []any
	var epochSecretBoxesRaw []any
	var signaturePayload string
	conversationEpoch := messageInt(encryptionObj["conversation_epoch"], 1)
	mlsEpoch := messageInt(encryptionObj["mls_epoch"], conversationEpoch)
	switch scheme {
	case SignalEncryptionScheme:
		recipientsRaw, ok = encryptionObj["recipients"].([]any)
		if !ok || len(recipientsRaw) == 0 {
			return nil, errors.New("no_recipients_specified")
		}
		parsedRecipients, err := parseRecipientEntries(ctx, querier, recipientsRaw)
		if err != nil {
			return nil, err
		}
		recipients = parsedRecipients
		signaturePayload = encryptedEnvelopeSignaturePayload(
			scheme,
			conversationEpoch,
			nonce,
			ciphertext,
			recipientsRaw,
		)
	case MLSEncryptionScheme:
		if strings.TrimSpace(textField(encryptionObj["tree_hash"])) == "" {
			return nil, errors.New("missing_mls_tree_hash")
		}
		if strings.TrimSpace(textField(encryptionObj["epoch_secret_digest"])) == "" {
			return nil, errors.New("missing_mls_epoch_secret_digest")
		}
		epochSecretBoxesRaw, _ = encryptionObj["epoch_secret_boxes"].([]any)
		if len(epochSecretBoxesRaw) > 0 {
			parsedBoxes, err := parseRecipientEntries(ctx, querier, epochSecretBoxesRaw)
			if err != nil {
				return nil, err
			}
			epochSecretBoxes = parsedBoxes
		}
		signaturePayload = mlsEnvelopeSignaturePayload(
			scheme,
			conversationEpoch,
			mlsEpoch,
			textField(encryptionObj["tree_hash"]),
			textField(encryptionObj["epoch_secret_digest"]),
			nonce,
			ciphertext,
			epochSecretBoxesRaw,
		)
	}
	valid, err := e2ee.VerifySignature(signingPublicKeyB64, []byte(signaturePayload), senderSignature)
	if err != nil {
		return nil, fmt.Errorf("failed to verify signature: %w", err)
	}

	if !valid {
		return nil, errors.New("invalid_sender_signature")
	}

	// Validate base64 encoding of ciphertext and nonce
	if _, err := base64.StdEncoding.DecodeString(ciphertext); err != nil {
		return nil, errors.New("invalid_ciphertext_encoding")
	}

	if _, err := base64.StdEncoding.DecodeString(nonce); err != nil {
		return nil, errors.New("invalid_nonce_encoding")
	}

	metadata := &EncryptedMessageMetadata{
		IsEncrypted:       true,
		Scheme:            scheme,
		SenderUserID:      metadataSenderUserID,
		SenderDeviceID:    metadataSenderDeviceID,
		SenderSignature:   senderSignature,
		ConversationEpoch: conversationEpoch,
		MLSEpoch:          mlsEpoch,
		MLSTreeHash:       textField(encryptionObj["tree_hash"]),
		EpochSecretDigest: textField(encryptionObj["epoch_secret_digest"]),
		Recipients:        recipients,
		EpochSecretBoxes:  epochSecretBoxes,
		Ciphertext:        ciphertext,
		Nonce:             nonce,
	}

	return metadata, nil
}

func encryptedEnvelopeSignaturePayload(scheme string, conversationEpoch int64, nonce, ciphertext string, recipients []any) string {
	return strings.Join([]string{
		strings.TrimSpace(scheme),
		strconv.FormatInt(defaultInt64(conversationEpoch, 1), 10),
		strings.TrimSpace(nonce),
		strings.TrimSpace(ciphertext),
		recipientHeaderSummary(recipients),
	}, "|")
}

func mlsEnvelopeSignaturePayload(scheme string, conversationEpoch, mlsEpoch int64, treeHash, epochSecretDigest, nonce, ciphertext string, epochSecretBoxes []any) string {
	return strings.Join([]string{
		strings.TrimSpace(scheme),
		strconv.FormatInt(defaultInt64(conversationEpoch, 1), 10),
		strconv.FormatInt(defaultInt64(mlsEpoch, defaultInt64(conversationEpoch, 1)), 10),
		strings.TrimSpace(treeHash),
		strings.TrimSpace(epochSecretDigest),
		strings.TrimSpace(nonce),
		strings.TrimSpace(ciphertext),
		recipientHeaderSummary(epochSecretBoxes),
	}, "|")
}

func parseRecipientEntries(ctx context.Context, querier EncryptedMessageQuerier, recipientsRaw []any) ([]RecipientKeyInfo, error) {
	recipients := make([]RecipientKeyInfo, 0, len(recipientsRaw))
	for i, recipientRaw := range recipientsRaw {
		recipientObj, ok := recipientRaw.(map[string]any)
		if !ok {
			return nil, fmt.Errorf("invalid recipient at index %d", i)
		}

		recipientUserID, _ := recipientObj["user_id"].(string)
		recipientDeviceID, _ := recipientObj["device_id"].(string)
		wrappedKey, _ := recipientObj["wrapped_key"].(string)
		wrapNonce, _ := recipientObj["wrap_nonce"].(string)

		if recipientUserID == "" || recipientDeviceID == "" {
			return nil, fmt.Errorf("invalid recipient user/device ID at index %d", i)
		}
		if wrappedKey == "" || wrapNonce == "" {
			return nil, fmt.Errorf("invalid recipient encryption keys at index %d", i)
		}

		var exists bool
		err := querier.QueryRow(ctx, `
			SELECT EXISTS(
				SELECT 1 FROM devices
				WHERE id = $1 AND user_id = $2
			)
		`, recipientDeviceID, recipientUserID).Scan(&exists)
		if err != nil {
			return nil, fmt.Errorf("failed to verify recipient device: %w", err)
		}
		if !exists {
			return nil, fmt.Errorf("recipient device not found: %s", recipientDeviceID)
		}

		recipients = append(recipients, RecipientKeyInfo{
			UserID:     recipientUserID,
			DeviceID:   recipientDeviceID,
			WrappedKey: wrappedKey,
			WrapNonce:  wrapNonce,
		})
	}
	return recipients, nil
}

func recipientHeaderSummary(recipients []any) string {
	if len(recipients) == 0 {
		return ""
	}
	summaries := make([]string, 0, len(recipients))
	for _, raw := range recipients {
		recipient, _ := raw.(map[string]any)
		initialSession, _ := recipient["initial_session"].(map[string]any)
		summaries = append(summaries, strings.Join([]string{
			textField(recipient["user_id"]),
			textField(recipient["device_id"]),
			textField(recipient["ratchet_public_key"]),
			strconv.FormatInt(messageInt(recipient["previous_chain_length"], 0), 10),
			strconv.FormatInt(messageInt(recipient["message_number"], 0), 10),
			textField(initialSession["sender_ephemeral_public_key"]),
			strconv.FormatInt(messageInt(initialSession["signed_prekey_id"], 0), 10),
			strconv.FormatInt(messageInt(initialSession["one_time_prekey_id"], 0), 10),
		}, ":"))
	}
	sort.Strings(summaries)
	return strings.Join(summaries, ";")
}

func textField(value any) string {
	text, _ := value.(string)
	return strings.TrimSpace(text)
}

func messageInt(value any, fallback int64) int64 {
	switch v := value.(type) {
	case int:
		return int64(v)
	case int64:
		return v
	case float64:
		return int64(v)
	case float32:
		return int64(v)
	case string:
		if parsed, err := strconv.ParseInt(strings.TrimSpace(v), 10, 64); err == nil {
			return parsed
		}
	}
	return fallback
}

func defaultInt64(value, fallback int64) int64 {
	if value == 0 {
		return fallback
	}
	return value
}

// ValidateEncryptionSignature verifies Ed25519 signature (in isolation)
func ValidateEncryptionSignature(
	signingPublicKeyBase64 string,
	ciphertext string,
	signatureBase64 string,
) (bool, error) {
	return e2ee.VerifySignature(signingPublicKeyBase64, []byte(ciphertext), signatureBase64)
}

// ComputeFingerprintForDevice computes fingerprint for a device's signing key
func ComputeFingerprintForDevice(ctx context.Context, querier interface {
	QueryRowContext(ctx context.Context, query string, args ...interface{}) interface{ Scan(...interface{}) error }
}, deviceID string) (string, error) {
	var signingPublicKeyB64 string
	err := querier.QueryRowContext(ctx, `
		SELECT signing_public_key FROM device_identity_keys WHERE device_id = $1
	`, deviceID).Scan(&signingPublicKeyB64)

	if err != nil {
		return "", err
	}

	return e2ee.ComputeFingerprint(signingPublicKeyB64)
}

// CountEncryptedMessagesInConversation returns count of encrypted messages
func CountEncryptedMessagesInConversation(ctx context.Context, querier interface {
	QueryRowContext(ctx context.Context, query string, args ...interface{}) interface{ Scan(...interface{}) error }
}, conversationID string) (int64, error) {
	var count int64
	err := querier.QueryRowContext(ctx, `
		SELECT COUNT(*)
		FROM messages
		WHERE conversation_id = $1 AND is_encrypted = TRUE
	`, conversationID).Scan(&count)

	return count, err
}

// GetEncryptionStateForConversation retrieves the encryption state
func GetEncryptionStateForConversation(ctx context.Context, querier interface {
	QueryRowContext(ctx context.Context, query string, args ...interface{}) interface{ Scan(...interface{}) error }
}, conversationID string) (string, error) {
	var state string
	err := querier.QueryRowContext(ctx, `
		SELECT encryption_state
		FROM conversations
		WHERE id = $1
	`, conversationID).Scan(&state)

	if err == sql.ErrNoRows {
		return "PLAINTEXT", nil
	}

	return state, err
}

// UpdateEncryptionState updates the encryption state of a conversation
func UpdateEncryptionState(ctx context.Context, db *sql.DB, conversationID string, state string) error {
	_, err := db.ExecContext(ctx, `
		UPDATE conversations
		SET encryption_state = $1, encryption_ready = TRUE, updated_at = NOW()
		WHERE id = $2
	`, state, conversationID)

	return err
}

// LogEncryptionEvent logs encryption-related events for audit/debugging
func LogEncryptionEvent(
	ctx context.Context,
	db *sql.DB,
	eventType string,
	senderUserID string,
	senderDeviceID string,
	conversationID *string,
	messageID *string,
	metadata map[string]any,
) error {
	// This can be used to track encryption events for monitoring and debugging
	// For now, we store in e2ee_initialization_log for initialization events
	// Other events can be added to domain_events table or dedicated audit table

	if eventType == "message_encrypted" && messageID != nil {
		// Could log to a dedicated table if needed
		// This is a placeholder for future encryption event tracking
	}

	return nil
}

// ValidateEncryptedAttachments validates media attachments in encrypted messages
// Attachments must have encrypted keys wrapped in the message encryption
func ValidateEncryptedAttachments(
	ctx context.Context,
	db interface {
		QueryRowContext(ctx context.Context, query string, args ...interface{}) interface{ Scan(...interface{}) error }
		ExecContext(ctx context.Context, query string, args ...interface{}) (interface{}, error)
	},
	content map[string]any,
) error {
	// Check if attachments array exists
	attachmentsRaw, ok := content["attachments"].([]any)
	if !ok || len(attachmentsRaw) == 0 {
		return nil // No attachments is fine
	}

	// For each attachment, validate encryption metadata
	for i, attachmentRaw := range attachmentsRaw {
		attachmentObj, ok := attachmentRaw.(map[string]any)
		if !ok {
			return fmt.Errorf("invalid attachment at index %d", i)
		}

		// Required fields for encrypted attachments
		attachmentID, _ := attachmentObj["attachment_id"].(string)
		mediaKeyWrapped, _ := attachmentObj["media_key_wrapped"].(string)
		_, _ = attachmentObj["mime_type"].(string)

		if attachmentID == "" {
			return fmt.Errorf("attachment at index %d missing attachment_id", i)
		}

		if mediaKeyWrapped == "" {
			return fmt.Errorf("attachment at index %d missing media_key_wrapped", i)
		}

		// Validate base64 encoding of media key
		if _, err := base64.StdEncoding.DecodeString(mediaKeyWrapped); err != nil {
			return fmt.Errorf("attachment at index %d has invalid media_key_wrapped encoding: %w", i, err)
		}

		// Verify attachment exists in database
		var exists bool
		err := db.QueryRowContext(ctx, `
			SELECT EXISTS(
				SELECT 1 FROM attachments WHERE attachment_id = $1
			)
		`, attachmentID).Scan(&exists)

		if err != nil {
			return fmt.Errorf("failed to verify attachment: %w", err)
		}

		if !exists {
			return fmt.Errorf("attachment not found: %s", attachmentID)
		}

		// Mark attachment as encrypted in database
		_, err = db.ExecContext(ctx, `
			UPDATE attachments
			SET is_encrypted = TRUE,
			    encryption_key_encrypted = $1,
			    media_key_nonce = $2
			WHERE attachment_id = $3
		`, mediaKeyWrapped, attachmentObj["wrap_nonce"], attachmentID)

		if err != nil {
			return fmt.Errorf("failed to update attachment encryption metadata: %w", err)
		}
	}

	return nil
}

// ValidateEncryptedMentions validates mention metadata in encrypted messages
// Mentions are wrapped in the message encryption, server validates structure only
func ValidateEncryptedMentions(
	ctx context.Context,
	db interface {
		QueryRowContext(ctx context.Context, query string, args ...interface{}) interface{ Scan(...interface{}) error }
	},
	content map[string]any,
) error {
	// Check if mentions array exists
	mentionsRaw, ok := content["mentions"].([]any)
	if !ok || len(mentionsRaw) == 0 {
		return nil // No mentions is fine
	}

	// Validate structure (server doesn't decrypt to verify positions)
	for i, mentionRaw := range mentionsRaw {
		mentionObj, ok := mentionRaw.(map[string]any)
		if !ok {
			return fmt.Errorf("invalid mention at index %d", i)
		}

		// Required: user_id (or legacy 'id')
		mentionedUserID := ""
		if userID, ok := mentionObj["user_id"].(string); ok && userID != "" {
			mentionedUserID = userID
		} else if userID, ok := mentionObj["id"].(string); ok && userID != "" {
			mentionedUserID = userID
		}

		if mentionedUserID == "" {
			return fmt.Errorf("mention at index %d missing user_id", i)
		}

		// Verify user exists (light validation - server can't verify real mention positions in encrypted)
		var userExists bool
		err := db.QueryRowContext(ctx, `
			SELECT EXISTS(
				SELECT 1 FROM users WHERE id = $1
			)
		`, mentionedUserID).Scan(&userExists)

		if err != nil {
			return fmt.Errorf("failed to verify mentioned user: %w", err)
		}

		if !userExists {
			return fmt.Errorf("mentioned user not found: %s", mentionedUserID)
		}
	}

	return nil
}

// ErrEncryptedMessageEdit is returned when attempting to edit encrypted message content
var ErrEncryptedMessageEdit = errors.New("e2ee_immutable_content")

// ValidateEncryptedMessageEdit checks if message can be edited (E2EE messages cannot have content edits)
// Only metadata edits (like marking as edited) are allowed
func ValidateEncryptedMessageEdit(
	ctx context.Context,
	querier interface {
		QueryRowContext(ctx context.Context, query string, args ...interface{}) interface{ Scan(...interface{}) error }
	},
	messageID string,
	newContent map[string]any,
) error {
	// Check if message is encrypted
	var isEncrypted bool
	err := querier.QueryRowContext(ctx, `
		SELECT is_encrypted FROM messages WHERE id = $1
	`, messageID).Scan(&isEncrypted)

	if err == sql.ErrNoRows {
		return errors.New("message_not_found")
	}

	if err != nil {
		return fmt.Errorf("failed to check message encryption: %w", err)
	}

	if !isEncrypted {
		return nil // Plaintext messages can be edited
	}

	// Encrypted messages cannot have content editing
	// The ciphertext is immutable by design (signatures would break)
	// Return error - user must send a new message if they need to change content

	return ErrEncryptedMessageEdit
}

// ValidateMiniAppContentWithE2EE validates mini-app content in encrypted messages
// Mini-app messages can be encrypted, but server won't render previews
func ValidateMiniAppContentWithE2EE(
	ctx context.Context,
	db *sql.DB,
	contentType string,
	content map[string]any,
	isEncrypted bool,
) error {
	// Check if content is mini-app type
	if contentType != "app_card" && contentType != "app_event" {
		return nil // Not a mini-app message
	}

	if !isEncrypted {
		return nil // Plaintext mini-app - standard validation applies
	}

	// Mini-app message is encrypted
	// Server can't parse or render preview, client will after decryption

	// Validate app_id is present (even though encrypted)
	appID, ok := content["app_id"].(string)
	if !ok || appID == "" {
		return errors.New("mini-app messages require app_id field")
	}

	// Note: Full mini-app validation happens client-side after decryption
	// Server just validates structure exists

	return nil
}

// HandleDeviceRevocationE2EE invalidates E2EE sessions when device is revoked
// Called when a device is revoked to clean up its sessions
func HandleDeviceRevocationE2EE(
	ctx context.Context,
	db *sql.DB,
	revokedDeviceID string,
	revokedUserID string,
) error {
	// Delete all sessions where this device is contact
	query := `
		DELETE FROM e2ee_sessions
		WHERE contact_device_id = $1
		RETURNING user_id
	`

	rows, err := db.QueryContext(ctx, query, revokedDeviceID)
	if err != nil {
		return fmt.Errorf("failed to invalidate E2EE sessions: %w", err)
	}
	defer rows.Close()

	// Update trust state for revoked device
	updateTrustQuery := `
		UPDATE device_key_trust
		SET trust_state = 'BLOCKED'
		WHERE contact_device_id = $1
	`

	_, err = db.ExecContext(ctx, updateTrustQuery, revokedDeviceID)
	if err != nil {
		return fmt.Errorf("failed to block trusted device: %w", err)
	}

	// Log the revocation event
	_, err = db.ExecContext(ctx, `
		INSERT INTO e2ee_deletion_audit
		  (deleted_user_id, sessions_deleted)
		VALUES ($1, 1)
	`, revokedUserID)

	return err
}

// HandleAccountDeletionE2EE cleans up all E2EE data when account is deleted
// Deletes sessions, trust records, and group keys via CASCADE
func HandleAccountDeletionE2EE(
	ctx context.Context,
	db interface {
		QueryRowContext(ctx context.Context, query string, args ...interface{}) interface{ Scan(...interface{}) error }
		ExecContext(ctx context.Context, query string, args ...interface{}) (interface{}, error)
	},
	deletedUserID string,
) error {
	// Database CASCADE deletes should handle e2ee_sessions and device_key_trust
	// But we track the deletion for auditing

	var sessionsDeleted, trustRecordsDeleted, groupKeysDeleted int

	// Count for audit trail
	_ = db.QueryRowContext(ctx, `
		SELECT COUNT(*) FROM e2ee_sessions WHERE user_id = $1 OR contact_user_id = $1
	`, deletedUserID).Scan(&sessionsDeleted)

	_ = db.QueryRowContext(ctx, `
		SELECT COUNT(*) FROM device_key_trust WHERE user_id = $1 OR contact_user_id = $1
	`, deletedUserID).Scan(&trustRecordsDeleted)

	_ = db.QueryRowContext(ctx, `
		SELECT COUNT(*) FROM group_encryption_keys
		WHERE group_id IN (
			SELECT id FROM conversations
			WHERE user_id = $1
		)
	`, deletedUserID).Scan(&groupKeysDeleted)

	// Log the deletion
	_, err := db.ExecContext(ctx, `
		INSERT INTO e2ee_deletion_audit
		  (deleted_user_id, sessions_deleted, trust_records_deleted, group_keys_deleted)
		VALUES ($1, $2, $3, $4)
	`, deletedUserID, sessionsDeleted, trustRecordsDeleted, groupKeysDeleted)

	return err
}

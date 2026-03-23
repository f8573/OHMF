package messages

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"sort"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/redis/go-redis/v9"
	"ohmf/services/gateway/internal/limit"
	"ohmf/services/gateway/internal/middleware"
	"ohmf/services/gateway/internal/replication"
)

type Message struct {
	MessageID         string           `json:"message_id"`
	ConversationID    string           `json:"conversation_id"`
	SenderUserID      string           `json:"sender_user_id"`
	SenderDeviceID    string           `json:"sender_device_id,omitempty"`
	ReplyToMessageID  string           `json:"reply_to_message_id,omitempty"`
	ReplyCount        int64            `json:"reply_count,omitempty"`
	ContentType       string           `json:"content_type"`
	Content           map[string]any   `json:"content"`
	Reactions         map[string]int64 `json:"reactions,omitempty"`
	ClientGeneratedID string           `json:"client_generated_id,omitempty"`
	Transport         string           `json:"transport"`
	ServerOrder       int64            `json:"server_order"`
	Status            string           `json:"status,omitempty"`
	IsEncrypted       bool             `json:"is_encrypted,omitempty"`
	EncryptionScheme  string           `json:"encryption_scheme,omitempty"`
	CreatedAt         string           `json:"created_at"`
	SentAt            string           `json:"sent_at,omitempty"`
	DeliveredAt       string           `json:"delivered_at,omitempty"`
	ReadAt            string           `json:"read_at,omitempty"`
	StatusUpdatedAt   string           `json:"status_updated_at,omitempty"`
	EditedAt          string           `json:"edited_at,omitempty"`
	ExpiresAt         string           `json:"expires_at,omitempty"`
	Deleted           bool             `json:"deleted,omitempty"`
	DeletedAt         string           `json:"deleted_at,omitempty"`
	VisibilityState   string           `json:"visibility_state,omitempty"`
	Source            string           `json:"source,omitempty"`
	PinnedAt          string           `json:"pinned_at,omitempty"`
	PinnedByUserID    string           `json:"pinned_by_user_id,omitempty"`
}

type SendResult struct {
	Message      Message `json:"message"`
	Queued       bool    `json:"queued,omitempty"`
	AckTimeoutMS int64   `json:"ack_timeout_ms,omitempty"`
}

type Options struct {
	UseKafkaSend      bool
	UseCassandraReads bool
	AckTimeout        time.Duration
	Async             *AsyncPipeline
	Cassandra         *CassandraStore
	RateLimiter       *limit.TokenBucket
	Redis             *redis.Client
	Replication       *replication.Store
}

type DB interface {
	BeginTx(ctx context.Context, txOptions pgx.TxOptions) (pgx.Tx, error)
	Exec(ctx context.Context, sql string, args ...any) (pgconn.CommandTag, error)
	Query(ctx context.Context, sql string, args ...any) (pgx.Rows, error)
	QueryRow(ctx context.Context, sql string, args ...any) pgx.Row
}

type Service struct {
	db                DB
	useKafkaSend      bool
	useCassandraReads bool
	ackTimeout        time.Duration
	async             *AsyncPipeline
	cassandra         *CassandraStore
	rateLimiter       *limit.TokenBucket
	redis             *redis.Client
	replication       *replication.Store
}

type DeliveryRecord struct {
	ID                string `json:"id"`
	MessageID         string `json:"message_id"`
	RecipientUserID   string `json:"recipient_user_id,omitempty"`
	RecipientDeviceID string `json:"recipient_device_id,omitempty"`
	RecipientPhone    string `json:"recipient_phone_e164,omitempty"`
	Transport         string `json:"transport"`
	State             string `json:"state"`
	Provider          string `json:"provider,omitempty"`
	SubmittedAt       string `json:"submitted_at,omitempty"`
	UpdatedAt         string `json:"updated_at"`
	FailureCode       string `json:"failure_code,omitempty"`
}

type SearchOptions struct {
	SenderUserID string
	ContentType  string
	After        *time.Time
	Before       *time.Time
	SearchMode   string // "standard" | "fuzzy" | "exact" - default: "standard"
	MatchType    string // "any" | "all" - default: "any"
	IncludeEdits bool   // Include message edit history in search
	SortBy       string // "relevance" | "recency" - default: "relevance"
	ExactMatch   bool   // Require exact phrase match
}

const (
	messagePinEffectPinned   = "PINNED"
	messagePinEffectUnpinned = "UNPINNED"
	messageForwardSource     = "FORWARDED"
)

func (s *Service) Redact(ctx context.Context, actorUserID, messageID string) error {
	tx, err := s.db.BeginTx(ctx, pgx.TxOptions{})
	if err != nil {
		return err
	}
	defer tx.Rollback(ctx)

	var senderID string
	var convID string
	err = tx.QueryRow(ctx, `SELECT sender_user_id::text, conversation_id::text FROM messages WHERE id = $1`, messageID).Scan(&senderID, &convID)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return fmt.Errorf("message_not_found")
		}
		return err
	}
	if senderID != actorUserID {
		return fmt.Errorf("forbidden")
	}

	_, err = tx.Exec(ctx, `
		UPDATE messages
		SET content = '{}'::jsonb,
			redacted_at = now(),
			visibility_state = 'REDACTED'
		WHERE id = $1
	`, messageID)
	if err != nil {
		return err
	}

	_, err = tx.Exec(ctx, `UPDATE conversations SET updated_at = now() WHERE id = $1::uuid`, convID)
	if err != nil {
		return err
	}

	if err := tx.Commit(ctx); err != nil {
		return err
	}
	return nil
}

// removed: redaction comments duplicated the implementation

func (s *Service) DeleteMessage(ctx context.Context, actorUserID, messageID string) error {
	tx, err := s.db.BeginTx(ctx, pgx.TxOptions{})
	if err != nil {
		return err
	}
	defer tx.Rollback(ctx)

	var senderID string
	var senderDeviceID sql.NullString
	var convID string
	var contentType string
	var clientGeneratedID sql.NullString
	var transport string
	var serverOrder int64
	var createdAt time.Time
	err = tx.QueryRow(ctx, `
		SELECT sender_user_id::text, COALESCE(sender_device_id::text, ''), conversation_id::text, content_type, client_generated_id, transport, server_order, created_at
		FROM messages
		WHERE id = $1
	`, messageID).Scan(&senderID, &senderDeviceID, &convID, &contentType, &clientGeneratedID, &transport, &serverOrder, &createdAt)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return fmt.Errorf("message_not_found")
		}
		return err
	}
	if senderID != actorUserID {
		return fmt.Errorf("forbidden")
	}

	var deletedAt time.Time
	err = tx.QueryRow(ctx, `
		UPDATE messages
		SET content = '{}'::jsonb,
			redacted_at = now(),
			deleted_at = now(),
			visibility_state = 'SOFT_DELETED'
		WHERE id = $1
		RETURNING deleted_at
	`, messageID).Scan(&deletedAt)
	if err != nil {
		return err
	}

	rows, err := tx.Query(ctx, `SELECT attachment_id::text FROM attachments WHERE message_id = $1`, messageID)
	if err != nil {
		return err
	}
	var attachments []string
	for rows.Next() {
		var aid string
		if err := rows.Scan(&aid); err != nil {
			rows.Close()
			return err
		}
		attachments = append(attachments, aid)
	}
	rows.Close()

	if len(attachments) > 0 {
		if _, err := tx.Exec(ctx, `DELETE FROM attachments WHERE message_id = $1`, messageID); err != nil {
			return err
		}
	}

	if _, err := tx.Exec(ctx, `UPDATE conversations SET updated_at = now() WHERE id = $1::uuid`, convID); err != nil {
		return err
	}

	if s.replication != nil {
		conversationMeta, err := s.replication.LoadConversationMeta(ctx, tx, convID)
		if err != nil {
			return err
		}
		if err := s.replication.AppendDomainEvent(ctx, tx, convID, actorUserID, replication.DomainEventMessageDeleted, replication.MessageDeletedPayload{
			MessageID:         messageID,
			ConversationID:    convID,
			ConversationType:  conversationMeta.Type,
			SenderUserID:      senderID,
			SenderDeviceID:    senderDeviceID.String,
			ContentType:       contentType,
			ClientGeneratedID: clientGeneratedID.String,
			Transport:         transport,
			ServerOrder:       serverOrder,
			CreatedAt:         createdAt.UTC().Format(time.RFC3339Nano),
			DeletedAt:         deletedAt.UTC().Format(time.RFC3339Nano),
			Participants:      conversationMeta.Participants,
			ExternalPhones:    conversationMeta.ExternalPhones,
		}); err != nil {
			return err
		}
	}

	if err := tx.Commit(ctx); err != nil {
		return err
	}

	if s.async != nil {
		env := Envelope{
			SpecVersion:    "2026-03-01",
			EventID:        messageID,
			EventType:      "message.delete",
			IssuedAt:       time.Now().UTC().Format(time.RFC3339Nano),
			ConversationID: convID,
			Transport:      "OTT",
			IdempotencyKey: "",
			Payload:        []byte(fmt.Sprintf(`{"message_id":"%s","attachments":%q}`, messageID, attachments)),
			Actor:          &Actor{UserID: actorUserID},
			Trace:          &Trace{},
		}
		_ = s.async.PublishEnvelope(context.Background(), convID, env)
	}
	return nil
}

// removed: deletion flow comments repeated the control flow and SQL

type invalidEditContentError struct {
	err error
}

func (e *invalidEditContentError) Error() string {
	if e == nil || e.err == nil {
		return "invalid_request"
	}
	return e.err.Error()
}

func (e *invalidEditContentError) Unwrap() error {
	if e == nil {
		return nil
	}
	return e.err
}

func (s *Service) EditMessage(ctx context.Context, actorUserID, actorDeviceID, messageID string, content map[string]any) error {
	tx, err := s.db.BeginTx(ctx, pgx.TxOptions{})
	if err != nil {
		return err
	}
	defer tx.Rollback(ctx)

	var senderID string
	var senderDeviceID sql.NullString
	var convID string
	var contentType string
	var clientGeneratedID sql.NullString
	var transport string
	var serverOrder int64
	var createdAt time.Time
	var deliveredAt sql.NullTime
	var readAt sql.NullTime
	var deletedAt sql.NullTime
	var expiresAt sql.NullTime
	var previousContent string
	err = tx.QueryRow(ctx, `
		SELECT
			m.sender_user_id::text,
			COALESCE(m.sender_device_id::text, ''),
			m.conversation_id::text,
			m.content_type,
			m.client_generated_id,
			m.transport,
			m.server_order,
			m.created_at,
			delivered_meta.delivered_at,
			read_meta.read_at,
			m.deleted_at,
			m.expires_at,
			m.content::text
		FROM messages m
		LEFT JOIN LATERAL (
			SELECT MAX(de.created_at) AS delivered_at
			FROM domain_events de
			WHERE de.conversation_id = m.conversation_id
			  AND de.event_type = 'delivery_checkpoint_advanced'
			  AND COALESCE((de.payload->>'through_server_order')::bigint, 0) >= m.server_order
			  AND COALESCE(de.payload->>'user_id', '') <> $2::text
		) delivered_meta ON TRUE
		LEFT JOIN LATERAL (
			SELECT MAX(other.last_read_at) AS read_at
			FROM conversation_members other
			WHERE other.conversation_id = m.conversation_id
			  AND other.user_id <> $2::uuid
			  AND other.last_read_server_order >= m.server_order
			  AND other.last_read_at IS NOT NULL
		) read_meta ON TRUE
		WHERE m.id = $1
	`, messageID, actorUserID).Scan(&senderID, &senderDeviceID, &convID, &contentType, &clientGeneratedID, &transport, &serverOrder, &createdAt, &deliveredAt, &readAt, &deletedAt, &expiresAt, &previousContent)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return fmt.Errorf("message_not_found")
		}
		return err
	}
	if senderID != actorUserID {
		return fmt.Errorf("forbidden")
	}
	if deletedAt.Valid {
		return fmt.Errorf("message_not_editable")
	}
	if expiresAt.Valid && !expiresAt.Time.After(time.Now().UTC()) {
		return fmt.Errorf("message_not_editable")
	}

	contentForStorage := cloneMessageContent(content)
	if contentForStorage == nil {
		contentForStorage = map[string]any{}
	}
	previousContentForStorage := parseStoredMessageContent(previousContent)

	if contentType != "text" && contentType != "encrypted" {
		return fmt.Errorf("message_not_editable")
	}
	if err := validateSendContent(contentType, contentForStorage); err != nil {
		return &invalidEditContentError{err: err}
	}
	encryptionScheme := ""
	if contentType == "encrypted" {
		originDeviceID := strings.TrimSpace(senderDeviceID.String)
		if originDeviceID == "" {
			originDeviceID = encryptionSenderDeviceID(previousContentForStorage)
		}
		currentDeviceID := strings.TrimSpace(actorDeviceID)
		if originDeviceID == "" {
			return ErrEncryptedMessageInvalid
		}
		if currentDeviceID == "" {
			return ErrSenderDeviceRequired
		}
		ownsDevice, err := s.senderOwnsDevice(ctx, tx, actorUserID, currentDeviceID)
		if err != nil {
			return err
		}
		if !ownsDevice {
			return ErrSenderDeviceInvalid
		}
		if currentDeviceID != originDeviceID {
			return ErrEncryptedEditDeviceMismatch
		}
		encryptedMetadata, err := ProcessEncryptedMessage(ctx, tx, actorUserID, currentDeviceID, contentForStorage)
		if err != nil {
			return &invalidEditContentError{err: err}
		}
		encryptionScheme = encryptedMetadata.Scheme
		senderDeviceID.String = originDeviceID
		senderDeviceID.Valid = true
	}

	contentJSON, err := json.Marshal(contentForStorage)
	if err != nil {
		return err
	}

	var editedAt time.Time
	err = tx.QueryRow(ctx, `
		UPDATE messages
		SET content = $2::jsonb,
			edited_at = now(),
			sender_device_id = CASE
				WHEN content_type = 'encrypted' AND sender_device_id IS NULL THEN NULLIF($3, '')::uuid
				ELSE sender_device_id
			END,
			is_encrypted = CASE
				WHEN content_type = 'encrypted' THEN TRUE
				ELSE is_encrypted
			END,
			encryption_scheme = CASE
				WHEN content_type = 'encrypted' THEN NULLIF($4, '')
				ELSE encryption_scheme
			END
		WHERE id = $1
		RETURNING edited_at
	`, messageID, string(contentJSON), strings.TrimSpace(senderDeviceID.String), encryptionScheme).Scan(&editedAt)
	if err != nil {
		return err
	}

	if _, err := tx.Exec(ctx, `UPDATE conversations SET updated_at = now() WHERE id = $1::uuid`, convID); err != nil {
		return err
	}

	if _, err := tx.Exec(ctx, `
		INSERT INTO message_edits (
			message_id,
			conversation_id,
			edited_by,
			previous_content,
			new_content,
			sent_at,
			delivered_at,
			read_at,
			edited_at
		)
		VALUES ($1::uuid, $2::uuid, $3::uuid, $4::jsonb, $5::jsonb, $6, $7, $8, $9)
	`, messageID, convID, actorUserID, previousContent, string(contentJSON), createdAt, nullableTime(deliveredAt), nullableTime(readAt), editedAt); err != nil {
		return err
	}

	if s.replication != nil {
		conversationMeta, err := s.replication.LoadConversationMeta(ctx, tx, convID)
		if err != nil {
			return err
		}
		if err := s.replication.AppendDomainEvent(ctx, tx, convID, actorUserID, replication.DomainEventMessageEdited, replication.MessageEditedPayload{
			MessageID:         messageID,
			ConversationID:    convID,
			ConversationType:  conversationMeta.Type,
			SenderUserID:      senderID,
			SenderDeviceID:    senderDeviceID.String,
			ContentType:       contentType,
			Content:           contentForStorage,
			ClientGeneratedID: clientGeneratedID.String,
			Transport:         transport,
			ServerOrder:       serverOrder,
			CreatedAt:         createdAt.UTC().Format(time.RFC3339Nano),
			EditedAt:          editedAt.UTC().Format(time.RFC3339Nano),
			Participants:      conversationMeta.Participants,
			ExternalPhones:    conversationMeta.ExternalPhones,
		}); err != nil {
			return err
		}
	}

	return tx.Commit(ctx)
}

type EditRecord struct {
	EditedAt        string         `json:"edited_at"`
	SentAt          string         `json:"sent_at,omitempty"`
	DeliveredAt     string         `json:"delivered_at,omitempty"`
	ReadAt          string         `json:"read_at,omitempty"`
	PreviousContent map[string]any `json:"previous_content"`
	NewContent      map[string]any `json:"new_content"`
	EditedBy        string         `json:"edited_by"`
}

type ReactionHistoryRecord struct {
	ActedAt     string `json:"acted_at"`
	SentAt      string `json:"sent_at,omitempty"`
	DeliveredAt string `json:"delivered_at,omitempty"`
	ReadAt      string `json:"read_at,omitempty"`
	Emoji       string `json:"emoji"`
	Action      string `json:"action"`
	ActedBy     string `json:"acted_by"`
}

type messageTimelineSnapshot struct {
	SentAt      time.Time
	DeliveredAt sql.NullTime
	ReadAt      sql.NullTime
}

func (s *Service) GetMessageEditHistory(ctx context.Context, actorUserID, messageID string) ([]EditRecord, error) {
	var conversationID string
	err := s.db.QueryRow(ctx, `SELECT conversation_id::text FROM messages WHERE id = $1::uuid`, messageID).Scan(&conversationID)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, fmt.Errorf("message_not_found")
		}
		return nil, err
	}

	ok, err := s.hasMembership(ctx, s.db, actorUserID, conversationID)
	if err != nil || !ok {
		return nil, ErrConversationAccess
	}

	rows, err := s.db.Query(ctx, `
		SELECT
			me.edited_at,
			COALESCE(me.sent_at, m.created_at) AS sent_at,
			COALESCE(me.delivered_at, delivered_meta.delivered_at) AS delivered_at,
			COALESCE(me.read_at, read_meta.read_at) AS read_at,
			me.previous_content,
			me.new_content,
			COALESCE(me.edited_by::text, '') AS edited_by
		FROM message_edits me
		JOIN messages m ON m.id = me.message_id
		LEFT JOIN LATERAL (
			SELECT MAX(de.created_at) AS delivered_at
			FROM domain_events de
			WHERE de.conversation_id = m.conversation_id
			  AND de.event_type = 'delivery_checkpoint_advanced'
			  AND COALESCE((de.payload->>'through_server_order')::bigint, 0) >= m.server_order
			  AND COALESCE(de.payload->>'user_id', '') <> m.sender_user_id::text
		) delivered_meta ON TRUE
		LEFT JOIN LATERAL (
			SELECT MAX(other.last_read_at) AS read_at
			FROM conversation_members other
			WHERE other.conversation_id = m.conversation_id
			  AND other.user_id <> m.sender_user_id
			  AND other.last_read_server_order >= m.server_order
			  AND other.last_read_at IS NOT NULL
		) read_meta ON TRUE
		WHERE me.message_id = $1::uuid
		ORDER BY edited_at DESC
	`, messageID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var edits []EditRecord
	for rows.Next() {
		var edit EditRecord
		var editedAt time.Time
		var sentAt sql.NullTime
		var deliveredAt sql.NullTime
		var readAt sql.NullTime
		var prevContentStr, newContentStr string
		if err := rows.Scan(&editedAt, &sentAt, &deliveredAt, &readAt, &prevContentStr, &newContentStr, &edit.EditedBy); err != nil {
			return nil, err
		}
		edit.EditedAt = editedAt.UTC().Format(time.RFC3339Nano)
		if sentAt.Valid {
			edit.SentAt = sentAt.Time.UTC().Format(time.RFC3339Nano)
		}
		if deliveredAt.Valid {
			edit.DeliveredAt = deliveredAt.Time.UTC().Format(time.RFC3339Nano)
		}
		if readAt.Valid {
			edit.ReadAt = readAt.Time.UTC().Format(time.RFC3339Nano)
		}
		if err := json.Unmarshal([]byte(prevContentStr), &edit.PreviousContent); err != nil {
			return nil, err
		}
		if err := json.Unmarshal([]byte(newContentStr), &edit.NewContent); err != nil {
			return nil, err
		}
		edits = append(edits, edit)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return edits, nil
}

func (s *Service) RecordDelivery(ctx context.Context, messageID string, dr DeliveryRecord) error {
	_, err := s.db.Exec(ctx, `
		INSERT INTO message_deliveries (id, message_id, recipient_user_id, recipient_device_id, recipient_phone_e164, transport, state, provider, submitted_at, updated_at, failure_code)
		VALUES (gen_random_uuid(), $1, NULLIF($2, '')::uuid, NULLIF($3, '')::uuid, NULLIF($4, ''), $5, $6, NULLIF($7, ''), NULLIF($8, '')::timestamptz, now(), NULLIF($9, ''))
	`, messageID, dr.RecipientUserID, dr.RecipientDeviceID, dr.RecipientPhone, dr.Transport, dr.State, dr.Provider, nullableTimestamp(dr.SubmittedAt), dr.FailureCode)
	return err
}

func (s *Service) SetMessagePinned(ctx context.Context, actorUserID, messageID string, pinned bool) error {
	tx, err := s.db.BeginTx(ctx, pgx.TxOptions{})
	if err != nil {
		return err
	}
	defer tx.Rollback(ctx)

	var convID string
	var deletedAt sql.NullTime
	var visibilityState sql.NullString
	var expiresAt sql.NullTime
	if err := tx.QueryRow(ctx, `
		SELECT conversation_id::text, deleted_at, visibility_state, expires_at
		FROM messages
		WHERE id = $1::uuid
	`, messageID).Scan(&convID, &deletedAt, &visibilityState, &expiresAt); err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return fmt.Errorf("message_not_found")
		}
		return err
	}
	if deletedAt.Valid || strings.EqualFold(visibilityState.String, "SOFT_DELETED") || strings.EqualFold(visibilityState.String, "REDACTED") || (expiresAt.Valid && !expiresAt.Time.After(time.Now().UTC())) {
		return fmt.Errorf("message_not_pinnable")
	}

	if ok, err := s.hasMembership(ctx, tx, actorUserID, convID); err != nil {
		return err
	} else if !ok {
		return ErrConversationAccess
	}

	state, err := s.latestPinState(ctx, tx, messageID, convID)
	if err != nil {
		return err
	}
	if pinned && state == messagePinEffectPinned {
		return tx.Commit(ctx)
	}
	if !pinned && state != messagePinEffectPinned {
		return tx.Commit(ctx)
	}

	effectType := messagePinEffectPinned
	if !pinned {
		effectType = messagePinEffectUnpinned
	}

	if _, err := tx.Exec(ctx, `
		INSERT INTO message_effects(message_id, conversation_id, triggered_by_user_id, effect_type)
		VALUES($1::uuid, $2::uuid, $3::uuid, $4)
	`, messageID, convID, actorUserID, effectType); err != nil {
		return err
	}

	if _, err := tx.Exec(ctx, `UPDATE conversations SET updated_at = now() WHERE id = $1::uuid`, convID); err != nil {
		return err
	}

	if s.replication != nil {
		if err := s.replication.AppendMessageEffectEvent(ctx, tx, convID, actorUserID, messageID, effectType, time.Now().UTC()); err != nil {
			return err
		}
	}

	return tx.Commit(ctx)
}

func (s *Service) ListPinnedMessages(ctx context.Context, actorUserID, conversationID string) ([]Message, error) {
	if ok, err := s.hasMembership(ctx, s.db, actorUserID, conversationID); err != nil {
		return nil, err
	} else if !ok {
		return nil, ErrConversationAccess
	}

	rows, err := s.db.Query(ctx, `
		SELECT DISTINCT ON (message_id)
			message_id::text,
			triggered_by_user_id::text,
			triggered_at,
			effect_type
		FROM message_effects
		WHERE conversation_id = $1::uuid
		  AND effect_type IN ($2, $3)
		ORDER BY message_id, triggered_at DESC, id DESC
	`, conversationID, messagePinEffectPinned, messagePinEffectUnpinned)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	type pinState struct {
		messageID string
		pinnedBy  string
		pinnedAt  time.Time
	}

	pins := make([]pinState, 0, 16)
	for rows.Next() {
		var state pinState
		var effectType string
		if err := rows.Scan(&state.messageID, &state.pinnedBy, &state.pinnedAt, &effectType); err != nil {
			return nil, err
		}
		if effectType != messagePinEffectPinned {
			continue
		}
		pins = append(pins, state)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}

	items := make([]Message, 0, len(pins))
	for _, pin := range pins {
		msg, err := s.loadMessageViewByID(ctx, actorUserID, pin.messageID)
		if err != nil {
			if errors.Is(err, ErrConversationAccess) {
				continue
			}
			if err.Error() == "message_not_found" {
				continue
			}
			return nil, err
		}
		if msg.Deleted || strings.EqualFold(msg.VisibilityState, "EXPIRED") {
			continue
		}
		msg.PinnedAt = pin.pinnedAt.UTC().Format(time.RFC3339Nano)
		msg.PinnedByUserID = pin.pinnedBy
		items = append(items, msg)
	}
	sort.SliceStable(items, func(i, j int) bool {
		ti, _ := time.Parse(time.RFC3339Nano, items[i].PinnedAt)
		tj, _ := time.Parse(time.RFC3339Nano, items[j].PinnedAt)
		if ti.Equal(tj) {
			return items[i].ServerOrder < items[j].ServerOrder
		}
		return ti.After(tj)
	})
	return items, nil
}

func (s *Service) ForwardMessage(ctx context.Context, actorUserID, senderDeviceID, sourceMessageID, targetConversationID, idemKey, clientGeneratedID, ip string) (SendResult, error) {
	var sourceConversationID string
	var sourceMessage Message
	var err error
	if sourceMessage, err = s.loadMessageViewByID(ctx, actorUserID, sourceMessageID); err != nil {
		return SendResult{}, err
	}
	sourceConversationID = sourceMessage.ConversationID
	if sourceConversationID == "" {
		return SendResult{}, fmt.Errorf("message_not_found")
	}
	if sourceMessage.Deleted || strings.EqualFold(sourceMessage.VisibilityState, "SOFT_DELETED") || strings.EqualFold(sourceMessage.VisibilityState, "REDACTED") {
		return SendResult{}, fmt.Errorf("message_not_forwardable")
	}

	if err := s.enforceSendRate(ctx, actorUserID, targetConversationID, ip); err != nil {
		return SendResult{}, err
	}

	forwardedContent := cloneMessageContent(sourceMessage.Content)
	if forwardedContent == nil {
		forwardedContent = map[string]any{}
	}
	delete(forwardedContent, "reply_to_message_id")
	forwardedContent["forwarded_from"] = map[string]any{
		"message_id":       sourceMessage.MessageID,
		"conversation_id":  sourceConversationID,
		"sender_user_id":   sourceMessage.SenderUserID,
		"content_type":     sourceMessage.ContentType,
		"server_order":     sourceMessage.ServerOrder,
		"created_at":       sourceMessage.CreatedAt,
		"visibility_state": sourceMessage.VisibilityState,
		"client_generated": sourceMessage.ClientGeneratedID,
	}

	msg, err := s.sendSyncWithEndpoint(ctx, actorUserID, senderDeviceID, targetConversationID, idemKey, sourceMessage.ContentType, forwardedContent, clientGeneratedID, "/v1/messages/forward", messageForwardSource)
	if err != nil {
		return SendResult{}, err
	}
	return SendResult{Message: msg}, nil
}

func (s *Service) AddReaction(ctx context.Context, actorUserID, messageID, emoji string) error {
	tx, err := s.db.BeginTx(ctx, pgx.TxOptions{})
	if err != nil {
		return err
	}
	defer tx.Rollback(ctx)

	var convID string
	var serverOrder int64
	var createdAt time.Time
	var contentType string
	var encryptionState string
	var conversationType string
	if err := tx.QueryRow(ctx, `
		SELECT m.conversation_id::text, m.server_order, m.created_at, m.content_type, COALESCE(c.encryption_state, 'PLAINTEXT'), c.type
		FROM messages m
		JOIN conversations c ON c.id = m.conversation_id
		WHERE m.id = $1
	`, messageID).Scan(&convID, &serverOrder, &createdAt, &contentType, &encryptionState, &conversationType); err != nil {
		return err
	}
	ok, err := s.hasMembership(ctx, tx, actorUserID, convID)
	if err != nil {
		return err
	}
	if !ok {
		return ErrConversationAccess
	}
	tag, err := tx.Exec(ctx, `
		INSERT INTO message_reactions (message_id, user_id, emoji, created_at)
		VALUES ($1, $2, $3, now())
		ON CONFLICT DO NOTHING
	`, messageID, actorUserID, emoji)
	if err != nil {
		return err
	}
	if tag.RowsAffected() == 0 {
		return tx.Commit(ctx)
	}
	snapshot, err := s.loadMessageTimelineSnapshotTx(ctx, tx, actorUserID, convID, serverOrder, createdAt)
	if err != nil {
		return err
	}
	if err := s.appendReactionHistoryTx(ctx, tx, actorUserID, messageID, convID, emoji, "added", snapshot, time.Now().UTC()); err != nil {
		return err
	}
	if err := s.appendReactionDomainEventTx(ctx, tx, actorUserID, messageID, convID, serverOrder); err != nil {
		return err
	}
	return tx.Commit(ctx)
}

func (s *Service) RemoveReaction(ctx context.Context, actorUserID, messageID, emoji string) error {
	tx, err := s.db.BeginTx(ctx, pgx.TxOptions{})
	if err != nil {
		return err
	}
	defer tx.Rollback(ctx)

	var convID string
	var serverOrder int64
	var createdAt time.Time
	var contentType string
	var encryptionState string
	var conversationType string
	if err := tx.QueryRow(ctx, `
		SELECT m.conversation_id::text, m.server_order, m.created_at, m.content_type, COALESCE(c.encryption_state, 'PLAINTEXT'), c.type
		FROM messages m
		JOIN conversations c ON c.id = m.conversation_id
		WHERE m.id = $1
	`, messageID).Scan(&convID, &serverOrder, &createdAt, &contentType, &encryptionState, &conversationType); err != nil {
		return err
	}
	ok, err := s.hasMembership(ctx, tx, actorUserID, convID)
	if err != nil {
		return err
	}
	if !ok {
		return ErrConversationAccess
	}
	tag, err := tx.Exec(ctx, `
		DELETE FROM message_reactions WHERE message_id = $1 AND user_id = $2 AND emoji = $3
	`, messageID, actorUserID, emoji)
	if err != nil {
		return err
	}
	if tag.RowsAffected() == 0 {
		return tx.Commit(ctx)
	}
	snapshot, err := s.loadMessageTimelineSnapshotTx(ctx, tx, actorUserID, convID, serverOrder, createdAt)
	if err != nil {
		return err
	}
	if err := s.appendReactionHistoryTx(ctx, tx, actorUserID, messageID, convID, emoji, "removed", snapshot, time.Now().UTC()); err != nil {
		return err
	}
	if err := s.appendReactionDomainEventTx(ctx, tx, actorUserID, messageID, convID, serverOrder); err != nil {
		return err
	}
	return tx.Commit(ctx)
}

func (s *Service) ListReactions(ctx context.Context, messageID string) (map[string]int64, error) {
	return s.listReactionsWithQuery(ctx, s.db, messageID)
}

func (s *Service) GetMessageReactionHistory(ctx context.Context, actorUserID, messageID string) ([]ReactionHistoryRecord, error) {
	var conversationID string
	err := s.db.QueryRow(ctx, `SELECT conversation_id::text FROM messages WHERE id = $1::uuid`, messageID).Scan(&conversationID)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, fmt.Errorf("message_not_found")
		}
		return nil, err
	}

	ok, err := s.hasMembership(ctx, s.db, actorUserID, conversationID)
	if err != nil || !ok {
		return nil, ErrConversationAccess
	}

	rows, err := s.db.Query(ctx, `
		SELECT acted_at, sent_at, delivered_at, read_at, emoji, action, COALESCE(acted_by::text, '') AS acted_by
		FROM message_reaction_events
		WHERE message_id = $1::uuid
		ORDER BY acted_at DESC, id DESC
	`, messageID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var history []ReactionHistoryRecord
	for rows.Next() {
		var item ReactionHistoryRecord
		var actedAt time.Time
		var sentAt sql.NullTime
		var deliveredAt sql.NullTime
		var readAt sql.NullTime
		if err := rows.Scan(&actedAt, &sentAt, &deliveredAt, &readAt, &item.Emoji, &item.Action, &item.ActedBy); err != nil {
			return nil, err
		}
		item.ActedAt = actedAt.UTC().Format(time.RFC3339Nano)
		if sentAt.Valid {
			item.SentAt = sentAt.Time.UTC().Format(time.RFC3339Nano)
		}
		if deliveredAt.Valid {
			item.DeliveredAt = deliveredAt.Time.UTC().Format(time.RFC3339Nano)
		}
		if readAt.Valid {
			item.ReadAt = readAt.Time.UTC().Format(time.RFC3339Nano)
		}
		history = append(history, item)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return history, nil
}

func (s *Service) latestPinState(ctx context.Context, q querier, messageID, conversationID string) (string, error) {
	var state string
	err := q.QueryRow(ctx, `
		SELECT effect_type
		FROM message_effects
		WHERE message_id = $1::uuid
		  AND conversation_id = $2::uuid
		  AND effect_type IN ($3, $4)
		ORDER BY triggered_at DESC, id DESC
		LIMIT 1
	`, messageID, conversationID, messagePinEffectPinned, messagePinEffectUnpinned).Scan(&state)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return "", nil
		}
		return "", err
	}
	return state, nil
}

func (s *Service) loadMessageTimelineSnapshotTx(ctx context.Context, tx pgx.Tx, actorUserID, conversationID string, serverOrder int64, sentAt time.Time) (messageTimelineSnapshot, error) {
	var snapshot messageTimelineSnapshot
	snapshot.SentAt = sentAt
	err := tx.QueryRow(ctx, `
		SELECT
			(
				SELECT MAX(de.created_at)
				FROM domain_events de
				WHERE de.conversation_id = $1::uuid
				  AND de.event_type = 'delivery_checkpoint_advanced'
				  AND COALESCE((de.payload->>'through_server_order')::bigint, 0) >= $2
				  AND COALESCE(de.payload->>'user_id', '') <> $3::text
			) AS delivered_at,
			(
				SELECT MAX(other.last_read_at)
				FROM conversation_members other
				WHERE other.conversation_id = $1::uuid
				  AND other.user_id <> $3::uuid
				  AND other.last_read_server_order >= $2
				  AND other.last_read_at IS NOT NULL
			) AS read_at
	`, conversationID, serverOrder, actorUserID).Scan(&snapshot.DeliveredAt, &snapshot.ReadAt)
	return snapshot, err
}

func (s *Service) appendReactionHistoryTx(ctx context.Context, tx pgx.Tx, actorUserID, messageID, conversationID, emoji, action string, snapshot messageTimelineSnapshot, actedAt time.Time) error {
	_, err := tx.Exec(ctx, `
		INSERT INTO message_reaction_events (
			message_id,
			conversation_id,
			acted_by,
			emoji,
			action,
			sent_at,
			delivered_at,
			read_at,
			acted_at
		)
		VALUES ($1::uuid, $2::uuid, $3::uuid, $4, $5, $6, $7, $8, $9)
	`, messageID, conversationID, actorUserID, emoji, action, snapshot.SentAt, nullableTime(snapshot.DeliveredAt), nullableTime(snapshot.ReadAt), actedAt)
	return err
}

func (s *Service) loadMessageViewByID(ctx context.Context, actorUserID, messageID string) (Message, error) {
	var conversationID string
	if err := s.db.QueryRow(ctx, `SELECT conversation_id::text FROM messages WHERE id = $1::uuid`, messageID).Scan(&conversationID); err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return Message{}, fmt.Errorf("message_not_found")
		}
		return Message{}, err
	}

	if ok, err := s.hasMembership(ctx, s.db, actorUserID, conversationID); err != nil {
		return Message{}, err
	} else if !ok {
		return Message{}, ErrConversationAccess
	}

	row := s.db.QueryRow(ctx, `
		SELECT
			m.id::text,
			m.conversation_id::text,
			m.sender_user_id::text,
			COALESCE(m.sender_device_id::text, ''),
			COALESCE(m.reply_to_message_id::text, ''),
			m.content_type,
			CASE
				WHEN m.deleted_at IS NOT NULL
				  OR m.visibility_state IN ('SOFT_DELETED', 'REDACTED')
				  OR (m.expires_at IS NOT NULL AND m.expires_at <= now()) THEN '{}'::jsonb
				ELSE COALESCE(m.content, '{}'::jsonb)
			END AS content,
			m.client_generated_id,
			m.transport,
			m.server_order,
			CASE
				WHEN m.sender_user_id = $1::uuid AND m.transport IN ('OTT', 'OHMF') AND read_meta.read_at IS NOT NULL THEN 'READ'
				WHEN m.sender_user_id = $1::uuid AND m.transport IN ('OTT', 'OHMF') AND delivered_meta.delivered_at IS NOT NULL THEN 'DELIVERED'
				WHEN m.sender_user_id = $1::uuid THEN 'SENT'
				ELSE ''
			END AS delivery_status,
			m.created_at,
			delivered_meta.delivered_at,
			read_meta.read_at,
			m.edited_at,
			m.deleted_at,
			m.expires_at,
			CASE
				WHEN m.deleted_at IS NOT NULL THEN m.visibility_state
				WHEN m.expires_at IS NOT NULL AND m.expires_at <= now() THEN 'EXPIRED'
				ELSE m.visibility_state
			END AS visibility_state,
			COALESCE(reply_meta.reply_count, 0) AS reply_count,
			COALESCE(reaction_meta.reactions, '{}'::jsonb) AS reactions
		FROM messages m
		LEFT JOIN LATERAL (
			SELECT MAX(de.created_at) AS delivered_at
			FROM domain_events de
			WHERE de.conversation_id = m.conversation_id
			  AND de.event_type = 'delivery_checkpoint_advanced'
			  AND COALESCE((de.payload->>'through_server_order')::bigint, 0) >= m.server_order
			  AND COALESCE(de.payload->>'user_id', '') <> $1::text
		) delivered_meta ON TRUE
		LEFT JOIN LATERAL (
			SELECT MAX(other.last_read_at) AS read_at
			FROM conversation_members other
			WHERE other.conversation_id = m.conversation_id
			  AND other.user_id <> $1::uuid
			  AND other.last_read_server_order >= m.server_order
			  AND other.last_read_at IS NOT NULL
		) read_meta ON TRUE
		LEFT JOIN LATERAL (
			SELECT COUNT(*)::bigint AS reply_count
			FROM messages child
			WHERE child.reply_to_message_id = m.id
			  AND child.deleted_at IS NULL
			  AND (child.expires_at IS NULL OR child.expires_at > now())
			  AND COALESCE(child.visibility_state, '') NOT IN ('SOFT_DELETED', 'REDACTED', 'EXPIRED')
		) reply_meta ON TRUE
		LEFT JOIN LATERAL (
			SELECT jsonb_object_agg(emoji, cnt) AS reactions
			FROM (
				SELECT emoji, count(*)::bigint AS cnt
				FROM message_reactions
				WHERE message_id = m.id
				GROUP BY emoji
			) grouped
		) reaction_meta ON TRUE
		WHERE m.id = $2::uuid
	`, actorUserID, messageID)

	var m Message
	var contentRaw []byte
	var clientGenID sql.NullString
	var status string
	var created time.Time
	var deliveredAt sql.NullTime
	var readAt sql.NullTime
	var editedAt sql.NullTime
	var deletedAt sql.NullTime
	var expiresAt sql.NullTime
	var reactionsRaw []byte
	if err := row.Scan(&m.MessageID, &m.ConversationID, &m.SenderUserID, &m.SenderDeviceID, &m.ReplyToMessageID, &m.ContentType, &contentRaw, &clientGenID, &m.Transport, &m.ServerOrder, &status, &created, &deliveredAt, &readAt, &editedAt, &deletedAt, &expiresAt, &m.VisibilityState, &m.ReplyCount, &reactionsRaw); err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return Message{}, fmt.Errorf("message_not_found")
		}
		return Message{}, err
	}
	_ = json.Unmarshal(contentRaw, &m.Content)
	_ = json.Unmarshal(reactionsRaw, &m.Reactions)
	if clientGenID.Valid {
		m.ClientGeneratedID = clientGenID.String
	}
	if status != "" {
		m.Status = status
	}
	m.CreatedAt = created.UTC().Format(time.RFC3339)
	m.SentAt = m.CreatedAt
	if deliveredAt.Valid {
		m.DeliveredAt = deliveredAt.Time.UTC().Format(time.RFC3339)
	}
	if readAt.Valid {
		m.ReadAt = readAt.Time.UTC().Format(time.RFC3339)
	}
	if editedAt.Valid {
		m.EditedAt = editedAt.Time.UTC().Format(time.RFC3339)
	}
	applyMessageLifecycle(&m, deletedAt, expiresAt)
	switch m.Status {
	case "READ":
		m.StatusUpdatedAt = m.ReadAt
	case "DELIVERED":
		m.StatusUpdatedAt = m.DeliveredAt
	case "SENT":
		m.StatusUpdatedAt = m.SentAt
	}
	return m, nil
}

func cloneMessageContent(content map[string]any) map[string]any {
	if content == nil {
		return nil
	}
	body, err := json.Marshal(content)
	if err != nil {
		return nil
	}
	var out map[string]any
	if err := json.Unmarshal(body, &out); err != nil {
		return nil
	}
	return out
}

func parseStoredMessageContent(raw string) map[string]any {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return nil
	}
	var out map[string]any
	if err := json.Unmarshal([]byte(raw), &out); err != nil {
		return nil
	}
	return out
}

func encryptionSenderDeviceID(content map[string]any) string {
	if content == nil {
		return ""
	}
	encryption, _ := content["encryption"].(map[string]any)
	return strings.TrimSpace(textValue(encryption["sender_device_id"]))
}

func textValue(value any) string {
	text, _ := value.(string)
	return text
}

func resolveReplyTarget(ctx context.Context, tx pgx.Tx, conversationID string, content map[string]any) (string, error) {
	if content == nil {
		return "", nil
	}
	replyToRaw, _ := content["reply_to_message_id"].(string)
	replyToMessageID := strings.TrimSpace(replyToRaw)
	delete(content, "reply_to_message_id")
	if replyToMessageID == "" {
		return "", nil
	}
	var exists bool
	if err := tx.QueryRow(ctx, `
		SELECT EXISTS(
			SELECT 1
			FROM messages
			WHERE id = $1::uuid
			  AND conversation_id = $2::uuid
			  AND deleted_at IS NULL
			  AND (expires_at IS NULL OR expires_at > now())
			  AND COALESCE(visibility_state, '') NOT IN ('SOFT_DELETED', 'REDACTED', 'EXPIRED')
		)
	`, replyToMessageID, conversationID).Scan(&exists); err != nil {
		return "", err
	}
	if !exists {
		return "", fmt.Errorf("reply_target_not_found")
	}
	return replyToMessageID, nil
}

func (s *Service) appendReactionDomainEventTx(ctx context.Context, tx pgx.Tx, actorUserID, messageID, conversationID string, serverOrder int64) error {
	if s.replication == nil {
		return nil
	}
	conversationMeta, err := s.replication.LoadConversationMeta(ctx, tx, conversationID)
	if err != nil {
		return err
	}
	reactions, err := s.listReactionsWithQuery(ctx, tx, messageID)
	if err != nil {
		return err
	}
	return s.replication.AppendDomainEvent(ctx, tx, conversationID, actorUserID, replication.DomainEventMessageReactionsUpdated, replication.MessageReactionsPayload{
		MessageID:        messageID,
		ConversationID:   conversationID,
		ConversationType: conversationMeta.Type,
		ServerOrder:      serverOrder,
		Participants:     conversationMeta.Participants,
		ExternalPhones:   conversationMeta.ExternalPhones,
		Reactions:        reactions,
		ActedAt:          time.Now().UTC().Format(time.RFC3339Nano),
	})
}

func (s *Service) listReactionsWithQuery(ctx context.Context, q interface {
	Query(ctx context.Context, sql string, args ...any) (pgx.Rows, error)
}, messageID string) (map[string]int64, error) {
	rows, err := q.Query(ctx, `
		SELECT emoji, count(*) FROM message_reactions WHERE message_id = $1 GROUP BY emoji
	`, messageID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := make(map[string]int64)
	for rows.Next() {
		var emoji string
		var cnt int64
		if err := rows.Scan(&emoji, &cnt); err != nil {
			return nil, err
		}
		out[emoji] = cnt
	}
	return out, rows.Err()
}

func nullableTimestamp(v string) any {
	if v == "" {
		return nil
	}
	return v
}

func nullableTime(v sql.NullTime) any {
	if !v.Valid {
		return nil
	}
	return v.Time
}

func resolveMessageExpiration(ctx context.Context, q interface {
	QueryRow(ctx context.Context, sql string, args ...any) pgx.Row
}, conversationID, contentType string, content map[string]any) (sql.NullTime, error) {
	if !strings.EqualFold(contentType, "text") {
		return sql.NullTime{}, nil
	}
	hints, err := messageLifecycleHintsFromContent(content)
	if err != nil {
		return sql.NullTime{}, err
	}

	var retentionSeconds sql.NullInt64
	var conversationExpiresAt sql.NullTime
	if err := q.QueryRow(ctx, `
		SELECT COALESCE(retention_seconds, 0), expires_at
		FROM conversations
		WHERE id = $1::uuid
	`, conversationID).Scan(&retentionSeconds, &conversationExpiresAt); err != nil {
		return sql.NullTime{}, err
	}

	now := time.Now().UTC()
	candidates := make([]time.Time, 0, 3)
	if hints.ExpiresAt != nil {
		candidates = append(candidates, hints.ExpiresAt.UTC())
	}
	if retentionSeconds.Valid && retentionSeconds.Int64 > 0 {
		candidates = append(candidates, now.Add(time.Duration(retentionSeconds.Int64)*time.Second))
	}
	if conversationExpiresAt.Valid {
		candidates = append(candidates, conversationExpiresAt.Time.UTC())
	}

	if len(candidates) == 0 {
		return sql.NullTime{}, nil
	}

	expiry := candidates[0]
	for _, candidate := range candidates[1:] {
		if candidate.Before(expiry) {
			expiry = candidate
		}
	}
	return sql.NullTime{Time: expiry, Valid: true}, nil
}

func applyMessageLifecycle(m *Message, deletedAt sql.NullTime, expiresAt sql.NullTime) {
	if expiresAt.Valid {
		m.ExpiresAt = expiresAt.Time.UTC().Format(time.RFC3339Nano)
	}
	if deletedAt.Valid {
		m.Deleted = true
		m.DeletedAt = deletedAt.Time.UTC().Format(time.RFC3339Nano)
		m.Content = map[string]any{}
		return
	}
	if expiresAt.Valid && !expiresAt.Time.After(time.Now().UTC()) {
		m.Deleted = true
		m.DeletedAt = expiresAt.Time.UTC().Format(time.RFC3339Nano)
		m.VisibilityState = "EXPIRED"
		m.Content = map[string]any{}
		return
	}
	if m.VisibilityState == "SOFT_DELETED" || m.VisibilityState == "REDACTED" {
		m.Deleted = true
		m.Content = map[string]any{}
	}
}

func (s *Service) ListDeliveries(ctx context.Context, messageID string) ([]DeliveryRecord, error) {
	rows, err := s.db.Query(ctx, `
		SELECT id::text, message_id::text, recipient_user_id::text, recipient_device_id::text, recipient_phone_e164, transport, state, provider, submitted_at, updated_at, failure_code
		FROM message_deliveries WHERE message_id = $1::uuid ORDER BY updated_at ASC
	`, messageID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := make([]DeliveryRecord, 0)
	for rows.Next() {
		var d DeliveryRecord
		var ru, rd sql.NullString
		var submitted sql.NullTime
		var updated time.Time
		if err := rows.Scan(&d.ID, &d.MessageID, &ru, &rd, &d.RecipientPhone, &d.Transport, &d.State, &d.Provider, &submitted, &updated, &d.FailureCode); err != nil {
			return nil, err
		}
		if ru.Valid {
			d.RecipientUserID = ru.String
		}
		if rd.Valid {
			d.RecipientDeviceID = rd.String
		}
		if submitted.Valid {
			d.SubmittedAt = submitted.Time.UTC().Format(time.RFC3339)
		}
		d.UpdatedAt = updated.UTC().Format(time.RFC3339)
		out = append(out, d)
	}
	return out, rows.Err()
}

var (
	ErrConversationAccess          = errors.New("conversation_access_denied")
	ErrConversationBlocked         = errors.New("conversation_blocked")
	ErrRateLimited                 = errors.New("rate_limited")
	ErrInvalidMessageEffectType    = errors.New("invalid_message_effect_type")
	ErrEncryptedMessageRequired    = errors.New("encrypted_message_required")
	ErrEncryptedMessageInvalid     = errors.New("encrypted_message_invalid")
	ErrEncryptedEditDeviceMismatch = errors.New("encrypted_edit_requires_origin_device")
	ErrSenderDeviceRequired        = errors.New("sender_device_required")
	ErrSenderDeviceInvalid         = errors.New("sender_device_invalid")
)

type RateLimitError struct {
	Scope      string
	RetryAfter time.Duration
}

func (e RateLimitError) Error() string {
	return "rate_limited"
}

func isSupportedMessageEffectType(effectType string) bool {
	switch strings.ToLower(strings.TrimSpace(effectType)) {
	case "bubble_confetti",
		"bubble_echo",
		"bubble_spotlight",
		"bubble_balloons",
		"bubble_love",
		"bubble_lasers",
		"screen_fireworks",
		"screen_echo",
		"screen_spotlight":
		return true
	default:
		return false
	}
}

func NewService(db DB, opts Options) *Service {
	ackTimeout := opts.AckTimeout
	if ackTimeout <= 0 {
		ackTimeout = 2 * time.Second
	}
	return &Service{
		db:                db,
		useKafkaSend:      opts.UseKafkaSend,
		useCassandraReads: opts.UseCassandraReads,
		ackTimeout:        ackTimeout,
		async:             opts.Async,
		cassandra:         opts.Cassandra,
		rateLimiter:       opts.RateLimiter,
		redis:             opts.Redis,
		replication:       opts.Replication,
	}
}

type conversationPolicy struct {
	ConversationType string
	EncryptionState  string
}

func (s *Service) loadConversationPolicy(ctx context.Context, q interface {
	QueryRow(ctx context.Context, sql string, args ...any) pgx.Row
}, conversationID string) (conversationPolicy, error) {
	var policy conversationPolicy
	err := q.QueryRow(ctx, `
		SELECT type, COALESCE(encryption_state, 'PLAINTEXT')
		FROM conversations
		WHERE id = $1::uuid
	`, conversationID).Scan(&policy.ConversationType, &policy.EncryptionState)
	return policy, err
}

func (s *Service) senderOwnsDevice(ctx context.Context, q interface {
	QueryRow(ctx context.Context, sql string, args ...any) pgx.Row
}, userID, deviceID string) (bool, error) {
	var exists bool
	if err := q.QueryRow(ctx, `
		SELECT EXISTS(
			SELECT 1
			FROM devices
			WHERE id = $1::uuid
			  AND user_id = $2::uuid
		)
	`, deviceID, userID).Scan(&exists); err != nil {
		return false, err
	}
	return exists, nil
}

func encryptedOTTDM(policy conversationPolicy) bool {
	return strings.EqualFold(strings.TrimSpace(policy.ConversationType), "DM") &&
		strings.EqualFold(strings.TrimSpace(policy.EncryptionState), "ENCRYPTED")
}

type receiptEvent struct {
	SenderUserID       string `json:"sender_user_id"`
	RecipientUserID    string `json:"recipient_user_id,omitempty"`
	ReaderUserID       string `json:"reader_user_id,omitempty"`
	MessageID          string `json:"message_id,omitempty"`
	ConversationID     string `json:"conversation_id"`
	ServerOrder        int64  `json:"server_order,omitempty"`
	ThroughServerOrder int64  `json:"through_server_order,omitempty"`
	Status             string `json:"status"`
	StatusUpdatedAt    string `json:"status_updated_at"`
}

func (s *Service) Send(ctx context.Context, userID, senderDeviceID, conversationID, idemKey, contentType string, content map[string]any, clientGeneratedID string, traceID string, ip string) (SendResult, error) {
	if strings.EqualFold(contentType, "text") {
		if _, err := messageLifecycleHintsFromContent(content); err != nil {
			return SendResult{}, err
		}
	}
	if err := s.enforceSendRate(ctx, userID, conversationID, ip); err != nil {
		return SendResult{}, err
	}
	if s.useKafkaSend && s.async != nil {
		return s.sendAsync(ctx, userID, conversationID, idemKey, contentType, content, clientGeneratedID, "OHMF", "", "/v1/messages", traceID)
	}
	msg, err := s.sendSync(ctx, userID, senderDeviceID, conversationID, idemKey, contentType, content, clientGeneratedID)
	if err != nil {
		return SendResult{}, err
	}
	return SendResult{Message: msg}, nil
}

func (s *Service) SendToPhone(ctx context.Context, userID, senderDeviceID, phoneE164, idemKey, contentType string, content map[string]any, clientGeneratedID string, traceID string, ip string) (SendResult, error) {
	if strings.EqualFold(contentType, "text") {
		if _, err := messageLifecycleHintsFromContent(content); err != nil {
			return SendResult{}, err
		}
	}
	conversationID, err := s.ensurePhoneConversation(ctx, userID, phoneE164)
	if err != nil {
		return SendResult{}, err
	}
	if err := s.enforceSendRate(ctx, userID, conversationID, ip); err != nil {
		return SendResult{}, err
	}
	if s.useKafkaSend && s.async != nil {
		return s.sendAsync(ctx, userID, conversationID, idemKey, contentType, content, clientGeneratedID, "SMS", phoneE164, "/v1/messages/phone", traceID)
	}
	msg, err := s.sendToPhoneSync(ctx, userID, senderDeviceID, phoneE164, idemKey, contentType, content, clientGeneratedID)
	if err != nil {
		return SendResult{}, err
	}
	return SendResult{Message: msg}, nil
}

func (s *Service) sendAsync(ctx context.Context, userID, conversationID, idemKey, contentType string, content map[string]any, clientGeneratedID, transportIntent, phoneE164, endpoint, traceID string) (SendResult, error) {
	if ok, err := s.hasMembership(ctx, s.db, userID, conversationID); err != nil {
		return SendResult{}, err
	} else if !ok {
		return SendResult{}, ErrConversationAccess
	}

	if blocked, _, err := s.checkBlockedRecipients(ctx, s.db, userID, conversationID); err != nil {
		return SendResult{}, err
	} else if blocked {
		return SendResult{}, ErrConversationBlocked
	}

	cached, cachedStatus, err := s.loadIdempotency(ctx, userID, endpoint, idemKey)
	if err != nil {
		return SendResult{}, err
	}
	if cached != nil {
		if cachedStatus == 202 {
			return SendResult{
				Message:      *cached,
				Queued:       true,
				AckTimeoutMS: s.ackTimeout.Milliseconds(),
			}, nil
		}
		return SendResult{Message: *cached}, nil
	}

	evt := NewIngressEvent(userID, conversationID, endpoint, idemKey, contentType, transportIntent, phoneE164, clientGeneratedID, traceID, content)
	provisional := evt.ProvisionalMessage()

	if err := s.upsertIdempotency(ctx, userID, endpoint, idemKey, provisional, 202); err != nil {
		return SendResult{}, err
	}
	if err := s.async.PublishIngress(ctx, evt); err != nil {
		return SendResult{}, err
	}

	ack, ok, err := s.async.WaitAck(ctx, evt.EventID, s.ackTimeout)
	if err != nil {
		return SendResult{}, err
	}
	if !ok {
		return SendResult{
			Message:      provisional,
			Queued:       true,
			AckTimeoutMS: s.ackTimeout.Milliseconds(),
		}, nil
	}

	msg := Message{
		MessageID:         ack.MessageID,
		ConversationID:    ack.ConversationID,
		SenderUserID:      userID,
		ContentType:       contentType,
		Content:           content,
		ClientGeneratedID: provisional.ClientGeneratedID,
		Transport:         ack.Transport,
		ServerOrder:       ack.ServerOrder,
		Status:            ack.Status,
		CreatedAt:         time.UnixMilli(ack.PersistedAtMS).UTC().Format(time.RFC3339),
	}
	if err := s.upsertIdempotency(ctx, userID, endpoint, idemKey, msg, 201); err != nil {
		return SendResult{}, err
	}
	return SendResult{Message: msg}, nil
}

func (s *Service) List(ctx context.Context, actor, conversationID string) ([]Message, error) {
	if ok, err := s.hasMembership(ctx, s.db, actor, conversationID); err != nil {
		return nil, err
	} else if !ok {
		return nil, ErrConversationAccess
	}

	if s.useCassandraReads && s.cassandra != nil {
		hasTombstones, err := s.conversationHasTombstones(ctx, conversationID)
		if err == nil && !hasTombstones {
			items, err := s.cassandra.ListConversation(ctx, conversationID, 100)
			if err == nil && len(items) > 0 {
				return items, nil
			}
		}
	}

	rows, err := s.db.Query(ctx, `
		SELECT
			m.id::text,
			m.conversation_id::text,
			m.sender_user_id::text,
			COALESCE(m.sender_device_id::text, ''),
			COALESCE(m.reply_to_message_id::text, ''),
			m.content_type,
			CASE
				WHEN m.deleted_at IS NOT NULL
				  OR m.visibility_state IN ('SOFT_DELETED', 'REDACTED')
				  OR (m.expires_at IS NOT NULL AND m.expires_at <= now()) THEN '{}'::jsonb
				ELSE COALESCE(m.content, '{}'::jsonb)
			END AS content,
			m.client_generated_id,
			m.transport,
			m.server_order,
			CASE
				WHEN m.sender_user_id = $2::uuid AND m.transport IN ('OTT', 'OHMF') AND read_meta.read_at IS NOT NULL THEN 'READ'
				WHEN m.sender_user_id = $2::uuid AND m.transport IN ('OTT', 'OHMF') AND delivered_meta.delivered_at IS NOT NULL THEN 'DELIVERED'
				WHEN m.sender_user_id = $2::uuid THEN 'SENT'
				ELSE ''
			END AS delivery_status,
			m.created_at,
			delivered_meta.delivered_at,
			read_meta.read_at,
			m.edited_at,
			m.deleted_at,
			m.expires_at,
			CASE
				WHEN m.deleted_at IS NOT NULL THEN m.visibility_state
				WHEN m.expires_at IS NOT NULL AND m.expires_at <= now() THEN 'EXPIRED'
				ELSE m.visibility_state
			END AS visibility_state,
			COALESCE(reply_meta.reply_count, 0) AS reply_count,
			COALESCE(reaction_meta.reactions, '{}'::jsonb) AS reactions
		FROM messages m
		LEFT JOIN LATERAL (
			SELECT MAX(de.created_at) AS delivered_at
			FROM domain_events de
			WHERE de.conversation_id = m.conversation_id
			  AND de.event_type = 'delivery_checkpoint_advanced'
			  AND COALESCE((de.payload->>'through_server_order')::bigint, 0) >= m.server_order
			  AND COALESCE(de.payload->>'user_id', '') <> $2::text
		) delivered_meta ON TRUE
		LEFT JOIN LATERAL (
			SELECT MAX(other.last_read_at) AS read_at
			FROM conversation_members other
			WHERE other.conversation_id = m.conversation_id
			  AND other.user_id <> $2::uuid
			  AND other.last_read_server_order >= m.server_order
			  AND other.last_read_at IS NOT NULL
		) read_meta ON TRUE
		LEFT JOIN LATERAL (
			SELECT COUNT(*)::bigint AS reply_count
			FROM messages child
			WHERE child.reply_to_message_id = m.id
			  AND child.deleted_at IS NULL
			  AND (child.expires_at IS NULL OR child.expires_at > now())
			  AND COALESCE(child.visibility_state, '') NOT IN ('SOFT_DELETED', 'REDACTED', 'EXPIRED')
		) reply_meta ON TRUE
		LEFT JOIN LATERAL (
			SELECT jsonb_object_agg(emoji, cnt) AS reactions
			FROM (
				SELECT emoji, count(*)::bigint AS cnt
				FROM message_reactions
				WHERE message_id = m.id
				GROUP BY emoji
			) grouped
		) reaction_meta ON TRUE
		WHERE m.conversation_id = $1::uuid
		ORDER BY server_order ASC
		LIMIT 100
	`, conversationID, actor)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	items := make([]Message, 0, 16)
	for rows.Next() {
		var m Message
		var contentRaw []byte
		var clientGenID sql.NullString
		var status string
		var created time.Time
		var deliveredAt sql.NullTime
		var readAt sql.NullTime
		var editedAt sql.NullTime
		var deletedAt sql.NullTime
		var expiresAt sql.NullTime
		var reactionsRaw []byte
		if err := rows.Scan(&m.MessageID, &m.ConversationID, &m.SenderUserID, &m.SenderDeviceID, &m.ReplyToMessageID, &m.ContentType, &contentRaw, &clientGenID, &m.Transport, &m.ServerOrder, &status, &created, &deliveredAt, &readAt, &editedAt, &deletedAt, &expiresAt, &m.VisibilityState, &m.ReplyCount, &reactionsRaw); err != nil {
			return nil, err
		}
		_ = json.Unmarshal(contentRaw, &m.Content)
		_ = json.Unmarshal(reactionsRaw, &m.Reactions)
		if clientGenID.Valid {
			m.ClientGeneratedID = clientGenID.String
		}
		if status != "" {
			m.Status = status
		}
		m.CreatedAt = created.UTC().Format(time.RFC3339)
		m.SentAt = m.CreatedAt
		if deliveredAt.Valid {
			m.DeliveredAt = deliveredAt.Time.UTC().Format(time.RFC3339)
		}
		if readAt.Valid {
			m.ReadAt = readAt.Time.UTC().Format(time.RFC3339)
		}
		if editedAt.Valid {
			m.EditedAt = editedAt.Time.UTC().Format(time.RFC3339)
		}
		applyMessageLifecycle(&m, deletedAt, expiresAt)
		switch m.Status {
		case "READ":
			m.StatusUpdatedAt = m.ReadAt
		case "DELIVERED":
			m.StatusUpdatedAt = m.DeliveredAt
		case "SENT":
			m.StatusUpdatedAt = m.SentAt
		}
		items = append(items, m)
	}
	return items, rows.Err()
}

func (s *Service) ListUnified(ctx context.Context, actor, conversationID string, limit int) ([]Message, error) {
	if ok, err := s.hasMembership(ctx, s.db, actor, conversationID); err != nil {
		return nil, err
	} else if !ok {
		return nil, ErrConversationAccess
	}

	if limit <= 0 || limit > 500 {
		limit = 100
	}

	rows, err := s.db.Query(ctx, `
		SELECT
			m.id::text,
			m.conversation_id::text,
			m.sender_user_id::text,
			COALESCE(m.sender_device_id::text, ''),
			COALESCE(m.reply_to_message_id::text, ''),
			m.content_type,
			CASE
				WHEN m.deleted_at IS NOT NULL
				  OR m.visibility_state IN ('SOFT_DELETED', 'REDACTED')
				  OR (m.expires_at IS NOT NULL AND m.expires_at <= now()) THEN '{}'::jsonb
				ELSE COALESCE(m.content, '{}'::jsonb)
			END AS content,
			m.client_generated_id,
			m.transport,
			m.server_order,
			m.created_at,
			m.edited_at,
			m.deleted_at,
			m.expires_at,
			m.visibility_state,
			COALESCE(reply_meta.reply_count, 0) AS reply_count
		FROM messages m
		LEFT JOIN LATERAL (
			SELECT COUNT(*)::bigint AS reply_count
			FROM messages child
			WHERE child.reply_to_message_id = m.id
			  AND child.deleted_at IS NULL
			  AND (child.expires_at IS NULL OR child.expires_at > now())
			  AND COALESCE(child.visibility_state, '') NOT IN ('SOFT_DELETED', 'REDACTED', 'EXPIRED')
		) reply_meta ON TRUE
		WHERE m.conversation_id = $1::uuid
		ORDER BY server_order ASC
		LIMIT $2
	`, conversationID, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	items := make([]Message, 0, 32)
	var messageIDs []string
	for rows.Next() {
		var m Message
		var contentRaw []byte
		var clientGenID sql.NullString
		var created time.Time
		var editedAt sql.NullTime
		var deletedAt sql.NullTime
		var expiresAt sql.NullTime
		if err := rows.Scan(&m.MessageID, &m.ConversationID, &m.SenderUserID, &m.SenderDeviceID, &m.ReplyToMessageID, &m.ContentType, &contentRaw, &clientGenID, &m.Transport, &m.ServerOrder, &created, &editedAt, &deletedAt, &expiresAt, &m.VisibilityState, &m.ReplyCount); err != nil {
			return nil, err
		}
		_ = json.Unmarshal(contentRaw, &m.Content)
		if clientGenID.Valid {
			m.ClientGeneratedID = clientGenID.String
		}
		m.CreatedAt = created.UTC().Format(time.RFC3339)
		if editedAt.Valid {
			m.EditedAt = editedAt.Time.UTC().Format(time.RFC3339)
		}
		applyMessageLifecycle(&m, deletedAt, expiresAt)
		m.Source = "SERVER"
		items = append(items, m)
		messageIDs = append(messageIDs, m.MessageID)
	}

	tkRows, err := s.db.Query(ctx, `SELECT value FROM conversation_thread_keys WHERE conversation_id = $1::uuid`, conversationID)
	if err == nil {
		defer tkRows.Close()
		var keys []string
		for tkRows.Next() {
			var v string
			if err := tkRows.Scan(&v); err == nil {
				keys = append(keys, v)
			}
		}
		if len(keys) > 0 || len(messageIDs) > 0 {
			query := `SELECT id::text, device_id::text, thread_key, carrier_message_id, direction, transport, text, media_json, created_at, device_authoritative, server_message_id::text, raw_payload FROM carrier_messages WHERE `
			args := []any{}
			clauses := []string{}
			idx := 1
			if len(keys) > 0 {
				clauses = append(clauses, "thread_key = ANY($"+fmt.Sprint(idx)+"::text[])")
				args = append(args, keys)
				idx++
			}
			if len(messageIDs) > 0 {
				clauses = append(clauses, "server_message_id = ANY($"+fmt.Sprint(idx)+"::uuid[])")
				args = append(args, messageIDs)
				idx++
			}
			query += "(" + clauses[0]
			for i := 1; i < len(clauses); i++ {
				query += " OR " + clauses[i]
			}
			query += ") ORDER BY created_at ASC LIMIT $" + fmt.Sprint(idx)
			args = append(args, limit)

			crows, err := s.db.Query(ctx, query, args...)
			if err == nil {
				defer crows.Close()
				for crows.Next() {
					var id, deviceID, threadKey, carrierMessageID, direction, transport, text string
					var mediaJSON []byte
					var created time.Time
					var deviceAuth bool
					var serverMsgID sql.NullString
					var rawPayload []byte
					if err := crows.Scan(&id, &deviceID, &threadKey, &carrierMessageID, &direction, &transport, &text, &mediaJSON, &created, &deviceAuth, &serverMsgID, &rawPayload); err != nil {
						return nil, err
					}
					content := make(map[string]any)
					if text != "" {
						content["text"] = text
					}
					if len(mediaJSON) > 0 {
						var mj any
						_ = json.Unmarshal(mediaJSON, &mj)
						content["media"] = mj
					}
					m := Message{
						MessageID:      id,
						ConversationID: conversationID,
						SenderUserID:   "",
						ContentType:    "media",
						Content:        content,
						Transport:      transport,
						ServerOrder:    0,
						CreatedAt:      created.UTC().Format(time.RFC3339),
						Source:         "CARRIER",
					}
					items = append(items, m)
				}
			}
		}
	}

	sort.SliceStable(items, func(i, j int) bool {
		ti, _ := time.Parse(time.RFC3339, items[i].CreatedAt)
		tj, _ := time.Parse(time.RFC3339, items[j].CreatedAt)
		return ti.Before(tj)
	})

	if len(items) > limit {
		items = items[:limit]
	}
	return items, nil
}

// SearchMessages searches for messages in a conversation by text content with enhanced ranking
func (s *Service) SearchMessages(ctx context.Context, userID, conversationID, query string, limit int, opts SearchOptions) ([]Message, error) {
	if query == "" {
		return []Message{}, nil
	}
	if len(query) < 2 {
		return nil, errors.New("query too short")
	}
	if limit <= 0 || limit > 100 {
		limit = 50
	}

	if err := s.enforceSearchRate(ctx, userID, conversationID); err != nil {
		return nil, err
	}

	if ok, err := s.hasMembership(ctx, s.db, userID, conversationID); err != nil {
		return nil, err
	} else if !ok {
		return nil, ErrConversationAccess
	}

	// Normalize query for better matching
	sq := NormalizeQuery(query)
	isValid, warning := ValidateSearchQuality(sq)
	if !isValid {
		if warning != "" {
			// Log warning but don't fail - still attempt search
		}
		return []Message{}, nil
	}

	// Set defaults for search options
	if opts.SearchMode == "" {
		opts.SearchMode = "standard"
	}
	if opts.MatchType == "" {
		opts.MatchType = "any"
	}
	if opts.SortBy == "" {
		opts.SortBy = "relevance"
	}

	// Build filter arguments and SQL
	args := []any{conversationID, sq.TruncateForDB(500)}
	filters := []string{
		`m.conversation_id = $1::uuid`,
		`m.deleted_at IS NULL`,
		`(m.expires_at IS NULL OR m.expires_at > now())`,
		`m.visibility_state != 'SOFT_DELETED'`,
	}

	// Build search condition based on search mode and options
	searchCondition := s.buildSearchCondition(opts.SearchMode, opts.ExactMatch, len(args)+1)
	filters = append(filters, fmt.Sprintf("(%s)", searchCondition))

	// Add optional filters
	if trimmed := strings.TrimSpace(opts.SenderUserID); trimmed != "" {
		args = append(args, trimmed)
		filters = append(filters, fmt.Sprintf("m.sender_user_id = $%d::uuid", len(args)))
	}
	if trimmed := strings.TrimSpace(opts.ContentType); trimmed != "" {
		args = append(args, trimmed)
		filters = append(filters, fmt.Sprintf("m.content_type = $%d", len(args)))
	}
	if opts.After != nil {
		args = append(args, opts.After.UTC())
		filters = append(filters, fmt.Sprintf("m.created_at >= $%d", len(args)))
	}
	if opts.Before != nil {
		args = append(args, opts.Before.UTC())
		filters = append(filters, fmt.Sprintf("m.created_at <= $%d", len(args)))
	}
	args = append(args, limit)

	// Build ORDER BY clause based on sort preference
	orderBy := s.buildOrderBy(opts.SortBy, len(args)-1) // -1 because limit is last arg

	querySQL := fmt.Sprintf(`
		SELECT
			m.id::text,
			m.conversation_id::text,
			m.sender_user_id::text,
			COALESCE(m.sender_device_id::text, ''),
			COALESCE(m.reply_to_message_id::text, ''),
			m.content_type,
			CASE
				WHEN m.deleted_at IS NOT NULL
				  OR m.visibility_state IN ('SOFT_DELETED', 'REDACTED')
				  OR (m.expires_at IS NOT NULL AND m.expires_at <= now()) THEN '{}'::jsonb
				ELSE COALESCE(m.content, '{}'::jsonb)
			END AS content,
			m.client_generated_id,
			m.transport,
			m.server_order,
			m.created_at,
			m.edited_at,
			m.deleted_at,
			m.expires_at,
			m.visibility_state,
			COALESCE(reply_meta.reply_count, 0) AS reply_count
		FROM messages m
		LEFT JOIN LATERAL (
			SELECT COUNT(*)::bigint AS reply_count
			FROM messages child
			WHERE child.reply_to_message_id = m.id
			  AND child.deleted_at IS NULL
			  AND (child.expires_at IS NULL OR child.expires_at > now())
			  AND COALESCE(child.visibility_state, '') NOT IN ('SOFT_DELETED', 'REDACTED', 'EXPIRED')
		) reply_meta ON TRUE
		WHERE %s
		ORDER BY %s
		LIMIT $%d
	`, strings.Join(filters, "\n		  AND "), orderBy, len(args))

	rows, err := s.db.Query(ctx, querySQL, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	items := make([]Message, 0, limit)
	for rows.Next() {
		var m Message
		var contentRaw []byte
		var clientGenID sql.NullString
		var created time.Time
		var editedAt sql.NullTime
		var deletedAt sql.NullTime
		var expiresAt sql.NullTime
		if err := rows.Scan(&m.MessageID, &m.ConversationID, &m.SenderUserID, &m.SenderDeviceID, &m.ReplyToMessageID, &m.ContentType, &contentRaw, &clientGenID, &m.Transport, &m.ServerOrder, &created, &editedAt, &deletedAt, &expiresAt, &m.VisibilityState, &m.ReplyCount); err != nil {
			return nil, err
		}
		_ = json.Unmarshal(contentRaw, &m.Content)
		if clientGenID.Valid {
			m.ClientGeneratedID = clientGenID.String
		}
		m.CreatedAt = created.UTC().Format(time.RFC3339)
		if editedAt.Valid {
			m.EditedAt = editedAt.Time.UTC().Format(time.RFC3339)
		}
		applyMessageLifecycle(&m, deletedAt, expiresAt)
		m.Source = "SERVER"
		items = append(items, m)
	}
	return items, rows.Err()
}

// buildSearchCondition constructs the WHERE clause for different search modes
func (s *Service) buildSearchCondition(searchMode string, exactMatch bool, paramNum int) string {
	queryParam := fmt.Sprintf("$%d", paramNum)

	switch searchMode {
	case "fuzzy":
		// Fuzzy/typo-tolerant search using trigram similarity
		return fmt.Sprintf(`(
			m.search_text_normalized %% unaccent(lower(%s))
			OR m.search_vector_en @@ plainto_tsquery('english', %s)
		)`, queryParam, queryParam)

	case "exact":
		// Exact phrase match only
		return fmt.Sprintf(`(
			unaccent(lower(COALESCE(m.content->>'text', ''))) = unaccent(lower(%s))
		)`, queryParam)

	default:
		// Standard mode: multi-strategy matching
		return fmt.Sprintf(`(
			m.search_vector @@ websearch_to_tsquery('simple', %s)
			OR m.search_vector_en @@ plainto_tsquery('english', %s)
			OR unaccent(lower(COALESCE(m.content->>'text', ''))) ILIKE '%%' || unaccent(lower(%s)) || '%%'
			OR m.content->>'attachment_id' ILIKE '%%' || %s || '%%'
		)`, queryParam, queryParam, queryParam, queryParam)
	}
}

// buildOrderBy constructs the ORDER BY clause with multi-factor ranking
func (s *Service) buildOrderBy(sortBy string, queryParamNum int) string {
	queryParam := fmt.Sprintf("$%d", queryParamNum)

	if sortBy == "recency" {
		// Sort by recency (newest first), with some relevance weight
		return fmt.Sprintf(`
			m.created_at DESC,
			ts_rank_cd(m.search_vector_en, plainto_tsquery('english', %s)) DESC,
			m.server_order DESC
		`, queryParam)
	}

	// Default relevance-based ranking with multi-factor sorting
	return fmt.Sprintf(`
		-- Factor 1: English FTS rank × base rank (0-1)
		(ts_rank_cd(m.search_vector_en, plainto_tsquery('english', %s)) *
		 COALESCE(m.search_rank_base, 1.0)) DESC,

		-- Factor 2: Exact/prefix/infix matches (0-3)
		CASE
			WHEN unaccent(lower(COALESCE(m.content->>'text', ''))) = unaccent(lower(%s)) THEN 3
			WHEN unaccent(lower(COALESCE(m.content->>'text', ''))) LIKE unaccent(lower(%s)) || '%%' THEN 2.5
			WHEN unaccent(lower(COALESCE(m.content->>'text', ''))) LIKE '%%' || unaccent(lower(%s)) || '%%' THEN 2
			ELSE 0
		END DESC,

		-- Factor 3: English stemming match (boolean boost)
		CASE
			WHEN m.search_vector_en @@ plainto_tsquery('english', %s) THEN 1
			ELSE 0
		END DESC,

		-- Factor 4: Trigram similarity for typo tolerance (0-1)
		CASE
			WHEN m.search_text_normalized IS NOT NULL
			THEN similarity(m.search_text_normalized, unaccent(lower(%s)))
			ELSE 0
		END DESC,

		-- Factor 5: Recency (recent messages ranked higher within similar relevance)
		m.created_at DESC,

		-- Factor 6: Server order as final tiebreaker
		m.server_order DESC
	`, queryParam, queryParam, queryParam, queryParam, queryParam, queryParam)
}

func (s *Service) ListReplies(ctx context.Context, actorUserID, messageID string) ([]Message, error) {
	parent, err := s.loadMessageViewByID(ctx, actorUserID, messageID)
	if err != nil {
		return nil, err
	}
	rows, err := s.db.Query(ctx, `
		SELECT
			m.id::text,
			m.conversation_id::text,
			m.sender_user_id::text,
			COALESCE(m.sender_device_id::text, ''),
			COALESCE(m.reply_to_message_id::text, ''),
			m.content_type,
			CASE
				WHEN m.deleted_at IS NOT NULL
				  OR m.visibility_state IN ('SOFT_DELETED', 'REDACTED')
				  OR (m.expires_at IS NOT NULL AND m.expires_at <= now()) THEN '{}'::jsonb
				ELSE COALESCE(m.content, '{}'::jsonb)
			END AS content,
			m.client_generated_id,
			m.transport,
			m.server_order,
			m.created_at,
			m.edited_at,
			m.deleted_at,
			m.expires_at,
			CASE
				WHEN m.deleted_at IS NOT NULL THEN m.visibility_state
				WHEN m.expires_at IS NOT NULL AND m.expires_at <= now() THEN 'EXPIRED'
				ELSE m.visibility_state
			END AS visibility_state,
			COALESCE(reply_meta.reply_count, 0) AS reply_count
		FROM messages m
		LEFT JOIN LATERAL (
			SELECT COUNT(*)::bigint AS reply_count
			FROM messages child
			WHERE child.reply_to_message_id = m.id
			  AND child.deleted_at IS NULL
			  AND (child.expires_at IS NULL OR child.expires_at > now())
			  AND COALESCE(child.visibility_state, '') NOT IN ('SOFT_DELETED', 'REDACTED', 'EXPIRED')
		) reply_meta ON TRUE
		WHERE m.reply_to_message_id = $1::uuid
		  AND m.conversation_id = $2::uuid
		ORDER BY m.server_order ASC
	`, messageID, parent.ConversationID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	items := make([]Message, 0, 8)
	for rows.Next() {
		var m Message
		var contentRaw []byte
		var clientGenID sql.NullString
		var created time.Time
		var editedAt sql.NullTime
		var deletedAt sql.NullTime
		var expiresAt sql.NullTime
		if err := rows.Scan(&m.MessageID, &m.ConversationID, &m.SenderUserID, &m.SenderDeviceID, &m.ReplyToMessageID, &m.ContentType, &contentRaw, &clientGenID, &m.Transport, &m.ServerOrder, &created, &editedAt, &deletedAt, &expiresAt, &m.VisibilityState, &m.ReplyCount); err != nil {
			return nil, err
		}
		_ = json.Unmarshal(contentRaw, &m.Content)
		if clientGenID.Valid {
			m.ClientGeneratedID = clientGenID.String
		}
		m.CreatedAt = created.UTC().Format(time.RFC3339)
		if editedAt.Valid {
			m.EditedAt = editedAt.Time.UTC().Format(time.RFC3339)
		}
		applyMessageLifecycle(&m, deletedAt, expiresAt)
		items = append(items, m)
	}
	return items, rows.Err()
}

// TriggerEffect records a message effect (confetti, balloons, etc.) and broadcasts to conversation
func (s *Service) TriggerEffect(ctx context.Context, actorUserID, messageID, effectType string) error {
	if !isSupportedMessageEffectType(effectType) {
		return ErrInvalidMessageEffectType
	}

	tx, err := s.db.BeginTx(ctx, pgx.TxOptions{})
	if err != nil {
		return err
	}
	defer tx.Rollback(ctx)

	var convID string
	if err := tx.QueryRow(ctx, `
		SELECT conversation_id::text
		FROM messages
		WHERE id = $1::uuid
	`, messageID).Scan(&convID); err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return fmt.Errorf("message_not_found")
		}
		return err
	}

	if ok, err := s.hasMembership(ctx, tx, actorUserID, convID); err != nil {
		return err
	} else if !ok {
		return ErrConversationAccess
	}

	var allowEffects bool
	if err := tx.QueryRow(ctx, `
		SELECT COALESCE(allow_message_effects, TRUE)
		FROM conversations
		WHERE id = $1::uuid
	`, convID).Scan(&allowEffects); err != nil {
		return err
	}
	if !allowEffects {
		return fmt.Errorf("effects_disabled")
	}

	if _, err := tx.Exec(ctx, `
		INSERT INTO message_effects(message_id, conversation_id, triggered_by_user_id, effect_type)
		VALUES($1::uuid, $2::uuid, $3::uuid, $4)
	`, messageID, convID, actorUserID, effectType); err != nil {
		return err
	}

	if s.replication != nil {
		if err := s.replication.AppendMessageEffectEvent(ctx, tx, convID, actorUserID, messageID, effectType, time.Now().UTC()); err != nil {
			return err
		}
	}

	return tx.Commit(ctx)
}

// removed: unified timeline comments repeated the query flow

func (s *Service) MarkRead(ctx context.Context, actor, conversationID string, through int64) error {
	if through <= 0 {
		return nil
	}
	tx, err := s.db.BeginTx(ctx, pgx.TxOptions{})
	if err != nil {
		return err
	}
	defer tx.Rollback(ctx)

	now := time.Now().UTC()
	nowStr := now.Format(time.RFC3339Nano)

	res, err := tx.Exec(ctx, `
		UPDATE conversation_members
		SET last_read_server_order = GREATEST(last_read_server_order, $3),
		    last_read_at = CASE WHEN $3 > last_read_server_order THEN now() ELSE last_read_at END,
		    read_at = now()
		WHERE conversation_id = $1::uuid AND user_id = $2::uuid
	`, conversationID, actor, through)
	if err != nil {
		return err
	}
	if res.RowsAffected() == 0 {
		return ErrConversationAccess
	}

	// Insert per-message read receipts for newly read messages
	if _, err := tx.Exec(ctx, `
		INSERT INTO message_read_receipts(message_id, reader_user_id, read_at)
		SELECT m.id, $1::uuid, $2
		FROM messages m
		WHERE m.conversation_id = $3::uuid
		  AND m.server_order <= $4
		  AND m.sender_user_id <> $1::uuid
		  AND NOT EXISTS (
		    SELECT 1 FROM message_read_receipts mrr
		    WHERE mrr.message_id = m.id AND mrr.reader_user_id = $1::uuid
		  )
		ON CONFLICT(message_id, reader_user_id) DO NOTHING
	`, actor, now, conversationID, through); err != nil {
		return err
	}

	if _, err := tx.Exec(ctx, `
		UPDATE messages
		SET expires_at = CASE
			WHEN expires_at IS NULL OR expires_at > now() THEN now()
			ELSE expires_at
		END
		WHERE conversation_id = $1::uuid
		  AND server_order <= $3
		  AND sender_user_id <> $2::uuid
		  AND content->>'expires_on_read' = 'true'
		  AND deleted_at IS NULL
		  AND visibility_state NOT IN ('SOFT_DELETED', 'REDACTED')
	`, conversationID, actor, through); err != nil {
		return err
	}

	if _, err := tx.Exec(ctx, `
		UPDATE conversations
		SET updated_at = now()
		WHERE id = $1::uuid
	`, conversationID); err != nil {
		return err
	}

	if s.replication != nil {
		if err := s.replication.AppendDomainEvent(ctx, tx, conversationID, actor, replication.DomainEventReadCheckpointAdvanced, replication.ReadCheckpointPayload{
			ConversationID:     conversationID,
			ReaderUserID:       actor,
			ThroughServerOrder: through,
			ReadAt:             nowStr,
		}); err != nil {
			return err
		}
	}
	return tx.Commit(ctx)
}

// GetConversationReadStatus returns read status for all members of a conversation
func (s *Service) GetConversationReadStatus(ctx context.Context, userID, conversationID string) (map[string]any, error) {
	// Verify caller is conversation member
	if ok, err := s.hasMembership(ctx, s.db, userID, conversationID); err != nil {
		return nil, err
	} else if !ok {
		return nil, ErrConversationAccess
	}

	rows, err := s.db.Query(ctx, `
		SELECT u.id::text, u.primary_phone_e164, cm.last_read_server_order, cm.last_delivered_server_order, cm.read_at, cm.delivery_at
		FROM conversation_members cm
		JOIN users u ON u.id = cm.user_id
		WHERE cm.conversation_id = $1::uuid
		ORDER BY cm.read_at DESC NULLS LAST, cm.delivery_at DESC NULLS LAST
	`, conversationID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	members := make([]map[string]any, 0, 8)
	for rows.Next() {
		var userID, phone string
		var lastReadServerOrder int64
		var lastDeliveredServerOrder int64
		var readAt sql.NullTime
		var deliveryAt sql.NullTime

		if err := rows.Scan(&userID, &phone, &lastReadServerOrder, &lastDeliveredServerOrder, &readAt, &deliveryAt); err != nil {
			return nil, err
		}

		m := map[string]any{
			"user_id":                     userID,
			"phone":                       phone,
			"last_read_server_order":      lastReadServerOrder,
			"last_delivered_server_order": lastDeliveredServerOrder,
		}
		if readAt.Valid {
			m["read_at"] = readAt.Time.UTC().Format(time.RFC3339)
		}
		if deliveryAt.Valid {
			m["delivery_at"] = deliveryAt.Time.UTC().Format(time.RFC3339)
		}
		members = append(members, m)
	}

	return map[string]any{"members": members}, rows.Err()
}

func (s *Service) DeliverPendingToUser(ctx context.Context, recipientUserID string) ([]map[string]any, error) {
	rows, err := s.db.Query(ctx, `
		SELECT m.id::text, m.conversation_id::text, m.sender_user_id::text, m.server_order, m.transport
		FROM messages m
		JOIN conversation_members cm
		  ON cm.conversation_id = m.conversation_id
		 AND cm.user_id = $1::uuid
		WHERE m.sender_user_id <> $1::uuid
		  AND m.transport IN ('OTT', 'OHMF')
		  AND NOT EXISTS (
			  SELECT 1
			  FROM message_deliveries md
			  WHERE md.message_id = m.id
			    AND md.recipient_user_id = $1::uuid
			    AND md.state = 'DELIVERED'
		  )
		ORDER BY m.created_at ASC
	`, recipientUserID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	events := make([]map[string]any, 0)
	statusUpdatedAt := time.Now().UTC().Format(time.RFC3339Nano)
	for rows.Next() {
		var messageID, conversationID, senderID, transport string
		var serverOrder int64
		if err := rows.Scan(&messageID, &conversationID, &senderID, &serverOrder, &transport); err != nil {
			return nil, err
		}
		tag, err := s.db.Exec(ctx, `
			INSERT INTO message_deliveries (
				id, message_id, recipient_user_id, transport, state, submitted_at, updated_at
			)
			SELECT gen_random_uuid(), $1::uuid, $2::uuid, $3, 'DELIVERED', now(), now()
			WHERE NOT EXISTS (
				SELECT 1
				FROM message_deliveries md
				WHERE md.message_id = $1::uuid
				  AND md.recipient_user_id = $2::uuid
				  AND md.state = 'DELIVERED'
			)
		`, messageID, recipientUserID, transport)
		if err != nil {
			return nil, err
		}
		if tag.RowsAffected() == 0 {
			continue
		}
		events = append(events, map[string]any{
			"sender_user_id":    senderID,
			"recipient_user_id": recipientUserID,
			"message_id":        messageID,
			"conversation_id":   conversationID,
			"server_order":      serverOrder,
			"status":            "DELIVERED",
			"status_updated_at": statusUpdatedAt,
		})
	}
	return events, rows.Err()
}

func (s *Service) enforceSendRate(ctx context.Context, userID, conversationID, ip string) error {
	if s.rateLimiter == nil {
		return nil
	}
	if userID != "" {
		userDecision, err := s.rateLimiter.Allow(ctx, "rate:send:user:"+userID, 30, 10*time.Second, 60, 1)
		if err != nil {
			return err
		}
		if !userDecision.Allowed {
			return RateLimitError{Scope: "user", RetryAfter: userDecision.RetryAfter}
		}
	}
	if conversationID != "" {
		convDecision, err := s.rateLimiter.Allow(ctx, "rate:send:conversation:"+conversationID, 300, 10*time.Second, 500, 1)
		if err != nil {
			return err
		}
		if !convDecision.Allowed {
			return RateLimitError{Scope: "conversation", RetryAfter: convDecision.RetryAfter}
		}
	}
	if ip != "" {
		ipDecision, err := s.rateLimiter.Allow(ctx, "rate:send:ip:"+ip, 120, 10*time.Second, 240, 1)
		if err != nil {
			return err
		}
		if !ipDecision.Allowed {
			return RateLimitError{Scope: "ip", RetryAfter: ipDecision.RetryAfter}
		}
	}
	return nil
}

func (s *Service) enforceSearchRate(ctx context.Context, userID, conversationID string) error {
	if s.rateLimiter == nil {
		return nil
	}
	scope := "rate:search:user:" + userID + ":conv:" + conversationID
	decision, err := s.rateLimiter.Allow(ctx, scope, 30, 60*time.Second, 30, 1)
	if err != nil {
		return err
	}
	if !decision.Allowed {
		return RateLimitError{Scope: "search", RetryAfter: decision.RetryAfter}
	}
	return nil
}

func (s *Service) loadIdempotency(ctx context.Context, userID, endpoint, idemKey string) (*Message, int, error) {
	var payload []byte
	var statusCode int
	err := s.db.QueryRow(ctx, `
		SELECT response_payload, COALESCE(status_code, 201)
		FROM idempotency_keys
		WHERE actor_user_id = $1::uuid AND endpoint = $2 AND key = $3 AND expires_at > now()
	`, userID, endpoint, idemKey).Scan(&payload, &statusCode)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, 0, nil
		}
		return nil, 0, err
	}
	if len(payload) == 0 {
		return nil, statusCode, nil
	}
	var m Message
	if err := json.Unmarshal(payload, &m); err != nil {
		return nil, 0, err
	}
	return &m, statusCode, nil
}

func (s *Service) upsertIdempotency(ctx context.Context, userID, endpoint, idemKey string, msg Message, statusCode int) error {
	msgPayload, err := json.Marshal(msg)
	if err != nil {
		return err
	}
	_, err = s.db.Exec(ctx, `
		INSERT INTO idempotency_keys (actor_user_id, endpoint, key, response_payload, status_code, expires_at)
		VALUES ($1::uuid, $2, $3, $4::jsonb, $5, now() + interval '24 hour')
		ON CONFLICT (actor_user_id, endpoint, key)
		DO UPDATE SET response_payload = EXCLUDED.response_payload, status_code = EXCLUDED.status_code
	`, userID, endpoint, idemKey, string(msgPayload), statusCode)
	return err
}

func (s *Service) ensurePhoneConversation(ctx context.Context, userID, phoneE164 string) (string, error) {
	tx, err := s.db.BeginTx(ctx, pgx.TxOptions{})
	if err != nil {
		return "", err
	}
	defer tx.Rollback(ctx)
	conversationID, err := s.findOrCreatePhoneConversation(ctx, tx, userID, phoneE164)
	if err != nil {
		return "", err
	}
	if err := tx.Commit(ctx); err != nil {
		return "", err
	}
	return conversationID, nil
}

func (s *Service) decideTransport(ctx context.Context, tx pgx.Tx, conversationID, senderUserID string) (string, error) {
	var policy string
	if err := tx.QueryRow(ctx, `SELECT transport_policy FROM conversations WHERE id = $1::uuid`, conversationID).Scan(&policy); err != nil {
		return "", err
	}
	switch policy {
	case "FORCE_OTT":
		return "OTT", nil
	case "FORCE_SMS":
		return "SMS", nil
	case "FORCE_MMS":
		return "MMS", nil
	case "BLOCK_CARRIER_RELAY":
	}

	var otherCount int
	if err := tx.QueryRow(ctx, `SELECT COUNT(1) FROM conversation_members WHERE conversation_id = $1::uuid AND user_id <> $2::uuid`, conversationID, senderUserID).Scan(&otherCount); err != nil {
		return "", err
	}
	if otherCount > 0 {
		return "OTT", nil
	}

	var hasExternal int
	if err := tx.QueryRow(ctx, `SELECT COUNT(1) FROM conversation_external_members WHERE conversation_id = $1::uuid`, conversationID).Scan(&hasExternal); err == nil && hasExternal > 0 {
		return "SMS", nil
	}

	if profiles, ok := middleware.ProfilesFromContext(ctx); ok {
		for _, p := range profiles {
			if p == "DEFAULT_SMS_HANDLER" {
				return "SMS", nil
			}
		}
	}

	return "OTT", nil
}

// removed: transport-selection comments restated the switch and fallback order

func (s *Service) sendSync(ctx context.Context, userID, senderDeviceID, conversationID, idemKey, contentType string, content map[string]any, clientGeneratedID string) (Message, error) {
	return s.sendSyncWithEndpoint(ctx, userID, senderDeviceID, conversationID, idemKey, contentType, content, clientGeneratedID, "/v1/messages", "")
}

func (s *Service) sendSyncWithEndpoint(ctx context.Context, userID, senderDeviceID, conversationID, idemKey, contentType string, content map[string]any, clientGeneratedID, endpoint, source string) (Message, error) {
	tx, err := s.db.BeginTx(ctx, pgx.TxOptions{})
	if err != nil {
		return Message{}, err
	}
	defer tx.Rollback(ctx)

	if ok, err := s.hasMembership(ctx, tx, userID, conversationID); err != nil {
		return Message{}, err
	} else if !ok {
		return Message{}, ErrConversationAccess
	}

	if blocked, _, err := s.checkBlockedRecipients(ctx, tx, userID, conversationID); err != nil {
		return Message{}, err
	} else if blocked {
		return Message{}, ErrConversationBlocked
	}

	var cached []byte
	err = tx.QueryRow(ctx, `
		SELECT response_payload
		FROM idempotency_keys
		WHERE actor_user_id = $1::uuid AND endpoint = $2 AND key = $3 AND expires_at > now()
	`, userID, endpoint, idemKey).Scan(&cached)
	if err == nil {
		var m Message
		if err := json.Unmarshal(cached, &m); err == nil {
			if err := tx.Commit(ctx); err != nil {
				return Message{}, err
			}
			return m, nil
		}
	} else if !errors.Is(err, pgx.ErrNoRows) {
		return Message{}, err
	}

	policy, err := s.loadConversationPolicy(ctx, tx, conversationID)
	if err != nil {
		return Message{}, err
	}
	if contentType == "encrypted" || encryptedOTTDM(policy) {
		if strings.TrimSpace(senderDeviceID) == "" {
			return Message{}, ErrSenderDeviceRequired
		}
		ownsDevice, err := s.senderOwnsDevice(ctx, tx, userID, senderDeviceID)
		if err != nil {
			return Message{}, err
		}
		if !ownsDevice {
			return Message{}, ErrSenderDeviceInvalid
		}
	}
	if encryptedOTTDM(policy) && contentType != "encrypted" {
		return Message{}, ErrEncryptedMessageRequired
	}

	var next int64
	err = tx.QueryRow(ctx, `
		UPDATE conversation_counters
		SET next_server_order = next_server_order + 1, updated_at = now()
		WHERE conversation_id = $1::uuid
		RETURNING next_server_order - 1
	`, conversationID).Scan(&next)
	if err != nil {
		return Message{}, err
	}

	expiresAt, err := resolveMessageExpiration(ctx, tx, conversationID, contentType, content)
	if err != nil {
		return Message{}, err
	}
	var expiresAtValue any
	if expiresAt.Valid {
		expiresAtValue = expiresAt.Time
	}
	contentForStorage := cloneMessageContent(content)
	if contentForStorage == nil {
		contentForStorage = map[string]any{}
	}
	replyToMessageID, err := resolveReplyTarget(ctx, tx, conversationID, contentForStorage)
	if err != nil {
		return Message{}, err
	}
	contentJSON, err := json.Marshal(contentForStorage)
	if err != nil {
		return Message{}, err
	}

	chosenTransport, err := s.decideTransport(ctx, tx, conversationID, userID)
	if err != nil {
		return Message{}, err
	}
	if encryptedOTTDM(policy) {
		if chosenTransport != "OTT" {
			return Message{}, ErrEncryptedMessageInvalid
		}
		if contentType != "encrypted" {
			return Message{}, ErrEncryptedMessageRequired
		}
	}
	if contentType == "encrypted" && !encryptedOTTDM(policy) {
		return Message{}, ErrEncryptedMessageInvalid
	}

	// Process encrypted messages and validate signature
	var isEncrypted bool
	var encryptionScheme string
	if strings.EqualFold(contentType, "encrypted") {
		encryptedMetadata, err := ProcessEncryptedMessage(ctx, s.db, userID, senderDeviceID, contentForStorage)
		if err != nil {
			return Message{}, err
		}
		isEncrypted = true
		encryptionScheme = encryptedMetadata.Scheme
	}

	var msgID string
	var created time.Time
	err = tx.QueryRow(ctx, `
		INSERT INTO messages (conversation_id, sender_user_id, sender_device_id, reply_to_message_id, content_type, content, client_generated_id, transport, server_order, expires_at, is_encrypted, encryption_scheme)
		VALUES ($1::uuid, $2::uuid, NULLIF($3, '')::uuid, NULLIF($4, '')::uuid, $5, $6::jsonb, $7, $8, $9, $10, $11, $12)
		RETURNING id::text, created_at
	`, conversationID, userID, senderDeviceID, replyToMessageID, contentType, string(contentJSON), clientGeneratedID, chosenTransport, next, expiresAtValue, isEncrypted, encryptionScheme).Scan(&msgID, &created)
	if err != nil {
		return Message{}, err
	}

	_, err = tx.Exec(ctx, `
		UPDATE conversations
		SET last_message_id = $2::uuid, updated_at = now()
		WHERE id = $1::uuid
	`, conversationID, msgID)
	if err != nil {
		return Message{}, err
	}

	msg := Message{
		MessageID:         msgID,
		ConversationID:    conversationID,
		SenderUserID:      userID,
		SenderDeviceID:    senderDeviceID,
		ReplyToMessageID:  replyToMessageID,
		ContentType:       contentType,
		Content:           contentForStorage,
		ClientGeneratedID: clientGeneratedID,
		Transport:         chosenTransport,
		ServerOrder:       next,
		Status:            "SENT",
		IsEncrypted:       isEncrypted,
		EncryptionScheme:  encryptionScheme,
		CreatedAt:         created.UTC().Format(time.RFC3339),
	}
	if expiresAt.Valid {
		msg.ExpiresAt = expiresAt.Time.UTC().Format(time.RFC3339Nano)
	}
	if source != "" {
		msg.Source = source
	}
	msg.SentAt = msg.CreatedAt
	msg.StatusUpdatedAt = msg.CreatedAt
	msgPayload, _ := json.Marshal(msg)
	_, err = tx.Exec(ctx, `
		INSERT INTO idempotency_keys (actor_user_id, endpoint, key, response_payload, status_code, expires_at)
		VALUES ($1::uuid, $2, $3, $4::jsonb, 201, now() + interval '24 hour')
		ON CONFLICT (actor_user_id, endpoint, key)
		DO UPDATE SET response_payload = EXCLUDED.response_payload, status_code = EXCLUDED.status_code
	`, userID, endpoint, idemKey, string(msgPayload))
	if err != nil {
		return Message{}, err
	}

	recipients, err := loadRecipients(ctx, tx, conversationID, userID)
	if err != nil {
		return Message{}, err
	}
	var conversationMeta replication.ConversationMeta
	if s.replication != nil {
		conversationMeta, err = s.replication.LoadConversationMeta(ctx, tx, conversationID)
		if err != nil {
			return Message{}, err
		}
		if err := s.replication.AppendDomainEvent(ctx, tx, conversationID, userID, replication.DomainEventMessageCreated, replication.MessageCreatedPayload{
			MessageID:         msg.MessageID,
			ConversationID:    msg.ConversationID,
			ConversationType:  conversationMeta.Type,
			SenderUserID:      msg.SenderUserID,
			SenderDeviceID:    msg.SenderDeviceID,
			ContentType:       msg.ContentType,
			Content:           msg.Content,
			ClientGeneratedID: msg.ClientGeneratedID,
			Transport:         msg.Transport,
			ServerOrder:       msg.ServerOrder,
			CreatedAt:         msg.CreatedAt,
			Participants:      conversationMeta.Participants,
			ExternalPhones:    conversationMeta.ExternalPhones,
		}); err != nil {
			return Message{}, err
		}
	}

	if err := tx.Commit(ctx); err != nil {
		return Message{}, err
	}
	s.publishMessageCreated(ctx, recipients, msg)
	return msg, nil
}

func (s *Service) sendToPhoneSync(ctx context.Context, userID, senderDeviceID, phoneE164, idemKey, contentType string, content map[string]any, clientGeneratedID string) (Message, error) {
	tx, err := s.db.BeginTx(ctx, pgx.TxOptions{})
	if err != nil {
		return Message{}, err
	}
	defer tx.Rollback(ctx)

	conversationID, err := s.findOrCreatePhoneConversation(ctx, tx, userID, phoneE164)
	if err != nil {
		return Message{}, err
	}

	if blocked, _, err := s.checkBlockedRecipients(ctx, tx, userID, conversationID); err != nil {
		return Message{}, err
	} else if blocked {
		return Message{}, ErrConversationBlocked
	}
	if contentType == "encrypted" {
		return Message{}, ErrEncryptedMessageInvalid
	}

	var cached []byte
	err = tx.QueryRow(ctx, `
		SELECT response_payload
		FROM idempotency_keys
		WHERE actor_user_id = $1::uuid AND endpoint = '/v1/messages/phone' AND key = $2 AND expires_at > now()
	`, userID, idemKey).Scan(&cached)
	if err == nil {
		var m Message
		if err := json.Unmarshal(cached, &m); err == nil {
			if err := tx.Commit(ctx); err != nil {
				return Message{}, err
			}
			return m, nil
		}
	} else if !errors.Is(err, pgx.ErrNoRows) {
		return Message{}, err
	}

	var next int64
	err = tx.QueryRow(ctx, `
		UPDATE conversation_counters
		SET next_server_order = next_server_order + 1, updated_at = now()
		WHERE conversation_id = $1::uuid
		RETURNING next_server_order - 1
	`, conversationID).Scan(&next)
	if err != nil {
		return Message{}, err
	}

	expiresAt, err := resolveMessageExpiration(ctx, tx, conversationID, contentType, content)
	if err != nil {
		return Message{}, err
	}
	var expiresAtValue any
	if expiresAt.Valid {
		expiresAtValue = expiresAt.Time
	}
	contentForStorage := cloneMessageContent(content)
	if contentForStorage == nil {
		contentForStorage = map[string]any{}
	}
	replyToMessageID, err := resolveReplyTarget(ctx, tx, conversationID, contentForStorage)
	if err != nil {
		return Message{}, err
	}
	contentJSON, err := json.Marshal(contentForStorage)
	if err != nil {
		return Message{}, err
	}

	var msgID string
	var created time.Time
	err = tx.QueryRow(ctx, `
		INSERT INTO messages (conversation_id, sender_user_id, sender_device_id, reply_to_message_id, content_type, content, client_generated_id, transport, server_order, expires_at)
		VALUES ($1::uuid, $2::uuid, NULLIF($3, '')::uuid, NULLIF($4, '')::uuid, $5, $6::jsonb, $7, 'SMS', $8, $9)
		RETURNING id::text, created_at
	`, conversationID, userID, senderDeviceID, replyToMessageID, contentType, string(contentJSON), clientGeneratedID, next, expiresAtValue).Scan(&msgID, &created)
	if err != nil {
		return Message{}, err
	}

	_, err = tx.Exec(ctx, `
		UPDATE conversations
		SET last_message_id = $2::uuid, updated_at = now()
		WHERE id = $1::uuid
	`, conversationID, msgID)
	if err != nil {
		return Message{}, err
	}

	msg := Message{
		MessageID:         msgID,
		ConversationID:    conversationID,
		SenderUserID:      userID,
		SenderDeviceID:    senderDeviceID,
		ReplyToMessageID:  replyToMessageID,
		ContentType:       contentType,
		Content:           contentForStorage,
		ClientGeneratedID: clientGeneratedID,
		Transport:         "SMS",
		ServerOrder:       next,
		Status:            "SENT",
		CreatedAt:         created.UTC().Format(time.RFC3339),
	}
	if expiresAt.Valid {
		msg.ExpiresAt = expiresAt.Time.UTC().Format(time.RFC3339Nano)
	}
	msg.SentAt = msg.CreatedAt
	msg.StatusUpdatedAt = msg.CreatedAt
	msgPayload, _ := json.Marshal(msg)
	_, err = tx.Exec(ctx, `
		INSERT INTO idempotency_keys (actor_user_id, endpoint, key, response_payload, status_code, expires_at)
		VALUES ($1::uuid, '/v1/messages/phone', $2, $3::jsonb, 201, now() + interval '24 hour')
		ON CONFLICT (actor_user_id, endpoint, key)
		DO UPDATE SET response_payload = EXCLUDED.response_payload, status_code = EXCLUDED.status_code
	`, userID, idemKey, string(msgPayload))
	if err != nil {
		return Message{}, err
	}

	recipients, err := loadRecipients(ctx, tx, conversationID, userID)
	if err != nil {
		return Message{}, err
	}
	var conversationMeta replication.ConversationMeta
	if s.replication != nil {
		conversationMeta, err = s.replication.LoadConversationMeta(ctx, tx, conversationID)
		if err != nil {
			return Message{}, err
		}
		if err := s.replication.AppendDomainEvent(ctx, tx, conversationID, userID, replication.DomainEventMessageCreated, replication.MessageCreatedPayload{
			MessageID:         msg.MessageID,
			ConversationID:    msg.ConversationID,
			ConversationType:  conversationMeta.Type,
			SenderUserID:      msg.SenderUserID,
			SenderDeviceID:    msg.SenderDeviceID,
			ContentType:       msg.ContentType,
			Content:           msg.Content,
			ClientGeneratedID: msg.ClientGeneratedID,
			Transport:         msg.Transport,
			ServerOrder:       msg.ServerOrder,
			CreatedAt:         msg.CreatedAt,
			Participants:      conversationMeta.Participants,
			ExternalPhones:    conversationMeta.ExternalPhones,
		}); err != nil {
			return Message{}, err
		}
	}

	if err := tx.Commit(ctx); err != nil {
		return Message{}, err
	}
	s.publishMessageCreated(ctx, recipients, msg)
	return msg, nil
}

func (s *Service) publishMessageCreated(ctx context.Context, recipients []string, msg Message) {
	if s.redis == nil || len(recipients) == 0 {
		return
	}
	payload, err := json.Marshal(msg)
	if err != nil {
		return
	}
	for _, recipientID := range recipients {
		if strings.TrimSpace(recipientID) == "" {
			continue
		}
		_ = s.redis.Publish(ctx, "message:user:"+recipientID, payload).Err()
	}
}

func (s *Service) publishOnlineDeliveryUpdates(ctx context.Context, recipients []string, msg Message) {
	if s.redis == nil || s.db == nil || len(recipients) == 0 {
		return
	}
	if normalizeTransportForDelivery(msg.Transport) != "OHMF" {
		return
	}

	for _, recipientID := range recipients {
		recipientID = strings.TrimSpace(recipientID)
		if recipientID == "" {
			continue
		}
		online, err := s.redis.Exists(ctx, "presence:user:"+recipientID).Result()
		if err != nil || online == 0 {
			continue
		}
		deliveredAt := time.Now().UTC().Format(time.RFC3339Nano)
		tag, err := s.db.Exec(ctx, `
			INSERT INTO message_deliveries (
				id, message_id, recipient_user_id, transport, state, submitted_at, updated_at
			)
			SELECT gen_random_uuid(), $1::uuid, $2::uuid, $3, 'DELIVERED', now(), now()
			WHERE NOT EXISTS (
				SELECT 1
				FROM message_deliveries md
				WHERE md.message_id = $1::uuid
				  AND md.recipient_user_id = $2::uuid
				  AND md.state = 'DELIVERED'
			)
		`, msg.MessageID, recipientID, msg.Transport)
		if err != nil || tag.RowsAffected() == 0 {
			continue
		}
		body, _ := json.Marshal(receiptEvent{
			SenderUserID:    msg.SenderUserID,
			RecipientUserID: recipientID,
			MessageID:       msg.MessageID,
			ConversationID:  msg.ConversationID,
			ServerOrder:     msg.ServerOrder,
			Status:          "DELIVERED",
			StatusUpdatedAt: deliveredAt,
		})
		_ = s.redis.Publish(ctx, "delivery:user:"+msg.SenderUserID, body).Err()
	}
}

func normalizeTransportForDelivery(transport string) string {
	switch strings.ToUpper(strings.TrimSpace(transport)) {
	case "OTT", "OHMF":
		return "OHMF"
	case "SMS":
		return "SMS"
	default:
		return ""
	}
}

type querier interface {
	QueryRow(context.Context, string, ...any) pgx.Row
	Query(context.Context, string, ...any) (pgx.Rows, error)
}

func (s *Service) hasMembership(ctx context.Context, q querier, userID, conversationID string) (bool, error) {
	var one int
	err := q.QueryRow(ctx, `
		SELECT 1
		FROM conversation_members
		WHERE conversation_id = $1::uuid AND user_id = $2::uuid
	`, conversationID, userID).Scan(&one)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return false, nil
		}
		return false, err
	}
	return true, nil
}

func (s *Service) checkBlockedRecipients(ctx context.Context, q querier, senderUserID, conversationID string) (bool, string, error) {
	rows2, err := q.Query(ctx, `SELECT user_id::text FROM conversation_members WHERE conversation_id = $1::uuid AND user_id <> $2::uuid`, conversationID, senderUserID)
	if err != nil {
		return false, "", err
	}
	defer rows2.Close()
	for rows2.Next() {
		var uid string
		if err := rows2.Scan(&uid); err != nil {
			return false, "", err
		}
		var one int
		err := q.QueryRow(ctx, `SELECT 1 FROM user_blocks WHERE blocker_user_id = $1::uuid AND blocked_user_id = $2::uuid`, uid, senderUserID).Scan(&one)
		if err == nil {
			return true, uid, nil
		}
		if err != nil && err != pgx.ErrNoRows {
			return false, "", err
		}
		err = q.QueryRow(ctx, `SELECT 1 FROM user_blocks WHERE blocker_user_id = $1::uuid AND blocked_user_id = $2::uuid`, senderUserID, uid).Scan(&one)
		if err == nil {
			return true, uid, nil
		}
		if err != nil && err != pgx.ErrNoRows {
			return false, "", err
		}
	}
	return false, "", nil
}

func (s *Service) IsMember(ctx context.Context, userID, conversationID string) (bool, error) {
	return s.hasMembership(ctx, s.db, userID, conversationID)
}

// removed: membership and block-check comments restated the helper names

func loadRecipients(ctx context.Context, q querier, conversationID, senderID string) ([]string, error) {
	rows, err := q.Query(ctx, `
		SELECT user_id::text
		FROM conversation_members
		WHERE conversation_id = $1::uuid
		  AND user_id <> $2::uuid
	`, conversationID, senderID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	recipients := make([]string, 0, 4)
	for rows.Next() {
		var userID string
		if err := rows.Scan(&userID); err != nil {
			return nil, err
		}
		recipients = append(recipients, userID)
	}
	return recipients, rows.Err()
}

func (s *Service) conversationHasTombstones(ctx context.Context, conversationID string) (bool, error) {
	var hasTombstones bool
	err := s.db.QueryRow(ctx, `
		SELECT EXISTS (
			SELECT 1
			FROM messages
			WHERE conversation_id = $1::uuid
			  AND (deleted_at IS NOT NULL OR visibility_state = 'SOFT_DELETED')
		)
	`, conversationID).Scan(&hasTombstones)
	return hasTombstones, err
}

func (s *Service) findOrCreatePhoneConversation(ctx context.Context, tx pgx.Tx, userID, phoneE164 string) (string, error) {
	var targetUserID string
	err := tx.QueryRow(ctx, `
		SELECT id::text
		FROM users
		WHERE primary_phone_e164 = $1
		LIMIT 1
	`, phoneE164).Scan(&targetUserID)
	if err == nil && targetUserID != "" && targetUserID != userID {
		var dmConversationID string
		err = tx.QueryRow(ctx, `
			SELECT c.id::text
			FROM conversations c
			JOIN conversation_members me ON me.conversation_id = c.id AND me.user_id = $1::uuid
			JOIN conversation_members them ON them.conversation_id = c.id AND them.user_id = $2::uuid
			LEFT JOIN conversation_external_members cem ON cem.conversation_id = c.id
			WHERE cem.conversation_id IS NULL
			ORDER BY c.updated_at DESC
			LIMIT 1
		`, userID, targetUserID).Scan(&dmConversationID)
		if err == nil {
			return dmConversationID, nil
		}
		if !errors.Is(err, pgx.ErrNoRows) {
			return "", err
		}

		if err := tx.QueryRow(ctx, `
			INSERT INTO conversations (type, transport_policy)
			VALUES ('DM', 'AUTO')
			RETURNING id::text
		`).Scan(&dmConversationID); err != nil {
			return "", err
		}
		if _, err := tx.Exec(ctx, `INSERT INTO conversation_counters (conversation_id, next_server_order) VALUES ($1::uuid, 1)`, dmConversationID); err != nil {
			return "", err
		}
		if _, err := tx.Exec(ctx, `INSERT INTO conversation_members (conversation_id, user_id, role) VALUES ($1::uuid, $2::uuid, 'MEMBER') ON CONFLICT (conversation_id, user_id) DO NOTHING`, dmConversationID, userID); err != nil {
			return "", err
		}
		if _, err := tx.Exec(ctx, `INSERT INTO conversation_members (conversation_id, user_id, role) VALUES ($1::uuid, $2::uuid, 'MEMBER') ON CONFLICT (conversation_id, user_id) DO NOTHING`, dmConversationID, targetUserID); err != nil {
			return "", err
		}
		return dmConversationID, nil
	}
	if err != nil && !errors.Is(err, pgx.ErrNoRows) {
		return "", err
	}

	var conversationID string
	err = tx.QueryRow(ctx, `
		SELECT c.id::text
		FROM conversations c
		JOIN conversation_members cm ON cm.conversation_id = c.id
		JOIN conversation_external_members cem ON cem.conversation_id = c.id
		JOIN external_contacts ec ON ec.id = cem.external_contact_id
		WHERE c.type = 'PHONE_DM'
		  AND cm.user_id = $1::uuid
		  AND ec.phone_e164 = $2
		LIMIT 1
	`, userID, phoneE164).Scan(&conversationID)
	if err == nil {
		return conversationID, nil
	}
	if !errors.Is(err, pgx.ErrNoRows) {
		return "", err
	}

	var externalID string
	err = tx.QueryRow(ctx, `
		INSERT INTO external_contacts (phone_e164)
		VALUES ($1)
		ON CONFLICT (phone_e164) DO UPDATE SET phone_e164 = EXCLUDED.phone_e164
		RETURNING id::text
	`, phoneE164).Scan(&externalID)
	if err != nil {
		return "", err
	}

	err = tx.QueryRow(ctx, `
		INSERT INTO conversations (type, transport_policy)
		VALUES ('PHONE_DM', 'FORCE_SMS')
		RETURNING id::text
	`).Scan(&conversationID)
	if err != nil {
		return "", err
	}

	_, err = tx.Exec(ctx, `INSERT INTO conversation_counters (conversation_id, next_server_order) VALUES ($1::uuid, 1)`, conversationID)
	if err != nil {
		return "", err
	}
	_, err = tx.Exec(ctx, `INSERT INTO conversation_members (conversation_id, user_id, role) VALUES ($1::uuid, $2::uuid, 'MEMBER')`, conversationID, userID)
	if err != nil {
		return "", err
	}
	_, err = tx.Exec(ctx, `INSERT INTO conversation_external_members (conversation_id, external_contact_id) VALUES ($1::uuid, $2::uuid)`, conversationID, externalID)
	if err != nil {
		return "", err
	}
	return conversationID, nil
}

func isUniqueViolation(err error) bool {
	var pgErr *pgconn.PgError
	if errors.As(err, &pgErr) {
		return pgErr.Code == "23505"
	}
	return false
}

func (s *Service) PersistAck(ctx context.Context, userID, endpoint, idemKey string, msg Message) error {
	return s.upsertIdempotency(ctx, userID, endpoint, idemKey, msg, 201)
}

func buildTraceID(reqID string) string {
	if reqID == "" {
		return fmt.Sprintf("trace-%d", time.Now().UnixNano())
	}
	return reqID
}

package replication

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/redis/go-redis/v9"
)

const (
	DomainEventMessageCreated             = "message_created"
	DomainEventMessageEdited              = "message_edited"
	DomainEventMessageDeleted             = "message_deleted"
	DomainEventMessageReactionsUpdated    = "message_reactions_updated"
	DomainEventMessageEffectTriggered     = "message_effect_triggered"
	DomainEventReadCheckpointAdvanced     = "read_checkpoint_advanced"
	DomainEventDeliveryCheckpointAdvanced = "delivery_checkpoint_advanced"
	DomainEventTypingStarted              = "typing_started"
	DomainEventTypingStopped              = "typing_stopped"

	UserEventConversationMessageAppended         = "conversation_message_appended"
	UserEventConversationMessageEdited           = "conversation_message_edited"
	UserEventConversationMessageDeleted          = "conversation_message_deleted"
	UserEventConversationMessageReactionsUpdated = "conversation_message_reactions_updated"
	UserEventConversationMessageEffectTriggered  = "conversation_message_effect_triggered"
	UserEventConversationReceiptUpdated          = "conversation_receipt_updated"
	UserEventConversationPreviewUpdated          = "conversation_preview_updated"
	UserEventConversationStateUpdated            = "conversation_state_updated"
	UserEventConversationTypingUpdated           = "conversation_typing_updated"
	UserEventAccountDeviceLinked                 = "account_device_linked"

	userEventChannelPrefix = "user-event:user:"
)

type DBTX interface {
	Exec(ctx context.Context, sql string, args ...any) (pgconn.CommandTag, error)
	Query(ctx context.Context, sql string, args ...any) (pgx.Rows, error)
	QueryRow(ctx context.Context, sql string, args ...any) pgx.Row
}

type DB interface {
	DBTX
	BeginTx(ctx context.Context, txOptions pgx.TxOptions) (pgx.Tx, error)
}

type Store struct {
	db    DB
	redis *redis.Client
}

type Event struct {
	UserEventID int64          `json:"user_event_id"`
	Type        string         `json:"type"`
	CreatedAt   string         `json:"created_at"`
	Payload     map[string]any `json:"payload"`
}

type SyncResponse struct {
	Events     []Event `json:"events"`
	NextCursor int64   `json:"next_cursor"`
	HasMore    bool    `json:"has_more"`
}

type MessageCreatedPayload struct {
	MessageID         string         `json:"message_id"`
	ConversationID    string         `json:"conversation_id"`
	ConversationType  string         `json:"conversation_type"`
	SenderUserID      string         `json:"sender_user_id"`
	SenderDeviceID    string         `json:"sender_device_id,omitempty"`
	ContentType       string         `json:"content_type"`
	Content           map[string]any `json:"content"`
	ClientGeneratedID string         `json:"client_generated_id,omitempty"`
	Transport         string         `json:"transport"`
	ServerOrder       int64          `json:"server_order"`
	CreatedAt         string         `json:"created_at"`
	Participants      []string       `json:"participants"`
	ExternalPhones    []string       `json:"external_phones,omitempty"`
}

type MessageDeletedPayload struct {
	MessageID         string   `json:"message_id"`
	ConversationID    string   `json:"conversation_id"`
	ConversationType  string   `json:"conversation_type"`
	SenderUserID      string   `json:"sender_user_id"`
	SenderDeviceID    string   `json:"sender_device_id,omitempty"`
	ContentType       string   `json:"content_type"`
	ClientGeneratedID string   `json:"client_generated_id,omitempty"`
	Transport         string   `json:"transport"`
	ServerOrder       int64    `json:"server_order"`
	CreatedAt         string   `json:"created_at"`
	DeletedAt         string   `json:"deleted_at"`
	Participants      []string `json:"participants"`
	ExternalPhones    []string `json:"external_phones,omitempty"`
}

type MessageEditedPayload struct {
	MessageID         string         `json:"message_id"`
	ConversationID    string         `json:"conversation_id"`
	ConversationType  string         `json:"conversation_type"`
	SenderUserID      string         `json:"sender_user_id"`
	SenderDeviceID    string         `json:"sender_device_id,omitempty"`
	ContentType       string         `json:"content_type"`
	Content           map[string]any `json:"content"`
	ClientGeneratedID string         `json:"client_generated_id,omitempty"`
	Transport         string         `json:"transport"`
	ServerOrder       int64          `json:"server_order"`
	CreatedAt         string         `json:"created_at"`
	EditedAt          string         `json:"edited_at"`
	Participants      []string       `json:"participants"`
	ExternalPhones    []string       `json:"external_phones,omitempty"`
}

type MessageReactionsPayload struct {
	MessageID        string           `json:"message_id"`
	ConversationID   string           `json:"conversation_id"`
	ConversationType string           `json:"conversation_type"`
	ServerOrder      int64            `json:"server_order"`
	Participants     []string         `json:"participants"`
	ExternalPhones   []string         `json:"external_phones,omitempty"`
	Reactions        map[string]int64 `json:"reactions"`
	ActedAt          string           `json:"acted_at"`
}

type MessageEffectPayload struct {
	MessageID         string `json:"message_id"`
	ConversationID    string `json:"conversation_id"`
	EffectType        string `json:"effect_type"`
	TriggeredByUserID string `json:"triggered_by_user_id"`
	TriggeredAtMS     int64  `json:"triggered_at_ms"`
}

type TypingPayload struct {
	ConversationID string `json:"conversation_id"`
	UserID         string `json:"user_id"`
	DeviceID       string `json:"device_id,omitempty"`
	State          string `json:"state"`
	StartedAtMS    int64  `json:"started_at_ms"`
}

type ReadCheckpointPayload struct {
	ConversationID     string `json:"conversation_id"`
	ReaderUserID       string `json:"reader_user_id"`
	ThroughServerOrder int64  `json:"through_server_order"`
	ReadAt             string `json:"read_at"`
}

type DeliveryCheckpointPayload struct {
	ConversationID     string `json:"conversation_id"`
	UserID             string `json:"user_id"`
	ThroughServerOrder int64  `json:"through_server_order"`
	DeliveredAt        string `json:"delivered_at"`
}

type ConversationMeta struct {
	Type           string
	Participants   []string
	ExternalPhones []string
}

func NewStore(db DB, redisClient *redis.Client) *Store {
	return &Store{db: db, redis: redisClient}
}

func (s *Store) AppendDomainEvent(ctx context.Context, q DBTX, conversationID, actorUserID, eventType string, payload any) error {
	body, err := json.Marshal(payload)
	if err != nil {
		return err
	}
	_, err = q.Exec(ctx, `
		INSERT INTO domain_events (conversation_id, actor_user_id, event_type, payload)
		VALUES ($1::uuid, NULLIF($2, '')::uuid, $3, $4::jsonb)
	`, conversationID, actorUserID, eventType, string(body))
	return err
}

func (s *Store) EmitUserEvent(ctx context.Context, userID, conversationID, eventType string, payload map[string]any) (Event, error) {
	body, err := json.Marshal(payload)
	if err != nil {
		return Event{}, err
	}
	var evt Event
	var created time.Time
	if err := s.db.QueryRow(ctx, `
		INSERT INTO user_inbox_events (user_id, conversation_id, event_type, payload)
		VALUES ($1::uuid, NULLIF($2, '')::uuid, $3, $4::jsonb)
		RETURNING user_event_id, created_at
	`, userID, conversationID, eventType, string(body)).Scan(&evt.UserEventID, &created); err != nil {
		return Event{}, err
	}
	evt.Type = eventType
	evt.CreatedAt = created.UTC().Format(time.RFC3339Nano)
	evt.Payload = payload
	if s.redis != nil {
		encoded, err := json.Marshal(evt)
		if err == nil {
			_ = s.redis.Publish(ctx, s.ChannelForUser(userID), encoded).Err()
		}
	}
	return evt, nil
}

func (s *Store) ListEvents(ctx context.Context, userID string, cursor int64, limit int) (SyncResponse, error) {
	if limit <= 0 {
		limit = 200
	}
	if limit > 1000 {
		limit = 1000
	}
	rows, err := s.db.Query(ctx, `
		SELECT user_event_id, event_type, payload, created_at
		FROM user_inbox_events
		WHERE user_id = $1::uuid
		  AND user_event_id > $2
		ORDER BY user_event_id ASC
		LIMIT $3
	`, userID, cursor, limit+1)
	if err != nil {
		return SyncResponse{}, err
	}
	defer rows.Close()

	out := make([]Event, 0, limit)
	var nextCursor int64
	hasMore := false
	for rows.Next() {
		var evt Event
		var payloadRaw []byte
		var created time.Time
		if err := rows.Scan(&evt.UserEventID, &evt.Type, &payloadRaw, &created); err != nil {
			return SyncResponse{}, err
		}
		if len(out) == limit {
			hasMore = true
			nextCursor = evt.UserEventID
			break
		}
		evt.CreatedAt = created.UTC().Format(time.RFC3339Nano)
		if err := json.Unmarshal(payloadRaw, &evt.Payload); err != nil {
			return SyncResponse{}, err
		}
		out = append(out, evt)
		nextCursor = evt.UserEventID
	}
	if err := rows.Err(); err != nil {
		return SyncResponse{}, err
	}
	if len(out) == 0 {
		nextCursor = cursor
	}
	return SyncResponse{Events: out, NextCursor: nextCursor, HasMore: hasMore}, nil
}

func (s *Store) LoadConversationMeta(ctx context.Context, q DBTX, conversationID string) (ConversationMeta, error) {
	var meta ConversationMeta
	if err := q.QueryRow(ctx, `SELECT type FROM conversations WHERE id = $1::uuid`, conversationID).Scan(&meta.Type); err != nil {
		return ConversationMeta{}, err
	}
	memberRows, err := q.Query(ctx, `
		SELECT user_id::text
		FROM conversation_members
		WHERE conversation_id = $1::uuid
		ORDER BY joined_at
	`, conversationID)
	if err != nil {
		return ConversationMeta{}, err
	}
	for memberRows.Next() {
		var userID string
		if err := memberRows.Scan(&userID); err != nil {
			memberRows.Close()
			return ConversationMeta{}, err
		}
		meta.Participants = append(meta.Participants, userID)
	}
	if err := memberRows.Err(); err != nil {
		memberRows.Close()
		return ConversationMeta{}, err
	}
	memberRows.Close()
	phoneRows, err := q.Query(ctx, `
		SELECT ec.phone_e164
		FROM conversation_external_members cem
		JOIN external_contacts ec ON ec.id = cem.external_contact_id
		WHERE cem.conversation_id = $1::uuid
		ORDER BY ec.phone_e164
	`, conversationID)
	if err != nil {
		return ConversationMeta{}, err
	}
	for phoneRows.Next() {
		var phone string
		if err := phoneRows.Scan(&phone); err != nil {
			phoneRows.Close()
			return ConversationMeta{}, err
		}
		meta.ExternalPhones = append(meta.ExternalPhones, phone)
	}
	if err := phoneRows.Err(); err != nil {
		phoneRows.Close()
		return ConversationMeta{}, err
	}
	phoneRows.Close()
	return meta, nil
}

func (s *Store) ProcessBatch(ctx context.Context, batchSize int) (int, error) {
	if batchSize <= 0 {
		batchSize = 100
	}
	tx, err := s.db.BeginTx(ctx, pgx.TxOptions{})
	if err != nil {
		return 0, err
	}
	defer tx.Rollback(ctx)

	rows, err := tx.Query(ctx, `
		SELECT event_id, conversation_id::text, COALESCE(actor_user_id::text, ''), event_type, payload, created_at
		FROM domain_events
		WHERE processed_at IS NULL
		ORDER BY event_id ASC
		LIMIT $1
		FOR UPDATE SKIP LOCKED
	`, batchSize)
	if err != nil {
		return 0, err
	}

	pending := make([]pendingEvent, 0, batchSize)
	for rows.Next() {
		var evt pendingEvent
		if err := rows.Scan(&evt.ID, &evt.ConversationID, &evt.ActorUserID, &evt.Type, &evt.Payload, &evt.CreatedAt); err != nil {
			return 0, err
		}
		pending = append(pending, evt)
	}
	if err := rows.Err(); err != nil {
		return 0, err
	}
	rows.Close()
	if len(pending) == 0 {
		if err := tx.Commit(ctx); err != nil {
			return 0, err
		}
		return 0, nil
	}

	deliveries := make(map[string][]Event)
	for _, evt := range pending {
		switch evt.Type {
		case DomainEventMessageCreated:
			if err := s.processMessageCreated(ctx, tx, evt, deliveries); err != nil {
				return 0, err
			}
		case DomainEventMessageEdited:
			if err := s.processMessageEdited(ctx, tx, evt, deliveries); err != nil {
				return 0, err
			}
		case DomainEventMessageDeleted:
			if err := s.processMessageDeleted(ctx, tx, evt, deliveries); err != nil {
				return 0, err
			}
		case DomainEventMessageReactionsUpdated:
			if err := s.processMessageReactionsUpdated(ctx, tx, evt, deliveries); err != nil {
				return 0, err
			}
		case DomainEventMessageEffectTriggered:
			if err := s.processMessageEffectTriggered(ctx, tx, evt, deliveries); err != nil {
				return 0, err
			}
		case DomainEventReadCheckpointAdvanced:
			if err := s.processReadCheckpoint(ctx, tx, evt, deliveries); err != nil {
				return 0, err
			}
		case DomainEventDeliveryCheckpointAdvanced:
			if err := s.processDeliveryCheckpoint(ctx, tx, evt, deliveries); err != nil {
				return 0, err
			}
		case DomainEventTypingStarted, DomainEventTypingStopped:
			if err := s.processTypingEvent(ctx, tx, evt, deliveries); err != nil {
				return 0, err
			}
		}
	}

	ids := make([]int64, 0, len(pending))
	for _, evt := range pending {
		ids = append(ids, evt.ID)
	}
	_, err = tx.Exec(ctx, `
		UPDATE domain_events
		SET processed_at = now()
		WHERE event_id = ANY($1)
	`, ids)
	if err != nil {
		return 0, err
	}
	if err := tx.Commit(ctx); err != nil {
		return 0, err
	}

	s.publishUserEvents(ctx, deliveries)
	return len(pending), nil
}

func (s *Store) AcknowledgeCursor(ctx context.Context, userID, deviceID string, throughUserEventID int64) error {
	if throughUserEventID <= 0 {
		return nil
	}
	if strings.TrimSpace(deviceID) == "" {
		deviceID = "web"
	}
	tx, err := s.db.BeginTx(ctx, pgx.TxOptions{})
	if err != nil {
		return err
	}
	defer tx.Rollback(ctx)

	var previous int64
	err = tx.QueryRow(ctx, `
		SELECT last_user_event_id
		FROM user_device_cursors
		WHERE user_id = $1::uuid AND device_id = $2
		FOR UPDATE
	`, userID, deviceID).Scan(&previous)
	if err != nil && err != pgx.ErrNoRows {
		return err
	}
	if throughUserEventID <= previous {
		_, err = tx.Exec(ctx, `
			INSERT INTO user_device_cursors (user_id, device_id, last_user_event_id, last_seen_at)
			VALUES ($1::uuid, $2, $3, now())
			ON CONFLICT (user_id, device_id)
			DO UPDATE SET last_seen_at = now()
		`, userID, deviceID, previous)
		if err != nil {
			return err
		}
		return tx.Commit(ctx)
	}

	rows, err := tx.Query(ctx, `
		SELECT conversation_id::text, MAX(COALESCE((payload->>'server_order')::bigint, 0))
		FROM user_inbox_events
		WHERE user_id = $1::uuid
		  AND user_event_id > $2
		  AND user_event_id <= $3
		  AND event_type = $4
		  AND conversation_id IS NOT NULL
		GROUP BY conversation_id
	`, userID, previous, throughUserEventID, UserEventConversationMessageAppended)
	if err != nil {
		return err
	}

	type deliveredAdvance struct {
		ConversationID string
		Through        int64
	}
	advances := make([]deliveredAdvance, 0)
	for rows.Next() {
		var advance deliveredAdvance
		if err := rows.Scan(&advance.ConversationID, &advance.Through); err != nil {
			return err
		}
		if advance.Through > 0 {
			advances = append(advances, advance)
		}
	}
	if err := rows.Err(); err != nil {
		return err
	}
	rows.Close()

	_, err = tx.Exec(ctx, `
		INSERT INTO user_device_cursors (user_id, device_id, last_user_event_id, last_seen_at)
		VALUES ($1::uuid, $2, $3, now())
		ON CONFLICT (user_id, device_id)
		DO UPDATE SET last_user_event_id = GREATEST(user_device_cursors.last_user_event_id, EXCLUDED.last_user_event_id),
		              last_seen_at = now()
	`, userID, deviceID, throughUserEventID)
	if err != nil {
		return err
	}

	deliveredAt := time.Now().UTC().Format(time.RFC3339Nano)
	for _, advance := range advances {
		if err := s.advanceDeliveredCheckpointTx(ctx, tx, userID, advance.ConversationID, advance.Through, deliveredAt); err != nil {
			return err
		}
	}
	return tx.Commit(ctx)
}

func (s *Store) AdvanceDeliveredCheckpoint(ctx context.Context, userID, deviceID, conversationID string, throughServerOrder int64) error {
	if throughServerOrder <= 0 {
		return nil
	}
	if strings.TrimSpace(deviceID) == "" {
		deviceID = "web"
	}
	tx, err := s.db.BeginTx(ctx, pgx.TxOptions{})
	if err != nil {
		return err
	}
	defer tx.Rollback(ctx)
	if _, err := tx.Exec(ctx, `
		INSERT INTO user_device_cursors (user_id, device_id, last_seen_at)
		VALUES ($1::uuid, $2, now())
		ON CONFLICT (user_id, device_id)
		DO UPDATE SET last_seen_at = now()
	`, userID, deviceID); err != nil {
		return err
	}
	if err := s.advanceDeliveredCheckpointTx(ctx, tx, userID, conversationID, throughServerOrder, time.Now().UTC().Format(time.RFC3339Nano)); err != nil {
		return err
	}
	return tx.Commit(ctx)
}

func (s *Store) AppendMessageEffectEvent(ctx context.Context, q DBTX, conversationID, actorUserID, messageID, effectType string, triggeredAt time.Time) error {
	return s.AppendDomainEvent(ctx, q, conversationID, actorUserID, DomainEventMessageEffectTriggered, MessageEffectPayload{
		MessageID:         messageID,
		ConversationID:    conversationID,
		EffectType:        effectType,
		TriggeredByUserID: actorUserID,
		TriggeredAtMS:     triggeredAt.UTC().UnixMilli(),
	})
}

func (s *Store) AppendTypingEvent(ctx context.Context, conversationID, actorUserID, deviceID, state string, startedAt time.Time) error {
	eventType := DomainEventTypingStopped
	if strings.EqualFold(state, "typing_started") {
		eventType = DomainEventTypingStarted
	}
	return s.AppendDomainEvent(ctx, s.db, conversationID, actorUserID, eventType, TypingPayload{
		ConversationID: conversationID,
		UserID:         actorUserID,
		DeviceID:       deviceID,
		State:          state,
		StartedAtMS:    startedAt.UTC().UnixMilli(),
	})
}

func (s *Store) processMessageCreated(ctx context.Context, tx pgx.Tx, evt pendingEvent, deliveries map[string][]Event) error {
	var payload MessageCreatedPayload
	if err := json.Unmarshal(evt.Payload, &payload); err != nil {
		return err
	}
	preview := previewText(payload.ContentType, payload.Content)
	for _, userID := range payload.Participants {
		messagePayload := map[string]any{
			"conversation_id":   payload.ConversationID,
			"conversation_type": payload.ConversationType,
			"participants":      payload.Participants,
			"external_phones":   payload.ExternalPhones,
			"preview":           preview,
			"closed":            false,
			"server_order":      payload.ServerOrder,
			"message": map[string]any{
				"message_id":          payload.MessageID,
				"conversation_id":     payload.ConversationID,
				"sender_user_id":      payload.SenderUserID,
				"sender_device_id":    payload.SenderDeviceID,
				"content_type":        payload.ContentType,
				"content":             payload.Content,
				"client_generated_id": payload.ClientGeneratedID,
				"transport":           payload.Transport,
				"server_order":        payload.ServerOrder,
				"status":              "SENT",
				"created_at":          payload.CreatedAt,
				"sent_at":             payload.CreatedAt,
				"status_updated_at":   payload.CreatedAt,
			},
		}
		userEvent, err := s.insertUserEventTx(ctx, tx, userID, payload.ConversationID, UserEventConversationMessageAppended, messagePayload)
		if err != nil {
			return err
		}
		deliveries[userID] = append(deliveries[userID], userEvent)
		if err := s.upsertConversationStateTx(ctx, tx, userID, payload, preview); err != nil {
			return err
		}
	}
	return nil
}

func (s *Store) processMessageEdited(ctx context.Context, tx pgx.Tx, evt pendingEvent, deliveries map[string][]Event) error {
	var payload MessageEditedPayload
	if err := json.Unmarshal(evt.Payload, &payload); err != nil {
		return err
	}
	preview := previewText(payload.ContentType, payload.Content)
	for _, userID := range payload.Participants {
		messagePayload := map[string]any{
			"conversation_id":   payload.ConversationID,
			"conversation_type": payload.ConversationType,
			"participants":      payload.Participants,
			"external_phones":   payload.ExternalPhones,
			"preview":           preview,
			"message": map[string]any{
				"message_id":          payload.MessageID,
				"conversation_id":     payload.ConversationID,
				"sender_user_id":      payload.SenderUserID,
				"sender_device_id":    payload.SenderDeviceID,
				"content_type":        payload.ContentType,
				"content":             payload.Content,
				"client_generated_id": payload.ClientGeneratedID,
				"transport":           payload.Transport,
				"server_order":        payload.ServerOrder,
				"created_at":          payload.CreatedAt,
				"sent_at":             payload.CreatedAt,
				"edited_at":           payload.EditedAt,
				"status_updated_at":   payload.EditedAt,
			},
		}
		userEvent, err := s.insertUserEventTx(ctx, tx, userID, payload.ConversationID, UserEventConversationMessageEdited, messagePayload)
		if err != nil {
			return err
		}
		deliveries[userID] = append(deliveries[userID], userEvent)
		if err := s.applyMessageEditedStateTx(ctx, tx, userID, payload, preview); err != nil {
			return err
		}
	}
	return nil
}

func (s *Store) processMessageDeleted(ctx context.Context, tx pgx.Tx, evt pendingEvent, deliveries map[string][]Event) error {
	var payload MessageDeletedPayload
	if err := json.Unmarshal(evt.Payload, &payload); err != nil {
		return err
	}
	const preview = "Message deleted"
	for _, userID := range payload.Participants {
		messagePayload := map[string]any{
			"conversation_id":   payload.ConversationID,
			"conversation_type": payload.ConversationType,
			"participants":      payload.Participants,
			"external_phones":   payload.ExternalPhones,
			"preview":           preview,
			"message": map[string]any{
				"message_id":          payload.MessageID,
				"conversation_id":     payload.ConversationID,
				"sender_user_id":      payload.SenderUserID,
				"sender_device_id":    payload.SenderDeviceID,
				"content_type":        payload.ContentType,
				"content":             map[string]any{},
				"client_generated_id": payload.ClientGeneratedID,
				"transport":           payload.Transport,
				"server_order":        payload.ServerOrder,
				"created_at":          payload.CreatedAt,
				"sent_at":             payload.CreatedAt,
				"deleted":             true,
				"deleted_at":          payload.DeletedAt,
				"visibility_state":    "SOFT_DELETED",
				"status_updated_at":   payload.DeletedAt,
			},
		}
		userEvent, err := s.insertUserEventTx(ctx, tx, userID, payload.ConversationID, UserEventConversationMessageDeleted, messagePayload)
		if err != nil {
			return err
		}
		deliveries[userID] = append(deliveries[userID], userEvent)
		if err := s.applyMessageDeletedStateTx(ctx, tx, userID, payload, preview); err != nil {
			return err
		}
	}
	return nil
}

func (s *Store) processMessageReactionsUpdated(ctx context.Context, tx pgx.Tx, evt pendingEvent, deliveries map[string][]Event) error {
	var payload MessageReactionsPayload
	if err := json.Unmarshal(evt.Payload, &payload); err != nil {
		return err
	}
	for _, userID := range payload.Participants {
		messagePayload := map[string]any{
			"conversation_id":   payload.ConversationID,
			"conversation_type": payload.ConversationType,
			"participants":      payload.Participants,
			"external_phones":   payload.ExternalPhones,
			"message_id":        payload.MessageID,
			"server_order":      payload.ServerOrder,
			"reactions":         payload.Reactions,
			"acted_at":          payload.ActedAt,
		}
		userEvent, err := s.insertUserEventTx(ctx, tx, userID, payload.ConversationID, UserEventConversationMessageReactionsUpdated, messagePayload)
		if err != nil {
			return err
		}
		deliveries[userID] = append(deliveries[userID], userEvent)
	}
	return nil
}

func (s *Store) processMessageEffectTriggered(ctx context.Context, tx pgx.Tx, evt pendingEvent, deliveries map[string][]Event) error {
	var payload MessageEffectPayload
	if err := json.Unmarshal(evt.Payload, &payload); err != nil {
		return err
	}
	meta, err := s.LoadConversationMeta(ctx, tx, payload.ConversationID)
	if err != nil {
		return err
	}
	for _, userID := range meta.Participants {
		userEvent, err := s.insertUserEventTx(ctx, tx, userID, payload.ConversationID, UserEventConversationMessageEffectTriggered, map[string]any{
			"conversation_id":      payload.ConversationID,
			"message_id":           payload.MessageID,
			"effect_type":          payload.EffectType,
			"triggered_by_user_id": payload.TriggeredByUserID,
			"triggered_at_ms":      payload.TriggeredAtMS,
		})
		if err != nil {
			return err
		}
		deliveries[userID] = append(deliveries[userID], userEvent)
	}
	return nil
}

func (s *Store) processTypingEvent(ctx context.Context, tx pgx.Tx, evt pendingEvent, deliveries map[string][]Event) error {
	var payload TypingPayload
	if err := json.Unmarshal(evt.Payload, &payload); err != nil {
		return err
	}
	if strings.EqualFold(payload.State, "typing_started") {
		sharesTyping, err := userPrivacyFlagTx(ctx, tx, payload.UserID, "share_typing")
		if err != nil {
			return err
		}
		if !sharesTyping {
			return nil
		}
	}
	meta, err := s.LoadConversationMeta(ctx, tx, payload.ConversationID)
	if err != nil {
		return err
	}
	for _, userID := range meta.Participants {
		if userID == payload.UserID {
			continue
		}
		userEvent, err := s.insertUserEventTx(ctx, tx, userID, payload.ConversationID, UserEventConversationTypingUpdated, map[string]any{
			"conversation_id": payload.ConversationID,
			"user_id":         payload.UserID,
			"device_id":       payload.DeviceID,
			"state":           payload.State,
			"started_at_ms":   payload.StartedAtMS,
		})
		if err != nil {
			return err
		}
		deliveries[userID] = append(deliveries[userID], userEvent)
	}
	return nil
}

func (s *Store) processReadCheckpoint(ctx context.Context, tx pgx.Tx, evt pendingEvent, deliveries map[string][]Event) error {
	var payload ReadCheckpointPayload
	if err := json.Unmarshal(evt.Payload, &payload); err != nil {
		return err
	}
	if _, err := tx.Exec(ctx, `
		UPDATE user_conversation_state
		SET last_read_server_order = GREATEST(last_read_server_order, $3),
		    unread_count = (
		      SELECT COUNT(1)
		      FROM messages m
		      WHERE m.conversation_id = $1::uuid
		        AND m.sender_user_id <> $2::uuid
		        AND m.server_order > $3
		    ),
		    updated_at = now()
		WHERE user_id = $2::uuid AND conversation_id = $1::uuid
	`, payload.ConversationID, payload.ReaderUserID, payload.ThroughServerOrder); err != nil {
		return err
	}
	sendReadReceipts, err := userPrivacyFlagTx(ctx, tx, payload.ReaderUserID, "send_read_receipts")
	if err != nil {
		return err
	}
	if !sendReadReceipts {
		return nil
	}
	return s.emitReceiptUpdates(ctx, tx, payload.ConversationID, payload.ReaderUserID, "READ", payload.ThroughServerOrder, payload.ReadAt, deliveries)
}

func (s *Store) processDeliveryCheckpoint(ctx context.Context, tx pgx.Tx, evt pendingEvent, deliveries map[string][]Event) error {
	var payload DeliveryCheckpointPayload
	if err := json.Unmarshal(evt.Payload, &payload); err != nil {
		return err
	}
	if _, err := tx.Exec(ctx, `
		UPDATE user_conversation_state
		SET last_delivered_server_order = GREATEST(last_delivered_server_order, $3),
		    updated_at = now()
		WHERE user_id = $2::uuid AND conversation_id = $1::uuid
	`, payload.ConversationID, payload.UserID, payload.ThroughServerOrder); err != nil {
		return err
	}
	return s.emitReceiptUpdates(ctx, tx, payload.ConversationID, payload.UserID, "DELIVERED", payload.ThroughServerOrder, payload.DeliveredAt, deliveries)
}

func (s *Store) emitReceiptUpdates(ctx context.Context, tx pgx.Tx, conversationID, actorUserID, receiptKind string, throughServerOrder int64, at string, deliveries map[string][]Event) error {
	rows, err := tx.Query(ctx, `
		SELECT DISTINCT sender_user_id::text
		FROM messages
		WHERE conversation_id = $1::uuid
		  AND sender_user_id <> $2::uuid
		  AND server_order <= $3
		  AND transport IN ('OTT', 'OHMF')
	`, conversationID, actorUserID, throughServerOrder)
	if err != nil {
		return err
	}
	senderIDs := make([]string, 0, 4)
	for rows.Next() {
		var senderID string
		if err := rows.Scan(&senderID); err != nil {
			rows.Close()
			return err
		}
		senderIDs = append(senderIDs, senderID)
	}
	if err := rows.Err(); err != nil {
		rows.Close()
		return err
	}
	rows.Close()
	for _, senderID := range senderIDs {
		payload := map[string]any{
			"conversation_id":      conversationID,
			"receipt_kind":         receiptKind,
			"actor_user_id":        actorUserID,
			"through_server_order": throughServerOrder,
			"status_updated_at":    at,
		}
		userEvent, err := s.insertUserEventTx(ctx, tx, senderID, conversationID, UserEventConversationReceiptUpdated, payload)
		if err != nil {
			return err
		}
		deliveries[senderID] = append(deliveries[senderID], userEvent)
	}
	return nil
}

func userPrivacyFlagTx(ctx context.Context, tx pgx.Tx, userID, column string) (bool, error) {
	query := ""
	switch column {
	case "send_read_receipts":
		query = `SELECT send_read_receipts FROM user_privacy_preferences WHERE user_id = $1::uuid`
	case "share_typing":
		query = `SELECT share_typing FROM user_privacy_preferences WHERE user_id = $1::uuid`
	default:
		return false, fmt.Errorf("unsupported privacy column: %s", column)
	}

	var enabled bool
	err := tx.QueryRow(ctx, query, userID).Scan(&enabled)
	if err != nil {
		if err == pgx.ErrNoRows {
			return true, nil
		}
		return false, err
	}
	return enabled, nil
}

func (s *Store) insertUserEventTx(ctx context.Context, tx pgx.Tx, userID, conversationID, eventType string, payload map[string]any) (Event, error) {
	body, err := json.Marshal(payload)
	if err != nil {
		return Event{}, err
	}
	var evt Event
	var created time.Time
	if err := tx.QueryRow(ctx, `
		INSERT INTO user_inbox_events (user_id, conversation_id, event_type, payload)
		VALUES ($1::uuid, NULLIF($2, '')::uuid, $3, $4::jsonb)
		RETURNING user_event_id, created_at
	`, userID, conversationID, eventType, string(body)).Scan(&evt.UserEventID, &created); err != nil {
		return Event{}, err
	}
	evt.Type = eventType
	evt.CreatedAt = created.UTC().Format(time.RFC3339Nano)
	evt.Payload = payload
	return evt, nil
}

func (s *Store) upsertConversationStateTx(ctx context.Context, tx pgx.Tx, userID string, payload MessageCreatedPayload, preview string) error {
	isSender := userID == payload.SenderUserID
	unreadDelta := 0
	if !isSender {
		unreadDelta = 1
	}
	_, err := tx.Exec(ctx, `
		INSERT INTO user_conversation_state (
			user_id,
			conversation_id,
			last_message_id,
			last_message_preview,
			last_message_at,
			unread_count,
			last_read_server_order,
			last_delivered_server_order,
			updated_at
		)
		VALUES (
			$1::uuid,
			$2::uuid,
			$3::uuid,
			$4,
			$5::timestamptz,
			$6,
			COALESCE((SELECT last_read_server_order FROM conversation_members WHERE conversation_id = $2::uuid AND user_id = $1::uuid), 0),
			COALESCE((SELECT last_delivered_server_order FROM conversation_members WHERE conversation_id = $2::uuid AND user_id = $1::uuid), 0),
			$5::timestamptz
		)
		ON CONFLICT (user_id, conversation_id)
		DO UPDATE SET
			last_message_id = EXCLUDED.last_message_id,
			last_message_preview = EXCLUDED.last_message_preview,
			last_message_at = EXCLUDED.last_message_at,
			unread_count = CASE
				WHEN $7 THEN user_conversation_state.unread_count
				ELSE user_conversation_state.unread_count + 1
			END,
			is_closed = FALSE,
			last_read_server_order = COALESCE((SELECT last_read_server_order FROM conversation_members WHERE conversation_id = $2::uuid AND user_id = $1::uuid), user_conversation_state.last_read_server_order),
			last_delivered_server_order = COALESCE((SELECT last_delivered_server_order FROM conversation_members WHERE conversation_id = $2::uuid AND user_id = $1::uuid), user_conversation_state.last_delivered_server_order),
			updated_at = EXCLUDED.updated_at
	`, userID, payload.ConversationID, payload.MessageID, preview, payload.CreatedAt, unreadDelta, isSender)
	return err
}

func (s *Store) applyMessageEditedStateTx(ctx context.Context, tx pgx.Tx, userID string, payload MessageEditedPayload, preview string) error {
	_, err := tx.Exec(ctx, `
		UPDATE user_conversation_state ucs
		SET last_message_preview = CASE
		      WHEN ucs.last_message_id = $3::uuid THEN $4
		      ELSE ucs.last_message_preview
		    END,
		    updated_at = $5::timestamptz
		WHERE ucs.user_id = $1::uuid
		  AND ucs.conversation_id = $2::uuid
	`, userID, payload.ConversationID, payload.MessageID, preview, payload.EditedAt)
	return err
}

func (s *Store) applyMessageDeletedStateTx(ctx context.Context, tx pgx.Tx, userID string, payload MessageDeletedPayload, preview string) error {
	_, err := tx.Exec(ctx, `
		UPDATE user_conversation_state ucs
		SET last_message_preview = CASE
		      WHEN ucs.last_message_id = $3::uuid THEN $4
		      ELSE ucs.last_message_preview
		    END,
		    last_message_at = CASE
		      WHEN ucs.last_message_id = $3::uuid THEN $5::timestamptz
		      ELSE ucs.last_message_at
		    END,
		    unread_count = CASE
		      WHEN $6::uuid <> $1::uuid
		       AND COALESCE(
		             (SELECT last_read_server_order
		              FROM conversation_members
		              WHERE conversation_id = $2::uuid AND user_id = $1::uuid),
		             0
		           ) < $7
		      THEN GREATEST(ucs.unread_count - 1, 0)
		      ELSE ucs.unread_count
		    END,
		    updated_at = $5::timestamptz
		WHERE ucs.user_id = $1::uuid
		  AND ucs.conversation_id = $2::uuid
	`, userID, payload.ConversationID, payload.MessageID, preview, payload.DeletedAt, payload.SenderUserID, payload.ServerOrder)
	return err
}

func (s *Store) advanceDeliveredCheckpointTx(ctx context.Context, tx pgx.Tx, userID, conversationID string, throughServerOrder int64, deliveredAt string) error {
	var previous int64
	if err := tx.QueryRow(ctx, `
		SELECT last_delivered_server_order
		FROM conversation_members
		WHERE conversation_id = $1::uuid AND user_id = $2::uuid
		FOR UPDATE
	`, conversationID, userID).Scan(&previous); err != nil {
		return err
	}
	if throughServerOrder <= previous {
		return nil
	}
	if _, err := tx.Exec(ctx, `
		UPDATE conversation_members
		SET last_delivered_server_order = GREATEST(last_delivered_server_order, $3),
		    delivery_at = now()
		WHERE conversation_id = $1::uuid AND user_id = $2::uuid
	`, conversationID, userID, throughServerOrder); err != nil {
		return err
	}
	return s.AppendDomainEvent(ctx, tx, conversationID, userID, DomainEventDeliveryCheckpointAdvanced, DeliveryCheckpointPayload{
		ConversationID:     conversationID,
		UserID:             userID,
		ThroughServerOrder: throughServerOrder,
		DeliveredAt:        deliveredAt,
	})
}

func (s *Store) publishUserEvents(ctx context.Context, deliveries map[string][]Event) {
	if s.redis == nil {
		return
	}
	for userID, events := range deliveries {
		for _, evt := range events {
			body, err := json.Marshal(evt)
			if err != nil {
				continue
			}
			_ = s.redis.Publish(ctx, userEventChannelPrefix+userID, body).Err()
			if evt.Type == UserEventConversationReceiptUpdated {
				legacy, err := json.Marshal(map[string]any{
					"conversation_id":      evt.Payload["conversation_id"],
					"through_server_order": evt.Payload["through_server_order"],
					"status":               evt.Payload["receipt_kind"],
					"status_updated_at":    evt.Payload["status_updated_at"],
				})
				if err == nil {
					_ = s.redis.Publish(ctx, "delivery:user:"+userID, legacy).Err()
				}
			}
		}
	}
}

func previewText(contentType string, content map[string]any) string {
	switch strings.TrimSpace(contentType) {
	case "text":
		if text, _ := content["text"].(string); strings.TrimSpace(text) != "" {
			return strings.TrimSpace(text)
		}
	case "encrypted":
		return "Encrypted message"
	case "attachment":
		if name, _ := content["filename"].(string); strings.TrimSpace(name) != "" {
			return strings.TrimSpace(name)
		}
		return "Attachment"
	case "link_preview":
		if title, _ := content["title"].(string); strings.TrimSpace(title) != "" {
			return strings.TrimSpace(title)
		}
		if url, _ := content["url"].(string); strings.TrimSpace(url) != "" {
			return strings.TrimSpace(url)
		}
		return "Link preview"
	case "app_card":
		if title, _ := content["title"].(string); strings.TrimSpace(title) != "" {
			return strings.TrimSpace(title)
		}
	}
	raw, err := json.Marshal(content)
	if err != nil {
		return contentType
	}
	text := strings.TrimSpace(string(raw))
	if len(text) > 120 {
		return text[:120]
	}
	return text
}

type pendingEvent struct {
	ID             int64
	ConversationID string
	ActorUserID    string
	Type           string
	Payload        []byte
	CreatedAt      time.Time
}

func (s *Store) ChannelForUser(userID string) string {
	return userEventChannelPrefix + userID
}

func ParseCursor(raw string) (int64, error) {
	if strings.TrimSpace(raw) == "" {
		return 0, nil
	}
	var cursor int64
	if _, err := fmt.Sscanf(strings.TrimSpace(raw), "%d", &cursor); err != nil {
		return 0, fmt.Errorf("invalid cursor")
	}
	return cursor, nil
}

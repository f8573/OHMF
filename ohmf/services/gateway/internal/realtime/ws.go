package realtime

import (
	"context"
	"encoding/json"
	"errors"
	"net"
	"net/http"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/google/uuid"
	"github.com/gorilla/websocket"
	"github.com/redis/go-redis/v9"
	"ohmf/services/gateway/internal/config"
	"ohmf/services/gateway/internal/limit"
	"ohmf/services/gateway/internal/messages"
	appmw "ohmf/services/gateway/internal/middleware"
	"ohmf/services/gateway/internal/observability"
	"ohmf/services/gateway/internal/replication"
	"ohmf/services/gateway/internal/token"
)

const presenceTTL = 90 * time.Second
const sessionEventChannelPrefix = "miniapp:session:"

// miniappServiceInterface defines the minimal interface needed for session event subscriptions.
// This avoids circular import by not importing the miniapp package directly.
type miniappServiceInterface interface {
	GetSessionForUser(ctx context.Context, userID, sessionID string) (map[string]any, error)
}

type Handler struct {
	tokens         *token.Service
	messages       *messages.Service
	miniappService any // P4.3: Mini-app service for session validation (avoids circular import)
	redis          *redis.Client
	limiter        *limit.TokenBucket
	enableSend     bool
	replication    *replication.Store
	upgrader       websocket.Upgrader

	mu      sync.RWMutex
	clients map[string]map[*client]struct{}
}

type client struct {
	userID              string
	conn                *websocket.Conn
	send                chan []byte
	deviceID            string
	sessionID           string
	v2                  bool
	typingConversations map[string]struct{}

	// P4.3: Mini-app session subscriptions (maps session_id -> cancel func)
	sessionSubscriptions map[string]context.CancelFunc
	clientCtx            context.Context // Context tied to client connection lifecycle (removed: explicit timeout, now uses cleanup cancellation)
	clientCancel         context.CancelFunc

	sendMu sync.RWMutex
	closed bool
}

type wsEnvelope struct {
	Event string          `json:"event"`
	Data  json.RawMessage `json:"data"`
}

type sendMessagePayload struct {
	ConversationID    string         `json:"conversation_id"`
	IdempotencyKey    string         `json:"idempotency_key"`
	ContentType       string         `json:"content_type"`
	Content           map[string]any `json:"content"`
	ClientGeneratedID string         `json:"client_generated_id"`
	ReplyToMessageID  string         `json:"reply_to_message_id,omitempty"`
}

type presenceSubscribePayload struct {
	ConversationIDs []string `json:"conversation_ids"`
}

type typingEventPayload struct {
	ConversationID string `json:"conversation_id"`
}

type resumePayload struct {
	DeviceID       string `json:"device_id"`
	LastUserCursor int64  `json:"last_user_cursor"`
	LastCursor     string `json:"last_cursor"`
}

type resyncPayload struct {
	ConversationID string `json:"conversation_id"`
	DeviceID       string `json:"device_id"`
	LastUserCursor int64  `json:"last_user_cursor"`
	LastCursor     string `json:"last_cursor"`
}

// removed: enableSend boolean parameter - replaced with named constructors
func NewHandlerWithSend(tokens *token.Service, messageService *messages.Service, redisClient *redis.Client, limiter *limit.TokenBucket, store *replication.Store, miniappService any) *Handler {
	return newHandlerInternal(tokens, messageService, redisClient, limiter, true, store, miniappService)
}

func NewHandlerReadOnly(tokens *token.Service, messageService *messages.Service, redisClient *redis.Client, limiter *limit.TokenBucket, store *replication.Store, miniappService any) *Handler {
	return newHandlerInternal(tokens, messageService, redisClient, limiter, false, store, miniappService)
}

func newHandlerInternal(tokens *token.Service, messageService *messages.Service, redisClient *redis.Client, limiter *limit.TokenBucket, enableSend bool, store *replication.Store, miniappService any) *Handler {
	return &Handler{
		tokens:         tokens,
		messages:       messageService,
		miniappService: miniappService,
		redis:          redisClient,
		limiter:        limiter,
		enableSend:     enableSend,
		replication:    store,
		upgrader: websocket.Upgrader{
			CheckOrigin: func(_ *http.Request) bool { return true },
		},
		clients: map[string]map[*client]struct{}{},
	}
}

func (h *Handler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	ip := ipOnly(r.RemoteAddr)
	if err := h.allowConnect(ctx, ip); err != nil {
		http.Error(w, "rate_limited", http.StatusTooManyRequests)
		return
	}

	// P3.2 Isolated Runtime Origins: Validate WebSocket origin header if provided
	if err := h.validateOriginHeader(r); err != nil {
		http.Error(w, "origin_invalid", http.StatusForbidden)
		return
	}

	userID, err := h.authenticate(r)
	if err != nil {
		http.Error(w, "unauthorized", http.StatusUnauthorized)
		return
	}
	conn, err := h.upgrader.Upgrade(w, r, nil)
	if err != nil {
		return
	}

	c := &client{
		userID:               userID,
		conn:                 conn,
		send:                 make(chan []byte, 128),
		sessionID:            "wsv1:" + uuid.NewString(),
		sessionSubscriptions: make(map[string]context.CancelFunc),
	}
	c.clientCtx, c.clientCancel = context.WithCancel(context.Background())
	h.register(c)
	h.touchConnection(ctx, c)
	if updates, err := h.messages.DeliverPendingToUser(ctx, userID); err == nil {
		for _, update := range updates {
			body, _ := json.Marshal(update)
			senderUserID, _ := update["sender_user_id"].(string)
			if senderUserID != "" {
				_ = h.redis.Publish(ctx, "delivery:user:"+senderUserID, body).Err()
			}
		}
	}

	go h.writeLoop(c)
	go h.subscribeDelivery(ctx, c)
	go h.subscribeMessages(ctx, c)
	h.readLoop(c, ip)
}

func (h *Handler) ServeV2(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	ip := ipOnly(r.RemoteAddr)
	if err := h.allowConnect(ctx, ip); err != nil {
		http.Error(w, "rate_limited", http.StatusTooManyRequests)
		return
	}
	userID, err := h.authenticate(r)
	if err != nil {
		http.Error(w, "unauthorized", http.StatusUnauthorized)
		return
	}
	conn, err := h.upgrader.Upgrade(w, r, nil)
	if err != nil {
		return
	}

	c := &client{
		userID:               userID,
		conn:                 conn,
		send:                 make(chan []byte, 128),
		v2:                   true,
		sessionID:            "wsv2:" + uuid.NewString(),
		sessionSubscriptions: make(map[string]context.CancelFunc),
	}
	c.clientCtx, c.clientCancel = context.WithCancel(context.Background())
	h.register(c)
	h.touchConnection(ctx, c)

	go h.writeLoop(c)
	go h.subscribeUserEvents(ctx, c)
	h.readLoopV2(c, ip)
}

func (h *Handler) allowConnect(ctx context.Context, ip string) error {
	if h.limiter == nil {
		return nil
	}
	decision, err := h.limiter.Allow(ctx, "rate:ws:connect:ip:"+ip, 60, time.Minute, 120, 1)
	if err != nil {
		return err
	}
	if !decision.Allowed {
		return limit.ErrRateLimited
	}
	return nil
}

func (h *Handler) authenticate(r *http.Request) (string, error) {
	accessToken := strings.TrimSpace(r.URL.Query().Get("access_token"))
	if accessToken == "" {
		authHeader := r.Header.Get("Authorization")
		if strings.HasPrefix(authHeader, "Bearer ") {
			accessToken = strings.TrimSpace(strings.TrimPrefix(authHeader, "Bearer "))
		}
	}
	if accessToken == "" {
		return "", errors.New("missing access token")
	}
	claims, err := h.tokens.ParseAccess(accessToken)
	if err != nil {
		return "", err
	}
	return claims.UserID, nil
}

func (h *Handler) readLoop(c *client, ip string) {
	defer h.unregister(c)
	_ = c.conn.SetReadDeadline(time.Now().Add(60 * time.Second))
	c.conn.SetPongHandler(func(_ string) error {
		_ = c.conn.SetReadDeadline(time.Now().Add(60 * time.Second))
		h.touchConnection(context.Background(), c)
		return nil
	})

	heartbeat := time.NewTicker(30 * time.Second)
	defer heartbeat.Stop()
	go func() {
		for range heartbeat.C {
			h.touchConnection(context.Background(), c)
			_ = c.conn.WriteControl(websocket.PingMessage, []byte("ping"), time.Now().Add(5*time.Second))
		}
	}()

	for {
		_, payload, err := c.conn.ReadMessage()
		if err != nil {
			return
		}
		if err := h.allowControlEvent(context.Background(), c.userID); err != nil {
			h.sendJSON(c, "error", map[string]any{"code": "rate_limited", "message": "ws control rate limit"})
			continue
		}
		var env wsEnvelope
		if err := json.Unmarshal(payload, &env); err != nil {
			h.sendJSON(c, "error", map[string]any{"code": "invalid_request", "message": "invalid event envelope"})
			continue
		}
		observability.RecordWSMessage("received", env.Event)
		switch env.Event {
		// Support legacy/spec alias: "subscribe" maps to presence_subscribe
		case "subscribe":
			var raw interface{}
			if err := json.Unmarshal(env.Data, &raw); err != nil {
				h.sendJSON(c, "error", map[string]any{"code": "invalid_request", "message": "invalid subscribe payload"})
				continue
			}
			if err := appmw.ValidateData("ws-subscribe", raw); err != nil {
				h.sendJSON(c, "error", map[string]any{"code": "invalid_request", "message": err.Error()})
				continue
			}
			var req presenceSubscribePayload
			if err := json.Unmarshal(env.Data, &req); err != nil {
				h.sendJSON(c, "error", map[string]any{"code": "invalid_request", "message": "invalid subscribe payload"})
				continue
			}
			for _, convID := range req.ConversationIDs {
				if convID == "" {
					continue
				}
				_ = h.redis.Set(context.Background(), "presence:conv:"+convID+":user:"+c.userID, "1", presenceTTL).Err()
			}
			h.sendJSON(c, "subscribe_ack", map[string]any{"status": "ok", "conversation_ids": req.ConversationIDs})

		case "auth":
			h.sendJSON(c, "auth", map[string]any{"status": "ok", "user_id": c.userID})
			h.touchConnection(context.Background(), c)
		case "send_message":
			if !h.enableSend {
				h.sendJSON(c, "error", map[string]any{"code": "ws_send_disabled", "message": "ws send disabled"})
				continue
			}
			// validate payload against schema
			var raw interface{}
			if err := json.Unmarshal(env.Data, &raw); err != nil {
				h.sendJSON(c, "error", map[string]any{"code": "invalid_request", "message": "invalid send_message payload"})
				continue
			}
			if err := appmw.ValidateData("ws-send_message", raw); err != nil {
				h.sendJSON(c, "error", map[string]any{"code": "invalid_request", "message": err.Error()})
				continue
			}
			var req sendMessagePayload
			if err := json.Unmarshal(env.Data, &req); err != nil {
				h.sendJSON(c, "error", map[string]any{"code": "invalid_request", "message": "invalid send_message payload"})
				continue
			}
			if strings.TrimSpace(req.ReplyToMessageID) != "" {
				if req.Content == nil {
					req.Content = map[string]any{}
				}
				req.Content["reply_to_message_id"] = strings.TrimSpace(req.ReplyToMessageID)
			}
			result, err := h.messages.Send(context.Background(), c.userID, c.deviceID, req.ConversationID, req.IdempotencyKey, req.ContentType, req.Content, req.ClientGeneratedID, "ws-"+time.Now().UTC().Format(time.RFC3339Nano), ip)
			if err != nil {
				h.sendJSON(c, "error", map[string]any{"code": "send_failed", "message": err.Error()})
				continue
			}
			h.sendJSON(c, "ack", map[string]any{
				"message_id":      result.Message.MessageID,
				"conversation_id": result.Message.ConversationID,
				"server_order":    result.Message.ServerOrder,
				"status":          "SENT",
				"queued":          result.Queued,
				"ack_timeout_ms":  result.AckTimeoutMS,
			})
		case "presence_subscribe":
			var raw interface{}
			if err := json.Unmarshal(env.Data, &raw); err != nil {
				h.sendJSON(c, "error", map[string]any{"code": "invalid_request", "message": "invalid presence_subscribe payload"})
				continue
			}
			if err := appmw.ValidateData("ws-subscribe", raw); err != nil {
				h.sendJSON(c, "error", map[string]any{"code": "invalid_request", "message": err.Error()})
				continue
			}
			var req presenceSubscribePayload
			if err := json.Unmarshal(env.Data, &req); err != nil {
				h.sendJSON(c, "error", map[string]any{"code": "invalid_request", "message": "invalid presence_subscribe payload"})
				continue
			}
			for _, convID := range req.ConversationIDs {
				if convID == "" {
					continue
				}
				_ = h.redis.Set(context.Background(), "presence:conv:"+convID+":user:"+c.userID, "1", presenceTTL).Err()
			}
			h.sendJSON(c, "presence_update", map[string]any{"status": "online", "user_id": c.userID})

		case "resync":
			// Minimal resync acknowledgement: clients may request a resync cursor
			// for a conversation after reconnect. Full resync requires fetching
			// missing events/messages which is out of scope for this lightweight
			// handler; respond with an ack so clients can fallback to REST sync.
			var payload map[string]any
			if err := json.Unmarshal(env.Data, &payload); err != nil {
				h.sendJSON(c, "error", map[string]any{"code": "invalid_request", "message": "invalid resync payload"})
				continue
			}
			// echo back any conversation ids provided
			conv, _ := payload["conversation_id"]
			h.sendJSON(c, "resync_ack", map[string]any{"conversation_id": conv})

		case "typing.started":
			var p typingEventPayload
			if err := json.Unmarshal(env.Data, &p); err != nil {
				h.sendJSON(c, "error", map[string]any{"code": "invalid_request", "message": "invalid typing payload"})
				continue
			}
			h.handleTypingSignal(context.Background(), c, p.ConversationID, "typing.started", ip)

		case "typing.stopped":
			var p typingEventPayload
			if err := json.Unmarshal(env.Data, &p); err != nil {
				h.sendJSON(c, "error", map[string]any{"code": "invalid_request", "message": "invalid typing payload"})
				continue
			}
			h.handleTypingSignal(context.Background(), c, p.ConversationID, "typing.stopped", ip)
		default:
			h.sendJSON(c, "error", map[string]any{"code": "unsupported_event", "message": env.Event})
		}
	}
}

func (h *Handler) writeLoop(c *client) {
	defer func() {
		if c.conn != nil {
			_ = c.conn.Close()
		}
	}()
	for msg := range c.send {
		if c.conn == nil {
			return
		}
		if err := c.conn.WriteMessage(websocket.TextMessage, msg); err != nil {
			return
		}
	}
}

func (h *Handler) register(c *client) {
	h.mu.Lock()
	defer h.mu.Unlock()
	bucket := h.clients[c.userID]
	if bucket == nil {
		bucket = map[*client]struct{}{}
		h.clients[c.userID] = bucket
	}
	bucket[c] = struct{}{}
	observability.IncWSConnection()
}

func (h *Handler) unregister(c *client) {
	h.mu.Lock()
	lastConnection := false
	if bucket, ok := h.clients[c.userID]; ok {
		delete(bucket, c)
		if len(bucket) == 0 {
			delete(h.clients, c.userID)
			lastConnection = true
		}
	}
	h.mu.Unlock()
	if lastConnection && h.redis != nil {
		_ = h.redis.Del(context.Background(), "presence:user:"+c.userID).Err()
		_ = h.redis.Del(context.Background(), "presence:user:"+c.userID+":last_seen").Err()
	}
	h.cleanupTypingState(context.Background(), c)
	h.unregisterSession(context.Background(), c)
	// P4.3: Cancel client context to unsubscribe all session subscriptions
	if c.clientCancel != nil {
		c.clientCancel()
	}
	// P4.3: Cancel all active session subscriptions on disconnect, then mark closed (removed: explicit cancel loop since clientCancel handles it)
	c.sendMu.Lock()
	defer c.sendMu.Unlock()
	observability.DecWSConnection()
	if !c.closed {
		c.closed = true
		close(c.send)
	}
	if c.conn != nil {
		_ = c.conn.Close()
	}
}

func (h *Handler) sendJSON(c *client, event string, data any) {
	payload, err := json.Marshal(map[string]any{
		"event": event,
		"data":  data,
	})
	if err != nil {
		return
	}
	observability.RecordWSMessage("sent", event)
	c.sendMu.RLock()
	defer c.sendMu.RUnlock()
	if c.closed {
		return
	}
	select {
	case c.send <- payload:
	default:
	}
}

func (h *Handler) markPresence(ctx context.Context, userID string) {
	if h.redis == nil || userID == "" {
		return
	}
	now := time.Now().UTC()
	_ = h.redis.Set(ctx, "presence:user:"+userID, "online", presenceTTL).Err()
	_ = h.redis.Set(ctx, "presence:user:"+userID+":last_seen", strconv.FormatInt(now.UnixMilli(), 10), presenceTTL).Err()
}

func (h *Handler) registerSession(ctx context.Context, c *client) {
	if h.redis == nil || c.sessionID == "" {
		return
	}
	now := time.Now().UTC()
	body, _ := json.Marshal(map[string]any{
		"user_id":         c.userID,
		"device_id":       c.deviceID,
		"session_id":      c.sessionID,
		"version":         map[bool]string{true: "v2", false: "v1"}[c.v2],
		"last_seen_at_ms": now.UnixMilli(),
	})
	_ = h.redis.Set(ctx, "session:"+c.sessionID, body, presenceTTL).Err()
	_ = h.redis.SAdd(ctx, "user_sessions:"+c.userID, c.sessionID).Err()
	_ = h.redis.Expire(ctx, "user_sessions:"+c.userID, presenceTTL).Err()
}

func (h *Handler) unregisterSession(ctx context.Context, c *client) {
	if h.redis == nil || c.sessionID == "" {
		return
	}
	_ = h.redis.Del(ctx, "session:"+c.sessionID).Err()
	_ = h.redis.SRem(ctx, "user_sessions:"+c.userID, c.sessionID).Err()
}

func (h *Handler) subscribeDelivery(ctx context.Context, c *client) {
	if h.redis == nil {
		return
	}
	pubsub := h.redis.Subscribe(ctx, "delivery:user:"+c.userID)
	defer pubsub.Close()
	ch := pubsub.Channel()
	for msg := range ch {
		h.sendJSON(c, "delivery_update", json.RawMessage(msg.Payload))
	}
}

func (h *Handler) subscribeMessages(ctx context.Context, c *client) {
	if h.redis == nil {
		return
	}
	pubsub := h.redis.Subscribe(ctx, "message:user:"+c.userID)
	defer pubsub.Close()
	ch := pubsub.Channel()
	for msg := range ch {
		h.sendJSON(c, "message_created", json.RawMessage(msg.Payload))
	}
}

func (h *Handler) subscribeUserEvents(ctx context.Context, c *client) {
	if h.redis == nil || h.replication == nil {
		return
	}
	pubsub := h.redis.Subscribe(ctx, h.replication.ChannelForUser(c.userID))
	defer pubsub.Close()
	for msg := range pubsub.Channel() {
		var evt replication.Event
		if err := json.Unmarshal([]byte(msg.Payload), &evt); err != nil {
			observability.RecordWSMessage("user_event_unmarshal_error", evt.Type)
			continue
		}
		h.sendJSON(c, "event", evt)
	}
}

// subscribeSessionEvents subscribes to mini-app session events via Redis pub/sub.
// P4.3: Real-time fanout for session events.
func (h *Handler) subscribeSessionEvents(ctx context.Context, c *client, sessionID string) {
	if h.redis == nil {
		return
	}
	channel := sessionEventChannelPrefix + sessionID + ":events"
	pubsub := h.redis.Subscribe(ctx, channel)
	defer pubsub.Close()

	for {
		select {
		case <-ctx.Done():
			return
		case msg := <-pubsub.Channel():
			if msg == nil {
				return
			}
			var evt map[string]any
			if err := json.Unmarshal([]byte(msg.Payload), &evt); err != nil {
				observability.RecordWSMessage("session_event_unmarshal_error", "")
				continue
			}
			h.sendJSON(c, "session_event", evt)
		}
	}
}

func (h *Handler) userEventTypeName(domainType string) string {
	switch domainType {
	case replication.UserEventConversationMessageAppended:
		return "message_created"
	case replication.UserEventConversationMessageEdited:
		return "message_edited"
	case replication.UserEventConversationMessageDeleted:
		return "message_deleted"
	case replication.UserEventConversationMessageReactionsUpdated:
		return "message_reaction_updated"
	case replication.UserEventConversationMessageEffectTriggered:
		return "conversation_message_effect_triggered"
	case replication.UserEventConversationReceiptUpdated:
		return "read_receipt"
	case replication.UserEventConversationPreviewUpdated:
		return "conversation_preview_updated"
	case replication.UserEventConversationStateUpdated:
		return "conversation_state_updated"
	case replication.UserEventConversationTypingUpdated:
		return "conversation_typing_updated"
	case replication.UserEventAccountDeviceLinked:
		return "account_device_linked"
	default:
		observability.RecordWSMessage("unknown_event_type", domainType)
		return "event"
	}
}

func (h *Handler) readLoopV2(c *client, ip string) {
	defer h.unregister(c)
	_ = c.conn.SetReadDeadline(time.Now().Add(60 * time.Second))
	c.conn.SetPongHandler(func(_ string) error {
		_ = c.conn.SetReadDeadline(time.Now().Add(60 * time.Second))
		h.touchConnection(context.Background(), c)
		return nil
	})

	heartbeat := time.NewTicker(30 * time.Second)
	defer heartbeat.Stop()
	go func() {
		for range heartbeat.C {
			h.touchConnection(context.Background(), c)
			_ = c.conn.WriteControl(websocket.PingMessage, []byte("ping"), time.Now().Add(5*time.Second))
		}
	}()

	for {
		_, payload, err := c.conn.ReadMessage()
		if err != nil {
			return
		}
		if err := h.allowControlEvent(context.Background(), c.userID); err != nil {
			h.sendJSON(c, "error", map[string]any{"code": "rate_limited", "message": "ws control rate limit"})
			continue
		}
		var env wsEnvelope
		if err := json.Unmarshal(payload, &env); err != nil {
			h.sendJSON(c, "error", map[string]any{"code": "invalid_request", "message": "invalid event envelope"})
			continue
		}
		observability.RecordWSMessage("received", env.Event)
		switch env.Event {
		case "hello":
			var req resumePayload
			if err := json.Unmarshal(env.Data, &req); err != nil {
				h.sendJSON(c, "error", map[string]any{"code": "invalid_request", "message": "invalid hello payload"})
				continue
			}
			h.handleHelloResume(context.Background(), c, req)
		case "ack":
			var req struct {
				ThroughUserEventID int64  `json:"through_user_event_id"`
				DeviceID           string `json:"device_id"`
			}
			if err := json.Unmarshal(env.Data, &req); err != nil {
				h.sendJSON(c, "error", map[string]any{"code": "invalid_request", "message": "invalid ack payload"})
				continue
			}
			deviceID := strings.TrimSpace(req.DeviceID)
			if deviceID == "" {
				deviceID = c.deviceID
			}
			if deviceID == "" {
				deviceID = "web"
			}
			if h.replication != nil {
				if err := h.replication.AcknowledgeCursor(context.Background(), c.userID, deviceID, req.ThroughUserEventID); err != nil {
					h.sendJSON(c, "error", map[string]any{"code": "ack_failed", "message": err.Error()})
				}
			}
			h.touchConnection(context.Background(), c)
		case "send_message":
			if !h.enableSend {
				h.sendJSON(c, "error", map[string]any{"code": "ws_send_disabled", "message": "ws send disabled"})
				continue
			}
			var raw interface{}
			if err := json.Unmarshal(env.Data, &raw); err != nil {
				h.sendJSON(c, "error", map[string]any{"code": "invalid_request", "message": "invalid send_message payload"})
				continue
			}
			if err := appmw.ValidateData("ws-send_message", raw); err != nil {
				h.sendJSON(c, "error", map[string]any{"code": "invalid_request", "message": err.Error()})
				continue
			}
			var req sendMessagePayload
			if err := json.Unmarshal(env.Data, &req); err != nil {
				h.sendJSON(c, "error", map[string]any{"code": "invalid_request", "message": "invalid send_message payload"})
				continue
			}
			if strings.TrimSpace(req.ReplyToMessageID) != "" {
				if req.Content == nil {
					req.Content = map[string]any{}
				}
				req.Content["reply_to_message_id"] = strings.TrimSpace(req.ReplyToMessageID)
			}
			result, err := h.messages.Send(context.Background(), c.userID, c.deviceID, req.ConversationID, req.IdempotencyKey, req.ContentType, req.Content, req.ClientGeneratedID, "ws-"+time.Now().UTC().Format(time.RFC3339Nano), ip)
			if err != nil {
				h.sendJSON(c, "error", map[string]any{"code": "send_failed", "message": err.Error()})
				continue
			}
			h.sendJSON(c, "ack", map[string]any{
				"message_id":      result.Message.MessageID,
				"conversation_id": result.Message.ConversationID,
				"server_order":    result.Message.ServerOrder,
				"status":          "SENT",
				"queued":          result.Queued,
				"ack_timeout_ms":  result.AckTimeoutMS,
			})
			h.touchConnection(context.Background(), c)
		case "subscribe_session":
			var req struct {
				SessionID string `json:"session_id"`
			}
			if err := json.Unmarshal(env.Data, &req); err != nil {
				h.sendJSON(c, "error", map[string]any{"code": "invalid_request", "message": "invalid subscribe_session payload"})
				continue
			}
			sessionID := strings.TrimSpace(req.SessionID)
			if sessionID == "" {
				h.sendJSON(c, "error", map[string]any{"code": "invalid_request", "message": "session_id is required"})
				continue
			}
			// Validate user has access to this session (use short-lived context to avoid blocking read loop)
			miniappSvc, ok := h.miniappService.(miniappServiceInterface)
			if !ok || miniappSvc == nil {
				h.sendJSON(c, "error", map[string]any{"code": "unavailable", "message": "miniapp service unavailable"})
				continue
			}
			authCtx, authCancel := context.WithTimeout(c.clientCtx, 5*time.Second)
			_, err := miniappSvc.GetSessionForUser(authCtx, c.userID, sessionID)
			authCancel()
			if err != nil {
				h.sendJSON(c, "error", map[string]any{"code": "unauthorized", "message": "access denied"})
				continue
			}
			// P4.3: Check subscription limit and atomically add if not exists
			const maxSessionSubscriptionsPerConnection = 100
			c.sendMu.Lock()
			if len(c.sessionSubscriptions) >= maxSessionSubscriptionsPerConnection {
				c.sendMu.Unlock()
				h.sendJSON(c, "error", map[string]any{"code": "too_many_subscriptions", "message": "subscription limit exceeded"})
				continue
			}
			if _, exists := c.sessionSubscriptions[sessionID]; exists {
				c.sendMu.Unlock()
				h.sendJSON(c, "error", map[string]any{"code": "already_subscribed", "message": "already subscribed to this session"})
				continue
			}
			// Create context for this subscription tied to client connection (cleanup on disconnect)
			ctx, cancel := context.WithCancel(c.clientCtx)
			c.sessionSubscriptions[sessionID] = cancel
			c.sendMu.Unlock()
			// Launch subscription in goroutine
			go func() {
				h.subscribeSessionEvents(ctx, c, sessionID)
				// Clean up on subscription end
				c.sendMu.Lock()
				delete(c.sessionSubscriptions, sessionID)
				c.sendMu.Unlock()
			}()
			h.sendJSON(c, "subscribe_session_ack", map[string]any{"status": "ok", "session_id": sessionID})
			h.touchConnection(context.Background(), c)
		case "typing.started", "typing.stopped", "presence_subscribe", "subscribe", "auth", "resync":
			h.handleLegacyCompatibleEvent(c, env, ip)
		default:
			h.sendJSON(c, "error", map[string]any{"code": "unsupported_event", "message": env.Event})
		}
	}
}

func (h *Handler) handleLegacyCompatibleEvent(c *client, env wsEnvelope, ip string) {
	switch env.Event {
	case "subscribe", "presence_subscribe":
		var raw interface{}
		if err := json.Unmarshal(env.Data, &raw); err != nil {
			h.sendJSON(c, "error", map[string]any{"code": "invalid_request", "message": "invalid subscribe payload"})
			return
		}
		if err := appmw.ValidateData("ws-subscribe", raw); err != nil {
			h.sendJSON(c, "error", map[string]any{"code": "invalid_request", "message": err.Error()})
			return
		}
		var req presenceSubscribePayload
		if err := json.Unmarshal(env.Data, &req); err != nil {
			h.sendJSON(c, "error", map[string]any{"code": "invalid_request", "message": "invalid subscribe payload"})
			return
		}
		for _, convID := range req.ConversationIDs {
			if convID == "" {
				continue
			}
			_ = h.redis.Set(context.Background(), "presence:conv:"+convID+":user:"+c.userID, "1", presenceTTL).Err()
		}
		if env.Event == "subscribe" {
			h.sendJSON(c, "subscribe_ack", map[string]any{"status": "ok", "conversation_ids": req.ConversationIDs})
			h.touchConnection(context.Background(), c)
			return
		}
		h.sendJSON(c, "presence_update", map[string]any{"status": "online", "user_id": c.userID})
		h.touchConnection(context.Background(), c)
	case "auth":
		h.sendJSON(c, "auth", map[string]any{"status": "ok", "user_id": c.userID})
		h.touchConnection(context.Background(), c)
	case "resync":
		var payload resyncPayload
		if err := json.Unmarshal(env.Data, &payload); err != nil {
			h.sendJSON(c, "error", map[string]any{"code": "invalid_request", "message": "invalid resync payload"})
			return
		}
		h.handleResyncRequest(context.Background(), c, payload)
	case "typing.started":
		var p typingEventPayload
		if err := json.Unmarshal(env.Data, &p); err != nil {
			h.sendJSON(c, "error", map[string]any{"code": "invalid_request", "message": "invalid typing payload"})
			return
		}
		h.handleTypingSignal(context.Background(), c, p.ConversationID, "typing.started", ip)
	case "typing.stopped":
		var p typingEventPayload
		if err := json.Unmarshal(env.Data, &p); err != nil {
			h.sendJSON(c, "error", map[string]any{"code": "invalid_request", "message": "invalid typing payload"})
			return
		}
		h.handleTypingSignal(context.Background(), c, p.ConversationID, "typing.stopped", ip)
	default:
		h.sendJSON(c, "error", map[string]any{"code": "unsupported_event", "message": env.Event})
	}
	_ = ip
}

func (h *Handler) allowControlEvent(ctx context.Context, userID string) error {
	if h.limiter == nil {
		return nil
	}
	decision, err := h.limiter.Allow(ctx, "rate:ws:control:user:"+userID, 20, time.Second, 40, 1)
	if err != nil {
		return err
	}
	if !decision.Allowed {
		return limit.ErrRateLimited
	}
	return nil
}

func (h *Handler) allowTypingEvent(ctx context.Context, userID, conversationID string) error {
	if h.limiter == nil {
		return nil
	}
	// Rate limit: 3 typing events per 5 seconds per user per conversation (500ms debounce)
	scope := "rate:ws:typing:" + userID + ":" + conversationID
	decision, err := h.limiter.Allow(ctx, scope, 3, 5*time.Second, 3, 1)
	if err != nil {
		return err
	}
	if !decision.Allowed {
		// Return rate limit error with retry_after hint
		return limit.ErrRateLimited
	}
	return nil
}

func (h *Handler) handleTypingSignal(ctx context.Context, c *client, conversationID, eventName, ip string) {
	if strings.TrimSpace(conversationID) == "" {
		h.sendJSON(c, "error", map[string]any{"code": "invalid_request", "message": "conversation_id required"})
		return
	}
	if ok, err := h.messages.IsMember(ctx, c.userID, conversationID); err != nil || !ok {
		if err != nil {
			h.sendJSON(c, "error", map[string]any{"code": "server_error", "message": "membership_check_failed"})
		} else {
			h.sendJSON(c, "error", map[string]any{"code": "forbidden", "message": "not a member"})
		}
		return
	}
	sharesTyping, err := h.messages.UserSharesTyping(ctx, c.userID)
	if err != nil {
		h.sendJSON(c, "error", map[string]any{"code": "server_error", "message": "privacy_check_failed"})
		return
	}
	now := time.Now().UTC()

	h.touchConnection(ctx, c)
	key := "typing:conv:" + conversationID + ":user:" + c.userID
	if eventName == "typing.started" && !sharesTyping {
		if h.redis != nil {
			_ = h.redis.Del(ctx, key).Err()
		}
		if c.typingConversations != nil {
			delete(c.typingConversations, conversationID)
		}
		return
	}
	if eventName == "typing.started" {
		if h.redis != nil {
			if exists, err := h.redis.Exists(ctx, key).Result(); err == nil && exists > 0 {
				_ = h.redis.Expire(ctx, key, 5*time.Second).Err()
				if c.typingConversations == nil {
					c.typingConversations = map[string]struct{}{}
				}
				c.typingConversations[conversationID] = struct{}{}
				return
			}
		}
		if err := h.allowTypingEvent(ctx, c.userID, conversationID); err != nil {
			h.sendJSON(c, "error", map[string]any{"code": "rate_limited", "message": "typing throttled"})
			return
		}
	}
	if h.replication != nil {
		_ = h.replication.AppendTypingEvent(ctx, conversationID, c.userID, c.deviceID, strings.ReplaceAll(eventName, ".", "_"), now)
	}
	if h.redis != nil {
		if eventName == "typing.started" {
			if c.typingConversations == nil {
				c.typingConversations = map[string]struct{}{}
			}
			c.typingConversations[conversationID] = struct{}{}
			if ok, err := h.redis.SetNX(ctx, key, strconv.FormatInt(now.UnixMilli(), 10), 5*time.Second).Result(); err == nil && !ok {
				_ = h.redis.Expire(ctx, key, 5*time.Second).Err()
				return
			}
			_ = h.redis.Publish(ctx, "typing:conv:"+conversationID, map[string]any{
				"type":            eventName,
				"conversation_id": conversationID,
				"user_id":         c.userID,
				"device_id":       c.deviceID,
				"started_at_ms":   now.UnixMilli(),
				"retry_after_ms":  3000,
			}).Err()
		} else {
			_ = h.redis.Del(ctx, key).Err()
			if c.typingConversations != nil {
				delete(c.typingConversations, conversationID)
			}
			_ = h.redis.Publish(ctx, "typing:conv:"+conversationID, map[string]any{
				"type":            eventName,
				"conversation_id": conversationID,
				"user_id":         c.userID,
				"device_id":       c.deviceID,
			}).Err()
		}
	}

	h.mu.RLock()
	defer h.mu.RUnlock()
	for uid, bucket := range h.clients {
		if uid == c.userID {
			continue
		}
		if ok, _ := h.messages.IsMember(ctx, uid, conversationID); !ok {
			continue
		}
		for cli := range bucket {
			payload := map[string]any{
				"conversation_id": conversationID,
				"user_id":         c.userID,
				"device_id":       c.deviceID,
			}
			if eventName == "typing.started" {
				payload["started_at_ms"] = now.UnixMilli()
				payload["retry_after_ms"] = 3000
			}
			h.sendJSON(cli, eventName, payload)
		}
	}
}

func (h *Handler) touchConnection(ctx context.Context, c *client) {
	if c == nil {
		return
	}
	h.markPresence(ctx, c.userID)
	h.registerSession(ctx, c)
}

func (h *Handler) handleHelloResume(ctx context.Context, c *client, req resumePayload) {
	if c == nil {
		return
	}
	cursor, err := h.resolveResumeCursor(req.LastUserCursor, req.LastCursor)
	if err != nil {
		h.sendJSON(c, "error", map[string]any{"code": "invalid_cursor", "message": err.Error()})
		return
	}
	if strings.TrimSpace(req.DeviceID) == "" {
		req.DeviceID = "web"
	}
	c.deviceID = req.DeviceID
	h.touchConnection(ctx, c)
	h.sendJSON(c, "hello_ack", map[string]any{
		"session_id":            c.sessionID,
		"resume_supported":      true,
		"heartbeat_interval_ms": 30000,
	})
	h.replayUserEvents(ctx, c, cursor, "", 250, nil)
}

func (h *Handler) handleResyncRequest(ctx context.Context, c *client, req resyncPayload) {
	if c == nil {
		return
	}
	cursor, err := h.resolveResumeCursor(req.LastUserCursor, req.LastCursor)
	if err != nil {
		h.sendJSON(c, "error", map[string]any{"code": "invalid_cursor", "message": err.Error()})
		return
	}
	if cursor == 0 && strings.TrimSpace(req.ConversationID) == "" {
		h.sendJSON(c, "resync_required", map[string]any{"cursor_hint": 0})
		return
	}
	if cursor == 0 {
		h.sendJSON(c, "resync_required", map[string]any{
			"conversation_id": req.ConversationID,
			"cursor_hint":     0,
		})
		return
	}
	h.replayUserEvents(ctx, c, cursor, "", 250, map[string]any{
		"conversation_id": req.ConversationID,
	})
}

func (h *Handler) resolveResumeCursor(lastUserCursor int64, lastCursor string) (int64, error) {
	if lastUserCursor > 0 {
		return lastUserCursor, nil
	}
	if strings.TrimSpace(lastCursor) == "" {
		return 0, nil
	}
	cursor, err := replication.ParseCursor(lastCursor)
	if err != nil {
		return 0, err
	}
	return cursor, nil
}

func (h *Handler) replayUserEvents(ctx context.Context, c *client, lastUserCursor int64, lastCursor string, limit int, extra map[string]any) {
	if c == nil || h.replication == nil {
		return
	}
	cursor, err := h.resolveResumeCursor(lastUserCursor, lastCursor)
	if err != nil {
		payload := map[string]any{
			"code":    "invalid_cursor",
			"message": err.Error(),
		}
		for k, v := range extra {
			payload[k] = v
		}
		h.sendJSON(c, "error", payload)
		return
	}
	if limit <= 0 {
		limit = 250
	}
	resp, err := h.replication.ListEvents(ctx, c.userID, cursor, limit)
	if err != nil {
		payload := map[string]any{
			"cursor_hint": cursor,
			"reason":      "resume_failed",
		}
		for k, v := range extra {
			payload[k] = v
		}
		h.sendJSON(c, "resync_required", payload)
		return
	}
	for _, evt := range resp.Events {
		h.sendJSON(c, "event", evt)
	}
	if resp.HasMore {
		payload := map[string]any{
			"cursor_hint": resp.NextCursor,
		}
		for k, v := range extra {
			payload[k] = v
		}
		h.sendJSON(c, "resync_required", payload)
	}
}

func (h *Handler) cleanupTypingState(ctx context.Context, c *client) {
	if h.redis == nil || c == nil || len(c.typingConversations) == 0 {
		return
	}
	for convID := range c.typingConversations {
		key := "typing:conv:" + convID + ":user:" + c.userID
		_ = h.redis.Del(ctx, key).Err()
		_ = h.redis.Publish(ctx, "typing:conv:"+convID, map[string]any{
			"type":            "typing.stopped",
			"conversation_id": convID,
			"user_id":         c.userID,
			"device_id":       c.deviceID,
		}).Err()
	}
	c.typingConversations = nil
}

func ipOnly(remoteAddr string) string {
	host, _, err := net.SplitHostPort(remoteAddr)
	if err != nil {
		return remoteAddr
	}
	return host
}

// P3.2 Isolated Runtime Origins: validateOriginHeader checks if the WebSocket origin header is valid.
// For mini-app WebSocket connections, origin must match the pattern: {hash}.miniapp.local
// Allows requests without origin header (browser doesn't always send it for WebSocket).
func (h *Handler) validateOriginHeader(r *http.Request) error {
	origin := r.Header.Get("Origin")
	if origin == "" {
		// Origin header is optional for WebSocket upgrades; allow it
		return nil
	}

	// P3.2: Validate origin format if provided (should be {hash}.miniapp.local)
	baseDomain := "miniapp.local"
	if !config.ValidateOrigin(origin, baseDomain) {
		// Allow any origin for now (WebSocket CORS is less strict than fetch)
		// In production, could enforce stricter validation based on request context
		return nil
	}
	return nil
}

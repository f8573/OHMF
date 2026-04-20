package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"net/http"
	"net/url"
	"strings"
	"sync"
	"time"

	"github.com/gorilla/websocket"
)

type population struct {
	users         []*loadUser
	devices       []*deviceClient
	conversations []*conversationFixture
}

type loadUser struct {
	UserID    string
	PhoneE164 string
	Devices   []*deviceClient
}

type conversationFixture struct {
	ID            string
	Participants  []*loadUser
	SenderDevices []*deviceClient
	AllDeviceIDs  []string
	nextSender    int
}

type deviceClient struct {
	userID       string
	deviceID     string
	phoneE164    string
	accessToken  string
	refreshToken string
	baseURL      string
	httpClient   *http.Client
	reducer      *reducer
	events       *jsonlWriter
	phase        string

	wsMu          sync.Mutex
	writeMu       sync.Mutex
	reconnectMu   sync.Mutex
	tokenMu       sync.RWMutex
	conn          *websocket.Conn
	pendingMu     sync.Mutex
	pendingSend   *pendingSend
	lastEventMu   sync.RWMutex
	lastUserEvent int64
	sendMu        sync.Mutex
}

type pendingSend struct {
	startedAt         time.Time
	conversationID    string
	targetDeviceIDs   []string
	clientGeneratedID string
	firstAckAt        time.Time
	ackCh             chan ackPayload
	errCh             chan error
}

func (p *pendingSend) observeAck(ack ackPayload, observedAt time.Time) (time.Time, bool) {
	if p == nil {
		return time.Time{}, false
	}
	if p.firstAckAt.IsZero() {
		p.firstAckAt = observedAt
	}
	if strings.TrimSpace(ack.MessageID) == "" || ack.Queued || ack.ServerOrder <= 0 {
		return time.Time{}, false
	}
	return p.firstAckAt, true
}

type wsEnvelope struct {
	Event string          `json:"event"`
	Data  json.RawMessage `json:"data"`
}

type helloPayload struct {
	DeviceID       string `json:"device_id"`
	LastUserCursor int64  `json:"last_user_cursor,omitempty"`
}

type sendMessagePayload struct {
	ConversationID    string         `json:"conversation_id"`
	IdempotencyKey    string         `json:"idempotency_key"`
	ContentType       string         `json:"content_type"`
	Content           map[string]any `json:"content"`
	ClientGeneratedID string         `json:"client_generated_id,omitempty"`
}

type ackPayload struct {
	MessageID      string `json:"message_id"`
	ConversationID string `json:"conversation_id"`
	ServerOrder    int64  `json:"server_order"`
	Status         string `json:"status"`
	Queued         bool   `json:"queued"`
	AckTimeoutMS   int64  `json:"ack_timeout_ms"`
}

type userEvent struct {
	UserEventID int64          `json:"user_event_id"`
	Type        string         `json:"type"`
	CreatedAt   string         `json:"created_at"`
	Payload     map[string]any `json:"payload"`
}

type jsonConn interface {
	WriteJSON(v any) error
}

func bootstrapPopulation(ctx context.Context, cfg config, phase string, targetDevices int, fixedUsers int, events *jsonlWriter) (*population, error) {
	httpClient := &http.Client{Timeout: 15 * time.Second}
	usersTarget := fixedUsers
	if usersTarget <= 0 {
		pairedDevices := int(math.Round(float64(targetDevices) * (0.3 / 1.3)))
		if pairedDevices < 0 {
			pairedDevices = 0
		}
		usersTarget = targetDevices - pairedDevices
	}
	if usersTarget < 2 {
		usersTarget = 2
	}
	pairedUsers := targetDevices - usersTarget
	if pairedUsers < 0 {
		pairedUsers = 0
	}
	if pairedUsers > usersTarget {
		pairedUsers = usersTarget
	}

	pop := &population{}
	for i := 0; i < usersTarget; i++ {
		primary, err := createVerifiedDevice(ctx, httpClient, cfg, phase, i, "primary", events)
		if err != nil {
			return nil, err
		}
		user := &loadUser{
			UserID:    primary.userID,
			PhoneE164: primary.phoneE164,
			Devices:   []*deviceClient{primary},
		}
		pop.users = append(pop.users, user)
		pop.devices = append(pop.devices, primary)
	}

	for i := 0; i < pairedUsers; i++ {
		user := pop.users[i]
		linked, err := createPairedDevice(ctx, httpClient, cfg, user.Devices[0], phase, i, events)
		if err != nil {
			return nil, err
		}
		user.Devices = append(user.Devices, linked)
		pop.devices = append(pop.devices, linked)
	}

	conversations, err := createConversationFixtures(ctx, httpClient, cfg, pop.users)
	if err != nil {
		return nil, err
	}
	pop.conversations = conversations
	return pop, nil
}

func createVerifiedDevice(ctx context.Context, client *http.Client, cfg config, phase string, index int, suffix string, events *jsonlWriter) (*deviceClient, error) {
	phone := buildPhone(cfg.runID, index)
	authStartedAt := time.Now().UTC()
	writeRawEvent(events, rawEvent{
		Timestamp: authStartedAt,
		Type:      "t_auth_start",
		Phase:     phase,
		Value:     phone,
	})
	start, err := postJSONWithRetry(ctx, client, strings.TrimRight(cfg.baseURL, "/")+"/v1/auth/phone/start", "", map[string]any{
		"phone_e164": phone,
		"channel":    "SMS",
	})
	if err != nil {
		recordPhaseFailure(events, "provision", classifyFailureReason(err), err)
		return nil, err
	}
	challengeID := textField(start["challenge_id"])
	verify, err := postJSONWithRetry(ctx, client, strings.TrimRight(cfg.baseURL, "/")+"/v1/auth/phone/verify", "", map[string]any{
		"challenge_id": challengeID,
		"otp_code":     "123456",
		"device": map[string]any{
			"platform":     "WEB",
			"device_name":  fmt.Sprintf("load-%s-%s-%d", phase, suffix, index),
			"capabilities": []string{"DEVICE_PAIRING_V1", "WEB_PUSH_V1", "MINI_APPS"},
		},
	})
	if err != nil {
		recordPhaseFailure(events, "provision", classifyFailureReason(err), err)
		return nil, err
	}

	userMap, _ := verify["user"].(map[string]any)
	deviceMap, _ := verify["device"].(map[string]any)
	tokensMap, _ := verify["tokens"].(map[string]any)
	device := &deviceClient{
		userID:       textField(userMap["user_id"]),
		deviceID:     textField(deviceMap["device_id"]),
		phoneE164:    phone,
		accessToken:  textField(tokensMap["access_token"]),
		refreshToken: textField(tokensMap["refresh_token"]),
		baseURL:      cfg.baseURL,
		httpClient:   client,
		phase:        phase,
	}
	writeRawEvent(events, rawEvent{
		Timestamp: time.Now().UTC(),
		Type:      "t_auth_verified",
		Phase:     phase,
		UserID:    device.userID,
		DeviceID:  device.deviceID,
		Value:     time.Since(authStartedAt).String(),
	})
	return device, nil
}

func createPairedDevice(ctx context.Context, client *http.Client, cfg config, primary *deviceClient, phase string, index int, events *jsonlWriter) (*deviceClient, error) {
	pairStartedAt := time.Now().UTC()
	writeRawEvent(events, rawEvent{
		Timestamp: pairStartedAt,
		Type:      "t_pair_start",
		Phase:     phase,
		UserID:    primary.userID,
		DeviceID:  primary.deviceID,
	})
	start, err := postJSONWithRetry(ctx, client, strings.TrimRight(cfg.baseURL, "/")+"/v1/auth/pairing/start", primary.accessToken, map[string]any{})
	if err != nil {
		recordPhaseFailure(events, "provision", classifyFailureReason(err), err)
		return nil, err
	}
	pairCode := textField(start["pairing_code"])
	complete, err := postJSONWithRetry(ctx, client, strings.TrimRight(cfg.baseURL, "/")+"/v1/auth/pairing/complete", "", map[string]any{
		"pairing_code": pairCode,
		"device": map[string]any{
			"platform":     "WEB",
			"device_name":  fmt.Sprintf("load-%s-linked-%d", phase, index),
			"capabilities": []string{"DEVICE_PAIRING_V1", "WEB_PUSH_V1", "MINI_APPS"},
		},
	})
	if err != nil {
		recordPhaseFailure(events, "provision", classifyFailureReason(err), err)
		return nil, err
	}
	device := &deviceClient{
		userID:       textField(complete["user_id"]),
		deviceID:     textField(complete["device_id"]),
		phoneE164:    primary.phoneE164,
		accessToken:  textField(complete["access_token"]),
		refreshToken: textField(complete["refresh_token"]),
		baseURL:      cfg.baseURL,
		httpClient:   client,
		phase:        phase,
	}
	writeRawEvent(events, rawEvent{
		Timestamp: time.Now().UTC(),
		Type:      "t_pair_complete",
		Phase:     phase,
		UserID:    device.userID,
		DeviceID:  device.deviceID,
		Value:     time.Since(pairStartedAt).String(),
	})
	return device, nil
}

func createConversationFixtures(ctx context.Context, client *http.Client, cfg config, users []*loadUser) ([]*conversationFixture, error) {
	if len(users) == 0 {
		return nil, nil
	}
	fixtures := make([]*conversationFixture, 0)
	for _, group := range partitionConversationGroups(users, 4) {

		participantIDs := make([]string, 0, len(group)-1)
		for i := 1; i < len(group); i++ {
			participantIDs = append(participantIDs, group[i].UserID)
		}
		created, err := postJSONWithRetry(ctx, client, strings.TrimRight(cfg.baseURL, "/")+"/v1/conversations", group[0].Devices[0].accessToken, map[string]any{
			"type":         "GROUP",
			"participants": participantIDs,
			"title":        fmt.Sprintf("Load Conversation %d", len(fixtures)+1),
		})
		if err != nil {
			return nil, err
		}
		conversationID := textField(created["conversation_id"])
		if _, err := patchJSONRequest(ctx, client, strings.TrimRight(cfg.baseURL, "/")+"/v1/conversations/"+conversationID+"/metadata", group[0].Devices[0].accessToken, map[string]any{
			"encryption_state": "PLAINTEXT",
		}); err != nil {
			return nil, err
		}

		fixture := &conversationFixture{ID: conversationID, Participants: group}
		for _, participant := range group {
			fixture.SenderDevices = append(fixture.SenderDevices, participant.Devices...)
			for _, device := range participant.Devices {
				fixture.AllDeviceIDs = append(fixture.AllDeviceIDs, device.deviceID)
			}
		}
		fixtures = append(fixtures, fixture)
	}
	return fixtures, nil
}

func partitionConversationGroups(users []*loadUser, preferredSize int) [][]*loadUser {
	if preferredSize <= 1 || len(users) == 0 {
		return [][]*loadUser{users}
	}
	groups := make([][]*loadUser, 0, (len(users)+preferredSize-1)/preferredSize)
	for offset := 0; offset < len(users); {
		remaining := len(users) - offset
		size := preferredSize
		if remaining <= preferredSize {
			size = remaining
		} else if remaining == preferredSize+1 {
			size = remaining
		}
		end := offset + size
		groups = append(groups, users[offset:end])
		offset = end
	}
	return groups
}

func (c *conversationFixture) nextSenderDevice() *deviceClient {
	if len(c.SenderDevices) == 0 {
		return nil
	}
	device := c.SenderDevices[c.nextSender%len(c.SenderDevices)]
	c.nextSender++
	return device
}

func (c *conversationFixture) targetDeviceIDs(senderDeviceID string) []string {
	targets := make([]string, 0)
	if len(c.AllDeviceIDs) > 0 {
		for _, deviceID := range c.AllDeviceIDs {
			if deviceID == senderDeviceID {
				continue
			}
			targets = append(targets, deviceID)
		}
		return targets
	}
	for _, participant := range c.Participants {
		for _, device := range participant.Devices {
			if device.deviceID == senderDeviceID {
				continue
			}
			targets = append(targets, device.deviceID)
		}
	}
	return targets
}

func (c *deviceClient) attach(reducer *reducer, events *jsonlWriter, phase string) {
	c.reducer = reducer
	c.events = events
	c.phase = phase
}

func (c *deviceClient) connect(ctx context.Context, reconnectID string) error {
	if reconnectID != "" {
		c.reducer.startReconnect(reconnectID, c.deviceID, time.Now().UTC())
	}
	deadline := time.Now().Add(45 * time.Second)
	var lastErr error
	for {
		if ctx.Err() != nil {
			if lastErr != nil {
				return lastErr
			}
			return ctx.Err()
		}
		writeRawEvent(c.events, rawEvent{
			Timestamp:   time.Now().UTC(),
			Type:        "t_ws_dial_start",
			Phase:       c.phase,
			UserID:      c.userID,
			DeviceID:    c.deviceID,
			ReconnectID: reconnectID,
		})
		conn, err := c.dial(ctx)
		if err != nil {
			lastErr = err
			if refreshErr := c.refresh(ctx); refreshErr == nil {
				conn, err = c.dial(ctx)
			}
		}
		if err != nil {
			lastErr = err
			if time.Now().After(deadline) {
				return lastErr
			}
			time.Sleep(1 * time.Second)
			continue
		}
		writeRawEvent(c.events, rawEvent{
			Timestamp:   time.Now().UTC(),
			Type:        "t_ws_upgrade_complete",
			Phase:       c.phase,
			UserID:      c.userID,
			DeviceID:    c.deviceID,
			ReconnectID: reconnectID,
		})

		if err := c.writeConnJSON(conn, map[string]any{
			"event": "hello",
			"data": helloPayload{
				DeviceID:       c.deviceID,
				LastUserCursor: c.lastUserCursor(),
			},
		}); err != nil {
			lastErr = err
			_ = c.events.write(rawEvent{
				Timestamp: time.Now().UTC(),
				Type:      "ws_write_error",
				Phase:     c.phase,
				UserID:    c.userID,
				DeviceID:  c.deviceID,
				Value:     err.Error(),
			})
			_ = conn.Close()
			if time.Now().After(deadline) {
				return lastErr
			}
			time.Sleep(1 * time.Second)
			continue
		}
		writeRawEvent(c.events, rawEvent{
			Timestamp:   time.Now().UTC(),
			Type:        "t_hello_sent",
			Phase:       c.phase,
			UserID:      c.userID,
			DeviceID:    c.deviceID,
			ReconnectID: reconnectID,
		})

		if err := conn.SetReadDeadline(time.Now().Add(10 * time.Second)); err != nil {
			lastErr = err
			_ = conn.Close()
			if time.Now().After(deadline) {
				return lastErr
			}
			time.Sleep(1 * time.Second)
			continue
		}
		helloAck := false
		for !helloAck {
			_, payload, err := conn.ReadMessage()
			if err != nil {
				lastErr = err
				_ = c.events.write(rawEvent{
					Timestamp: time.Now().UTC(),
					Type:      "ws_read_error",
					Phase:     c.phase,
					UserID:    c.userID,
					DeviceID:  c.deviceID,
					Value:     err.Error(),
				})
				_ = conn.Close()
				break
			}
			handledHello, _, handleErr := c.handleEnvelope(ctx, conn, payload)
			if handleErr != nil {
				lastErr = handleErr
				_ = c.events.write(rawEvent{
					Timestamp: time.Now().UTC(),
					Type:      "ws_handle_error",
					Phase:     c.phase,
					UserID:    c.userID,
					DeviceID:  c.deviceID,
					Value:     handleErr.Error(),
				})
				_ = conn.Close()
				break
			}
			if handledHello {
				helloAck = true
				if reconnectID != "" {
					c.reducer.markReconnectReady(reconnectID, time.Now().UTC())
				}
			}
		}
		if !helloAck {
			if time.Now().After(deadline) {
				return lastErr
			}
			time.Sleep(1 * time.Second)
			continue
		}
		_ = conn.SetReadDeadline(time.Time{})

		c.setConn(conn)
		if reconnectID != "" {
			c.reducer.markReconnectSynced(reconnectID, time.Now().UTC(), true)
		}
		writeRawEvent(c.events, rawEvent{Timestamp: time.Now().UTC(), Type: "t_sync_complete", Phase: c.phase, UserID: c.userID, DeviceID: c.deviceID, ReconnectID: reconnectID})
		go c.readLoop()
		return nil
	}
}

func (c *deviceClient) dial(ctx context.Context) (*websocket.Conn, error) {
	wsURL := websocketURL(c.baseURL)
	query := wsURL.Query()
	query.Set("access_token", c.accessTokenValue())
	wsURL.RawQuery = query.Encode()
	dialer := websocket.Dialer{HandshakeTimeout: 10 * time.Second}
	conn, _, err := dialer.DialContext(ctx, wsURL.String(), nil)
	return conn, err
}

func (c *deviceClient) readLoop() {
	conn := c.currentConn()
	if conn == nil {
		return
	}
	for {
		_, payload, err := conn.ReadMessage()
		if err != nil {
			c.dropConn(conn)
			_ = c.events.write(rawEvent{
				Timestamp: time.Now().UTC(),
				Type:      "ws_read_error",
				Phase:     c.phase,
				UserID:    c.userID,
				DeviceID:  c.deviceID,
				Value:     err.Error(),
			})
			return
		}
		if _, _, err := c.handleEnvelope(context.Background(), conn, payload); err != nil {
			_ = c.events.write(rawEvent{
				Timestamp: time.Now().UTC(),
				Type:      "ws_handle_error",
				Phase:     c.phase,
				UserID:    c.userID,
				DeviceID:  c.deviceID,
				Value:     err.Error(),
			})
		}
	}
}

func (c *deviceClient) handleEnvelope(ctx context.Context, conn *websocket.Conn, payload []byte) (bool, bool, error) {
	var env wsEnvelope
	if err := json.Unmarshal(payload, &env); err != nil {
		return false, false, err
	}
	_ = c.events.write(rawEvent{
		Timestamp: time.Now().UTC(),
		Type:      "ws_frame",
		Phase:     c.phase,
		UserID:    c.userID,
		DeviceID:  c.deviceID,
		Value:     env.Event,
	})

	switch env.Event {
	case "hello_ack":
		writeRawEvent(c.events, rawEvent{Timestamp: time.Now().UTC(), Type: "t_ws_ready", Phase: c.phase, UserID: c.userID, DeviceID: c.deviceID})
		return true, false, nil
	case "ack":
		var ack ackPayload
		if err := json.Unmarshal(env.Data, &ack); err != nil {
			return false, false, err
		}
		c.pendingMu.Lock()
		pending := c.pendingSend
		c.pendingMu.Unlock()
		if pending != nil {
			select {
			case pending.ackCh <- ack:
			default:
			}
		}
		return false, false, nil
	case "error":
		var body map[string]any
		_ = json.Unmarshal(env.Data, &body)
		err := fmt.Errorf("%s", textField(body["message"]))
		c.pendingMu.Lock()
		pending := c.pendingSend
		c.pendingMu.Unlock()
		if pending != nil {
			select {
			case pending.errCh <- err:
			default:
			}
		}
		return false, false, nil
	case "event":
		var evt userEvent
		if err := json.Unmarshal(env.Data, &evt); err != nil {
			return false, false, err
		}
		c.processUserEvent(conn, evt)
		return false, false, nil
	case "resync_required":
		writeRawEvent(c.events, rawEvent{Timestamp: time.Now().UTC(), Type: "resync_required", Phase: c.phase, UserID: c.userID, DeviceID: c.deviceID})
		writeRawEvent(c.events, rawEvent{Timestamp: time.Now().UTC(), Type: "t_replay_start", Phase: c.phase, UserID: c.userID, DeviceID: c.deviceID})
		if err := c.syncFromCursor(ctx); err != nil {
			return false, false, err
		}
		writeRawEvent(c.events, rawEvent{Timestamp: time.Now().UTC(), Type: "t_sync_complete", Phase: c.phase, UserID: c.userID, DeviceID: c.deviceID})
		return false, true, nil
	default:
		return false, false, nil
	}
}

func (c *deviceClient) processUserEvent(conn jsonConn, evt userEvent) {
	if evt.UserEventID > 0 {
		c.lastEventMu.Lock()
		if evt.UserEventID > c.lastUserEvent {
			c.lastUserEvent = evt.UserEventID
		}
		c.lastEventMu.Unlock()
		if conn != nil {
			if err := c.writeConnJSON(conn, map[string]any{
				"event": "ack",
				"data": map[string]any{
					"through_user_event_id": evt.UserEventID,
					"device_id":             c.deviceID,
				},
			}); err != nil {
				_ = c.events.write(rawEvent{
					Timestamp:   time.Now().UTC(),
					Type:        "ws_write_error",
					Phase:       c.phase,
					UserID:      c.userID,
					DeviceID:    c.deviceID,
					UserEventID: evt.UserEventID,
					Value:       err.Error(),
				})
			}
		}
	}
	_ = c.events.write(rawEvent{
		Timestamp:      time.Now().UTC(),
		Type:           "recipient_event",
		Phase:          c.phase,
		UserID:         c.userID,
		DeviceID:       c.deviceID,
		UserEventID:    evt.UserEventID,
		ConversationID: textField(evt.Payload["conversation_id"]),
		Value:          evt.Type,
	})

	if evt.Type != "conversation_message_appended" {
		return
	}
	messageMap, _ := evt.Payload["message"].(map[string]any)
	clientGeneratedID := textField(messageMap["client_generated_id"])
	rec := receiveRecord{
		MessageID:      textField(messageMap["message_id"]),
		ConversationID: textField(messageMap["conversation_id"]),
		DeviceID:       c.deviceID,
		ReceivedAt:     time.Now().UTC(),
		ServerOrder:    int64Field(messageMap["server_order"]),
	}
	c.reducer.recordReceive(rec)
	c.pendingMu.Lock()
	pending := c.pendingSend
	c.pendingMu.Unlock()
	if pending != nil &&
		pending.conversationID == rec.ConversationID &&
		rec.DeviceID == c.deviceID &&
		rec.MessageID != "" &&
		(clientGeneratedID == "" || pending.clientGeneratedID == "" || clientGeneratedID == pending.clientGeneratedID) {
		select {
		case pending.ackCh <- ackPayload{
			MessageID:      rec.MessageID,
			ConversationID: rec.ConversationID,
			ServerOrder:    rec.ServerOrder,
			Status:         "SENT",
		}:
		default:
		}
	}
	_ = c.events.write(rawEvent{
		Timestamp:      rec.ReceivedAt,
		Type:           "t_recipient_receive",
		Phase:          c.phase,
		UserID:         c.userID,
		DeviceID:       c.deviceID,
		MessageID:      rec.MessageID,
		ConversationID: rec.ConversationID,
		ServerOrder:    rec.ServerOrder,
	})
}

func (c *deviceClient) sendText(ctx context.Context, conversationID string, text string, targetDeviceIDs []string) error {
	c.sendMu.Lock()
	defer c.sendMu.Unlock()

	startedAt := time.Now().UTC()
	clientGeneratedID := fmt.Sprintf("%s-%d", c.deviceID, startedAt.UnixNano())
	pending := &pendingSend{
		startedAt:         startedAt,
		conversationID:    conversationID,
		targetDeviceIDs:   targetDeviceIDs,
		clientGeneratedID: clientGeneratedID,
		ackCh:             make(chan ackPayload, 2),
		errCh:             make(chan error, 1),
	}
	c.pendingMu.Lock()
	c.pendingSend = pending
	c.pendingMu.Unlock()
	defer func() {
		c.pendingMu.Lock()
		c.pendingSend = nil
		c.pendingMu.Unlock()
	}()

	content := map[string]any{"text": text}
	payload := map[string]any{
		"event": "send_message",
		"data": sendMessagePayload{
			ConversationID:    conversationID,
			IdempotencyKey:    fmt.Sprintf("%s-%d", c.deviceID, startedAt.UnixNano()),
			ContentType:       "text",
			Content:           content,
			ClientGeneratedID: clientGeneratedID,
		},
	}
	_ = c.events.write(rawEvent{
		Timestamp:      startedAt,
		Type:           "t_client_send",
		Phase:          c.phase,
		UserID:         c.userID,
		DeviceID:       c.deviceID,
		ConversationID: conversationID,
		Details: map[string]any{
			"client_generated_id": clientGeneratedID,
			"target_device_ids":   append([]string(nil), targetDeviceIDs...),
		},
	})

	conn, err := c.ensureConnected(ctx)
	if err != nil {
		return err
	}
	if err := c.writePayloadWithReconnect(ctx, conn, payload); err != nil {
		_ = c.events.write(rawEvent{
			Timestamp:      time.Now().UTC(),
			Type:           "ws_write_error",
			Phase:          c.phase,
			UserID:         c.userID,
			DeviceID:       c.deviceID,
			ConversationID: conversationID,
			Value:          err.Error(),
		})
		return err
	}

	timeout := time.NewTimer(15 * time.Second)
	defer timeout.Stop()

	for {
		select {
		case ack := <-pending.ackCh:
			ackObservedAt := time.Now().UTC()
			ackAt, definitive := pending.observeAck(ack, ackObservedAt)
			if !definitive {
				continue
			}
			c.reducer.recordSend(sendRecord{
				MessageID:       ack.MessageID,
				ConversationID:  ack.ConversationID,
				SenderUserID:    c.userID,
				SenderDeviceID:  c.deviceID,
				ClientSendAt:    startedAt,
				AckAt:           ackAt,
				ServerOrder:     ack.ServerOrder,
				TargetDeviceIDs: targetDeviceIDs,
				TargetCount:     len(targetDeviceIDs),
			})
			_ = c.events.write(rawEvent{
				Timestamp:      ackAt,
				Type:           "t_send_ack",
				Phase:          c.phase,
				UserID:         c.userID,
				DeviceID:       c.deviceID,
				MessageID:      ack.MessageID,
				ConversationID: ack.ConversationID,
				ServerOrder:    ack.ServerOrder,
				Details: map[string]any{
					"queued":                   ack.Queued,
					"client_send_at_unix_nano": startedAt.UnixNano(),
					"target_device_ids":        append([]string(nil), targetDeviceIDs...),
				},
			})
			return nil
		case err := <-pending.errCh:
			return err
		case <-timeout.C:
			return fmt.Errorf("send_message ack timeout for %s", c.deviceID)
		case <-ctx.Done():
			return ctx.Err()
		}
	}
}

func (c *deviceClient) reconnect(ctx context.Context, reason string) error {
	reconnectID := fmt.Sprintf("%s-%d", reason, time.Now().UTC().UnixNano())
	_ = c.events.write(rawEvent{
		Timestamp:   time.Now().UTC(),
		Type:        "t_reconnect_start",
		Phase:       c.phase,
		UserID:      c.userID,
		DeviceID:    c.deviceID,
		ReconnectID: reconnectID,
	})
	c.reconnectMu.Lock()
	defer c.reconnectMu.Unlock()
	c.close()
	return c.connect(ctx, reconnectID)
}

func (c *deviceClient) close() {
	c.wsMu.Lock()
	conn := c.conn
	c.conn = nil
	c.wsMu.Unlock()
	if conn != nil {
		_ = conn.Close()
	}
}

func (c *deviceClient) currentConn() *websocket.Conn {
	c.wsMu.Lock()
	defer c.wsMu.Unlock()
	return c.conn
}

func (c *deviceClient) setConn(conn *websocket.Conn) {
	c.wsMu.Lock()
	c.conn = conn
	c.wsMu.Unlock()
}

func (c *deviceClient) dropConn(conn *websocket.Conn) {
	c.wsMu.Lock()
	current := c.conn
	if current == conn {
		c.conn = nil
	}
	c.wsMu.Unlock()
	if current == conn && current != nil {
		_ = current.Close()
	}
}

func (c *deviceClient) ensureConnected(ctx context.Context) (*websocket.Conn, error) {
	if conn := c.currentConn(); conn != nil {
		return conn, nil
	}
	c.reconnectMu.Lock()
	defer c.reconnectMu.Unlock()
	if conn := c.currentConn(); conn != nil {
		return conn, nil
	}
	if err := c.connect(ctx, ""); err != nil {
		return nil, err
	}
	if conn := c.currentConn(); conn != nil {
		return conn, nil
	}
	return nil, fmt.Errorf("device %s not connected", c.deviceID)
}

func (c *deviceClient) writePayloadWithReconnect(ctx context.Context, conn *websocket.Conn, payload any) error {
	if err := c.writeConnJSON(conn, payload); err == nil {
		return nil
	}
	c.dropConn(conn)
	reconnected, reconnectErr := c.ensureConnected(ctx)
	if reconnectErr != nil {
		return reconnectErr
	}
	return c.writeConnJSON(reconnected, payload)
}

func (c *deviceClient) syncFromCursor(ctx context.Context) error {
	cursor := c.lastUserCursor()
	target := fmt.Sprintf("%s/v2/sync?cursor=%d&limit=1000", strings.TrimRight(c.baseURL, "/"), cursor)
	for {
		payload, err := c.authenticatedGetJSON(ctx, target)
		if err != nil {
			return err
		}
		events, _ := payload["events"].([]any)
		for _, raw := range events {
			item, _ := raw.(map[string]any)
			body, _ := json.Marshal(item)
			var evt userEvent
			if err := json.Unmarshal(body, &evt); err != nil {
				continue
			}
			c.processUserEvent(noopConn{}, evt)
		}
		hasMore, _ := payload["has_more"].(bool)
		if !hasMore {
			return nil
		}
		nextCursor := int64Field(payload["next_cursor"])
		target = fmt.Sprintf("%s/v2/sync?cursor=%d&limit=1000", strings.TrimRight(c.baseURL, "/"), nextCursor)
	}
}

func (c *deviceClient) refresh(ctx context.Context) error {
	payload, err := postJSONRequest(ctx, c.httpClient, strings.TrimRight(c.baseURL, "/")+"/v1/auth/refresh", "", map[string]any{
		"refresh_token": c.refreshTokenValue(),
	})
	if err != nil {
		return err
	}
	tokens, _ := payload["tokens"].(map[string]any)
	c.updateTokens(textField(tokens["access_token"]), textField(tokens["refresh_token"]))
	writeRawEvent(c.events, rawEvent{
		Timestamp: time.Now().UTC(),
		Type:      "token_refresh",
		Phase:     c.phase,
		UserID:    c.userID,
		DeviceID:  c.deviceID,
	})
	return nil
}

func (c *deviceClient) authenticatedGetJSON(ctx context.Context, target string) (map[string]any, error) {
	payload, err := getJSONRequest(ctx, c.httpClient, target, c.accessTokenValue())
	if err == nil || !isUnauthorizedError(err) {
		return payload, err
	}
	if refreshErr := c.refresh(ctx); refreshErr != nil {
		return nil, refreshErr
	}
	return getJSONRequest(ctx, c.httpClient, target, c.accessTokenValue())
}

func (c *deviceClient) accessTokenValue() string {
	c.tokenMu.RLock()
	defer c.tokenMu.RUnlock()
	return c.accessToken
}

func (c *deviceClient) refreshTokenValue() string {
	c.tokenMu.RLock()
	defer c.tokenMu.RUnlock()
	return c.refreshToken
}

func (c *deviceClient) updateTokens(accessToken string, refreshToken string) {
	c.tokenMu.Lock()
	c.accessToken = accessToken
	c.refreshToken = refreshToken
	c.tokenMu.Unlock()
}

func (c *deviceClient) lastUserCursor() int64 {
	c.lastEventMu.RLock()
	defer c.lastEventMu.RUnlock()
	return c.lastUserEvent
}

func buildPhone(runID string, index int) string {
	core := runID
	if len(core) > 6 {
		core = core[len(core)-6:]
	}
	return fmt.Sprintf("+1555%s%04d", core, index)
}

func (c *deviceClient) writeConnJSON(conn jsonConn, payload any) error {
	if conn == nil {
		return errors.New("websocket connection is nil")
	}
	c.writeMu.Lock()
	defer c.writeMu.Unlock()
	return conn.WriteJSON(payload)
}

func websocketURL(baseURL string) url.URL {
	parsed, _ := url.Parse(strings.TrimRight(baseURL, "/") + "/v2/ws")
	switch parsed.Scheme {
	case "https":
		parsed.Scheme = "wss"
	default:
		parsed.Scheme = "ws"
	}
	return *parsed
}

func isTimeout(err error) bool {
	type timeout interface {
		Timeout() bool
	}
	var netErr timeout
	return err != nil && (strings.Contains(strings.ToLower(err.Error()), "timeout") || (errors.As(err, &netErr) && netErr.Timeout()))
}

type noopConn struct{}

func (noopConn) WriteJSON(any) error { return nil }

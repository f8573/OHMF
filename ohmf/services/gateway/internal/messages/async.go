package messages

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/google/uuid"
	"github.com/redis/go-redis/v9"
	"ohmf/services/gateway/internal/bus"
)

const (
	ackKeyPrefix          = "msg:ack:"
	ackSignalPrefix       = "msg:ack:notify:"
	legacyAckPollInterval = time.Second
)

type IngressEvent struct {
	EventID           string         `json:"event_id"`
	MessageID         string         `json:"message_id"`
	ConversationID    string         `json:"conversation_id"`
	SenderUserID      string         `json:"sender_user_id"`
	SenderDeviceID    string         `json:"sender_device_id,omitempty"`
	IdempotencyKey    string         `json:"idempotency_key"`
	Endpoint          string         `json:"endpoint"`
	ClientGeneratedID string         `json:"client_generated_id,omitempty"`
	ContentType       string         `json:"content_type"`
	Content           map[string]any `json:"content"`
	TransportIntent   string         `json:"transport_intent"`
	RecipientPhone    string         `json:"recipient_phone,omitempty"`
	SentAtMS          int64          `json:"sent_at_ms"`
	TraceID           string         `json:"trace_id"`
}

type PersistedAck struct {
	EventID        string `json:"event_id"`
	MessageID      string `json:"message_id"`
	ConversationID string `json:"conversation_id"`
	ServerOrder    int64  `json:"server_order"`
	Status         string `json:"status"`
	Transport      string `json:"transport"`
	PersistedAtMS  int64  `json:"persisted_at_ms"`
}

type AsyncPipeline struct {
	producer bus.IngressProducer
	redis    *redis.Client
}

func NewAsyncPipeline(producer bus.IngressProducer, redisClient *redis.Client) *AsyncPipeline {
	if producer == nil || redisClient == nil {
		return nil
	}
	return &AsyncPipeline{
		producer: producer,
		redis:    redisClient,
	}
}

func (p *AsyncPipeline) PublishIngress(ctx context.Context, evt IngressEvent) error {
	if p == nil || p.producer == nil {
		return nil
	}
	return p.producer.PublishIngress(ctx, evt.ConversationID, evt)
}

// PublishEnvelope publishes an already-built Envelope to the ingress topic.
func (p *AsyncPipeline) PublishEnvelope(ctx context.Context, conversationID string, env Envelope) error {
	if p == nil || p.producer == nil {
		return nil
	}
	return p.producer.PublishIngress(ctx, conversationID, env)
}

func (p *AsyncPipeline) WaitAck(ctx context.Context, eventID string, timeout time.Duration) (PersistedAck, bool, error) {
	if p == nil || p.redis == nil {
		return PersistedAck{}, false, nil
	}
	waitCtx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()

	key := AckRedisKey(eventID)
	if ack, ok, err := p.readAck(waitCtx, key); ok || err != nil {
		return ack, ok, err
	}

	pubsub := p.redis.Subscribe(waitCtx, ackSignalChannel(eventID))
	defer pubsub.Close()
	if _, err := pubsub.Receive(waitCtx); err != nil {
		return ackWaitResult(ctx, waitCtx, err)
	}

	// Re-check the durable ack key after the subscription is live so a key write
	// that raced with subscription setup is still observed without polling.
	if ack, ok, err := p.readAck(waitCtx, key); ok || err != nil {
		return ack, ok, err
	}

	msgCh := pubsub.Channel()
	legacyTicker := time.NewTicker(legacyAckPollInterval)
	defer legacyTicker.Stop()

	for {
		select {
		case <-waitCtx.Done():
			// One final authoritative key read avoids timing out on an ack that
			// landed just before the wait context expired.
			if ack, ok, err := p.readAck(context.Background(), key); ok || err != nil {
				return ack, ok, err
			}
			return ackWaitResult(ctx, waitCtx, waitCtx.Err())
		case msg, ok := <-msgCh:
			if !ok {
				if ack, found, err := p.readAck(waitCtx, key); found || err != nil {
					return ack, found, err
				}
				return ackWaitResult(ctx, waitCtx, errors.New("redis ack subscription closed"))
			}
			ack, err := decodeAckPayload(msg.Payload)
			if err != nil {
				return PersistedAck{}, false, err
			}
			return ack, true, nil
		case <-legacyTicker.C:
			// Sparse fallback polling is retained for rolling-deploy compatibility
			// and missed/lost Pub/Sub notifications. The durable ack key remains
			// authoritative; Pub/Sub is only a wake-up optimization.
			if ack, ok, err := p.readAck(waitCtx, key); ok || err != nil {
				return ack, ok, err
			}
		}
	}
}

func NewIngressEvent(userID, senderDeviceID, conversationID, endpoint, idemKey, contentType, transportIntent, recipientPhone, clientGeneratedID, traceID string, content map[string]any) IngressEvent {
	if traceID == "" {
		traceID = uuid.NewString()
	}
	return IngressEvent{
		EventID:           uuid.NewString(),
		MessageID:         uuid.NewString(),
		ConversationID:    conversationID,
		SenderUserID:      userID,
		SenderDeviceID:    senderDeviceID,
		IdempotencyKey:    idemKey,
		Endpoint:          endpoint,
		ClientGeneratedID: clientGeneratedID,
		ContentType:       contentType,
		Content:           content,
		TransportIntent:   transportIntent,
		RecipientPhone:    recipientPhone,
		SentAtMS:          time.Now().UTC().UnixMilli(),
		TraceID:           traceID,
	}
}

func (e IngressEvent) ProvisionalMessage() Message {
	return Message{
		MessageID:         e.MessageID,
		ConversationID:    e.ConversationID,
		SenderUserID:      e.SenderUserID,
		ContentType:       e.ContentType,
		Content:           e.Content,
		Transport:         e.TransportIntent,
		ClientGeneratedID: e.ClientGeneratedID,
		ServerOrder:       0,
		Status:            "QUEUED",
		CreatedAt:         time.UnixMilli(e.SentAtMS).UTC().Format(time.RFC3339),
	}
}

func AckRedisKey(eventID string) string {
	return fmt.Sprintf("%s%s", ackKeyPrefix, eventID)
}

func ackSignalChannel(eventID string) string {
	return fmt.Sprintf("%s%s", ackSignalPrefix, eventID)
}

func (p *AsyncPipeline) readAck(ctx context.Context, key string) (PersistedAck, bool, error) {
	payload, err := p.redis.Get(ctx, key).Result()
	if err == nil {
		ack, err := decodeAckPayload(payload)
		if err != nil {
			return PersistedAck{}, false, err
		}
		return ack, true, nil
	}
	if err != nil && err != redis.Nil {
		return PersistedAck{}, false, err
	}
	return PersistedAck{}, false, nil
}

func decodeAckPayload(payload string) (PersistedAck, error) {
	var ack PersistedAck
	if err := json.Unmarshal([]byte(payload), &ack); err != nil {
		return PersistedAck{}, err
	}
	return ack, nil
}

func ackWaitResult(parentCtx, waitCtx context.Context, err error) (PersistedAck, bool, error) {
	if parentErr := parentCtx.Err(); parentErr != nil {
		return PersistedAck{}, false, parentErr
	}
	if errors.Is(err, context.DeadlineExceeded) || errors.Is(waitCtx.Err(), context.DeadlineExceeded) {
		return PersistedAck{}, false, nil
	}
	return PersistedAck{}, false, err
}

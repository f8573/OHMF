package messages

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/google/uuid"
	"github.com/redis/go-redis/v9"
	"ohmf/services/gateway/internal/bus"
)

const (
	ackKeyPrefix = "msg:ack:"
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
	key := ackKeyPrefix + eventID
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		payload, err := p.redis.Get(ctx, key).Result()
		if err == nil {
			var ack PersistedAck
			if uErr := json.Unmarshal([]byte(payload), &ack); uErr != nil {
				return PersistedAck{}, false, uErr
			}
			return ack, true, nil
		}
		if err != nil && err != redis.Nil {
			return PersistedAck{}, false, err
		}
		select {
		case <-ctx.Done():
			return PersistedAck{}, false, ctx.Err()
		case <-time.After(50 * time.Millisecond):
		}
	}
	return PersistedAck{}, false, nil
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

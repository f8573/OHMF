package messages

import (
	"context"
	"encoding/json"
	"fmt"
	"sync"
	"time"

	"github.com/google/uuid"
	"github.com/redis/go-redis/v9"
	"ohmf/services/gateway/internal/bus"
)

const (
	ackKeyPrefix   = "msg:ack:"
	ackChannelName = "msg:ack:events"
)

type IngressEvent struct {
	EventID           string         `json:"event_id"`
	MessageID         string         `json:"message_id"`
	ConversationID    string         `json:"conversation_id"`
	SenderUserID      string         `json:"sender_user_id"`
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
	waiters  map[string][]chan PersistedAck
	mu       sync.Mutex
}

func NewAsyncPipeline(producer bus.IngressProducer, redisClient *redis.Client) *AsyncPipeline {
	if producer == nil || redisClient == nil {
		return nil
	}
	pipeline := &AsyncPipeline{
		producer: producer,
		redis:    redisClient,
		waiters:  make(map[string][]chan PersistedAck),
	}
	go pipeline.runAckSubscriber(context.Background())
	return pipeline
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
	if timeout <= 0 {
		timeout = 2 * time.Second
	}
	key := ackKeyPrefix + eventID
	waiter := make(chan PersistedAck, 1)
	p.registerWaiter(eventID, waiter)
	defer p.unregisterWaiter(eventID, waiter)

	if ack, ok, err := p.loadPersistedAck(ctx, key); err != nil {
		return PersistedAck{}, false, err
	} else if ok {
		return ack, true, nil
	}

	timer := time.NewTimer(timeout)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return PersistedAck{}, false, ctx.Err()
	case ack := <-waiter:
		return ack, true, nil
	case <-timer.C:
		if ack, ok, err := p.loadPersistedAck(ctx, key); err != nil {
			return PersistedAck{}, false, err
		} else if ok {
			return ack, true, nil
		}
		return PersistedAck{}, false, nil
	}
}

func NewIngressEvent(userID, conversationID, endpoint, idemKey, contentType, transportIntent, recipientPhone, clientGeneratedID, traceID string, content map[string]any) IngressEvent {
	if traceID == "" {
		traceID = uuid.NewString()
	}
	return IngressEvent{
		EventID:           uuid.NewString(),
		MessageID:         uuid.NewString(),
		ConversationID:    conversationID,
		SenderUserID:      userID,
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

func AckRedisChannel() string {
	return ackChannelName
}

func (p *AsyncPipeline) loadPersistedAck(ctx context.Context, key string) (PersistedAck, bool, error) {
	payload, err := p.redis.Get(ctx, key).Result()
	if err == redis.Nil {
		return PersistedAck{}, false, nil
	}
	if err != nil {
		return PersistedAck{}, false, err
	}
	var ack PersistedAck
	if err := json.Unmarshal([]byte(payload), &ack); err != nil {
		return PersistedAck{}, false, err
	}
	return ack, true, nil
}

func (p *AsyncPipeline) registerWaiter(eventID string, waiter chan PersistedAck) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.waiters[eventID] = append(p.waiters[eventID], waiter)
}

func (p *AsyncPipeline) unregisterWaiter(eventID string, waiter chan PersistedAck) {
	p.mu.Lock()
	defer p.mu.Unlock()
	waiters := p.waiters[eventID]
	if len(waiters) == 0 {
		return
	}
	filtered := waiters[:0]
	for _, item := range waiters {
		if item == waiter {
			continue
		}
		filtered = append(filtered, item)
	}
	if len(filtered) == 0 {
		delete(p.waiters, eventID)
		return
	}
	p.waiters[eventID] = filtered
}

func (p *AsyncPipeline) dispatchAck(ack PersistedAck) {
	if p == nil || ack.EventID == "" {
		return
	}
	p.mu.Lock()
	waiters := append([]chan PersistedAck(nil), p.waiters[ack.EventID]...)
	p.mu.Unlock()
	for _, waiter := range waiters {
		select {
		case waiter <- ack:
		default:
		}
	}
}

func (p *AsyncPipeline) runAckSubscriber(ctx context.Context) {
	if p == nil || p.redis == nil {
		return
	}
	for {
		select {
		case <-ctx.Done():
			return
		default:
		}
		pubsub := p.redis.Subscribe(ctx, ackChannelName)
		if _, err := pubsub.Receive(ctx); err != nil {
			_ = pubsub.Close()
			time.Sleep(250 * time.Millisecond)
			continue
		}
		for {
			msg, err := pubsub.ReceiveMessage(ctx)
			if err != nil {
				_ = pubsub.Close()
				time.Sleep(250 * time.Millisecond)
				break
			}
			var ack PersistedAck
			if err := json.Unmarshal([]byte(msg.Payload), &ack); err != nil {
				continue
			}
			p.dispatchAck(ack)
		}
	}
}

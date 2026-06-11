package messages

import (
	"testing"
	"time"
)

func TestAsyncPipelineDispatchAckToWaiter(t *testing.T) {
	pipeline := &AsyncPipeline{
		waiters: make(map[string][]chan PersistedAck),
	}
	waiter := make(chan PersistedAck, 1)
	pipeline.registerWaiter("evt-1", waiter)

	ack := PersistedAck{
		EventID:        "evt-1",
		MessageID:      "msg-1",
		ConversationID: "conv-1",
		ServerOrder:    5,
	}
	pipeline.dispatchAck(ack)

	select {
	case got := <-waiter:
		if got.EventID != ack.EventID || got.MessageID != ack.MessageID {
			t.Fatalf("unexpected ack dispatch: %#v", got)
		}
	case <-time.After(250 * time.Millisecond):
		t.Fatal("timed out waiting for dispatched ack")
	}

	pipeline.unregisterWaiter("evt-1", waiter)
	if len(pipeline.waiters) != 0 {
		t.Fatalf("expected waiter cleanup, got %#v", pipeline.waiters)
	}
}

func TestAckRedisChannel(t *testing.T) {
	if got := AckRedisChannel(); got != "msg:ack:events" {
		t.Fatalf("unexpected ack channel: %q", got)
	}
}

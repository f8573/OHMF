package main

import (
	"testing"
	"time"
)

func TestPercentileDuration(t *testing.T) {
	values := []time.Duration{
		10 * time.Millisecond,
		20 * time.Millisecond,
		30 * time.Millisecond,
		40 * time.Millisecond,
		50 * time.Millisecond,
	}

	if got := percentileDuration(values, 0.50); got != 30*time.Millisecond {
		t.Fatalf("expected p50 30ms, got %v", got)
	}
	if got := percentileDuration(values, 0.95); got != 50*time.Millisecond {
		t.Fatalf("expected p95 50ms, got %v", got)
	}
}

func TestReducerCountsExpectedLostDuplicateAndOutOfOrder(t *testing.T) {
	reducer := newReducer()
	now := time.Now().UTC()
	reducer.recordSend(sendRecord{
		MessageID:       "message-1",
		ConversationID:  "conversation-1",
		SenderUserID:    "user-1",
		SenderDeviceID:  "device-1",
		ClientSendAt:    now,
		AckAt:           now.Add(50 * time.Millisecond),
		ServerOrder:     2,
		TargetDeviceIDs: []string{"device-2", "device-3"},
		TargetCount:     2,
	})
	reducer.recordReceive(receiveRecord{
		MessageID:      "message-1",
		ConversationID: "conversation-1",
		DeviceID:       "device-2",
		ServerOrder:    2,
		ReceivedAt:     now.Add(75 * time.Millisecond),
	})
	reducer.recordReceive(receiveRecord{
		MessageID:      "message-1",
		ConversationID: "conversation-1",
		DeviceID:       "device-2",
		ServerOrder:    2,
		ReceivedAt:     now.Add(80 * time.Millisecond),
	})
	reducer.recordReceive(receiveRecord{
		MessageID:      "message-older",
		ConversationID: "conversation-1",
		DeviceID:       "device-2",
		ServerOrder:    1,
		ReceivedAt:     now.Add(90 * time.Millisecond),
	})

	summary := reducer.snapshotSummary(summaryInput{
		concurrentUsers:   2,
		concurrentDevices: 3,
		conversations:     1,
		duration:          time.Minute,
		convergenceRate:   1,
	})
	if summary.Correctness.ExpectedDeliveries != 2 {
		t.Fatalf("expected 2 deliveries, got %d", summary.Correctness.ExpectedDeliveries)
	}
	if summary.Correctness.SuccessfulDeliveries != 1 {
		t.Fatalf("expected 1 successful delivery, got %d", summary.Correctness.SuccessfulDeliveries)
	}
	if summary.Correctness.LostDeliveries != 1 {
		t.Fatalf("expected 1 lost delivery, got %d", summary.Correctness.LostDeliveries)
	}
	if summary.Correctness.DuplicateDeliveries != 1 {
		t.Fatalf("expected 1 duplicate delivery, got %d", summary.Correctness.DuplicateDeliveries)
	}
	if summary.Correctness.OutOfOrderDeliveries != 1 {
		t.Fatalf("expected 1 out-of-order delivery, got %d", summary.Correctness.OutOfOrderDeliveries)
	}
}

func TestConvergenceHashNormalizesTimeline(t *testing.T) {
	items := []map[string]any{
		{
			"message_id":   "message-1",
			"server_order": int64(1),
			"content_type": "text",
			"transport":    "OHMF",
			"content":      map[string]any{"text": "hello"},
		},
	}
	first, err := convergenceHash(items)
	if err != nil {
		t.Fatalf("first hash: %v", err)
	}
	second, err := convergenceHash(items)
	if err != nil {
		t.Fatalf("second hash: %v", err)
	}
	if first == "" || first != second {
		t.Fatalf("expected stable convergence hash, got %q and %q", first, second)
	}
}

func TestBacklogDrainTime(t *testing.T) {
	start := time.Now().UTC()
	samples := []lagSample{
		{Timestamp: start, TotalLag: 120},
		{Timestamp: start.Add(5 * time.Second), TotalLag: 60},
		{Timestamp: start.Add(9 * time.Second), TotalLag: 9},
	}
	got := backlogDrainTime(samples, 0, 10)
	if got != 9*time.Second {
		t.Fatalf("expected 9s drain time, got %v", got)
	}
}

func TestPendingSendObserveAckUsesFirstObservedAckForQueuedSend(t *testing.T) {
	pending := &pendingSend{}
	firstObserved := time.Now().UTC()
	if ackAt, definitive := pending.observeAck(ackPayload{
		MessageID: "provisional-message",
		Queued:    true,
	}, firstObserved); definitive || !ackAt.IsZero() {
		t.Fatalf("expected provisional ack to be non-definitive, got definitive=%v ackAt=%v", definitive, ackAt)
	}

	secondObserved := firstObserved.Add(750 * time.Millisecond)
	ackAt, definitive := pending.observeAck(ackPayload{
		MessageID:      "final-message",
		ConversationID: "conversation-1",
		ServerOrder:    42,
		Status:         "SENT",
	}, secondObserved)
	if !definitive {
		t.Fatalf("expected final ack to be definitive")
	}
	if !ackAt.Equal(firstObserved) {
		t.Fatalf("expected final ack to preserve first observed ack time %v, got %v", firstObserved, ackAt)
	}
}

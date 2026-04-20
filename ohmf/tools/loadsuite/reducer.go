package main

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"math"
	"sort"
	"strings"
	"sync"
	"time"
)

type reducer struct {
	mu                            sync.Mutex
	sends                         map[string]sendRecord
	receives                      map[string]map[string]receiveRecord
	duplicateDeliveries           int64
	outOfOrderDeliveries          int64
	lastOrderByDeviceConversation map[string]int64
	reconnects                    map[string]reconnectRecord
	resourcePeakCPU               float64
	resourcePeakMemory            float64
	resourcePeakKafkaLag          int64
}

func newReducer() *reducer {
	return &reducer{
		sends:                         make(map[string]sendRecord),
		receives:                      make(map[string]map[string]receiveRecord),
		lastOrderByDeviceConversation: make(map[string]int64),
		reconnects:                    make(map[string]reconnectRecord),
	}
}

func (r *reducer) recordSend(send sendRecord) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.sends[send.MessageID] = send
}

func (r *reducer) recordReceive(rec receiveRecord) {
	r.mu.Lock()
	defer r.mu.Unlock()

	key := rec.ConversationID + "|" + rec.DeviceID
	if prev := r.lastOrderByDeviceConversation[key]; prev > 0 && rec.ServerOrder > 0 && rec.ServerOrder < prev {
		r.outOfOrderDeliveries++
	}
	if rec.ServerOrder > r.lastOrderByDeviceConversation[key] {
		r.lastOrderByDeviceConversation[key] = rec.ServerOrder
	}

	deviceReceives := r.receives[rec.MessageID]
	if deviceReceives == nil {
		deviceReceives = make(map[string]receiveRecord)
		r.receives[rec.MessageID] = deviceReceives
	}
	if _, exists := deviceReceives[rec.DeviceID]; exists {
		r.duplicateDeliveries++
		return
	}
	deviceReceives[rec.DeviceID] = rec
}

func (r *reducer) startReconnect(id, deviceID string, at time.Time) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.reconnects[id] = reconnectRecord{ID: id, DeviceID: deviceID, StartedAt: at}
}

func (r *reducer) markReconnectReady(id string, at time.Time) {
	r.mu.Lock()
	defer r.mu.Unlock()
	rec := r.reconnects[id]
	rec.ReadyAt = at
	r.reconnects[id] = rec
}

func (r *reducer) markReconnectSynced(id string, at time.Time, success bool) {
	r.mu.Lock()
	defer r.mu.Unlock()
	rec := r.reconnects[id]
	rec.SyncAt = at
	rec.Successful = success
	r.reconnects[id] = rec
}

func (r *reducer) updateResourcePeak(cpu, memory float64, kafkaLag int64) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if cpu > r.resourcePeakCPU {
		r.resourcePeakCPU = cpu
	}
	if memory > r.resourcePeakMemory {
		r.resourcePeakMemory = memory
	}
	if kafkaLag > r.resourcePeakKafkaLag {
		r.resourcePeakKafkaLag = kafkaLag
	}
}

func (r *reducer) snapshotSummary(input summaryInput) summary {
	r.mu.Lock()
	defer r.mu.Unlock()

	sendLatencies := make([]time.Duration, 0, len(r.sends))
	deliveryLatencies := make([]time.Duration, 0)
	var expectedDeliveries int64
	var successfulDeliveries int64
	measuredSends := 0

	totalRecipients := 0
	for _, send := range r.sends {
		if !input.measurementStart.IsZero() && send.ClientSendAt.Before(input.measurementStart) {
			continue
		}
		if !input.measurementEnd.IsZero() && send.ClientSendAt.After(input.measurementEnd) {
			continue
		}
		if !send.AckAt.IsZero() && !send.ClientSendAt.IsZero() {
			sendLatencies = append(sendLatencies, send.AckAt.Sub(send.ClientSendAt))
		}
		measuredSends++
		expectedDeliveries += int64(send.TargetCount)
		totalRecipients += send.TargetCount

		for _, targetDeviceID := range send.TargetDeviceIDs {
			rec, ok := r.receives[send.MessageID][targetDeviceID]
			if !ok {
				continue
			}
			successfulDeliveries++
			if !rec.ReceivedAt.IsZero() && !send.ClientSendAt.IsZero() {
				deliveryLatencies = append(deliveryLatencies, rec.ReceivedAt.Sub(send.ClientSendAt))
			}
		}
	}

	var reconnectSuccesses int
	for _, rec := range r.reconnects {
		if rec.Successful {
			reconnectSuccesses++
		}
	}

	testSeconds := input.duration.Seconds()
	if testSeconds <= 0 {
		testSeconds = 1
	}
	avgRecipients := 0.0
	if measuredSends > 0 {
		avgRecipients = float64(totalRecipients) / float64(measuredSends)
	}

	return summary{
		Workload: workloadSummary{
			ConcurrentUsers:         input.concurrentUsers,
			ConcurrentDevices:       input.concurrentDevices,
			Conversations:           input.conversations,
			AverageRecipientsPerMsg: roundTo(avgRecipients, 2),
			MessageRate:             roundTo(input.messageRate, 2),
			TestDuration:            formatDuration(input.duration),
			ConnectReadinessPercent: roundTo(input.connectReadiness*100, 2),
			ConnectPhaseDuration:    formatDuration(input.connectPhaseDuration),
			FirstFailurePhase:       input.firstFailurePhase,
			FirstFailureReason:      input.firstFailureReason,
		},
		Performance: performanceSummary{
			MessagesPerSecond:          roundTo(float64(measuredSends)/testSeconds, 2),
			DeliveriesPerSecond:        roundTo(float64(successfulDeliveries)/testSeconds, 2),
			P50SendToAckLatency:        formatDuration(percentileDuration(sendLatencies, 0.50)),
			P95SendToAckLatency:        formatDuration(percentileDuration(sendLatencies, 0.95)),
			P99SendToAckLatency:        formatDuration(percentileDuration(sendLatencies, 0.99)),
			P50EndToEndDeliveryLatency: formatDuration(percentileDuration(deliveryLatencies, 0.50)),
			P95EndToEndDeliveryLatency: formatDuration(percentileDuration(deliveryLatencies, 0.95)),
			P99EndToEndDeliveryLatency: formatDuration(percentileDuration(deliveryLatencies, 0.99)),
		},
		Correctness: correctnessSummary{
			ExpectedDeliveries:         expectedDeliveries,
			SuccessfulDeliveries:       successfulDeliveries,
			LostDeliveries:             maxInt64(0, expectedDeliveries-successfulDeliveries),
			DuplicateDeliveries:        r.duplicateDeliveries,
			OutOfOrderDeliveries:       r.outOfOrderDeliveries,
			MultiDeviceConvergenceRate: roundTo(input.convergenceRate, 4),
		},
		Resilience: resilienceSummary{
			ReconnectSuccessRate: roundTo(rate(reconnectSuccesses, len(r.reconnects)), 4),
			RestartRecoveryTime:  formatDuration(input.restartRecoveryTime),
			BacklogDrainTime:     formatDuration(input.backlogDrainTime),
		},
		ResourceUsage: resourceUsageSummary{
			PeakCPU:      formatPercent(maxFloat(input.peakCPU, r.resourcePeakCPU)),
			PeakMemory:   formatBytes(maxFloat(input.peakMemory, r.resourcePeakMemory)),
			DBP95Latency: formatDuration(input.dbP95Latency),
			PeakKafkaLag: maxInt64(r.resourcePeakKafkaLag, input.peakKafkaLag),
		},
	}
}

func evaluateScalePass(result summary) bool {
	return result.Correctness.LostDeliveries == 0 &&
		result.Correctness.DuplicateDeliveries == 0 &&
		result.Correctness.OutOfOrderDeliveries == 0 &&
		result.Correctness.MultiDeviceConvergenceRate >= 1.0 &&
		result.Workload.ConnectReadinessPercent >= 98.0 &&
		parseDuration(result.Performance.P95SendToAckLatency) <= 2500*time.Millisecond &&
		parseDuration(result.Performance.P95EndToEndDeliveryLatency) <= 3*time.Second &&
		(result.ResourceUsage.PeakKafkaLag == 0 || parseDuration(result.Resilience.BacklogDrainTime) <= 15*time.Second) &&
		result.Workload.FirstFailurePhase == "" &&
		result.Workload.FirstFailureReason == ""
}

type summaryInput struct {
	concurrentUsers      int
	concurrentDevices    int
	conversations        int
	duration             time.Duration
	measurementStart     time.Time
	measurementEnd       time.Time
	messageRate          float64
	connectReadiness     float64
	connectPhaseDuration time.Duration
	firstFailurePhase    string
	firstFailureReason   string
	convergenceRate      float64
	restartRecoveryTime  time.Duration
	backlogDrainTime     time.Duration
	dbP95Latency         time.Duration
	peakKafkaLag         int64
	peakCPU              float64
	peakMemory           float64
}

func percentileDuration(values []time.Duration, quantile float64) time.Duration {
	if len(values) == 0 {
		return 0
	}
	sorted := make([]time.Duration, len(values))
	copy(sorted, values)
	sort.Slice(sorted, func(i, j int) bool { return sorted[i] < sorted[j] })
	if quantile <= 0 {
		return sorted[0]
	}
	if quantile >= 1 {
		return sorted[len(sorted)-1]
	}
	idx := int(math.Ceil(quantile*float64(len(sorted)))) - 1
	if idx < 0 {
		idx = 0
	}
	if idx >= len(sorted) {
		idx = len(sorted) - 1
	}
	return sorted[idx]
}

func convergenceHash(items []map[string]any) (string, error) {
	normalized := make([]map[string]any, 0, len(items))
	for _, item := range items {
		if item == nil {
			continue
		}
		entry := map[string]any{
			"message_id":   textField(item["message_id"]),
			"server_order": int64Field(item["server_order"]),
			"content_type": textField(item["content_type"]),
			"deleted":      boolField(item["deleted"]),
			"transport":    textField(item["transport"]),
			"created_at":   textField(item["created_at"]),
			"visibility":   textField(item["visibility_state"]),
		}
		if content, ok := item["content"].(map[string]any); ok {
			entry["content_text"] = textField(content["text"])
		}
		normalized = append(normalized, entry)
	}
	body, err := json.Marshal(normalized)
	if err != nil {
		return "", err
	}
	sum := sha256.Sum256(body)
	return hex.EncodeToString(sum[:]), nil
}

func backlogDrainTime(samples []lagSample, baseline int64, threshold int64) time.Duration {
	if len(samples) == 0 {
		return 0
	}
	start := samples[0].Timestamp
	target := baseline + threshold
	for _, sample := range samples {
		if sample.TotalLag <= target {
			return sample.Timestamp.Sub(start)
		}
	}
	return 0
}

func formatDuration(d time.Duration) string {
	if d <= 0 {
		return "0s"
	}
	switch {
	case d < time.Millisecond:
		return fmt.Sprintf("%dus", d.Microseconds())
	case d < time.Second:
		return fmt.Sprintf("%dms", d.Milliseconds())
	default:
		seconds := d.Seconds()
		if seconds < 10 {
			return fmt.Sprintf("%.2fs", seconds)
		}
		return fmt.Sprintf("%.1fs", seconds)
	}
}

func parseDuration(v string) time.Duration {
	v = strings.TrimSpace(v)
	switch {
	case strings.HasSuffix(v, "us"):
		raw := strings.TrimSuffix(v, "us")
		var micros int64
		fmt.Sscanf(raw, "%d", &micros)
		return time.Duration(micros) * time.Microsecond
	case strings.HasSuffix(v, "ms"):
		raw := strings.TrimSuffix(v, "ms")
		var millis int64
		fmt.Sscanf(raw, "%d", &millis)
		return time.Duration(millis) * time.Millisecond
	default:
		d, _ := time.ParseDuration(v)
		return d
	}
}

func formatPercent(v float64) string {
	if v <= 0 {
		return "0.00%"
	}
	return fmt.Sprintf("%.2f%%", v)
}

func formatBytes(v float64) string {
	if v <= 0 {
		return "0 B"
	}
	const unit = 1024.0
	units := []string{"B", "KiB", "MiB", "GiB", "TiB"}
	value := v
	idx := 0
	for value >= unit && idx < len(units)-1 {
		value /= unit
		idx++
	}
	return fmt.Sprintf("%.2f %s", value, units[idx])
}

func roundTo(v float64, places int) float64 {
	pow := math.Pow10(places)
	return math.Round(v*pow) / pow
}

func rate(successes, total int) float64 {
	if total <= 0 {
		return 0
	}
	return float64(successes) / float64(total)
}

func maxInt64(values ...int64) int64 {
	var out int64
	for _, value := range values {
		if value > out {
			out = value
		}
	}
	return out
}

func maxFloat(values ...float64) float64 {
	out := 0.0
	for _, value := range values {
		if value > out {
			out = value
		}
	}
	return out
}

func textField(v any) string {
	switch typed := v.(type) {
	case string:
		return strings.TrimSpace(typed)
	default:
		return ""
	}
}

func int64Field(v any) int64 {
	switch typed := v.(type) {
	case int:
		return int64(typed)
	case int64:
		return typed
	case float64:
		return int64(typed)
	default:
		return 0
	}
}

func boolField(v any) bool {
	if typed, ok := v.(bool); ok {
		return typed
	}
	return false
}

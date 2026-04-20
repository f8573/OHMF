package observability

import (
	"bufio"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

func TestMetricsHandlerExposesGatewayMetrics(t *testing.T) {
	handler := HTTPMetricsMiddleware(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusAccepted)
	}))

	req := httptest.NewRequest(http.MethodGet, "/healthz", nil)
	rr := httptest.NewRecorder()
	handler.ServeHTTP(rr, req)

	IncWSConnection()
	RecordWSMessage("received", "subscribe")
	RecordWSMessage("sent", "subscribe_ack")
	DecWSConnection()
	RecordMessageSend("/v1/messages", "text", "OHMF", "accepted", false, 15*time.Millisecond)
	RecordMessagePersist("/v1/messages", "sync", "text", "OHMF", 8*time.Millisecond)
	RecordRealtimeOnlineDeliveryUpdate("OHMF", "DELIVERED", 20*time.Millisecond)
	RecordRealtimeResume("accepted")
	RecordRealtimeReplayBatch("resume", 3, false)
	RecordRealtimeReplayDuration("resume", "complete", 12*time.Millisecond)
	RecordRealtimeResyncRequired("has_more")
	RecordAuthVerify("success", 25*time.Millisecond)
	RecordPairingComplete("success", 18*time.Millisecond)
	RecordRealtimeWSUpgrade("v2", "success")
	RecordRealtimeHello("fresh", "complete", 3*time.Millisecond)
	persistedAt := time.Now().Add(-25 * time.Millisecond)
	publishedAt := time.Now().Add(-10 * time.Millisecond)
	RecordRealtimeUserEventPublished("conversation_message_appended", 42, persistedAt, publishedAt)
	RecordRealtimeUserEventWriteCompleted("conversation_message_appended", 42, time.Now())
	RecordReplicationWakeup("notify")
	RecordReplicationDomainEventAge(120 * time.Millisecond)
	RecordReplicationBatch(12)
	RecordReplicationTransaction("ok", 45*time.Millisecond)
	RecordReplicationRows("user_events_inserted", 24)

	metricsReq := httptest.NewRequest(http.MethodGet, "/metrics", nil)
	metricsRR := httptest.NewRecorder()
	MetricsHandler().ServeHTTP(metricsRR, metricsReq)

	if metricsRR.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", metricsRR.Code)
	}
	body := metricsRR.Body.String()
	if !strings.Contains(body, "ohmf_gateway_http_requests_total") {
		t.Fatalf("expected http metrics in body")
	}
	if !strings.Contains(body, "ohmf_gateway_ws_messages_total") {
		t.Fatalf("expected ws metrics in body")
	}
	if !strings.Contains(body, "ohmf_gateway_messages_send_requests_total") {
		t.Fatalf("expected send metrics in body")
	}
	if !strings.Contains(body, "ohmf_gateway_messages_persisted_total") {
		t.Fatalf("expected persist metrics in body")
	}
	if !strings.Contains(body, "ohmf_gateway_realtime_resume_requests_total") {
		t.Fatalf("expected resume metrics in body")
	}
	if !strings.Contains(body, "ohmf_gateway_auth_verify_requests_total") {
		t.Fatalf("expected auth verify metrics in body")
	}
	if !strings.Contains(body, "ohmf_gateway_realtime_ws_upgrades_total") {
		t.Fatalf("expected ws upgrade metrics in body")
	}
	if !strings.Contains(body, "ohmf_gateway_realtime_user_event_persist_to_publish_latency_seconds") {
		t.Fatalf("expected user event publish latency metrics in body")
	}
	if !strings.Contains(body, "ohmf_gateway_realtime_user_event_publish_to_ws_write_latency_seconds") {
		t.Fatalf("expected user event websocket write latency metrics in body")
	}
	if !strings.Contains(body, "ohmf_gateway_replication_wakeups_total") {
		t.Fatalf("expected replication wake metrics in body")
	}
	if !strings.Contains(body, "ohmf_gateway_replication_transaction_latency_seconds") {
		t.Fatalf("expected replication transaction metrics in body")
	}
}

func TestStatusRecorderPreservesHijacker(t *testing.T) {
	rec := &hijackableRecorder{ResponseRecorder: httptest.NewRecorder()}
	handler := HTTPMetricsMiddleware(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		h, ok := w.(http.Hijacker)
		if !ok {
			t.Fatalf("wrapped response writer no longer implements Hijacker")
		}
		if _, _, err := h.Hijack(); err != nil {
			t.Fatalf("hijack failed: %v", err)
		}
	}))

	req := httptest.NewRequest(http.MethodGet, "/v2/ws", nil)
	handler.ServeHTTP(rec, req)
}

type hijackableRecorder struct {
	*httptest.ResponseRecorder
}

func (r *hijackableRecorder) Hijack() (net.Conn, *bufio.ReadWriter, error) {
	server, client := net.Pipe()
	rw := bufio.NewReadWriter(bufio.NewReader(server), bufio.NewWriter(server))
	_ = client.Close()
	return server, rw, nil
}

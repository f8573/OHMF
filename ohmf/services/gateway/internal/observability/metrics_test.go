package observability

import (
	"bufio"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
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
	RecordRedisAckFailedAfterPersistence()
	RecordAckTimeoutAfterPersistence()
	RecordIdempotentSuccessAfterAckTimeout()
	RecordSendHandler500("other")
	DecWSConnection()

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
	if !strings.Contains(body, "redis_ack_failed_after_persistence_total") {
		t.Fatalf("expected redis ack recovery metric in body")
	}
	if !strings.Contains(body, "ack_timeout_after_persistence_total") {
		t.Fatalf("expected ack timeout recovery metric in body")
	}
	if !strings.Contains(body, "send_handler_500_total") {
		t.Fatalf("expected send handler 500 metric in body")
	}
}

func TestRecordDBQueryAppearsInMetrics(t *testing.T) {
	RecordDBQuery("SELECT id FROM messages", nil, 2e6)           // 2ms, ok
	RecordDBQuery("INSERT INTO messages VALUES ($1)", nil, 1e6)  // 1ms, ok
	RecordDBQuery("UPDATE messages SET x=1", errTest, 500e3)     // error

	metricsReq := httptest.NewRequest(http.MethodGet, "/metrics", nil)
	rr := httptest.NewRecorder()
	MetricsHandler().ServeHTTP(rr, metricsReq)

	body := rr.Body.String()
	if !strings.Contains(body, "ohmf_gateway_db_queries_total") {
		t.Fatalf("expected db query counter in metrics output")
	}
	if !strings.Contains(body, "ohmf_gateway_db_query_latency_seconds_bucket") {
		t.Fatalf("expected db query latency histogram in metrics output")
	}
}

type testError struct{ msg string }

func (e *testError) Error() string { return e.msg }

var errTest = &testError{"injected db error"}

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

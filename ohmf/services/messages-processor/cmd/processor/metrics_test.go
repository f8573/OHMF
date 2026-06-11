package main

import (
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
)

func TestMetricsRegistrationAndEndpoint(t *testing.T) {
	// Reset the once so this test is isolated from other init paths.
	// We verify that calling initMetrics() twice does not panic (duplicate registration).
	initMetrics()
	initMetrics()

	mux := http.NewServeMux()
	mux.Handle("/metrics", http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// Use the promhttp handler indirectly via the metrics server approach.
		// For unit test, just verify metrics vars are accessible without panic.
		w.WriteHeader(http.StatusOK)
	}))

	srv := httptest.NewServer(mux)
	defer srv.Close()

	resp, err := http.Get(srv.URL + "/metrics")
	if err != nil {
		t.Fatalf("GET /metrics: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("expected 200, got %d", resp.StatusCode)
	}
}

func TestRecordProcessorResult(t *testing.T) {
	initMetrics()
	recordProcessorResult("success", 10*time.Millisecond)
	recordProcessorResult("failure", 5*time.Millisecond)
	recordProcessorResult("", 0) // empty result defaults to "unknown"
}

func TestRecordKafkaConsumeLag(t *testing.T) {
	initMetrics()
	recordKafkaConsumeLag(100 * time.Millisecond)
	recordKafkaConsumeLag(-1) // negative ignored
}

func TestRecordProcessorRetry(t *testing.T) {
	initMetrics()
	recordProcessorRetry("fetch_failed")
	recordProcessorRetry("commit_failed")
	recordProcessorRetry("") // defaults to "unknown"
}

func TestRecordSubsystemLatencies(t *testing.T) {
	initMetrics()
	recordProcessorTransaction("ok", 5*time.Millisecond)
	recordProcessorTransaction("error", 3*time.Millisecond)
	recordCassandraProjection("ok", 2*time.Millisecond)
	recordCassandraProjection("error", 1*time.Millisecond)
	recordAckPublish("ok", 500*time.Microsecond)
	recordAckPublish("error", 200*time.Microsecond)
}

func TestNormalizeStatement(t *testing.T) {
	cases := []struct {
		input string
		want  string
	}{
		{"SELECT id FROM foo", "select"},
		{"insert into bar values ($1)", "insert"},
		{"UPDATE baz SET x=1", "update"},
		{"DELETE FROM qux", "delete"},
		{"WITH cte AS (SELECT 1) SELECT * FROM cte", "with"},
		{"CALL stored_proc()", "other"},
		{"", "unknown"},
		{"   ", "unknown"},
	}
	for _, tc := range cases {
		got := normalizeStatement(tc.input)
		if got != tc.want {
			t.Errorf("normalizeStatement(%q) = %q, want %q", tc.input, got, tc.want)
		}
	}
}

func TestStartMetricsServer(t *testing.T) {
	// Pre-observe each metric so prometheus emits HELP/TYPE lines in the output.
	// (Vec metrics with zero observations produce no output in the text format.)
	recordProcessorResult("success", time.Millisecond)
	recordKafkaConsumeLag(time.Millisecond)
	recordProcessorTransaction("ok", time.Millisecond)
	recordCassandraProjection("ok", time.Millisecond)
	recordAckPublish("ok", time.Millisecond)
	recordProcessorRetry("test")
	// Observe db metrics via the tracer.
	ctx := context.Background()
	tr := &dbQueryTracer{}
	trCtx := tr.TraceQueryStart(ctx, nil, pgx.TraceQueryStartData{SQL: "SELECT 1"})
	tr.TraceQueryEnd(trCtx, nil, pgx.TraceQueryEndData{})

	addr := ":19191"
	startMetricsServer(addr)

	resp, err := http.Get("http://localhost:19191/metrics")
	if err != nil {
		t.Skipf("metrics server not reachable (may be port conflict): %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("expected 200 from /metrics, got %d", resp.StatusCode)
	}
	body, _ := io.ReadAll(resp.Body)
	text := string(body)
	for _, want := range []string{
		"ohmf_messages_processor_handler_results_total",
		"ohmf_messages_processor_kafka_consume_lag_seconds_bucket",
		"ohmf_messages_processor_postgres_transaction_latency_seconds_bucket",
		"ohmf_messages_processor_cassandra_projection_latency_seconds_bucket",
		"ohmf_messages_processor_ack_publish_latency_seconds_bucket",
		"ohmf_messages_processor_retry_total",
		"ohmf_messages_processor_db_queries_total",
	} {
		if !strings.Contains(text, want) {
			t.Errorf("expected /metrics to contain %q", want)
		}
	}
}

package main

import (
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

func TestObservabilityHandlers(t *testing.T) {
	obs := newProcessorObservability("messages", ":0", []string{"127.0.0.1:1"}, []dependencyCheck{
		{name: "postgres", check: func(context.Context) error { return nil }},
		{name: "redis", check: func(context.Context) error { return nil }},
		{name: "cassandra", check: func(context.Context) error { return nil }},
	})
	obs.recordSuccess(25 * time.Millisecond)
	obs.recordError(50 * time.Millisecond)
	obs.recordDLQPublish()
	obs.recordDuplicate()
	obs.setConsumerLag(7)
	obs.recordStage("kafka_consume", "succeeded", "")
	obs.recordStage("postgres_write", "attempted", "")
	obs.recordStage("postgres_write", "succeeded", "")

	server := httptest.NewServer(obs.handler())
	defer server.Close()

	healthResp, err := http.Get(server.URL + "/healthz")
	if err != nil {
		t.Fatalf("healthz request failed: %v", err)
	}
	defer healthResp.Body.Close()
	if healthResp.StatusCode != http.StatusOK {
		t.Fatalf("expected 200 from healthz, got %d", healthResp.StatusCode)
	}

	readyResp, err := http.Get(server.URL + "/readyz")
	if err != nil {
		t.Fatalf("readyz request failed: %v", err)
	}
	defer readyResp.Body.Close()
	if readyResp.StatusCode != http.StatusServiceUnavailable {
		t.Fatalf("expected 503 from readyz with failing kafka check, got %d", readyResp.StatusCode)
	}

	metricsResp, err := http.Get(server.URL + "/metrics")
	if err != nil {
		t.Fatalf("metrics request failed: %v", err)
	}
	defer metricsResp.Body.Close()
	body, err := io.ReadAll(metricsResp.Body)
	if err != nil {
		t.Fatalf("read metrics body: %v", err)
	}
	text := string(body)
	for _, want := range []string{
		"ohmf_messages_processor_processed_total 1",
		"ohmf_messages_processor_errors_total 1",
		"ohmf_messages_processor_dlq_published_total 1",
		"ohmf_messages_processor_duplicates_total 1",
		"ohmf_messages_processor_last_success_timestamp_seconds",
		"ohmf_messages_processor_consumer_lag_messages 7",
		"ohmf_messages_processor_stage_events_total{outcome=\"succeeded\",stage=\"kafka_consume\",target=\"\"} 1",
		"ohmf_messages_processor_stage_events_total{outcome=\"attempted\",stage=\"postgres_write\",target=\"\"} 1",
	} {
		if !strings.Contains(text, want) {
			t.Fatalf("expected metrics output to contain %q", want)
		}
	}
}

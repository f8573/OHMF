package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"net/http"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/collectors"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/segmentio/kafka-go"
)

type dependencyCheck struct {
	name  string
	check func(context.Context) error
}

type stageMetricLabel struct {
	stage   string
	outcome string
	target  string
}

type processorObservability struct {
	service         string
	addr            string
	brokers         []string
	checks          []dependencyCheck
	registry        *prometheus.Registry
	processedTotal  prometheus.Counter
	errorTotal      prometheus.Counter
	dlqPublished    prometheus.Counter
	duplicateTotal  prometheus.Counter
	stageEvents     *prometheus.CounterVec
	processingDelay prometheus.Histogram
	lastSuccess     prometheus.Gauge
	consumerLag     prometheus.Gauge
	dependencyReady *prometheus.GaugeVec
}

func newProcessorObservability(service, addr string, brokers []string, checks []dependencyCheck) *processorObservability {
	namespace := fmt.Sprintf("ohmf_%s_processor", service)
	registry := prometheus.NewRegistry()
	obs := &processorObservability{
		service:  service,
		addr:     addr,
		brokers:  brokers,
		checks:   checks,
		registry: registry,
		processedTotal: prometheus.NewCounter(prometheus.CounterOpts{
			Name: namespace + "_processed_total",
			Help: "Total number of processor events completed successfully.",
		}),
		errorTotal: prometheus.NewCounter(prometheus.CounterOpts{
			Name: namespace + "_errors_total",
			Help: "Total number of processor event failures.",
		}),
		dlqPublished: prometheus.NewCounter(prometheus.CounterOpts{
			Name: namespace + "_dlq_published_total",
			Help: "Total number of events published to the DLQ.",
		}),
		duplicateTotal: prometheus.NewCounter(prometheus.CounterOpts{
			Name: namespace + "_duplicates_total",
			Help: "Total number of duplicate or idempotent events skipped.",
		}),
		stageEvents: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: namespace + "_stage_events_total",
			Help: "Per-stage processor event counters by stage, outcome, and target.",
		}, []string{"stage", "outcome", "target"}),
		processingDelay: prometheus.NewHistogram(prometheus.HistogramOpts{
			Name:    namespace + "_processing_duration_seconds",
			Help:    "End-to-end processing time for a consumed event.",
			Buckets: prometheus.DefBuckets,
		}),
		lastSuccess: prometheus.NewGauge(prometheus.GaugeOpts{
			Name: namespace + "_last_success_timestamp_seconds",
			Help: "Unix timestamp of the last successful processor completion.",
		}),
		consumerLag: prometheus.NewGauge(prometheus.GaugeOpts{
			Name: namespace + "_consumer_lag_messages",
			Help: "Approximate Kafka consumer lag in messages for the last consumed record.",
		}),
		dependencyReady: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Name: namespace + "_dependency_ready",
			Help: "Readiness result for each processor dependency.",
		}, []string{"dependency"}),
	}
	registry.MustRegister(
		collectors.NewGoCollector(),
		collectors.NewProcessCollector(collectors.ProcessCollectorOpts{}),
		obs.processedTotal,
		obs.errorTotal,
		obs.dlqPublished,
		obs.duplicateTotal,
		obs.stageEvents,
		obs.processingDelay,
		obs.lastSuccess,
		obs.consumerLag,
		obs.dependencyReady,
	)
	obs.initializeStageEventLabels()
	return obs
}

func (o *processorObservability) start() {
	srv := &http.Server{
		Addr:              o.addr,
		Handler:           o.handler(),
		ReadHeaderTimeout: 5 * time.Second,
	}
	go func() {
		if err := srv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			log.Printf("%s-processor observability server failed: %v", o.service, err)
		}
	}()
}

func (o *processorObservability) handler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("/healthz", func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("ok"))
	})
	mux.HandleFunc("/readyz", o.handleReady)
	mux.Handle("/metrics", promhttp.HandlerFor(o.registry, promhttp.HandlerOpts{}))
	return mux
}

func (o *processorObservability) handleReady(w http.ResponseWriter, r *http.Request) {
	ctx, cancel := context.WithTimeout(r.Context(), 2*time.Second)
	defer cancel()

	status := make(map[string]string, len(o.checks)+1)
	ready := true
	for _, check := range o.checks {
		if err := check.check(ctx); err != nil {
			ready = false
			status[check.name] = err.Error()
			o.dependencyReady.WithLabelValues(check.name).Set(0)
			continue
		}
		status[check.name] = "ok"
		o.dependencyReady.WithLabelValues(check.name).Set(1)
	}
	if err := kafkaReady(ctx, o.brokers); err != nil {
		ready = false
		status["kafka"] = err.Error()
		o.dependencyReady.WithLabelValues("kafka").Set(0)
	} else {
		status["kafka"] = "ok"
		o.dependencyReady.WithLabelValues("kafka").Set(1)
	}

	code := http.StatusOK
	if !ready {
		code = http.StatusServiceUnavailable
	}
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	_ = json.NewEncoder(w).Encode(map[string]any{
		"ok":           ready,
		"service":      o.service + "-processor",
		"dependencies": status,
	})
}

func (o *processorObservability) setConsumerLag(lag float64) {
	o.consumerLag.Set(lag)
}

func (o *processorObservability) recordSuccess(duration time.Duration) {
	o.processedTotal.Inc()
	o.processingDelay.Observe(duration.Seconds())
	o.lastSuccess.SetToCurrentTime()
}

func (o *processorObservability) recordError(duration time.Duration) {
	o.errorTotal.Inc()
	o.processingDelay.Observe(duration.Seconds())
}

func (o *processorObservability) recordDLQPublish() {
	o.dlqPublished.Inc()
}

func (o *processorObservability) recordDuplicate() {
	o.duplicateTotal.Inc()
}

func (o *processorObservability) recordStage(stage, outcome, target string) {
	o.stageEvents.WithLabelValues(stage, outcome, target).Inc()
}

func (o *processorObservability) initializeStageEventLabels() {
	for _, label := range []stageMetricLabel{
		{stage: "kafka_consume", outcome: "succeeded", target: ""},
		{stage: "decode", outcome: "succeeded", target: ""},
		{stage: "dedupe", outcome: "skipped", target: ""},
		{stage: "postgres_write", outcome: "attempted", target: ""},
		{stage: "postgres_write", outcome: "succeeded", target: ""},
		{stage: "postgres_write", outcome: "failed", target: ""},
		{stage: "cassandra_write", outcome: "attempted", target: ""},
		{stage: "cassandra_write", outcome: "succeeded", target: ""},
		{stage: "cassandra_write", outcome: "failed", target: ""},
		{stage: "redis_ack", outcome: "attempted", target: "set"},
		{stage: "redis_ack", outcome: "succeeded", target: "set"},
		{stage: "redis_ack", outcome: "failed", target: "set"},
		{stage: "downstream_publish", outcome: "succeeded", target: "persisted"},
		{stage: "downstream_publish", outcome: "succeeded", target: "microservice"},
		{stage: "kafka_offset_commit", outcome: "succeeded", target: ""},
		{stage: "handler_return", outcome: "success", target: ""},
		{stage: "handler_return", outcome: "error", target: ""},
	} {
		o.stageEvents.WithLabelValues(label.stage, label.outcome, label.target).Add(0)
	}
}

func kafkaReady(ctx context.Context, brokers []string) error {
	var lastErr error
	for _, broker := range brokers {
		conn, err := kafka.DialContext(ctx, "tcp", broker)
		if err != nil {
			lastErr = err
			continue
		}
		_ = conn.Close()
		return nil
	}
	if lastErr == nil {
		lastErr = errors.New("no kafka brokers configured")
	}
	return lastErr
}

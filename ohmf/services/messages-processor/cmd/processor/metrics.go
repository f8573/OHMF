package main

import (
	"context"
	"log"
	"net/http"
	"strings"
	"sync"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

var metricsOnce sync.Once

var processorResultsTotal = prometheus.NewCounterVec(
	prometheus.CounterOpts{
		Name: "ohmf_messages_processor_handler_results_total",
		Help: "Total number of messages-processor handler outcomes.",
	},
	[]string{"result"},
)

var processorLatency = prometheus.NewHistogramVec(
	prometheus.HistogramOpts{
		Name:    "ohmf_messages_processor_processing_latency_seconds",
		Help:    "Processing latency for messages-processor messages.",
		Buckets: []float64{0.001, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2, 5, 10},
	},
	[]string{"result"},
)

var kafkaConsumeLagLatency = prometheus.NewHistogram(
	prometheus.HistogramOpts{
		Name:    "ohmf_messages_processor_kafka_consume_lag_seconds",
		Help:    "Lag between Kafka message timestamp and messages-processor handling start.",
		Buckets: []float64{0.001, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2, 5, 10},
	},
)

var processorTransactionLatency = prometheus.NewHistogramVec(
	prometheus.HistogramOpts{
		Name:    "ohmf_messages_processor_postgres_transaction_latency_seconds",
		Help:    "Latency of the Postgres transaction used to persist an ingress message.",
		Buckets: []float64{0.001, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2, 5, 10},
	},
	[]string{"result"},
)

var cassandraProjectionLatency = prometheus.NewHistogramVec(
	prometheus.HistogramOpts{
		Name:    "ohmf_messages_processor_cassandra_projection_latency_seconds",
		Help:    "Latency of Cassandra projection writes performed by the messages processor.",
		Buckets: []float64{0.001, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2, 5, 10},
	},
	[]string{"result"},
)

var ackPublishLatency = prometheus.NewHistogramVec(
	prometheus.HistogramOpts{
		Name:    "ohmf_messages_processor_ack_publish_latency_seconds",
		Help:    "Latency to durably store and publish a persisted ack for gateway waiters.",
		Buckets: []float64{0.0005, 0.001, 0.0025, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2},
	},
	[]string{"result"},
)

var processorRetriesTotal = prometheus.NewCounterVec(
	prometheus.CounterOpts{
		Name: "ohmf_messages_processor_retry_total",
		Help: "Total number of messages-processor retries or retry-like deferrals.",
	},
	[]string{"reason"},
)

var dbQueriesTotal = prometheus.NewCounterVec(
	prometheus.CounterOpts{
		Name: "ohmf_messages_processor_db_queries_total",
		Help: "Total number of PostgreSQL queries observed by the messages processor.",
	},
	[]string{"statement", "outcome"},
)

var dbQueryLatency = prometheus.NewHistogramVec(
	prometheus.HistogramOpts{
		Name:    "ohmf_messages_processor_db_query_latency_seconds",
		Help:    "Latency of PostgreSQL queries observed by the messages processor.",
		Buckets: []float64{0.0005, 0.001, 0.0025, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2, 5},
	},
	[]string{"statement", "outcome"},
)

type dbTraceKey struct{}
type dbStatementKey struct{}

type dbQueryTracer struct{}

func initMetrics() {
	metricsOnce.Do(func() {
		prometheus.MustRegister(
			processorResultsTotal,
			processorLatency,
			kafkaConsumeLagLatency,
			processorTransactionLatency,
			cassandraProjectionLatency,
			ackPublishLatency,
			processorRetriesTotal,
			dbQueriesTotal,
			dbQueryLatency,
		)
	})
}

func startMetricsServer(addr string) {
	initMetrics()
	go func() {
		server := &http.Server{
			Addr:              addr,
			Handler:           promhttp.Handler(),
			ReadHeaderTimeout: 5 * time.Second,
		}
		if err := server.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Printf("messages-processor metrics server failed: %v", err)
		}
	}()
}

func recordProcessorResult(result string, duration time.Duration) {
	initMetrics()
	if strings.TrimSpace(result) == "" {
		result = "unknown"
	}
	processorResultsTotal.WithLabelValues(result).Inc()
	if duration >= 0 {
		processorLatency.WithLabelValues(result).Observe(duration.Seconds())
	}
}

func recordKafkaConsumeLag(duration time.Duration) {
	initMetrics()
	if duration >= 0 {
		kafkaConsumeLagLatency.Observe(duration.Seconds())
	}
}

func recordProcessorTransaction(result string, duration time.Duration) {
	initMetrics()
	if strings.TrimSpace(result) == "" {
		result = "unknown"
	}
	if duration >= 0 {
		processorTransactionLatency.WithLabelValues(result).Observe(duration.Seconds())
	}
}

func recordCassandraProjection(result string, duration time.Duration) {
	initMetrics()
	if strings.TrimSpace(result) == "" {
		result = "unknown"
	}
	if duration >= 0 {
		cassandraProjectionLatency.WithLabelValues(result).Observe(duration.Seconds())
	}
}

func recordAckPublish(result string, duration time.Duration) {
	initMetrics()
	if strings.TrimSpace(result) == "" {
		result = "unknown"
	}
	if duration >= 0 {
		ackPublishLatency.WithLabelValues(result).Observe(duration.Seconds())
	}
}

func recordProcessorRetry(reason string) {
	initMetrics()
	if strings.TrimSpace(reason) == "" {
		reason = "unknown"
	}
	processorRetriesTotal.WithLabelValues(reason).Inc()
}

func (t *dbQueryTracer) TraceQueryStart(ctx context.Context, _ *pgx.Conn, data pgx.TraceQueryStartData) context.Context {
	ctx = context.WithValue(ctx, dbTraceKey{}, time.Now())
	return context.WithValue(ctx, dbStatementKey{}, data.SQL)
}

func (t *dbQueryTracer) TraceQueryEnd(ctx context.Context, _ *pgx.Conn, data pgx.TraceQueryEndData) {
	initMetrics()
	statement, _ := ctx.Value(dbStatementKey{}).(string)
	start, _ := ctx.Value(dbTraceKey{}).(time.Time)
	outcome := "ok"
	if data.Err != nil {
		outcome = "error"
	}
	statement = normalizeStatement(statement)
	dbQueriesTotal.WithLabelValues(statement, outcome).Inc()
	if !start.IsZero() {
		dbQueryLatency.WithLabelValues(statement, outcome).Observe(time.Since(start).Seconds())
	}
}

func normalizeStatement(sql string) string {
	sql = strings.TrimSpace(strings.ToLower(sql))
	fields := strings.Fields(sql)
	if len(fields) == 0 {
		return "unknown"
	}
	switch fields[0] {
	case "select", "insert", "update", "delete", "with":
		return fields[0]
	default:
		return "other"
	}
}

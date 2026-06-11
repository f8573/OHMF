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
		Name: "ohmf_delivery_processor_handler_results_total",
		Help: "Total number of delivery-processor handler outcomes.",
	},
	[]string{"result"},
)

var processorLatency = prometheus.NewHistogramVec(
	prometheus.HistogramOpts{
		Name:    "ohmf_delivery_processor_processing_latency_seconds",
		Help:    "Processing latency for delivery-processor messages.",
		Buckets: []float64{0.001, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2, 5, 10},
	},
	[]string{"result"},
)

var processorRetriesTotal = prometheus.NewCounterVec(
	prometheus.CounterOpts{
		Name: "ohmf_delivery_processor_retry_total",
		Help: "Total number of delivery-processor retries or retry-like deferrals.",
	},
	[]string{"reason"},
)

var dbQueriesTotal = prometheus.NewCounterVec(
	prometheus.CounterOpts{
		Name: "ohmf_delivery_processor_db_queries_total",
		Help: "Total number of PostgreSQL queries observed by the delivery processor.",
	},
	[]string{"statement", "outcome"},
)

var dbQueryLatency = prometheus.NewHistogramVec(
	prometheus.HistogramOpts{
		Name:    "ohmf_delivery_processor_db_query_latency_seconds",
		Help:    "Latency of PostgreSQL queries observed by the delivery processor.",
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
			log.Printf("delivery-processor metrics server failed: %v", err)
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

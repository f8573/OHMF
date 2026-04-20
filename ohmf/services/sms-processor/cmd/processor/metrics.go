package main

import (
	"log"
	"net/http"
	"strings"
	"sync"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

var metricsOnce sync.Once

var processorResultsTotal = prometheus.NewCounterVec(
	prometheus.CounterOpts{
		Name: "ohmf_sms_processor_handler_results_total",
		Help: "Total number of sms-processor handler outcomes.",
	},
	[]string{"result"},
)

var processorLatency = prometheus.NewHistogramVec(
	prometheus.HistogramOpts{
		Name:    "ohmf_sms_processor_processing_latency_seconds",
		Help:    "Processing latency for sms-processor messages.",
		Buckets: []float64{0.001, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2, 5, 10},
	},
	[]string{"result"},
)

var processorRetriesTotal = prometheus.NewCounterVec(
	prometheus.CounterOpts{
		Name: "ohmf_sms_processor_retry_total",
		Help: "Total number of sms-processor retries or retry-like deferrals.",
	},
	[]string{"reason"},
)

func initMetrics() {
	metricsOnce.Do(func() {
		prometheus.MustRegister(
			processorResultsTotal,
			processorLatency,
			processorRetriesTotal,
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
			log.Printf("sms-processor metrics server failed: %v", err)
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

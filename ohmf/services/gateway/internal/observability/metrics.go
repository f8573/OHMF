package observability

import (
	"bufio"
	"net"
	"net/http"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/go-chi/chi/v5"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

var metricsOnce sync.Once

var httpRequestsTotal = prometheus.NewCounterVec(
	prometheus.CounterOpts{
		Name: "ohmf_gateway_http_requests_total",
		Help: "Total number of HTTP requests handled by the gateway.",
	},
	[]string{"method", "route", "status"},
)

var httpRequestDuration = prometheus.NewHistogramVec(
	prometheus.HistogramOpts{
		Name:    "ohmf_gateway_http_request_duration_seconds",
		Help:    "HTTP request latency for the gateway.",
		Buckets: prometheus.DefBuckets,
	},
	[]string{"method", "route", "status"},
)

var httpRequestsInflight = prometheus.NewGaugeVec(
	prometheus.GaugeOpts{
		Name: "ohmf_gateway_http_requests_in_flight",
		Help: "Current number of in-flight HTTP requests handled by the gateway.",
	},
	[]string{"method", "route"},
)

var wsConnectionsActive = prometheus.NewGauge(
	prometheus.GaugeOpts{
		Name: "ohmf_gateway_ws_connections_active",
		Help: "Current number of active WebSocket gateway connections.",
	},
)

var wsMessagesTotal = prometheus.NewCounterVec(
	prometheus.CounterOpts{
		Name: "ohmf_gateway_ws_messages_total",
		Help: "Total number of WebSocket messages handled by the gateway.",
	},
	[]string{"direction", "event"},
)

var redisAckFailedAfterPersistenceTotal = prometheus.NewCounter(
	prometheus.CounterOpts{
		Name: "redis_ack_failed_after_persistence_total",
		Help: "Total number of async sends that observed a durable message before Redis ack delivery failed.",
	},
)

var ackTimeoutAfterPersistenceTotal = prometheus.NewCounter(
	prometheus.CounterOpts{
		Name: "ack_timeout_after_persistence_total",
		Help: "Total number of async sends that timed out after the processor had already durably persisted the message.",
	},
)

var idempotentSuccessAfterAckTimeoutTotal = prometheus.NewCounter(
	prometheus.CounterOpts{
		Name: "idempotent_success_after_ack_timeout_total",
		Help: "Total number of canonical idempotent responses returned after a post-persistence ack timeout.",
	},
)

var sendHandler500Total = prometheus.NewCounterVec(
	prometheus.CounterOpts{
		Name: "send_handler_500_total",
		Help: "Total number of send handler 500 responses, partitioned by cause.",
	},
	[]string{"cause"},
)

var dbQueriesTotal = prometheus.NewCounterVec(
	prometheus.CounterOpts{
		Name: "ohmf_gateway_db_queries_total",
		Help: "Total number of gateway PostgreSQL queries observed through pgx tracing.",
	},
	[]string{"statement", "outcome"},
)

var dbQueryLatency = prometheus.NewHistogramVec(
	prometheus.HistogramOpts{
		Name:    "ohmf_gateway_db_query_latency_seconds",
		Help:    "Latency of gateway PostgreSQL queries observed through pgx tracing.",
		Buckets: []float64{0.0005, 0.001, 0.0025, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2, 5},
	},
	[]string{"statement", "outcome"},
)

type statusRecorder struct {
	http.ResponseWriter
	status int
}

func (r *statusRecorder) WriteHeader(status int) {
	r.status = status
	r.ResponseWriter.WriteHeader(status)
}

func (r *statusRecorder) Flush() {
	if f, ok := r.ResponseWriter.(http.Flusher); ok {
		f.Flush()
	}
}

func (r *statusRecorder) Hijack() (net.Conn, *bufio.ReadWriter, error) {
	if h, ok := r.ResponseWriter.(http.Hijacker); ok {
		return h.Hijack()
	}
	return nil, nil, http.ErrNotSupported
}

func (r *statusRecorder) Push(target string, opts *http.PushOptions) error {
	if p, ok := r.ResponseWriter.(http.Pusher); ok {
		return p.Push(target, opts)
	}
	return http.ErrNotSupported
}

func (r *statusRecorder) Unwrap() http.ResponseWriter {
	return r.ResponseWriter
}

func initMetrics() {
	metricsOnce.Do(func() {
		prometheus.MustRegister(
			httpRequestsTotal,
			httpRequestDuration,
			httpRequestsInflight,
			wsConnectionsActive,
			wsMessagesTotal,
			redisAckFailedAfterPersistenceTotal,
			ackTimeoutAfterPersistenceTotal,
			idempotentSuccessAfterAckTimeoutTotal,
			sendHandler500Total,
			dbQueriesTotal,
			dbQueryLatency,
		)
	})
}

func MetricsHandler() http.Handler {
	initMetrics()
	return promhttp.Handler()
}

func HTTPMetricsMiddleware(next http.Handler) http.Handler {
	initMetrics()
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		start := time.Now()
		recorder := &statusRecorder{ResponseWriter: w, status: http.StatusOK}
		routeLabel := routePattern(r)
		httpRequestsInflight.WithLabelValues(r.Method, routeLabel).Inc()
		defer func() {
			status := strconv.Itoa(recorder.status)
			httpRequestsInflight.WithLabelValues(r.Method, routeLabel).Dec()
			httpRequestsTotal.WithLabelValues(r.Method, routeLabel, status).Inc()
			httpRequestDuration.WithLabelValues(r.Method, routeLabel, status).Observe(time.Since(start).Seconds())
		}()
		next.ServeHTTP(recorder, r)
	})
}

func IncWSConnection() {
	initMetrics()
	wsConnectionsActive.Inc()
}

func DecWSConnection() {
	initMetrics()
	wsConnectionsActive.Dec()
}

func RecordWSMessage(direction, event string) {
	initMetrics()
	if direction == "" {
		direction = "unknown"
	}
	if event == "" {
		event = "unknown"
	}
	wsMessagesTotal.WithLabelValues(direction, event).Inc()
}

func RecordRedisAckFailedAfterPersistence() {
	initMetrics()
	redisAckFailedAfterPersistenceTotal.Inc()
}

func RecordAckTimeoutAfterPersistence() {
	initMetrics()
	ackTimeoutAfterPersistenceTotal.Inc()
}

func RecordIdempotentSuccessAfterAckTimeout() {
	initMetrics()
	idempotentSuccessAfterAckTimeoutTotal.Inc()
}

func RecordSendHandler500(cause string) {
	initMetrics()
	if cause == "" {
		cause = "unknown"
	}
	sendHandler500Total.WithLabelValues(cause).Inc()
}

func RecordDBQuery(statement string, err error, duration time.Duration) {
	initMetrics()
	outcome := "ok"
	if err != nil {
		outcome = "error"
	}
	statement = normalizeStatement(statement)
	dbQueriesTotal.WithLabelValues(statement, outcome).Inc()
	if duration >= 0 {
		dbQueryLatency.WithLabelValues(statement, outcome).Observe(duration.Seconds())
	}
}

func routePattern(r *http.Request) string {
	if rctx := chi.RouteContext(r.Context()); rctx != nil {
		if pattern := rctx.RoutePattern(); pattern != "" {
			return pattern
		}
	}
	if r.URL.Path == "" {
		return "/"
	}
	return r.URL.Path
}

func normalizeStatement(sql string) string {
	sql = strings.TrimSpace(strings.ToLower(sql))
	if sql == "" {
		return "unknown"
	}
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

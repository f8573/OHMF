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

var messageSendRequestsTotal = prometheus.NewCounterVec(
	prometheus.CounterOpts{
		Name: "ohmf_gateway_messages_send_requests_total",
		Help: "Total number of message send attempts handled by the gateway.",
	},
	[]string{"endpoint", "content_type", "transport", "outcome", "queued"},
)

var messageSendAckLatency = prometheus.NewHistogramVec(
	prometheus.HistogramOpts{
		Name:    "ohmf_gateway_messages_send_ack_latency_seconds",
		Help:    "Gateway-observed latency from send request handling start to ack or terminal outcome.",
		Buckets: []float64{0.001, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2, 5, 10},
	},
	[]string{"endpoint", "content_type", "transport", "outcome", "queued"},
)

var messagesPersistedTotal = prometheus.NewCounterVec(
	prometheus.CounterOpts{
		Name: "ohmf_gateway_messages_persisted_total",
		Help: "Total number of newly persisted messages produced by the gateway send paths.",
	},
	[]string{"endpoint", "mode", "content_type", "transport"},
)

var messagePersistLatency = prometheus.NewHistogramVec(
	prometheus.HistogramOpts{
		Name:    "ohmf_gateway_messages_persist_latency_seconds",
		Help:    "Latency from gateway send-path start to message persistence.",
		Buckets: []float64{0.001, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2, 5, 10},
	},
	[]string{"endpoint", "mode", "content_type", "transport"},
)

var realtimeOnlineDeliveryUpdatesTotal = prometheus.NewCounterVec(
	prometheus.CounterOpts{
		Name: "ohmf_gateway_realtime_online_delivery_updates_total",
		Help: "Total number of online delivery updates emitted by the gateway realtime path.",
	},
	[]string{"transport", "status"},
)

var realtimeOnlineDeliveryLatency = prometheus.NewHistogramVec(
	prometheus.HistogramOpts{
		Name:    "ohmf_gateway_realtime_online_delivery_update_latency_seconds",
		Help:    "Latency from message persistence to online delivery update emission when that timestamp is available.",
		Buckets: []float64{0.001, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2, 5, 10, 30},
	},
	[]string{"transport", "status"},
)

var realtimeResumeRequestsTotal = prometheus.NewCounterVec(
	prometheus.CounterOpts{
		Name: "ohmf_gateway_realtime_resume_requests_total",
		Help: "Total number of realtime resume or reconnect requests handled by the gateway.",
	},
	[]string{"result"},
)

var realtimeReplayEventsTotal = prometheus.NewCounterVec(
	prometheus.CounterOpts{
		Name: "ohmf_gateway_realtime_replay_events_total",
		Help: "Total number of replayed user events sent during reconnect or resume handling.",
	},
	[]string{"source"},
)

var realtimeReplayBatchSize = prometheus.NewHistogramVec(
	prometheus.HistogramOpts{
		Name:    "ohmf_gateway_realtime_replay_batch_size",
		Help:    "Batch size of replayed user events during reconnect or resume handling.",
		Buckets: []float64{0, 1, 2, 5, 10, 25, 50, 100, 250, 500, 1000},
	},
	[]string{"source", "has_more"},
)

var realtimeResyncRequiredTotal = prometheus.NewCounterVec(
	prometheus.CounterOpts{
		Name: "ohmf_gateway_realtime_resync_required_total",
		Help: "Total number of times the gateway required an explicit resync after reconnect or replay.",
	},
	[]string{"reason"},
)

var authVerifyRequestsTotal = prometheus.NewCounterVec(
	prometheus.CounterOpts{
		Name: "ohmf_gateway_auth_verify_requests_total",
		Help: "Total number of phone verification completions handled by the gateway.",
	},
	[]string{"outcome"},
)

var authVerifyLatency = prometheus.NewHistogramVec(
	prometheus.HistogramOpts{
		Name:    "ohmf_gateway_auth_verify_latency_seconds",
		Help:    "Latency of phone verification completions handled by the gateway.",
		Buckets: []float64{0.001, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2, 5, 10},
	},
	[]string{"outcome"},
)

var authPairingCompleteRequestsTotal = prometheus.NewCounterVec(
	prometheus.CounterOpts{
		Name: "ohmf_gateway_auth_pairing_complete_requests_total",
		Help: "Total number of device pairing completions handled by the gateway.",
	},
	[]string{"outcome"},
)

var authPairingCompleteLatency = prometheus.NewHistogramVec(
	prometheus.HistogramOpts{
		Name:    "ohmf_gateway_auth_pairing_complete_latency_seconds",
		Help:    "Latency of device pairing completions handled by the gateway.",
		Buckets: []float64{0.001, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2, 5, 10},
	},
	[]string{"outcome"},
)

var realtimeWSUpgradesTotal = prometheus.NewCounterVec(
	prometheus.CounterOpts{
		Name: "ohmf_gateway_realtime_ws_upgrades_total",
		Help: "Total number of WebSocket upgrade attempts handled by the gateway.",
	},
	[]string{"version", "result"},
)

var realtimeHelloLatency = prometheus.NewHistogramVec(
	prometheus.HistogramOpts{
		Name:    "ohmf_gateway_realtime_hello_latency_seconds",
		Help:    "Latency from receiving a realtime hello to hello completion.",
		Buckets: []float64{0.0005, 0.001, 0.0025, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2, 5},
	},
	[]string{"mode", "result"},
)

var realtimeReplayDuration = prometheus.NewHistogramVec(
	prometheus.HistogramOpts{
		Name:    "ohmf_gateway_realtime_replay_duration_seconds",
		Help:    "Duration of realtime replay handling batches.",
		Buckets: []float64{0.0005, 0.001, 0.0025, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2, 5},
	},
	[]string{"source", "result"},
)

var realtimeUserEventsPublishedTotal = prometheus.NewCounterVec(
	prometheus.CounterOpts{
		Name: "ohmf_gateway_realtime_user_events_published_total",
		Help: "Total number of realtime user events published to Redis for websocket fanout.",
	},
	[]string{"event_type"},
)

var realtimeUserEventPersistToPublishLatency = prometheus.NewHistogramVec(
	prometheus.HistogramOpts{
		Name:    "ohmf_gateway_realtime_user_event_persist_to_publish_latency_seconds",
		Help:    "Latency from message persistence to publishing the corresponding realtime user event.",
		Buckets: []float64{0.0005, 0.001, 0.0025, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2, 5, 10},
	},
	[]string{"event_type"},
)

var realtimeUserEventPublishToWSWriteLatency = prometheus.NewHistogramVec(
	prometheus.HistogramOpts{
		Name:    "ohmf_gateway_realtime_user_event_publish_to_ws_write_latency_seconds",
		Help:    "Latency from publishing a realtime user event to websocket write completion for that event.",
		Buckets: []float64{0.0005, 0.001, 0.0025, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2, 5, 10},
	},
	[]string{"event_type"},
)

var replicationWakeupsTotal = prometheus.NewCounterVec(
	prometheus.CounterOpts{
		Name: "ohmf_gateway_replication_wakeups_total",
		Help: "Total number of sync fanout worker wakeups by wake reason.",
	},
	[]string{"reason"},
)

var replicationDomainEventAge = prometheus.NewHistogram(
	prometheus.HistogramOpts{
		Name:    "ohmf_gateway_replication_domain_event_age_seconds",
		Help:    "Age of a domain event when the sync fanout worker picks it up for processing.",
		Buckets: []float64{0.001, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2, 5, 10, 30},
	},
)

var replicationBatchSize = prometheus.NewHistogram(
	prometheus.HistogramOpts{
		Name:    "ohmf_gateway_replication_batch_size",
		Help:    "Number of domain events processed in a sync fanout batch.",
		Buckets: []float64{0, 1, 2, 5, 10, 25, 50, 100, 250, 500, 1000},
	},
)

var replicationTransactionLatency = prometheus.NewHistogramVec(
	prometheus.HistogramOpts{
		Name:    "ohmf_gateway_replication_transaction_latency_seconds",
		Help:    "Latency of the Postgres transaction used to process a sync fanout batch.",
		Buckets: []float64{0.0005, 0.001, 0.0025, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2, 5, 10},
	},
	[]string{"result"},
)

var replicationRowsAffected = prometheus.NewHistogramVec(
	prometheus.HistogramOpts{
		Name:    "ohmf_gateway_replication_rows_affected",
		Help:    "Number of user-event or conversation-state rows affected by a sync fanout batch.",
		Buckets: []float64{0, 1, 2, 5, 10, 25, 50, 100, 250, 500, 1000, 2500, 5000},
	},
	[]string{"kind"},
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

type publishedUserEventRecord struct {
	EventType   string
	PublishedAt time.Time
}

var publishedUserEventsMu sync.Mutex
var publishedUserEvents = map[int64]publishedUserEventRecord{}
var lastPublishedUserEventCleanup time.Time

const publishedUserEventRetention = 10 * time.Minute
const publishedUserEventCleanupInterval = time.Minute

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
			messageSendRequestsTotal,
			messageSendAckLatency,
			messagesPersistedTotal,
			messagePersistLatency,
			realtimeOnlineDeliveryUpdatesTotal,
			realtimeOnlineDeliveryLatency,
			realtimeResumeRequestsTotal,
			realtimeReplayEventsTotal,
			realtimeReplayBatchSize,
			realtimeResyncRequiredTotal,
			authVerifyRequestsTotal,
			authVerifyLatency,
			authPairingCompleteRequestsTotal,
			authPairingCompleteLatency,
			realtimeWSUpgradesTotal,
			realtimeHelloLatency,
			realtimeReplayDuration,
			realtimeUserEventsPublishedTotal,
			realtimeUserEventPersistToPublishLatency,
			realtimeUserEventPublishToWSWriteLatency,
			replicationWakeupsTotal,
			replicationDomainEventAge,
			replicationBatchSize,
			replicationTransactionLatency,
			replicationRowsAffected,
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

func RecordMessageSend(endpoint, contentType, transport, outcome string, queued bool, duration time.Duration) {
	initMetrics()
	endpoint = normalizeMetricLabel(endpoint, "unknown")
	contentType = normalizeMetricLabel(contentType, "unknown")
	transport = normalizeMetricLabel(transport, "unknown")
	outcome = normalizeMetricLabel(outcome, "unknown")
	queuedLabel := strconv.FormatBool(queued)
	messageSendRequestsTotal.WithLabelValues(endpoint, contentType, transport, outcome, queuedLabel).Inc()
	if duration >= 0 {
		messageSendAckLatency.WithLabelValues(endpoint, contentType, transport, outcome, queuedLabel).Observe(duration.Seconds())
	}
}

func RecordMessagePersist(endpoint, mode, contentType, transport string, duration time.Duration) {
	initMetrics()
	endpoint = normalizeMetricLabel(endpoint, "unknown")
	mode = normalizeMetricLabel(mode, "unknown")
	contentType = normalizeMetricLabel(contentType, "unknown")
	transport = normalizeMetricLabel(transport, "unknown")
	messagesPersistedTotal.WithLabelValues(endpoint, mode, contentType, transport).Inc()
	if duration >= 0 {
		messagePersistLatency.WithLabelValues(endpoint, mode, contentType, transport).Observe(duration.Seconds())
	}
}

func RecordRealtimeOnlineDeliveryUpdate(transport, status string, duration time.Duration) {
	initMetrics()
	transport = normalizeMetricLabel(transport, "unknown")
	status = normalizeMetricLabel(status, "unknown")
	realtimeOnlineDeliveryUpdatesTotal.WithLabelValues(transport, status).Inc()
	if duration >= 0 {
		realtimeOnlineDeliveryLatency.WithLabelValues(transport, status).Observe(duration.Seconds())
	}
}

func RecordRealtimeResume(result string) {
	initMetrics()
	realtimeResumeRequestsTotal.WithLabelValues(normalizeMetricLabel(result, "unknown")).Inc()
}

func RecordRealtimeReplayBatch(source string, events int, hasMore bool) {
	initMetrics()
	source = normalizeMetricLabel(source, "unknown")
	if events < 0 {
		events = 0
	}
	realtimeReplayEventsTotal.WithLabelValues(source).Add(float64(events))
	realtimeReplayBatchSize.WithLabelValues(source, strconv.FormatBool(hasMore)).Observe(float64(events))
}

func RecordRealtimeResyncRequired(reason string) {
	initMetrics()
	realtimeResyncRequiredTotal.WithLabelValues(normalizeMetricLabel(reason, "unknown")).Inc()
}

func RecordAuthVerify(outcome string, duration time.Duration) {
	initMetrics()
	outcome = normalizeMetricLabel(outcome, "unknown")
	authVerifyRequestsTotal.WithLabelValues(outcome).Inc()
	if duration >= 0 {
		authVerifyLatency.WithLabelValues(outcome).Observe(duration.Seconds())
	}
}

func RecordPairingComplete(outcome string, duration time.Duration) {
	initMetrics()
	outcome = normalizeMetricLabel(outcome, "unknown")
	authPairingCompleteRequestsTotal.WithLabelValues(outcome).Inc()
	if duration >= 0 {
		authPairingCompleteLatency.WithLabelValues(outcome).Observe(duration.Seconds())
	}
}

func RecordRealtimeWSUpgrade(version, result string) {
	initMetrics()
	realtimeWSUpgradesTotal.WithLabelValues(normalizeMetricLabel(version, "unknown"), normalizeMetricLabel(result, "unknown")).Inc()
}

func RecordRealtimeHello(mode, result string, duration time.Duration) {
	initMetrics()
	mode = normalizeMetricLabel(mode, "unknown")
	result = normalizeMetricLabel(result, "unknown")
	if duration >= 0 {
		realtimeHelloLatency.WithLabelValues(mode, result).Observe(duration.Seconds())
	}
}

func RecordRealtimeReplayDuration(source, result string, duration time.Duration) {
	initMetrics()
	source = normalizeMetricLabel(source, "unknown")
	result = normalizeMetricLabel(result, "unknown")
	if duration >= 0 {
		realtimeReplayDuration.WithLabelValues(source, result).Observe(duration.Seconds())
	}
}

func RecordRealtimeUserEventPublished(eventType string, userEventID int64, persistedAt, publishedAt time.Time) {
	initMetrics()
	eventType = normalizeMetricLabel(eventType, "unknown")
	realtimeUserEventsPublishedTotal.WithLabelValues(eventType).Inc()
	if !persistedAt.IsZero() && !publishedAt.IsZero() && !publishedAt.Before(persistedAt) {
		realtimeUserEventPersistToPublishLatency.WithLabelValues(eventType).Observe(publishedAt.Sub(persistedAt).Seconds())
	}
	if userEventID > 0 && !publishedAt.IsZero() {
		publishedUserEventsMu.Lock()
		maybeCleanupPublishedUserEventsLocked(publishedAt)
		publishedUserEvents[userEventID] = publishedUserEventRecord{
			EventType:   eventType,
			PublishedAt: publishedAt,
		}
		publishedUserEventsMu.Unlock()
	}
}

func RecordRealtimeUserEventWriteCompleted(eventType string, userEventID int64, wroteAt time.Time) {
	initMetrics()
	if userEventID <= 0 || wroteAt.IsZero() {
		return
	}
	publishedUserEventsMu.Lock()
	record, ok := publishedUserEvents[userEventID]
	if ok && !record.PublishedAt.IsZero() && wroteAt.After(record.PublishedAt) {
		label := normalizeMetricLabel(record.EventType, "unknown")
		if strings.TrimSpace(eventType) != "" {
			label = normalizeMetricLabel(eventType, label)
		}
		realtimeUserEventPublishToWSWriteLatency.WithLabelValues(label).Observe(wroteAt.Sub(record.PublishedAt).Seconds())
	}
	maybeCleanupPublishedUserEventsLocked(wroteAt)
	publishedUserEventsMu.Unlock()
}

func RecordReplicationWakeup(reason string) {
	initMetrics()
	replicationWakeupsTotal.WithLabelValues(normalizeMetricLabel(reason, "unknown")).Inc()
}

func RecordReplicationDomainEventAge(age time.Duration) {
	initMetrics()
	if age >= 0 {
		replicationDomainEventAge.Observe(age.Seconds())
	}
}

func RecordReplicationBatch(size int) {
	initMetrics()
	if size < 0 {
		size = 0
	}
	replicationBatchSize.Observe(float64(size))
}

func RecordReplicationTransaction(result string, duration time.Duration) {
	initMetrics()
	result = normalizeMetricLabel(result, "unknown")
	if duration >= 0 {
		replicationTransactionLatency.WithLabelValues(result).Observe(duration.Seconds())
	}
}

func RecordReplicationRows(kind string, count int) {
	initMetrics()
	if count < 0 {
		count = 0
	}
	replicationRowsAffected.WithLabelValues(normalizeMetricLabel(kind, "unknown")).Observe(float64(count))
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

func normalizeMetricLabel(value, fallback string) string {
	value = strings.TrimSpace(strings.ToLower(value))
	if value == "" {
		return fallback
	}
	value = strings.ReplaceAll(value, " ", "_")
	return value
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
	head := fields[0]
	switch head {
	case "select", "insert", "update", "delete", "with":
		return head
	default:
		return "other"
	}
}

func maybeCleanupPublishedUserEventsLocked(now time.Time) {
	if now.IsZero() {
		now = time.Now().UTC()
	}
	if !lastPublishedUserEventCleanup.IsZero() && now.Sub(lastPublishedUserEventCleanup) < publishedUserEventCleanupInterval {
		return
	}
	cutoff := now.Add(-publishedUserEventRetention)
	for userEventID, record := range publishedUserEvents {
		if record.PublishedAt.Before(cutoff) {
			delete(publishedUserEvents, userEventID)
		}
	}
	lastPublishedUserEventCleanup = now
}

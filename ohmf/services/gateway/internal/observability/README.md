# 19.11 — Gateway: Observability & Telemetry

Mapping: OHMF spec section 20 (Observability).

Purpose
- Centralize metrics, tracing, and structured logging for the gateway and its internal components.

Expected behavior
- Produce Prometheus metrics (exporter), OpenTelemetry traces and baggage, and JSON structured logs to stdout.

Metrics (recommended)
- http_requests_total{method,route,status}
- ws_connections_active
- messages_ingested_total
- db_query_duration_seconds (histogram)
- event_bus_publish_latency_seconds

Metrics (implemented in gateway exporter)
- ohmf_gateway_http_requests_total
- ohmf_gateway_http_request_duration_seconds
- ohmf_gateway_http_requests_in_flight
- ohmf_gateway_ws_connections_active
- ohmf_gateway_ws_messages_total
- ohmf_gateway_messages_send_requests_total
- ohmf_gateway_messages_send_ack_latency_seconds
- ohmf_gateway_messages_persisted_total
- ohmf_gateway_messages_persist_latency_seconds
- ohmf_gateway_realtime_online_delivery_updates_total
- ohmf_gateway_realtime_online_delivery_update_latency_seconds
- ohmf_gateway_realtime_resume_requests_total
- ohmf_gateway_realtime_replay_events_total
- ohmf_gateway_realtime_replay_batch_size
- ohmf_gateway_realtime_resync_required_total

Trace semantics
- Start root span per incoming request; propagate context to downstream services via `traceparent` header (W3C trace context).

Log schema (JSON)
- Fields: timestamp, level, msg, service, request_id, trace_id, user_id, route, duration_ms

Implementation constraints
- Do not block request paths for telemetry writes; use async exporters and batching.
- Provide sampling controls for traces (default 0.1 for high-volume endpoints).

Security considerations
- Do not log PII; redact phone numbers beyond last 4 digits unless explicitly allowed.

Operational notes
- Alerts for error rate > 1% over 5m, queue backpressure, or event bus publish failures.

Testing requirements
- Integration test for trace propagation through gateway to backend.

References
- infra for collector configuration.

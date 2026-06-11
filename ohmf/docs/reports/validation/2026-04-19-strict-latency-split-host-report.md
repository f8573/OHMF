# Strict-Latency Reduction And Split-Host Load Campaign

Date: 2026-04-19

## Scope

This report captures the implementation and validation work for the strict-latency reduction plan and the split-host load campaign foundation for OHMF.

The goals were:

- shorten the strict `send-to-ack` path without weakening the metric definition
- reduce `persist -> publish` latency in the realtime fanout path
- add a benchmark compose profile that matches the host-PC test topology
- extend the load suite so the host can act as a controller and 1-2 laptops can act as remote agents

The strict client metric definition was preserved. `send-to-ack` still waits for the definitive sender-side appended event, not a provisional ingress response.

## Summary Of What Was Accomplished

The work completed in this round:

- converted gateway ack waiting from Redis polling to a push-first in-memory waiter model with Redis pub/sub durability fallback
- moved messages-processor ack emission to immediately after the Postgres commit
- removed synchronous Cassandra projection from the ack-critical path by introducing an asynchronous projector service on `msg.persisted.v1`
- split sync fanout into dedicated worker services for benchmark mode
- replaced sync-fanout idle polling with `LISTEN/NOTIFY` plus a `1s` fallback poll
- bulked fanout writes for message-created and related replication flows
- added processor and replication latency histograms required to reason about the bottlenecks directly
- added distributed `controller` and `agent` load-suite modes, assignment APIs, agent clock-offset correction, event uploads, and host-side merged reporting
- fixed two benchmark-profile runtime defects discovered during validation:
  - duplicate Docker image builds under the same tag in the compose override
  - sync-fanout services accidentally starting the API entrypoint instead of the worker binary

## Implementation Details

### 1. Strict `send-to-ack` path

The critical-path changes are in:

- `ohmf/services/gateway/internal/messages/async.go`
- `ohmf/services/messages-processor/cmd/processor/main.go`
- `ohmf/services/messages-processor/cmd/processor/metrics.go`
- `ohmf/services/messages-processor/cmd/projector/main.go`
- `ohmf/services/messages-processor/Dockerfile.projector`

What changed:

- The gateway async pipeline now registers in-process waiters per `event_id`.
- The messages-processor stores the persisted ack in Redis and also publishes it to a shared Redis channel.
- `WaitAck` now:
  - checks Redis once for a fast already-persisted hit
  - waits on the in-process channel for the common case
  - falls back to a final Redis read on timeout
- The messages-processor now emits the ack immediately after the Postgres transaction commits.
- Cassandra projection is no longer on the strict ack path in benchmark mode.
- Cassandra projection is handled asynchronously by the new `messages-projector` service consuming `msg.persisted.v1`.

### 2. `persist -> publish` fanout path

The fanout-path changes are in:

- `ohmf/services/gateway/internal/replication/store.go`
- `ohmf/services/gateway/internal/worker/sync_fanout.go`
- `ohmf/services/gateway/internal/observability/metrics.go`
- `ohmf/services/gateway/cmd/api/main.go`
- `ohmf/services/gateway/cmd/worker/main.go`
- `ohmf/services/gateway/internal/config/config.go`

What changed:

- Domain-event append now issues `pg_notify('ohmf_domain_events', ...)` on commit.
- Sync fanout can run in dedicated worker processes instead of inside the API in benchmark mode.
- Workers `LISTEN` for notifications and drain until empty.
- A `1s` fallback poll remains for missed notifications.
- Replication now records:
  - wake reason `notify|poll`
  - domain-event age at pickup
  - batch size
  - transaction duration
  - rows affected for inbox events and conversation state
- The replication store now uses bulk inserts and bulk upserts for fanout-heavy paths rather than looping per recipient row.

### 3. Benchmark compose profile

The benchmark-profile changes are in:

- `ohmf/infra/docker/docker-compose.benchmark.yml`
- `ohmf/infra/observability/prometheus.yml`
- `ohmf/services/gateway/Dockerfile`

The benchmark profile now supports:

- `api x1`
- `messages-processor x2`
- `sync-fanout-worker x2`
- `delivery-processor x1`
- `sms-processor x1`
- `messages-projector x1`

Important validation-time fixes:

- `messages-processor-b` now reuses the base `ohmf-messages-processor` image rather than trying to build a second image under a conflicting tag.
- `sync-fanout-worker-a` and `sync-fanout-worker-b` now correctly override the image entrypoint to `/bin/worker`.

### 4. Split-host load-suite controller/agent support

The distributed load-suite work is centered in:

- `ohmf/tools/loadsuite/controller.go`
- `ohmf/tools/loadsuite/controller_test.go`
- `ohmf/tools/loadsuite/model.go`
- `ohmf/tools/loadsuite/system.go`
- `ohmf/tools/loadsuite/client.go`
- `ohmf/tools/loadsuite/fixture.go`
- `ohmf/tools/loadsuite/campaign.go`

Implemented controller endpoints:

- `POST /agents/register`
- `GET /agents/{id}/assignment`
- `GET /agents/{id}/timesync`
- `POST /agents/{id}/events`
- `POST /agents/{id}/phase-complete`

Implemented controller/agent behavior:

- host provisions the fixture once
- devices are sharded per agent
- each conversation has one owner agent that schedules sends
- agents batch-upload raw events every second
- controller merges corrected event streams into one canonical report

Additional robustness fixes:

- zero-share agents are excluded from assignment ownership and device placement
- agent event uploads requeue on transient controller failure instead of dropping pending raw events
- agent clock offset is refreshed every 60 seconds, not only at startup

## Metrics Added

### Messages-processor

- `ohmf_messages_processor_kafka_consume_lag_seconds`
- `ohmf_messages_processor_postgres_transaction_latency_seconds`
- `ohmf_messages_processor_cassandra_projection_latency_seconds`
- `ohmf_messages_processor_ack_publish_latency_seconds`

### Gateway replication

- `ohmf_gateway_replication_wakeups_total`
- `ohmf_gateway_replication_domain_event_age_seconds`
- `ohmf_gateway_replication_batch_size`
- `ohmf_gateway_replication_transaction_latency_seconds`
- `ohmf_gateway_replication_rows_affected`

### Existing gateway send/realtime metrics used by this work

- `ohmf_gateway_messages_send_ack_latency_seconds`
- `ohmf_gateway_messages_persist_latency_seconds`
- `ohmf_gateway_realtime_hello_latency_seconds`
- `ohmf_gateway_realtime_user_event_persist_to_publish_latency_seconds`

## Validation Performed

### Build and test validation

The following commands passed:

```powershell
& 'C:\Users\James\Downloads\Messages\ohmf\.tools\go\bin\go.exe' test ./tools/loadsuite -count=1
& 'C:\Users\James\Downloads\Messages\ohmf\.tools\go\bin\go.exe' test ./internal/messages ./internal/observability ./internal/worker ./internal/replication -count=1
& 'C:\Users\James\Downloads\Messages\ohmf\.tools\go\bin\go.exe' test ./cmd/processor/... ./cmd/projector/... -count=1
node --check C:\Users\James\Downloads\Messages\scripts\test-gates.js
docker compose -f C:\Users\James\Downloads\Messages\ohmf\infra\docker\docker-compose.yml -f C:\Users\James\Downloads\Messages\ohmf\infra\docker\docker-compose.benchmark.yml config -q
```

### Live benchmark-profile validation

The benchmark stack was brought up with:

```powershell
docker compose -f C:\Users\James\Downloads\Messages\ohmf\infra\docker\docker-compose.yml -f C:\Users\James\Downloads\Messages\ohmf\infra\docker\docker-compose.benchmark.yml up -d --build
```

The following local validation runs completed against the corrected benchmark topology.

#### Benchmark smoke: active sustain

Artifact directory:

- `C:\Users\James\Downloads\Messages\ohmf\docs\reports\validation\1776653388805`

Summary:

- concurrent users: `15`
- concurrent devices: `20`
- conversations: `4`
- connect readiness: `100.00%`
- p50/p95/p99 send-to-ack: `50ms / 125ms / 133ms`
- p50/p95/p99 end-to-end delivery: `677ms / 952ms / 958ms`
- expected deliveries: `80`
- successful deliveries: `80`
- lost deliveries: `0`
- duplicate deliveries: `0`
- out-of-order deliveries: `0`
- convergence: `1.0000`
- DB p95 latency: `8ms`
- peak Kafka lag: `0`

#### Host-only distributed controller validation

Artifact directory:

- `C:\Users\James\Downloads\Messages\ohmf\docs\reports\validation\1776653599209`

Summary:

- concurrent users: `15`
- concurrent devices: `20`
- conversations: `4`
- connect readiness: `100.00%`
- p50/p95/p99 send-to-ack: `48ms / 132ms / 132ms`
- p50/p95/p99 end-to-end delivery: `726ms / 873ms / 875ms`
- expected deliveries: `48`
- successful deliveries: `48`
- lost deliveries: `0`
- duplicate deliveries: `0`
- out-of-order deliveries: `0`
- convergence: `1.0000`
- DB p95 latency: `7ms`
- peak Kafka lag: `0`

### Metric exposure verification

The rebuilt benchmark services exposed the new metric families successfully.

Verified on:

- `http://localhost:19091/metrics` for messages-processor histograms
- `http://localhost:19094/metrics` for dedicated sync-fanout worker metrics
- `http://localhost:18080/metrics` for gateway send and realtime histograms

The dedicated sync-fanout worker showed live `notify` wakeups after the corrected entrypoint fix, confirming that the benchmark profile was running the worker binary rather than accidental API replicas.

## What The Validation Proved

This round proved:

- the strict ack path is materially shorter at smoke scale on the benchmark topology
- the benchmark stack can run with dedicated sync-fanout workers and an async Cassandra projector
- the distributed controller/agent path compiles, runs, and produces a canonical merged report
- the worker-side replication metrics are live and observable in the intended dedicated-worker topology
- correctness remained clean in live validation:
  - `0` lost deliveries
  - `0` duplicate deliveries
  - `0` out-of-order deliveries
  - `100%` multi-device convergence

## Known Remaining Work

This round did not yet produce the final recruiter-facing multi-machine ceiling numbers.

Still pending:

- run a real controller-plus-laptops campaign on the LAN
- rerun the full active ladder at `375` and above on the corrected benchmark topology
- validate whether the stricter benchmark topology materially lowers `persist -> publish` p95 at moderate and high tiers
- complete the one-laptop and two-laptop acceptance targets:
  - `375` active devices with one laptop
  - `450` active devices with two laptops
  - `600+` connected idle devices
  - `200` active-device resilience run

If the high-tier strict p95 numbers remain above target after those reruns, the next step should be architectural rather than incremental: move fanout fully off Postgres domain-event processing and onto a dedicated Kafka-backed fanout pipeline.

## Recommended Next Execution Order

1. Run a single-laptop distributed `375-device` active sustain on the benchmark profile.
2. Compare `send-to-ack`, `persist -> publish`, and correctness against the last single-host `375-device` baseline.
3. Run a two-laptop `450-device` active sustain if the `375-device` tier passes.
4. Run the `200-device` resilience scenario on the corrected benchmark topology.

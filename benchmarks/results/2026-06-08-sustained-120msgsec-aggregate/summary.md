# Sustained 120 msg/sec aggregate validation - 2026-06-08

## Scope

- Local single-node Kubernetes only: `k3d` cluster `ohmf-b1`
- Overlay: `deploy/k8s/overlays/local-k3s-full`
- Driver: committed `benchmarks/cmd/loadgen`, host-executed as `benchmarks/bin/loadgen.exe`
- System under test commit: `2aa00eac7f4f5a57457034dee2f6f3b9410b5d38`
- Artifact generation head: `2aa00eac7f4f5a57457034dee2f6f3b9410b5d38`
- Path under test: authenticated `POST /v1/messages` accept path into the local Kafka/Postgres/Cassandra pipeline
- Latency labels in this artifact are **client-observed HTTP accept latency only**, not end-to-end delivery latency

## Result

Stage B1 aggregate **did not pass**.

The corrected multi-user run used `60` authenticated senders with `1` conversation each and three phases:

- Warmup 1: `10 users x 1 msg/sec x 60s`
- Warmup 2: `30 users x 2 msg/sec x 60s`
- Main: `60 users x 2 msg/sec x 720s`

Observed counts:

- Warmup 1: `600/600` accepted, `0` failures
- Warmup 2: `959/3600` accepted, `2641` failed as `http_429_ip`
- Main: `8640/86400` accepted, `77760` failed as `http_429_ip`
- Total requested: `90600`
- Total accepted: `10199`
- Total failed: `80401`, all bucketed as `http_429_ip`
- Postgres delta: `10199`
- Cassandra delta: `10199`
- Duplicates: `0`
- Missing: `0`
- Kafka lag recovery to `0`: `1.94s`

The main phase accepted exactly `8640 / 720 = 12.00 msg/sec`, which matches the gateway per-IP limiter rather than the per-user limiter.

## Supported claim

This artifact supports the narrower claim that **the committed aggregate multi-user driver corrected the single-user test design enough to clear per-user send limiting as the dominant bottleneck, but the host-driven run was still bounded by the gateway per-IP limiter**. On commit `2aa00eac7f4f5a57457034dee2f6f3b9410b5d38`, the `10 users x 1 msg/sec` warmup passed fully, the higher-rate phases failed only as `http_429_ip`, and the accepted subset reconciled cleanly to Postgres and Cassandra with Kafka lag returning to zero in `1.94s`.

## Unsupported claims

- OHMF can sustain `120 accepted msg/sec` from a single driver source IP in this local configuration
- OHMF can sustain `120 accepted msg/sec` aggregate system throughput in this local host-driven setup
- Zero loss at `120 msg/sec`
- Any `p95` or `p99` delivery latency claim
- `3,100` concurrent clients
- Production readiness
- HA, failover, or multi-node resilience
- Any cloud or production benchmark interpretation

## What changed from the prior failed run

The earlier single-sender attempt was a valid **per-user limiter validation**. This aggregate rerun proves the per-user limiter was no longer the blocking factor:

- The `10 users x 1 msg/sec` warmup passed with `600/600` accepted
- No `http_429_user` failures were observed in any phase
- All higher-rate failures were `http_429_ip`

That means the next throughput correction is not "disable the limiter"; it is "distribute load across multiple real source IPs if the goal is aggregate system throughput rather than single-source gateway throughput."

## Limiting factor

The dominant failure mode in the aggregate run was the gateway per-IP limiter, not persistence or processor throughput. The committed send path enforces:

- Per user: `30 messages / 10 seconds`, burst `60` in [service.go](/ohmf/services/gateway/internal/messages/service.go:2679)
- Per IP: `120 messages / 10 seconds`, burst `240` in [service.go](/ohmf/services/gateway/internal/messages/service.go:2698)

Because the driver ran from one host source IP through `kubectl port-forward`, the aggregate phases were bounded by the per-IP limiter. The main-phase acceptance rate of `12.00 msg/sec` exactly matches the sustained refill rate of that limiter.

## Environment

| Field | Value |
| --- | --- |
| System-under-test commit | `2aa00eac7f4f5a57457034dee2f6f3b9410b5d38` |
| Cluster | `k3d-ohmf-b1` |
| k3d | `v5.9.0` |
| Kubernetes server | `v1.35.5+k3s1` |
| Docker | Docker Desktop `4.32.0`, Engine `27.0.3` |
| Host | `windows/amd64`, `32` CPUs |
| Overlay | `deploy/k8s/overlays/local-k3s-full` |
| Images | `ohmf-gateway:dev`, `ohmf-apps:dev`, `ohmf-messages-processor:dev` |
| Principal provisioning mode | `seed_db` for benchmark setup only; send path remained real authenticated HTTP through the gateway |

## Latency

These are **client-observed HTTP accept latencies** for accepted requests only:

- p50: `36.21 ms`
- p95: `1303.67 ms`
- p99: `2026.01 ms`

## Restarts and resource observations

- Pod restarts before run: all active pods at `0`
- Pod restarts after run: all active pods still at `0`
- HPA: none present in this overlay
- Typical pre-run pod usage:
  - Cassandra about `102m CPU`, `1342Mi`
  - Kafka about `19m CPU`, `741Mi`
  - Gateway about `4m CPU`, `20Mi`
  - Postgres about `9m CPU`, `47Mi`
- Peak sampled pod usage during the run:
  - Cassandra up to about `195m CPU`, `1350Mi`
  - Gateway up to about `208m CPU`, `78Mi`
  - Kafka up to about `65m CPU`, `795Mi`
  - Postgres up to about `121m CPU`, `128Mi`
  - Messages processor up to about `85m CPU`, `9Mi`
  - Redis up to about `75m CPU`, `8Mi`

## Claim boundary

Supported:

- The prior single-user failure is valid evidence of per-user rate limiting
- This aggregate host-driven rerun shows the next limiter is per-IP, not per-user
- For the accepted subset, Postgres/Cassandra/Kafka reconciliation remained clean

Unsupported:

- Sustained `120 accepted msg/sec` from one host source IP
- Aggregate system throughput beyond the shared-source-IP gateway boundary
- End-to-end delivery latency
- Production, HA, or cloud claims

## Raw files

- Raw driver summary: [driver-summary.raw.md](/benchmarks/results/2026-06-08-sustained-120msgsec-aggregate/driver-summary.raw.md)
- Raw driver JSON: [driver-summary.raw.json](/benchmarks/results/2026-06-08-sustained-120msgsec-aggregate/driver-summary.raw.json)
- Environment capture: [env.json](/benchmarks/results/2026-06-08-sustained-120msgsec-aggregate/env.json)
- Driver stdout: [driver.stdout.log](/benchmarks/results/2026-06-08-sustained-120msgsec-aggregate/driver.stdout.log)
- Driver stderr: [driver.stderr.log](/benchmarks/results/2026-06-08-sustained-120msgsec-aggregate/driver.stderr.log)
- Observation snapshots: [observations](/benchmarks/results/2026-06-08-sustained-120msgsec-aggregate/observations)

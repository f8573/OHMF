# Local k3s throughput attempt - 120 msg/sec - 2026-06-08

## Scope

This artifact records a **local single-node** `k3d` / `k3s` run of `deploy/k8s/overlays/local-k3s-full` using the committed benchmark load driver. It is not a production, HA, or cloud benchmark.

## Outcome

Stage B1 **failed**.

The clean target run attempted `120 msg/sec` for a configured `120s` send window and recorded:

- `requested=14400`
- `sent_attempts=14400`
- `accepted=419`
- `failed http_429=13981`
- `pg_delta=419`
- `cass_delta=419`
- `duplicates=0`
- `missing=0`
- Kafka lag `-> 0` in `2.05s`

This does **not** support a claim that OHMF sustains `120 accepted msg/sec` through the local full pipeline.

## Why it failed

The observed failure mode was front-door throttling, not downstream persistence loss. The committed gateway send path enforces a per-user limiter of `30 messages / 10 seconds` with burst `60` in [service.go](/C:/Users/James/Downloads/Messages/ohmf/services/gateway/internal/messages/service.go:2679). The accepted total of `419` over `120s` closely matches that limiter envelope.

## What the run still proved

- Accepted messages reconciled cleanly to Postgres: `419`
- Cassandra shadow writes matched accepted messages: `419`
- Kafka consumer-group lag returned to `0` within `2.05s`
- No duplicates were counted
- No missing accepted messages were counted
- Pod restarts stayed at `0` before and after the run

## Environment

| Field | Value |
| --- | --- |
| Commit SHA | `a767b25091b9eeecbe83b163d6abd669253a5917` |
| Cluster | `k3d-ohmf-b1` |
| k3d | `v5.9.0` |
| Kubernetes server | `v1.35.5+k3s1` |
| Docker | Docker Desktop `4.32.0`, Engine `27.0.3` |
| Host | `windows/amd64`, `32` CPUs, Docker memory `31.46 GiB` |
| Overlay | `deploy/k8s/overlays/local-k3s-full` |
| Images | `ohmf-gateway:dev`, `ohmf-apps:dev`, `ohmf-messages-processor:dev` |

## Latency label

All latency numbers in the paired benchmark artifact are **client-observed HTTP accept latency** only:

- p50 `39.24 ms`
- p95 `2027.84 ms`
- p99 `2116.66 ms`

These are **not** delivery latencies.

## Resource notes

- No HPA exists in this overlay
- Peak sampled pod CPU during the run:
  - Cassandra about `851m`
  - Kafka about `347m`
  - Gateway about `127m`
  - Postgres about `39m`
  - Messages processor about `31m`
- Peak sampled pod memory during the run:
  - Cassandra about `1331Mi`
  - Kafka about `402Mi`
  - Gateway about `28Mi`
  - Postgres about `85Mi`
  - Messages processor about `7Mi`

## Claim boundary

Supported:

- On this local single-node cluster and this commit, the committed driver did **not** achieve `120 accepted msg/sec`
- For the accepted subset, Postgres/Cassandra/Kafka reconciliation remained clean

Unsupported:

- Sustained `120 accepted msg/sec`
- Zero loss at `120 msg/sec`
- `p95` delivery latency
- `3,100` clients
- Production readiness
- HA or failover

## Paired artifact

- Benchmark summary: [summary.md](/C:/Users/James/Downloads/Messages/benchmarks/results/2026-06-08-sustained-120msgsec/summary.md)

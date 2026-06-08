# Local k3s throughput attempt - 120 msg/sec aggregate - 2026-06-08

## Scope

This artifact records a **local single-node** `k3d` / `k3s` run of `deploy/k8s/overlays/local-k3s-full` using the committed aggregate multi-user benchmark driver. It is not a production, HA, or cloud benchmark.

## Outcome

Stage B1 aggregate **failed as designed-limited**.

The corrected run used `60` authenticated senders at `2 msg/sec` each, plus the required warmups:

- `10 users x 1 msg/sec x 60s` -> `600/600` accepted
- `30 users x 2 msg/sec x 60s` -> `959/3600` accepted, `2641` failed as `http_429_ip`
- `60 users x 2 msg/sec x 720s` -> `8640/86400` accepted, `77760` failed as `http_429_ip`

Totals:

- `requested=90600`
- `accepted=10199`
- `failed http_429_ip=80401`
- `pg_delta=10199`
- `cass_delta=10199`
- `duplicates=0`
- `missing=0`
- Kafka lag `-> 0` in `1.94s`

This does **not** support a claim that OHMF sustains `120 accepted msg/sec` through the local full pipeline from one host source IP.

## Why it failed

The aggregate rerun corrected the single-user benchmark design, but the host-driven setup still shared one gateway source IP. The committed gateway send path enforces:

- Per-user limit in [service.go](/C:/Users/James/Downloads/Messages/ohmf/services/gateway/internal/messages/service.go:2679)
- Per-IP limit in [service.go](/C:/Users/James/Downloads/Messages/ohmf/services/gateway/internal/messages/service.go:2698)

The evidence boundary moved:

- The prior run validated **per-user** limiting
- This aggregate rerun validated that the next front-door bound is **per-IP** limiting
- No `http_429_user` failures were observed here

The accepted main-phase rate was exactly `12.00 msg/sec`, matching the configured per-IP refill rate of `120 messages / 10 seconds`.

## What the run still proved

- Multi-user aggregate scheduling worked and cleared the prior single-user bottleneck
- Accepted messages reconciled cleanly to Postgres: `10199`
- Cassandra shadow writes matched accepted messages: `10199`
- Kafka consumer-group lag returned to `0` within `1.94s`
- No duplicates were counted
- No missing accepted messages were counted
- Pod restarts stayed at `0` before and after the run

## Resource notes

- No HPA exists in this overlay
- Peak sampled pod CPU during the run:
  - Cassandra about `195m`
  - Gateway about `208m`
  - Kafka about `65m`
  - Postgres about `121m`
  - Messages processor about `85m`
  - Redis about `75m`
- Peak sampled pod memory during the run:
  - Cassandra about `1350Mi`
  - Kafka about `795Mi`
  - Postgres about `128Mi`
  - Gateway about `78Mi`
  - Messages processor about `9Mi`
  - Redis about `8Mi`

## Claim boundary

Supported:

- The single-user failure remains valid rate-limiter evidence
- This aggregate host-driven rerun shows the next limiter is per-IP, not per-user
- For the accepted subset, Postgres/Cassandra/Kafka reconciliation remained clean

Unsupported:

- Sustained `120 accepted msg/sec` from one driver source IP
- Aggregate throughput beyond the shared-source-IP gateway boundary
- `p95` delivery latency
- `3,100` clients
- Production readiness
- HA or failover

## Paired artifact

- Benchmark summary: [summary.md](/C:/Users/James/Downloads/Messages/benchmarks/results/2026-06-08-sustained-120msgsec-aggregate/summary.md)

# Local k3s pipeline diagnostic rung - 30 msg/sec multisource - 2026-06-08

## Outcome

Stage B1 multisource **passed**.

- Loadgen pods / source IPs observed: `12` / `12`
- Per-pod target: `mixed 2-3 msg/sec per pod`
- Aggregate target: `30 msg/sec`
- Main-phase duration: `120s`
- Requested / sent / accepted: `4500` / `4500` / `4500`
- Postgres / Cassandra delta: `4500` / `4500`
- Missing / duplicates: `0` / `0`
- Kafka lag settled to zero: `True` in `2.070s`

## Claim boundary

Sustained 30 aggregate msg/sec through the local Kubernetes full pipeline across 12 source IPs, with 4500/4500 accepted messages reconciling to Postgres (4500) and Cassandra (4500) and Kafka lag returning to zero in 2.07 seconds.

Unsupported:

- Any p95 or p99 delivery latency claim
- 3,100 concurrent clients
- Production readiness
- HA or failover
- Any cloud or production benchmark interpretation

## Paired artifact

- Benchmark summary: [summary.md](C:/Users/James/Downloads/Messages/benchmarks/results/2026-06-08-pipeline-diagnostic-ladder/30msgsec/summary.md)

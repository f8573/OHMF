# Local k3s processor stage diagnostic - 45 msg/sec multisource - 2026-06-08

## Outcome

Stage B1 multisource **passed**.

- Loadgen pods / source IPs observed: `12` / `12`
- Per-pod target: `mixed 3-4 msg/sec per pod`
- Aggregate target: `45 msg/sec`
- Main-phase duration: `120s`
- Requested / sent / accepted: `6750` / `6750` / `6750`
- Postgres / Cassandra delta: `6750` / `6750`
- Missing / duplicates: `0` / `0`
- Kafka lag settled to zero: `True` in `2.060s`

## Claim boundary

Sustained 45 aggregate msg/sec through the local Kubernetes full pipeline across 12 source IPs, with 6750/6750 accepted messages reconciling to Postgres (6750) and Cassandra (6750) and Kafka lag returning to zero in 2.06 seconds.

Unsupported:

- Any p95 or p99 delivery latency claim
- 3,100 concurrent clients
- Production readiness
- HA or failover
- Any cloud or production benchmark interpretation

## Paired artifact

- Benchmark summary: [summary.md](benchmarks/results/2026-06-08-processor-stage-instrumentation/45msgsec/summary.md)

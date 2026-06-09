# Local k3s processor-scaling dry run - 2 replicas - 105 msg/sec - 2026-06-09

## Outcome

Stage B1 multisource **passed**.

- Loadgen pods / source IPs observed: `12` / `12`
- Per-pod target: `mixed 8-9 msg/sec per pod`
- Aggregate target: `105 msg/sec`
- Main-phase duration: `60s`
- Requested / sent / accepted: `8775` / `8775` / `8775`
- Postgres / Cassandra delta: `8775` / `8775`
- Missing / duplicates: `0` / `0`
- Kafka lag settled to zero: `True` in `1.980s`

## Claim boundary

Sustained 105 aggregate msg/sec through the local Kubernetes full pipeline across 12 source IPs, with 8775/8775 accepted messages reconciling to Postgres (8775) and Cassandra (8775) and Kafka lag returning to zero in 1.98 seconds.

Unsupported:

- Any p95 or p99 delivery latency claim
- 3,100 concurrent clients
- Production readiness
- HA or failover
- Any cloud or production benchmark interpretation

## Paired artifact

- Benchmark summary: [summary.md](benchmarks/tmp/processor-scaling-dry-run-fix2-20260609t062434/2replicas-105msgsec/summary.md)

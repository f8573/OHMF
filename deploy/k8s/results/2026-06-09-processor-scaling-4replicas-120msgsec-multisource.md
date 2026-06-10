# Local k3s processor-scaling rung - 4 replicas - 120 msg/sec - 2026-06-09

## Outcome

Stage B1 multisource **passed**.

- Loadgen pods / source IPs observed: `12` / `12`
- Per-pod target: `10 msg/sec per pod`
- Aggregate target: `120 msg/sec`
- Main-phase duration: `600s`
- Requested / sent / accepted: `74700` / `74700` / `74700`
- Postgres / Cassandra delta: `74700` / `74700`
- Missing / duplicates: `0` / `0`
- Kafka lag settled to zero: `True` in `103.145s`

## Claim boundary

Sustained 120 aggregate msg/sec through the local Kubernetes full pipeline across 12 source IPs, with 74700/74700 accepted messages reconciling to Postgres (74700) and Cassandra (74700) and Kafka lag returning to zero in 103.15 seconds.

Unsupported:

- Any p95 or p99 delivery latency claim
- 3,100 concurrent clients
- Production readiness
- HA or failover
- Any cloud or production benchmark interpretation

## Paired artifact

- Benchmark summary: [summary.md](benchmarks/results/2026-06-09-processor-scaling-matrix/4replicas-120msgsec/summary.md)

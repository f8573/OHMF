# Local k3s Stage B1 rerun rung - 90 msg/sec multisource - 2026-06-08

## Outcome

Stage B1 multisource **failed**.

- Loadgen pods / source IPs observed: `12` / `12`
- Per-pod target: `mixed 7-8 msg/sec per pod`
- Aggregate target: `90 msg/sec`
- Main-phase duration: `120s`
- Requested / sent / accepted: `13050` / `13050` / `13050`
- Postgres / Cassandra delta: `7404` / `8116`
- Missing / duplicates: `5646` / `0`
- Kafka lag settled to zero: `True` in `213.191s`

## Claim boundary

The multisource local Kubernetes run did not establish a passing 90 msg/sec aggregate claim. It remains useful only as bounded evidence for the accepted subset: 13050/13050 accepted, Postgres delta 7404, Cassandra delta 8116, Kafka lag settled_to_zero=True.

Unsupported:

- OHMF can sustain 90 accepted messages/sec aggregate in this local configuration
- Any p95 or p99 delivery latency claim
- 3,100 concurrent clients
- Production readiness
- HA or failover
- Any cloud or production benchmark interpretation

## Paired artifact

- Benchmark summary: [summary.md](C:/Users/James/Downloads/Messages/benchmarks/results/2026-06-08-stage-b1-rerun-throughput/90msgsec/summary.md)

# Local k3s pipeline diagnostic rung - 60 msg/sec multisource - 2026-06-08

## Outcome

Stage B1 multisource **failed**.

- Loadgen pods / source IPs observed: `12` / `12`
- Per-pod target: `5 msg/sec per pod`
- Aggregate target: `60 msg/sec`
- Main-phase duration: `120s`
- Requested / sent / accepted: `9000` / `9000` / `9000`
- Postgres / Cassandra delta: `7553` / `7991`
- Missing / duplicates: `1447` / `0`
- Kafka lag settled to zero: `True` in `31.873s`

## Claim boundary

The multisource local Kubernetes run did not establish a passing 60 msg/sec aggregate claim. It remains useful only as bounded evidence for the accepted subset: 9000/9000 accepted, Postgres delta 7553, Cassandra delta 7991, Kafka lag settled_to_zero=True.

Unsupported:

- OHMF can sustain 60 accepted messages/sec aggregate in this local configuration
- Any p95 or p99 delivery latency claim
- 3,100 concurrent clients
- Production readiness
- HA or failover
- Any cloud or production benchmark interpretation

## Paired artifact

- Benchmark summary: [summary.md](C:/Users/James/Downloads/Messages/benchmarks/results/2026-06-08-pipeline-diagnostic-ladder/60msgsec/summary.md)

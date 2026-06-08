# Local k3s throughput result - 120 msg/sec multisource - 2026-06-08

## Outcome

Stage B1 multisource **failed**.

- Loadgen pods / source IPs observed: `12` / `12`
- Per-pod rate: `10 msg/sec`
- Aggregate target: `120 msg/sec`
- Main-phase duration: `600s`
- Requested / sent / accepted: `82800` / `82800` / `82792`
- Postgres / Cassandra delta: `29301` / `30267`
- Missing / duplicates: `53491` / `0`
- Kafka lag settled to zero: `False` in `183.004s`

## Claim boundary

The multisource local Kubernetes run did not establish a passing 120 msg/sec aggregate claim. It remains useful only as bounded evidence for the accepted subset: 82792/82800 accepted, Postgres delta 29301, Cassandra delta 30267, Kafka lag settled_to_zero=False.

Unsupported:

- OHMF can sustain 120 accepted messages/sec aggregate in this local configuration
- Any p95 or p99 delivery latency claim
- 3,100 concurrent clients
- Production readiness
- HA or failover
- Any cloud or production benchmark interpretation

## Paired artifact

- Benchmark summary: [summary.md](C:/Users/James/Downloads/Messages/benchmarks/results/2026-06-08-sustained-120msgsec-multisource/summary.md)

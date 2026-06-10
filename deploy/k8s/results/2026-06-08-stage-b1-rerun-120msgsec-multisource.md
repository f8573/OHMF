# Local k3s Stage B1 rerun rung - 120 msg/sec multisource - 2026-06-08

## Outcome

Stage B1 multisource **failed**.

- Loadgen pods / source IPs observed: `12` / `12`
- Per-pod target: `10 msg/sec per pod`
- Aggregate target: `120 msg/sec`
- Main-phase duration: `600s`
- Requested / sent / accepted: `74700` / `74700` / `74685`
- Postgres / Cassandra delta: `26307` / `27146`
- Missing / duplicates: `48378` / `0`
- Kafka lag settled to zero: `False` in `903.001s`

## Claim boundary

The multisource local Kubernetes run did not establish a passing 120 msg/sec aggregate claim. It remains useful only as bounded evidence for the accepted subset: 74685/74700 accepted, Postgres delta 26307, Cassandra delta 27146, Kafka lag settled_to_zero=False.

Unsupported:

- OHMF can sustain 120 accepted messages/sec aggregate in this local configuration
- Any p95 or p99 delivery latency claim
- 3,100 concurrent clients
- Production readiness
- HA or failover
- Any cloud or production benchmark interpretation

## Paired artifact

- Benchmark summary: [summary.md](benchmarks/results/2026-06-08-stage-b1-rerun-throughput/120msgsec/summary.md)

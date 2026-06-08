# Local k3s Stage B1 rerun rung - 105 msg/sec multisource - 2026-06-08

## Outcome

Stage B1 multisource **failed**.

- Loadgen pods / source IPs observed: `12` / `12`
- Per-pod target: `mixed 8-9 msg/sec per pod`
- Aggregate target: `105 msg/sec`
- Main-phase duration: `120s`
- Requested / sent / accepted: `15075` / `15075` / `15075`
- Postgres / Cassandra delta: `7269` / `8039`
- Missing / duplicates: `7806` / `0`
- Kafka lag settled to zero: `True` in `303.511s`

## Claim boundary

The multisource local Kubernetes run did not establish a passing 105 msg/sec aggregate claim. It remains useful only as bounded evidence for the accepted subset: 15075/15075 accepted, Postgres delta 7269, Cassandra delta 8039, Kafka lag settled_to_zero=True.

Unsupported:

- OHMF can sustain 105 accepted messages/sec aggregate in this local configuration
- Any p95 or p99 delivery latency claim
- 3,100 concurrent clients
- Production readiness
- HA or failover
- Any cloud or production benchmark interpretation

## Paired artifact

- Benchmark summary: [summary.md](C:/Users/James/Downloads/Messages/benchmarks/results/2026-06-08-stage-b1-rerun-throughput/105msgsec/summary.md)

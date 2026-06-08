# Claims And Evidence

Status: Stage A complete. Stage B1 attempted twice and remains bounded by gateway front-door limiting. Later stages remain not yet run.

| Claim | Status | Evidence artifact | Notes |
| --- | --- | --- | --- |
| Local benchmark artifacts capture unique `run_id`, versions, machine, and limitations | passed for Stage A smoke | [2026-06-08T12-38-06Z-stage-a-smoke-5x10](C:/Users/James/Downloads/Messages/benchmarks/results/2026-06-08T12-38-06Z-stage-a-smoke-5x10/summary.md) | Includes run-scoped conversation reconciliation, versions, machine, and limitations |
| Local benchmark smoke at `5 msg/sec for 10s` reconciles cleanly | passed | [2026-06-08T12-38-06Z-stage-a-smoke-5x10](C:/Users/James/Downloads/Messages/benchmarks/results/2026-06-08T12-38-06Z-stage-a-smoke-5x10/summary.md) | `50/50` accepted, `pg_delta=50`, `cass_delta=50`, lag -> `0` |
| Local resilience overlay preserves PVC-backed state across Kafka pod restart after stabilization | passed with limitation | [2026-06-08-local-k3s-resilience-kafka-restart.md](C:/Users/James/Downloads/Messages/deploy/k8s/results/2026-06-08-local-k3s-resilience-kafka-restart.md) | Immediate post-restart run exposed a transient single-broker availability gap |
| Per-user gateway send limiting is validated under overload | passed | [2026-06-08-sustained-120msgsec](C:/Users/James/Downloads/Messages/benchmarks/results/2026-06-08-sustained-120msgsec/summary.md) | `14400` attempted sends from one sender produced `13981` rate-limited responses; the `419` accepted messages reconciled cleanly to Postgres and Cassandra with Kafka lag returning to `0` |
| Aggregate multi-user host-driven throughput remains front-door limited | failed as IP-limited aggregate run | [2026-06-08-sustained-120msgsec-aggregate](C:/Users/James/Downloads/Messages/benchmarks/results/2026-06-08-sustained-120msgsec-aggregate/summary.md) | The corrected `60 users x 2 msg/sec` run cleared per-user limiting as the dominant bottleneck, but higher-rate phases failed only as `http_429_ip`; accepted subset still reconciled cleanly |
| Burst local throughput claim | not yet run | not yet run | Must publish highest repeatable passing level only |
| Local restart/recovery claim | partially supported | [2026-06-08-local-k3s-resilience-kafka-restart.md](C:/Users/James/Downloads/Messages/deploy/k8s/results/2026-06-08-local-k3s-resilience-kafka-restart.md) | Single-node local PVC only; no HA or failover claim |

Supported claim boundary:

- local Kubernetes validation only
- client-observed accept latency only
- current host-driven aggregate benchmark evidence is bounded by gateway per-IP limiting
- no production, HA, failover, or durability claims without separate evidence

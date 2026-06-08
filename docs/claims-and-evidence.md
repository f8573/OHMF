# Claims And Evidence

Status: Stage A complete. Stage B1 now includes front-door limiter evidence, a clean multisource ingress-failure result at 120 msg/sec, and a diagnostic ladder that establishes a current full-pipeline pass at 45 msg/sec with failure at 60 msg/sec. Later stages remain not yet run.

| Claim | Status | Evidence artifact | Notes |
| --- | --- | --- | --- |
| Local benchmark artifacts capture unique `run_id`, versions, machine, and limitations | passed for Stage A smoke | [2026-06-08T12-38-06Z-stage-a-smoke-5x10](C:/Users/James/Downloads/Messages/benchmarks/results/2026-06-08T12-38-06Z-stage-a-smoke-5x10/summary.md) | Includes run-scoped conversation reconciliation, versions, machine, and limitations |
| Local benchmark smoke at `5 msg/sec for 10s` reconciles cleanly | passed | [2026-06-08T12-38-06Z-stage-a-smoke-5x10](C:/Users/James/Downloads/Messages/benchmarks/results/2026-06-08T12-38-06Z-stage-a-smoke-5x10/summary.md) | `50/50` accepted, `pg_delta=50`, `cass_delta=50`, lag -> `0` |
| Local resilience overlay preserves PVC-backed state across Kafka pod restart after stabilization | passed with limitation | [2026-06-08-local-k3s-resilience-kafka-restart.md](C:/Users/James/Downloads/Messages/deploy/k8s/results/2026-06-08-local-k3s-resilience-kafka-restart.md) | Immediate post-restart run exposed a transient single-broker availability gap |
| Per-user gateway send limiting is validated under overload | passed | [2026-06-08-sustained-120msgsec](C:/Users/James/Downloads/Messages/benchmarks/results/2026-06-08-sustained-120msgsec/summary.md) | `14400` attempted sends from one sender produced `13981` rate-limited responses; the `419` accepted messages reconciled cleanly to Postgres and Cassandra with Kafka lag returning to `0` |
| Aggregate multi-user host-driven throughput remains front-door limited | failed as IP-limited aggregate run | [2026-06-08-sustained-120msgsec-aggregate](C:/Users/James/Downloads/Messages/benchmarks/results/2026-06-08-sustained-120msgsec-aggregate/summary.md) | The corrected `60 users x 2 msg/sec` run cleared per-user limiting as the dominant bottleneck, but higher-rate phases failed only as `http_429_ip`; accepted subset still reconciled cleanly |
| Multisource gateway ingress can accept near-target load while backend reconciliation fails bounded recovery | failed as backend pipeline run | [2026-06-08-sustained-120msgsec-multisource](C:/Users/James/Downloads/Messages/benchmarks/results/2026-06-08-sustained-120msgsec-multisource/summary.md) | Clean-baseline run used `12` source IPs at `10 msg/sec` each; gateway accepted `82792/82800` sends with only `8` `http_500` failures, but Postgres/Cassandra deltas stayed far below accepted sends and Kafka lag did not return to `0` within `183s` |
| Full local Kubernetes pipeline currently reconciles at 45 msg/sec but not at 60 msg/sec | bounded diagnostic evidence | [2026-06-08-pipeline-diagnostic-ladder](C:/Users/James/Downloads/Messages/benchmarks/results/2026-06-08-pipeline-diagnostic-ladder/summary.md) | Clean multisource ladder across `12` source IPs passed `30` and `45 msg/sec` with full reconciliation; at `60 msg/sec`, Kafka lag still drained to `0` but run-scoped reconciliation stopped at `pg_delta=7553`, `cass_delta=7991` for `9000` accepted |
| Burst local throughput claim | not yet run | not yet run | Must publish highest repeatable passing level only |
| Local restart/recovery claim | partially supported | [2026-06-08-local-k3s-resilience-kafka-restart.md](C:/Users/James/Downloads/Messages/deploy/k8s/results/2026-06-08-local-k3s-resilience-kafka-restart.md) | Single-node local PVC only; no HA or failover claim |

Supported claim boundary:

- local Kubernetes validation only
- client-observed accept latency only
- host-driven aggregate evidence remains bounded by gateway per-IP limiting
- clean multisource aggregate evidence shows gateway ingress near `120 msg/sec`, but does not support backend end-to-end persistence/reconciliation at that rate
- current clean full-pipeline claim boundary is `45 msg/sec` aggregate across `12` source IPs in this local configuration
- no production, HA, failover, or durability claims without separate evidence

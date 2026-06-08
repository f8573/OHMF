# Validation Matrix

Status: Stage A complete. Stage B1 attempted and failed. Stages B2-D remain not yet run.

| Test | Status | Artifact | Notes |
| --- | --- | --- | --- |
| Stage A loadgen smoke (`5 msg/sec for 10s`) | passed | [2026-06-08T12-38-06Z-stage-a-smoke-5x10](C:/Users/James/Downloads/Messages/benchmarks/results/2026-06-08T12-38-06Z-stage-a-smoke-5x10/summary.md) | `accepted=50`, `pg_delta=50`, `cass_delta=50`, lag -> `0` |
| Resilience overlay apply + Kafka restart | passed with limitation | [2026-06-08-local-k3s-resilience-kafka-restart.md](C:/Users/James/Downloads/Messages/deploy/k8s/results/2026-06-08-local-k3s-resilience-kafka-restart.md) | PVCs stayed bound; immediate restart window showed `45/50` reconciliation before stabilized rerun returned to `50/50` |
| Sustained 120 msg/sec x 12 min | failed | [2026-06-08-sustained-120msgsec](C:/Users/James/Downloads/Messages/benchmarks/results/2026-06-08-sustained-120msgsec/summary.md) | Clean target run recorded `accepted=419/14400`, `pg_delta=419`, `cass_delta=419`, `http_429=13981`; accepted subset reconciled but the target rate did not pass |
| Burst ladder | not yet run | not yet run | Publish highest repeatable passing only |
| Backlog recovery | not yet run | not yet run | Reconcile by run_id / conversation |
| Gateway failure | not yet run | not yet run | Hard failures itemized |
| Kafka restart | not yet run | not yet run | Single-broker availability gap must be documented |
| Cassandra restart | not yet run | not yet run | Postgres remains authoritative |
| Simulated node failure | not yet run | not yet run | Local simulated loss only |
| HPA under real message load | not yet run | not yet run | Synthetic HPA remains fallback if threshold cannot be crossed |

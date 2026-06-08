# Local k3s resilience Kafka restart - 2026-06-08

## Scope

This artifact records a real local run of
`deploy/k8s/overlays/local-k3s-resilience` on a single-node `k3d` cluster.

It validates:

- PVC-backed Postgres, Kafka, and Cassandra on `local-path`
- authenticated sends still work on the resilience overlay
- deleting the single Kafka pod triggers broker restart and later recovery

It does **not** validate:

- HA
- broker failover
- zero-loss during broker unavailability
- production durability

## Cluster and storage

- Cluster: `k3d-ohmf-m4`
- Kubernetes server: `v1.21.7+k3s1`
- StorageClass: `local-path`
- PVCs:
  - `postgres-data`
  - `kafka-data`
  - `cassandra-data`

All three PVCs bound successfully before the workload rolled out.

## Baseline run before Kafka restart

Loadgen artifact:

- [`benchmarks/results/2026-06-08T04-15-23Z-stage-a-smoke-5x10/summary.md`](/C:/Users/James/Downloads/Messages/benchmarks/results/2026-06-08T04-15-23Z-stage-a-smoke-5x10/summary.md)

Observed:

- requested: `50`
- accepted: `50`
- Postgres delta: `50`
- Cassandra delta: `50`
- Kafka lag -> `0` in about `2s`

## Kafka pod restart

Action taken:

```bash
kubectl -n ohmf delete pod -l app.kubernetes.io/name=kafka
kubectl -n ohmf rollout status deploy/kafka --timeout=420s
kubectl -n ohmf rollout status deploy/messages-processor --timeout=420s
```

Observed after the restart:

- Kafka came back as a new pod
- PVCs remained bound
- `messages-processor` logged a broker dial timeout during recovery

## Immediate post-restart run

Loadgen artifact:

- [`benchmarks/results/2026-06-08T04-16-24Z-stage-a-smoke-5x10/summary.md`](/C:/Users/James/Downloads/Messages/benchmarks/results/2026-06-08T04-16-24Z-stage-a-smoke-5x10/summary.md)

Observed:

- requested: `50`
- accepted: `50`
- Postgres delta: `45`
- Cassandra delta: `45`
- missing: `5`
- Kafka lag later returned to `0`

Interpretation:

- this is a **single-broker availability gap**
- it is **not** evidence of failover
- zero-loss cannot be claimed across the immediate restart window

## Stabilized post-restart run

Loadgen artifact:

- [`benchmarks/results/2026-06-08T04-17-55Z-stage-a-smoke-5x10/summary.md`](/C:/Users/James/Downloads/Messages/benchmarks/results/2026-06-08T04-17-55Z-stage-a-smoke-5x10/summary.md)

Observed after an additional settle window:

- requested: `50`
- accepted: `50`
- Postgres delta: `50`
- Cassandra delta: `50`
- Kafka lag -> `0` in about `2.2s`

## Supported claim from this artifact

On a local single-node `k3d` cluster using the `local-k3s-resilience` overlay,
PVC-backed Kafka/Postgres/Cassandra state survived a Kafka pod restart and the
system returned to clean run-scoped reconciliation after stabilization.

## Explicit limitations

- The restart test exposed a transient single-broker availability gap.
- This artifact does not support any HA, failover, or zero-loss claim.
- Kafka lag remains consumer-group scoped rather than run scoped.

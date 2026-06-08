# stage-a-smoke-5x10

## Run metadata

- Run ID: `20260608t123806z-11c8ab84`
- Driver location: `host`
- Completed UTC: `2026-06-08T12:38:20Z`
- Git HEAD: `239e1098fd06be7d43e2cd51a7d2e4182535cede`
- Working tree at generation: `dirty: M benchmarks/cmd/loadgen/main.go; ?? benchmarks/results/; ?? docs/claims-and-evidence.md; ?? docs/production-readiness-gap.md; ?? docs/validation-matrix.md; ?? ohmf/infra/k8s/; ?? testing/`
- Git HEAD note: git_head is the base commit visible at artifact generation time; artifact changes may be uncommitted until the validation commit lands
- Host: `skibidiohiogyat` on `windows/amd64`

## Counts

- Requested unique sends: `50`
- Sent attempts: `50`
- Accepted unique idempotency keys: `50`
- Accepted responses: `50`
- Duplicate accepted keys: `0`
- Missing (`accepted - pg_delta`): `0`
- Send failures: none

## Latency

- Samples: `50`
- p50 accept latency: `38.64 ms`
- p95 accept latency: `43.55 ms`
- p99 accept latency: `50.08 ms`

## Reconciliation

- Mode: `Postgres reconciled by fresh test conversation ids derived from run_id; Cassandra reconciled by fresh test conversation ids plus UTC partition bucket; there is no dedicated persisted run_id column`
- Postgres delta: `50`
- Cassandra delta: `50`
- Kafka lag reached zero in `1.98 s`

## Limitations

- Client latency is client-observed POST-to-ack latency only, labeled by driver execution location.
- Kafka lag reconciliation is not run-scoped in the current schema/tooling; it is consumer-group wide and should be treated as isolated-cluster evidence.

## Supported claim

This artifact supports only a local host benchmark claim for scenario "stage-a-smoke-5x10" at run_id "20260608t123806z-11c8ab84", with client-observed accept latency and run-scoped Postgres/Cassandra reconciliation by fresh test conversation where available.

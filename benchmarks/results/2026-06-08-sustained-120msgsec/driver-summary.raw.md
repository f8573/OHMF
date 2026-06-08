# sustained-120msgsec

## Run metadata

- Run ID: `20260608t143945z-fc4eebd4`
- Driver location: `host`
- Completed UTC: `2026-06-08T14:42:07Z`
- Git HEAD: `a767b25091b9eeecbe83b163d6abd669253a5917`
- Working tree at generation: `dirty: ?? benchmarks/results/2026-06-08-sustained-120msgsec/; ?? ohmf/infra/k8s/; ?? testing/`
- Git HEAD note: git_head is the base commit visible at artifact generation time; artifact changes may be uncommitted until the validation commit lands
- Host: `skibidiohiogyat` on `windows/amd64`

## Counts

- Requested unique sends: `14400`
- Sent attempts: `14400`
- Accepted unique idempotency keys: `419`
- Accepted responses: `419`
- Duplicate accepted keys: `0`
- Missing (`accepted - pg_delta`): `0`
- Send failure `http_429`: `13981`

## Latency

- Samples: `419`
- p50 accept latency: `39.24 ms`
- p95 accept latency: `2027.84 ms`
- p99 accept latency: `2116.66 ms`

## Reconciliation

- Mode: `Postgres reconciled by fresh test conversation ids derived from run_id; Cassandra reconciled by fresh test conversation ids plus UTC partition bucket; there is no dedicated persisted run_id column`
- Postgres delta: `419`
- Cassandra delta: `419`
- Kafka lag reached zero in `2.05 s`

## Limitations

- Client latency is client-observed POST-to-ack latency only, labeled by driver execution location.
- Kafka lag reconciliation is not run-scoped in the current schema/tooling; it is consumer-group wide and should be treated as isolated-cluster evidence.

## Supported claim

This artifact supports only a local host benchmark claim for scenario "sustained-120msgsec" at run_id "20260608t143945z-fc4eebd4", with client-observed accept latency and run-scoped Postgres/Cassandra reconciliation by fresh test conversation where available.

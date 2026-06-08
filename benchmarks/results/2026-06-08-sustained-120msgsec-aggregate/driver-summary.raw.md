# sustained-120-aggregate

## Run metadata

- Run ID: `20260608t162740z-857c16c8`
- Driver location: `host`
- Completed UTC: `2026-06-08T16:43:41Z`
- Git HEAD: `2aa00eac7f4f5a57457034dee2f6f3b9410b5d38`
- System-under-test commit: `2aa00eac7f4f5a57457034dee2f6f3b9410b5d38`
- Working tree at generation: `dirty: ?? benchmarks/results/2026-06-08-sustained-120msgsec-aggregate/; ?? ohmf/infra/k8s/; ?? testing/`
- Git HEAD note: git_head is the base commit visible at artifact generation time; artifact changes may be uncommitted until the validation commit lands
- Host: `skibidiohiogyat` on `windows/amd64`

## Phases

- `warmup-10x1x60`: users=`10`, per_user_rate=`1 msg/sec`, aggregate=`10 msg/sec`, duration=`60s`, requested=`600`, accepted=`600`
- `warmup-30x2x60`: users=`30`, per_user_rate=`2 msg/sec`, aggregate=`60 msg/sec`, duration=`60s`, requested=`3600`, accepted=`959`
- `warmup-30x2x60` failure `http_429_ip`: `2641`
- `main-60x2x720`: users=`60`, per_user_rate=`2 msg/sec`, aggregate=`120 msg/sec`, duration=`720s`, requested=`86400`, accepted=`8640`
- `main-60x2x720` failure `http_429_ip`: `77760`

## Counts

- Requested unique sends: `90600`
- Sent attempts: `90600`
- Accepted unique idempotency keys: `10199`
- Accepted responses: `10199`
- Duplicate accepted keys: `0`
- Missing (`accepted - pg_delta`): `0`
- Send failure `http_429_ip`: `80401`

## Latency

- Samples: `10199`
- p50 accept latency: `36.21 ms`
- p95 accept latency: `1303.67 ms`
- p99 accept latency: `2026.01 ms`

## Reconciliation

- Mode: `Postgres reconciled by fresh test conversation ids derived from run_id; Cassandra reconciled by fresh test conversation ids plus UTC partition bucket; there is no dedicated persisted run_id column`
- Postgres delta: `10199`
- Cassandra delta: `10199`
- Kafka lag reached zero in `1.94 s`

## Limitations

- Client latency is client-observed POST-to-ack latency only, labeled by driver execution location.
- Kafka lag reconciliation is not run-scoped in the current schema/tooling; it is consumer-group wide and should be treated as isolated-cluster evidence.

## Supported claim

This artifact supports only a local host benchmark claim for scenario "sustained-120-aggregate" at run_id "20260608t162740z-857c16c8", with client-observed accept latency and run-scoped Postgres/Cassandra reconciliation by fresh test conversation where available.

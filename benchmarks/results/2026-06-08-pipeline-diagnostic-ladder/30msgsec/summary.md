# pipeline-diagnostic-30msgsec-multisource

## Outcome

- Status: `passed`
- Loadgen pods: `12`
- Unique source IPs: `12`
- Per-pod target: `mixed 2-3 msg/sec per pod`
- Aggregate target rate: `30 msg/sec`
- Main-phase duration: `120s`

## Counts

- Requested: `4500`
- Sent attempts: `4500`
- Accepted: `4500`
- Failed: `0`
- Duplicates: `0`
- Missing: `0`
- Postgres delta: `4500`
- Cassandra delta: `4500`

## Latency

- Label: `client-observed HTTP accept latency`
- Sample count: `4500`
- p50: `2020.293 ms`
- p95: `2026.021 ms`
- p99: `2037.283 ms`

## Kafka

- Lag settled to zero: `True`
- Lag zero seconds: `2.070`
- Lag at end: `0`

## Supported claim

Sustained 30 aggregate msg/sec through the local Kubernetes full pipeline across 12 source IPs, with 4500/4500 accepted messages reconciling to Postgres (4500) and Cassandra (4500) and Kafka lag returning to zero in 2.07 seconds.

## Unsupported claims

- Any p95 or p99 delivery latency claim
- 3,100 concurrent clients
- Production readiness
- HA or failover
- Any cloud or production benchmark interpretation

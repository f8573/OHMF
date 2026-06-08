# pipeline-diagnostic-45msgsec-multisource

## Outcome

- Status: `passed`
- Loadgen pods: `12`
- Unique source IPs: `12`
- Per-pod target: `mixed 3-4 msg/sec per pod`
- Aggregate target rate: `45 msg/sec`
- Main-phase duration: `120s`

## Counts

- Requested: `6750`
- Sent attempts: `6750`
- Accepted: `6750`
- Failed: `0`
- Duplicates: `0`
- Missing: `0`
- Postgres delta: `6750`
- Cassandra delta: `6750`

## Latency

- Label: `client-observed HTTP accept latency`
- Sample count: `6750`
- p50: `2020.427 ms`
- p95: `2027.414 ms`
- p99: `2048.899 ms`

## Kafka

- Lag settled to zero: `True`
- Lag zero seconds: `1.849`
- Lag at end: `0`

## Supported claim

Sustained 45 aggregate msg/sec through the local Kubernetes full pipeline across 12 source IPs, with 6750/6750 accepted messages reconciling to Postgres (6750) and Cassandra (6750) and Kafka lag returning to zero in 1.85 seconds.

## Unsupported claims

- Any p95 or p99 delivery latency claim
- 3,100 concurrent clients
- Production readiness
- HA or failover
- Any cloud or production benchmark interpretation

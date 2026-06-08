# stage-b1-rerun-75msgsec-multisource

## Outcome

- Status: `failed`
- Loadgen pods: `12`
- Unique source IPs: `12`
- Per-pod target: `mixed 6-7 msg/sec per pod`
- Aggregate target rate: `75 msg/sec`
- Main-phase duration: `120s`

## Counts

- Requested: `11025`
- Sent attempts: `11025`
- Accepted: `11025`
- Failed: `0`
- Duplicates: `0`
- Missing: `3635`
- Postgres delta: `7390`
- Cassandra delta: `7963`

## Latency

- Label: `client-observed HTTP accept latency`
- Sample count: `11025`
- p50: `2021.051 ms`
- p95: `2032.545 ms`
- p99: `2086.945 ms`

## Kafka

- Lag settled to zero: `True`
- Lag zero seconds: `122.472`
- Lag at end: `0`

## Supported claim

The multisource local Kubernetes run did not establish a passing 75 msg/sec aggregate claim. It remains useful only as bounded evidence for the accepted subset: 11025/11025 accepted, Postgres delta 7390, Cassandra delta 7963, Kafka lag settled_to_zero=True.

## Unsupported claims

- OHMF can sustain 75 accepted messages/sec aggregate in this local configuration
- Any p95 or p99 delivery latency claim
- 3,100 concurrent clients
- Production readiness
- HA or failover
- Any cloud or production benchmark interpretation

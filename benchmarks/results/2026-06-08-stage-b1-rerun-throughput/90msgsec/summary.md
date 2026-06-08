# stage-b1-rerun-90msgsec-multisource

## Outcome

- Status: `failed`
- Loadgen pods: `12`
- Unique source IPs: `12`
- Per-pod target: `mixed 7-8 msg/sec per pod`
- Aggregate target rate: `90 msg/sec`
- Main-phase duration: `120s`

## Counts

- Requested: `13050`
- Sent attempts: `13050`
- Accepted: `13050`
- Failed: `0`
- Duplicates: `0`
- Missing: `5646`
- Postgres delta: `7404`
- Cassandra delta: `8116`

## Latency

- Label: `client-observed HTTP accept latency`
- Sample count: `13050`
- p50: `2020.926 ms`
- p95: `2031.592 ms`
- p99: `2097.284 ms`

## Kafka

- Lag settled to zero: `True`
- Lag zero seconds: `213.191`
- Lag at end: `0`

## Supported claim

The multisource local Kubernetes run did not establish a passing 90 msg/sec aggregate claim. It remains useful only as bounded evidence for the accepted subset: 13050/13050 accepted, Postgres delta 7404, Cassandra delta 8116, Kafka lag settled_to_zero=True.

## Unsupported claims

- OHMF can sustain 90 accepted messages/sec aggregate in this local configuration
- Any p95 or p99 delivery latency claim
- 3,100 concurrent clients
- Production readiness
- HA or failover
- Any cloud or production benchmark interpretation

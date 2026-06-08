# processor-stage-60msgsec-multisource

## Outcome

- Status: `failed`
- Loadgen pods: `12`
- Unique source IPs: `12`
- Per-pod target: `5 msg/sec per pod`
- Aggregate target rate: `60 msg/sec`
- Main-phase duration: `120s`

## Counts

- Requested: `9000`
- Sent attempts: `9000`
- Accepted: `9000`
- Failed: `0`
- Duplicates: `0`
- Missing: `1554`
- Postgres delta: `7446`
- Cassandra delta: `7909`

## Latency

- Label: `client-observed HTTP accept latency`
- Sample count: `9000`
- p50: `2020.794 ms`
- p95: `2031.118 ms`
- p99: `2069.259 ms`

## Kafka

- Lag settled to zero: `True`
- Lag zero seconds: `38.718`
- Lag at end: `0`

## Supported claim

The multisource local Kubernetes run did not establish a passing 60 msg/sec aggregate claim. It remains useful only as bounded evidence for the accepted subset: 9000/9000 accepted, Postgres delta 7446, Cassandra delta 7909, Kafka lag settled_to_zero=True.

## Unsupported claims

- OHMF can sustain 60 accepted messages/sec aggregate in this local configuration
- Any p95 or p99 delivery latency claim
- 3,100 concurrent clients
- Production readiness
- HA or failover
- Any cloud or production benchmark interpretation

# sustained-120msgsec-multisource

## Outcome

- Status: `failed`
- Loadgen pods: `12`
- Unique source IPs: `12`
- Per-pod rate: `10 msg/sec`
- Aggregate target rate: `120 msg/sec`
- Main-phase duration: `600s`

## Counts

- Requested: `82800`
- Sent attempts: `82800`
- Accepted: `82792`
- Failed: `8`
- Duplicates: `0`
- Missing: `53491`
- Postgres delta: `29301`
- Cassandra delta: `30267`

## Latency

- Label: `client-observed HTTP accept latency`
- Sample count: `82792`
- p50: `2020.419 ms`
- p95: `2025.472 ms`
- p99: `2032.887 ms`

## Kafka

- Lag settled to zero: `False`
- Lag zero seconds: `183.004`
- Lag at end: `47820`

## Supported claim

The multisource local Kubernetes run did not establish a passing 120 msg/sec aggregate claim. It remains useful only as bounded evidence for the accepted subset: 82792/82800 accepted, Postgres delta 29301, Cassandra delta 30267, Kafka lag settled_to_zero=False.

## Failures

- `http_500`: `8`

## Unsupported claims

- OHMF can sustain 120 accepted messages/sec aggregate in this local configuration
- Any p95 or p99 delivery latency claim
- 3,100 concurrent clients
- Production readiness
- HA or failover
- Any cloud or production benchmark interpretation

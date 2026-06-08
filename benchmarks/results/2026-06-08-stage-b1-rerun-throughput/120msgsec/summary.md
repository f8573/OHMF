# stage-b1-rerun-120msgsec-multisource

## Outcome

- Status: `failed`
- Loadgen pods: `12`
- Unique source IPs: `12`
- Per-pod target: `10 msg/sec per pod`
- Aggregate target rate: `120 msg/sec`
- Main-phase duration: `600s`

## Counts

- Requested: `74700`
- Sent attempts: `74700`
- Accepted: `74685`
- Failed: `15`
- Duplicates: `0`
- Missing: `48378`
- Postgres delta: `26307`
- Cassandra delta: `27146`

## Latency

- Label: `client-observed HTTP accept latency`
- Sample count: `74685`
- p50: `2021.228 ms`
- p95: `2035.392 ms`
- p99: `2099.780 ms`

## Kafka

- Lag settled to zero: `False`
- Lag zero seconds: `903.001`
- Lag at end: `28212`

## Supported claim

The multisource local Kubernetes run did not establish a passing 120 msg/sec aggregate claim. It remains useful only as bounded evidence for the accepted subset: 74685/74700 accepted, Postgres delta 26307, Cassandra delta 27146, Kafka lag settled_to_zero=False.

## Failures

- `http_500`: `15`

## Unsupported claims

- OHMF can sustain 120 accepted messages/sec aggregate in this local configuration
- Any p95 or p99 delivery latency claim
- 3,100 concurrent clients
- Production readiness
- HA or failover
- Any cloud or production benchmark interpretation

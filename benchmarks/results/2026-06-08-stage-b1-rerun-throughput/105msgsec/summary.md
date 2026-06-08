# stage-b1-rerun-105msgsec-multisource

## Outcome

- Status: `failed`
- Loadgen pods: `12`
- Unique source IPs: `12`
- Per-pod target: `mixed 8-9 msg/sec per pod`
- Aggregate target rate: `105 msg/sec`
- Main-phase duration: `120s`

## Counts

- Requested: `15075`
- Sent attempts: `15075`
- Accepted: `15075`
- Failed: `0`
- Duplicates: `0`
- Missing: `7806`
- Postgres delta: `7269`
- Cassandra delta: `8039`

## Latency

- Label: `client-observed HTTP accept latency`
- Sample count: `15075`
- p50: `2021.039 ms`
- p95: `2032.638 ms`
- p99: `2067.989 ms`

## Kafka

- Lag settled to zero: `True`
- Lag zero seconds: `303.511`
- Lag at end: `0`

## Supported claim

The multisource local Kubernetes run did not establish a passing 105 msg/sec aggregate claim. It remains useful only as bounded evidence for the accepted subset: 15075/15075 accepted, Postgres delta 7269, Cassandra delta 8039, Kafka lag settled_to_zero=True.

## Unsupported claims

- OHMF can sustain 105 accepted messages/sec aggregate in this local configuration
- Any p95 or p99 delivery latency claim
- 3,100 concurrent clients
- Production readiness
- HA or failover
- Any cloud or production benchmark interpretation

# Stage B1 throughput rerun - 2026-06-08

## Outcome

Validated 105 msg/sec through the local Kubernetes full pipeline with exact reconciliation across Kafka consumption, Postgres persistence, Cassandra persistence, downstream publishes, and offset commits.

- Highest passing sustained rate: `105`
- `120 msg/sec` passed: `False`
- Highest attempted rate: `120`

A prior 60 msg/sec under-reconciliation result was traced to stale container image deployment; subsequent unique-tag rollout with processor stage instrumentation reconciled exactly.

## Rollout Discipline

- Cluster context: `k3d-ohmf-b1`
- Namespace: `ohmf`
- `stage_events_total` gate: `True`
- Rollout verification status: `True`

| Service | Intended image | Deployment image | Pod image ID |
| --- | --- | --- | --- |
| gateway | `ohmf-gateway:b1rerun-20260608t213926169462z` | `ohmf-gateway:b1rerun-20260608t213926169462z` | `sha256:63fed96f726735f3870641537e71c8f93d70acfe99270b61c9f1abd86b4b2fd1` |
| apps | `ohmf-apps:b1rerun-20260608t213926169462z` | `ohmf-apps:b1rerun-20260608t213926169462z` | `sha256:bb0125359b381050f7cb44f247b691c2797d4a8f4b268505d7de4094b3984e58` |
| messages-processor | `ohmf-messages-processor:b1rerun-20260608t213926169462z` | `ohmf-messages-processor:b1rerun-20260608t213926169462z` | `sha256:ecfaf0d62268c10070b4b0af97f01b717278ae3024857a40e20911d60dd9eb70` |

## Stage Counter Table

| Rate | Status | Requested | Sent | Accepted | Consumed | PG ok | Cassandra ok | Redis ack ok | Persisted publish | Microservice publish | Offset commits | Handler errors | PG exact | Cassandra exact | Lag zero sec | p50 ms | p95 ms | p99 ms | Restart delta |
| --- | --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | --- |
| 75 msg/sec | `passed` | 11025 | 11025 | 11025 | 11025 | 11025 | 11025 | 11025 | 11025 | 11025 | 11025 | 0 | 11025 | 11025 | 122.472 | 2021.051 | 2032.545 | 2086.945 | `0` |
| 90 msg/sec | `passed` | 13050 | 13050 | 13050 | 13050 | 13050 | 13050 | 13050 | 13050 | 13050 | 13050 | 0 | 13050 | 13050 | 213.191 | 2020.926 | 2031.592 | 2097.284 | `0` |
| 105 msg/sec | `passed` | 15075 | 15075 | 15075 | 15075 | 15075 | 15075 | 15075 | 15075 | 15075 | 15075 | 0 | 15075 | 15075 | 303.511 | 2021.039 | 2032.638 | 2067.989 | `0` |
| 120 msg/sec | `failed` | 74700 | 74700 | 74685 | 46568 | 46568 | 46568 | 46568 | 46568 | 46567 | 46567 | 0 | 50881 | 51755 | 903.001 | 2021.228 | 2035.392 | 2099.780 | `0` |

## Artifacts

- Combined JSON: `benchmarks\results\2026-06-08-stage-b1-rerun-throughput\summary.json`
- Deploy note: `deploy/k8s/results/2026-06-08-stage-b1-rerun-throughput.md`
- `75 msg/sec`: `benchmarks\results\2026-06-08-stage-b1-rerun-throughput\75msgsec`
- `90 msg/sec`: `benchmarks\results\2026-06-08-stage-b1-rerun-throughput\90msgsec`
- `105 msg/sec`: `benchmarks\results\2026-06-08-stage-b1-rerun-throughput\105msgsec`
- `120 msg/sec`: `benchmarks\results\2026-06-08-stage-b1-rerun-throughput\120msgsec`

## Unsupported Claims Still Remaining

- Any delivery-latency claim beyond client-observed HTTP accept latency
- Any burst-throughput headline beyond the highest sustained passing rung
- Any production, HA, failover, or cloud benchmark interpretation
- Any large-client-count claim such as 3,100 concurrent clients

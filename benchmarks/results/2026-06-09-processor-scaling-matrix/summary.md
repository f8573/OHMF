# Processor scaling matrix - 2026-06-09

## Outcome

- Hypothesis: The earlier 120 msg/sec miss was caused by single messages-processor throughput, not gateway ingress or Kafka partition count.
- Question answer: Yes. Adding processors made `120 msg/sec` pass strict full-pipeline reconciliation.
- Hypothesis result: `validated`
- Supported claim: Validated 120 msg/sec local Kubernetes ingress with exact full-pipeline reconciliation after scaling the messages-processor consumer group across Kafka partitions.
- Evidence boundary: With 4 `messages-processor` replicas, OHMF sustained 120 msg/sec in the local Kubernetes full-pipeline profile: `74,700/74,700` accepted, `74,700` Postgres rows, `74,700` Cassandra rows, and Kafka lag returned to zero in `103.145s`.

- `105 msg/sec` strict pass across replicas: `False` (1, 4)
- Note: the standalone 2-replica 105 msg/sec rerun passed, but the strict six-cell matrix rollup marks the cell failed because the original rung required recovery after runner interruption and did not satisfy the matrix's exact evidence rules. This does not affect the 120 msg/sec conclusion, which depends on the clean 4-replica 120 msg/sec passing cell.

## Rollout Discipline

- Cluster context: `k3d-ohmf-b1`
- Namespace: `ohmf`
- `stage_events_total` gate: `True`
- Rollout verification status: `True`

| Service | Intended image | Deployment image | Pod image ID |
| --- | --- | --- | --- |
| gateway | `ohmf-gateway:procscale-20260609t172819512779z` | `ohmf-gateway:procscale-20260609t172819512779z` | `sha256:65583391120581d3ba1b5ab85d93adf87fbd2ee4809e72d4a3165180043fafdf` |
| apps | `ohmf-apps:procscale-20260609t172819512779z` | `ohmf-apps:procscale-20260609t172819512779z` | `sha256:bb0125359b381050f7cb44f247b691c2797d4a8f4b268505d7de4094b3984e58` |
| messages-processor | `ohmf-messages-processor:procscale-20260609t172819512779z` | `ohmf-messages-processor:procscale-20260609t172819512779z` | `sha256:ecfaf0d62268c10070b4b0af97f01b717278ae3024857a40e20911d60dd9eb70, sha256:ecfaf0d62268c10070b4b0af97f01b717278ae3024857a40e20911d60dd9eb70` |

## Matrix

| Replicas | Rate | Status | Requested | Accepted | Consumed | PG ok | Cassandra ok | Offset commits | Handler errors | PG exact | Cassandra exact | Lag zero sec | Lag at end | Restart delta | Assigned members | Expected members | Assigned partitions |
| ---: | ---: | --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | --- | ---: | ---: | ---: |
| 1 | 105 | `passed` | 15075 | 15075 | 15075 | 15075 | 15075 | 15075 | 0 | 15075 | 15075 | 410.278 | 0 | `0` | 1 | 1 | 12 |
| 1 | 120 | `failed` | 74700 | 74700 | 38990 | 38990 | 38990 | 38989 | 0 | 74700 | 74700 | 602.316 | 37742 | `0` | 1 | 1 | 12 |
| 2 | 105 | `failed` | 15075 | 15075 | 12651 | 12650 | 12650 | 12649 | 0 | 0 | 0 | 2.231 | 0 | `0` | 2 | 2 | 12 |
| 2 | 120 | `failed` | 74700 | 74700 | 70702 | 70701 | 70701 | 70700 | 0 | 74700 | 74700 | 600.481 | 7897 | `0` | 2 | 2 | 12 |
| 4 | 105 | `passed` | 15075 | 15075 | 15075 | 15075 | 15075 | 15075 | 0 | 15075 | 15075 | 2.021 | 0 | `0` | 4 | 4 | 12 |
| 4 | 120 | `passed` | 74700 | 74700 | 74700 | 74700 | 74700 | 74700 | 0 | 74700 | 74700 | 103.145 | 0 | `0` | 4 | 4 | 12 |

## Assignment Evidence

| Replicas | Rate | Scale-ready members | Scale-ready partition counts | Post-run members | Post-drain members |
| ---: | ---: | --- | --- | --- | --- |
| 1 | 105 | `messages-processor@messages-processor-5ffb7c68d9-d2zsw` | `{"messages-processor@messages-processor-5ffb7c68d9-d2zsw": 12}` | `messages-processor@messages-processor-5ffb7c68d9-d2zsw` | `messages-processor@messages-processor-5ffb7c68d9-d2zsw` |
| 1 | 120 | `messages-processor@messages-processor-5ffb7c68d9-d2zsw` | `{"messages-processor@messages-processor-5ffb7c68d9-d2zsw": 12}` | `messages-processor@messages-processor-5ffb7c68d9-d2zsw` | `messages-processor@messages-processor-5ffb7c68d9-d2zsw` |
| 2 | 105 | `messages-processor@messages-processor-5ffb7c68d9-d2zsw, messages-processor@messages-processor-5ffb7c68d9-g885z` | `{"messages-processor@messages-processor-5ffb7c68d9-d2zsw": 6, "messages-processor@messages-processor-5ffb7c68d9-g885z": 6}` | `messages-processor@messages-processor-5ffb7c68d9-d2zsw, messages-processor@messages-processor-5ffb7c68d9-g885z` | `messages-processor@messages-processor-5ffb7c68d9-d2zsw, messages-processor@messages-processor-5ffb7c68d9-g885z` |
| 2 | 120 | `messages-processor@messages-processor-5ffb7c68d9-84v9d, messages-processor@messages-processor-5ffb7c68d9-d2zsw` | `{"messages-processor@messages-processor-5ffb7c68d9-84v9d": 6, "messages-processor@messages-processor-5ffb7c68d9-d2zsw": 6}` | `messages-processor@messages-processor-5ffb7c68d9-84v9d, messages-processor@messages-processor-5ffb7c68d9-d2zsw` | `messages-processor@messages-processor-5ffb7c68d9-84v9d, messages-processor@messages-processor-5ffb7c68d9-d2zsw` |
| 4 | 105 | `messages-processor@messages-processor-5ffb7c68d9-84v9d, messages-processor@messages-processor-5ffb7c68d9-d2zsw, messages-processor@messages-processor-5ffb7c68d9-kwkjt, messages-processor@messages-processor-5ffb7c68d9-vlhb8` | `{"messages-processor@messages-processor-5ffb7c68d9-84v9d": 3, "messages-processor@messages-processor-5ffb7c68d9-d2zsw": 3, "messages-processor@messages-processor-5ffb7c68d9-kwkjt": 3, "messages-processor@messages-processor-5ffb7c68d9-vlhb8": 3}` | `messages-processor@messages-processor-5ffb7c68d9-84v9d, messages-processor@messages-processor-5ffb7c68d9-d2zsw, messages-processor@messages-processor-5ffb7c68d9-kwkjt, messages-processor@messages-processor-5ffb7c68d9-vlhb8` | `messages-processor@messages-processor-5ffb7c68d9-84v9d, messages-processor@messages-processor-5ffb7c68d9-d2zsw, messages-processor@messages-processor-5ffb7c68d9-kwkjt, messages-processor@messages-processor-5ffb7c68d9-vlhb8` |
| 4 | 120 | `messages-processor@messages-processor-5ffb7c68d9-84v9d, messages-processor@messages-processor-5ffb7c68d9-d2zsw, messages-processor@messages-processor-5ffb7c68d9-kwkjt, messages-processor@messages-processor-5ffb7c68d9-vlhb8` | `{"messages-processor@messages-processor-5ffb7c68d9-84v9d": 3, "messages-processor@messages-processor-5ffb7c68d9-d2zsw": 3, "messages-processor@messages-processor-5ffb7c68d9-kwkjt": 3, "messages-processor@messages-processor-5ffb7c68d9-vlhb8": 3}` | `messages-processor@messages-processor-5ffb7c68d9-84v9d, messages-processor@messages-processor-5ffb7c68d9-d2zsw, messages-processor@messages-processor-5ffb7c68d9-kwkjt, messages-processor@messages-processor-5ffb7c68d9-vlhb8` | `messages-processor@messages-processor-5ffb7c68d9-84v9d, messages-processor@messages-processor-5ffb7c68d9-d2zsw, messages-processor@messages-processor-5ffb7c68d9-kwkjt, messages-processor@messages-processor-5ffb7c68d9-vlhb8` |

## Artifacts

- Combined JSON: `benchmarks\results\2026-06-09-processor-scaling-matrix\summary.json`
- Deploy note: `deploy/k8s/results/2026-06-09-processor-scaling-matrix.md`
- `1 replicas @ 105 msg/sec`: `benchmarks\results\2026-06-09-processor-scaling-matrix\1replicas-105msgsec`
- `1 replicas @ 120 msg/sec`: `benchmarks\results\2026-06-09-processor-scaling-matrix\1replicas-120msgsec`
- `2 replicas @ 105 msg/sec`: `benchmarks\results\2026-06-09-processor-scaling-matrix\2replicas-105msgsec`
- `2 replicas @ 120 msg/sec`: `benchmarks\results\2026-06-09-processor-scaling-matrix\2replicas-120msgsec`
- `4 replicas @ 105 msg/sec`: `benchmarks\results\2026-06-09-processor-scaling-matrix\4replicas-105msgsec`
- `4 replicas @ 120 msg/sec`: `benchmarks\results\2026-06-09-processor-scaling-matrix\4replicas-120msgsec`

Required capture files per rung live under each rung's `observations/` directory:

- `messages-processor-logs-scale-ready.txt`, `messages-processor-logs-post-run.txt`, `messages-processor-logs-post-drain.txt`
- `messages-processor-consumer-group-scale-ready.txt`, `messages-processor-consumer-group-post-run.txt`, `messages-processor-consumer-group-post-drain.txt`
- `pods-scale-ready.txt`, `pods-post-run.txt`, `pods-post-drain.txt`

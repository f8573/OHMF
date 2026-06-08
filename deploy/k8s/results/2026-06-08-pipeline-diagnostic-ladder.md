# Pipeline Diagnostic Ladder - 2026-06-08

- Overall status: `bounded_failure`
- Highest fully reconciled rate: `45`
- First failed rate: `60`

## Rungs

### 30 msg/sec

- Status: `passed`
- Requested / accepted / failed: `4500` / `4500` / `0`
- Postgres / Cassandra delta: `4500` / `4500`
- Missing / duplicates: `0` / `0`
- Kafka lag settled / end: `True` / `0`
- Lag zero seconds: `2.069686`
- Source IPs: `12`
- Per-pod target: `mixed 2-3 msg/sec per pod`
- Processor peak: `{"cpu": "111m", "memory": "11Mi"}`
- Gateway peak: `{"cpu": "150m", "memory": "105Mi"}`
- Artifact: [30msgsec](benchmarks/results/2026-06-08-pipeline-diagnostic-ladder/30msgsec/summary.md)

### 45 msg/sec

- Status: `passed`
- Requested / accepted / failed: `6750` / `6750` / `0`
- Postgres / Cassandra delta: `6750` / `6750`
- Missing / duplicates: `0` / `0`
- Kafka lag settled / end: `True` / `0`
- Lag zero seconds: `1.848948`
- Source IPs: `12`
- Per-pod target: `mixed 3-4 msg/sec per pod`
- Processor peak: `{"cpu": "114m", "memory": "11Mi"}`
- Gateway peak: `{"cpu": "145m", "memory": "103Mi"}`
- Artifact: [45msgsec](benchmarks/results/2026-06-08-pipeline-diagnostic-ladder/45msgsec/summary.md)

### 60 msg/sec

- Status: `failed`
- Requested / accepted / failed: `9000` / `9000` / `0`
- Postgres / Cassandra delta: `7553` / `7991`
- Missing / duplicates: `1447` / `0`
- Kafka lag settled / end: `True` / `0`
- Lag zero seconds: `31.872974`
- Source IPs: `12`
- Per-pod target: `5 msg/sec per pod`
- Processor peak: `{"cpu": "113m", "memory": "12Mi"}`
- Gateway peak: `{"cpu": "165m", "memory": "114Mi"}`
- Artifact: [60msgsec](benchmarks/results/2026-06-08-pipeline-diagnostic-ladder/60msgsec/summary.md)

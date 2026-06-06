# OHMF Benchmarks

This directory is the home for OHMF's load-testing methodology and results. It is written to be
honest about the current state: **OHMF does not yet contain a WebSocket load-test harness or captured
benchmark artifacts.** This README defines what a credible benchmark must capture so that results,
once produced, can be reproduced and trusted rather than asserted.

## Current status

- ❌ No WebSocket load-test driver (e.g. k6, Gatling, or a Go-based client) is committed.
- ❌ No captured results: no latency histograms, no throughput series, no message-loss accounting,
  no environment manifests.
- ⚠️ The only load-oriented file in the repo is
  `ohmf/services/gateway/_tools/e2ee-load-test.go`. It is an **in-process simulation of E2EE message
  generation** — it does not open WebSocket connections to the gateway, does not measure p95 latency,
  and does not measure message loss. It is a micro-benchmark scaffold, not system-level evidence.
  (It also currently does not compile cleanly; see [Follow-ups](#follow-ups).)

### The target claim (not yet substantiated here)

The intended headline benchmark is:

> sustained ~3,100 concurrent WebSocket clients, p95 latency under 150 ms, and zero observed message
> loss under the tested configuration.

This is a **design target**. Until the harness, environment manifest, and raw output in this
directory support it, it should not be used externally as a proven result. When it is reproduced, the
exact configuration that produced it must be recorded alongside the numbers.

## What a credible benchmark run must capture

Any result added here should be reproducible from the committed material. A run is "credible" when a
reviewer can re-run it and explain every number.

### 1. How to run it

- The exact command(s) and driver (tool + version).
- The number of virtual clients, ramp-up profile, message rate per client, and message size.
- Connection pattern: WebSocket connect → subscribe → send/receive → disconnect.
- The target deployment: root `docker-compose.yml`, the full
  `ohmf/infra/docker/docker-compose.yml` stack, or something else.

### 2. Environment assumptions that matter

Latency and capacity numbers are meaningless without the environment. Record at minimum:

- Host: CPU model, core count, RAM, OS, and whether it is bare metal, a VM, or Docker Desktop.
- Whether driver and system-under-test run on the **same machine** (co-located load generation
  inflates results and competes for CPU) or separate hosts/network.
- Kafka topic partition counts, Postgres/Cassandra/Redis resource limits, and gateway replica count.
- Any `APP_*` configuration that changes the path under test (e.g. `APP_USE_KAFKA_SEND`,
  `APP_USE_CASSANDRA_READS`, `APP_SHADOW_POSTGRES_WRITE`).

### 3. Metrics collected

- **Latency:** end-to-end send→delivery latency, reported as p50 / p95 / p99, not just average.
  State exactly which two timestamps bound the measurement.
- **Throughput:** sustained messages/second and concurrent connection count held.
- **Errors:** connection failures, send errors, timeouts.
- **Resource use:** CPU/memory of gateway and processors, Kafka consumer lag (processors expose
  `/metrics`), Postgres connection saturation.

### 4. How message loss is defined

State the definition precisely, because "zero message loss" is only meaningful when bounded:

- Each sent message carries a unique id.
- A message is **delivered** if it is observed by its intended recipient(s) within a stated timeout.
- **Loss** = sent − delivered (within timeout), reconciled against the authoritative
  `message_deliveries` table in Postgres.
- Report the observation window and timeout, and whether late-but-eventually-delivered messages count
  as loss. Note that OHMF is **at-least-once**, so duplicates are possible and must be counted
  separately from loss.

### 5. Where sample output lives

Committed runs should include:

- The raw driver output (or a summarized export) under `benchmarks/results/<date>-<scenario>/`.
- The environment manifest used (compose file + any overrides + host description).
- A short prose summary: what was tested, what the numbers were, and what they do and do not prove.

## Suggested directory layout (once results exist)

```
benchmarks/
├── README.md                 # this file
├── scenarios/                # load-test driver scripts (k6 / Go client / etc.)
└── results/
    └── <date>-<scenario>/
        ├── manifest.md       # environment + exact config
        ├── raw/              # raw tool output
        └── summary.md        # what it proves / does not prove
```

## Follow-ups

To turn the target claim into evidence:

1. Add a real WebSocket load driver under `benchmarks/scenarios/` that connects, subscribes, sends,
   and verifies receipt with per-message ids.
2. Fix or remove `ohmf/services/gateway/_tools/e2ee-load-test.go` (it uses `"=" * 60`, which is not
   valid Go), and relabel it clearly as an E2EE micro-benchmark, not a system load test.
3. Run against the full `ohmf/infra/docker/docker-compose.yml` stack with the driver on a separate
   host, capture results under `benchmarks/results/`, and update the status section above.

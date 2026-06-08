# Sustained 120 msg/sec validation - 2026-06-08

## Scope

- Local single-node Kubernetes only: `k3d` cluster `ohmf-b1`
- Overlay: `deploy/k8s/overlays/local-k3s-full`
- Driver: committed `benchmarks/cmd/loadgen`, host-executed as `benchmarks/bin/loadgen.exe`
- Path under test: authenticated `POST /v1/messages` accept path into the local Kafka/Postgres/Cassandra pipeline
- Latency labels in this artifact are **client-observed HTTP accept latency only**, not end-to-end delivery latency

## Result

Stage B1 **did not pass**.

The clean target run used a configured send duration of `120s` at `120 msg/sec` after a diagnostic warmup and a namespace reset to clear auth-start limiter state. The committed driver recorded:

- Requested: `14400`
- Sent attempts: `14400`
- Accepted: `419`
- Failed sends: `13981`, all bucketed as `http_429`
- Postgres delta: `419`
- Cassandra delta: `419`
- Duplicates: `0`
- Missing: `0`
- Kafka lag recovery to `0`: `2.05s`

Accepted throughput averaged about `3.49 accepted msg/sec` over the configured `120s` send window, far below the `120 accepted msg/sec` Stage B1 target.

## Supported claim

This artifact supports only the claim that **OHMF, as configured on commit `a767b25091b9eeecbe83b163d6abd669253a5917` and exercised by the committed local load driver on a single-node `k3d` cluster, did not sustain `120 accepted messages/sec` through the local full pipeline**. During the clean target run, the gateway accepted `419/14400` requests, all accepted messages reconciled to Postgres and Cassandra, duplicates stayed at `0`, missing stayed at `0`, and Kafka consumer-group lag returned to `0` within `2.05s`.

## Unsupported claims

- OHMF can sustain `120 accepted messages/sec` in this local configuration
- Zero loss at `120 msg/sec`
- Any `p95` or `p99` delivery latency claim
- `3,100` concurrent clients
- Production readiness
- HA, failover, or multi-node resilience
- Any cloud or production benchmark interpretation

## Environment

| Field | Value |
| --- | --- |
| Commit SHA | `a767b25091b9eeecbe83b163d6abd669253a5917` |
| Host | `skibidiohiogyat` |
| OS / Arch | `windows/amd64` |
| Host CPUs | `32` |
| Docker | Docker Desktop `4.32.0`, Engine `27.0.3` |
| Docker memory | `31.46 GiB` |
| Cluster | `k3d-ohmf-b1` |
| k3d | `v5.9.0` |
| Kubernetes server | `v1.35.5+k3s1` |
| Node runtime | `containerd://2.2.3-k3s1` |
| Overlay | `deploy/k8s/overlays/local-k3s-full` |
| Image tags | `ohmf-gateway:dev`, `ohmf-apps:dev`, `ohmf-messages-processor:dev` |

## Commands run

```powershell
git status --short
git rev-parse HEAD
git log --oneline -5
kubectl kustomize deploy/k8s/overlays/local-k3s-full
docker run --rm -v "${PWD}:/src" -w /src -e CGO_ENABLED=0 -e GOOS=windows -e GOARCH=amd64 golang:1.25 sh -lc "mkdir -p benchmarks/bin && /usr/local/go/bin/go build -o benchmarks/bin/loadgen.exe ./benchmarks/cmd/loadgen"
.\tools\bin\k3d.exe cluster create ohmf-b1
docker build -t ohmf-gateway:dev ohmf/services/gateway
docker build -t ohmf-apps:dev -f ohmf/services/apps/Dockerfile .
docker build -t ohmf-messages-processor:dev ohmf/services/messages-processor
.\tools\bin\k3d.exe image import ohmf-gateway:dev ohmf-apps:dev ohmf-messages-processor:dev -c ohmf-b1
kubectl apply --validate=false -k deploy/k8s/overlays/local-k3s-full
kubectl -n ohmf rollout status deploy/postgres --timeout=240s
kubectl -n ohmf rollout status deploy/redis --timeout=240s
kubectl -n ohmf rollout status deploy/cassandra --timeout=420s
kubectl -n ohmf rollout status deploy/kafka --timeout=420s
kubectl -n ohmf wait --for=condition=complete job/kafka-init --timeout=240s
kubectl -n ohmf rollout status deploy/apps --timeout=240s
kubectl -n ohmf rollout status deploy/messages-processor --timeout=240s
kubectl -n ohmf rollout status deploy/gateway --timeout=240s
kubectl -n ohmf port-forward svc/gateway 18080:8081
.\benchmarks\bin\loadgen.exe -config benchmarks\tmp\stage-b1\warmup-10x60.json -output-dir benchmarks\tmp\stage-b1\warmup-10x60-result
kubectl delete namespace ohmf --wait=true --timeout=240s
kubectl apply --validate=false -k deploy/k8s/overlays/local-k3s-full
.\benchmarks\bin\loadgen.exe -config benchmarks\tmp\stage-b1\sustained-120x120.json -output-dir benchmarks\results\2026-06-08-sustained-120msgsec
```

## Warmup diagnostic

The required warmup was useful diagnostic evidence but is **not** the main result:

- Scenario: `10 msg/sec for 60s`
- Outcome: `239/600` accepted, `361` failed as `http_429`
- Reconciliation: `pg_delta=239`, `cass_delta=239`, Kafka lag `-> 0` in `2.15s`

Because the warmup consumed OTP-start rate-limit budget, the namespace was deleted and recreated before the clean target run.

## Counts

| Field | Value |
| --- | --- |
| Requested | `14400` |
| Sent attempts | `14400` |
| Accepted | `419` |
| Failed `http_429` | `13981` |
| Duplicates | `0` |
| Missing | `0` |
| Postgres delta | `419` |
| Cassandra delta | `419` |

## Latency

These are **client-observed HTTP accept latencies** for accepted requests only:

- p50: `39.24 ms`
- p95: `2027.84 ms`
- p99: `2116.66 ms`

## Kafka lag

- Lag snapshots during the run stayed near zero; the highest captured snapshot showed total lag `1`
- Driver reconciliation recorded lag back to `0` within `2.05s`
- Lag evidence is consumer-group scoped, not run scoped

## Restarts and resource observations

- Pod restarts before run: all active pods at `0`
- Pod restarts after run: all active pods still at `0`
- HPA: none present in this overlay
- Typical pre-run pod usage:
  - Cassandra about `26m CPU`, `1331Mi`
  - Kafka about `49m CPU`, `371Mi`
  - Gateway about `4m CPU`, `9Mi`
  - Postgres about `11m CPU`, `45Mi`
- In-run peak sampled pod usage:
  - Cassandra up to about `851m CPU`, `1331Mi`
  - Kafka up to about `347m CPU`, `402Mi`
  - Gateway up to about `127m CPU`, `28Mi`
  - Postgres up to about `39m CPU`, `85Mi`
  - Messages processor up to about `31m CPU`, `7Mi`

## Limiting factor

The dominant failure mode was front-door rate limiting, not persistence loss. The committed gateway send path enforces a per-user limiter of `30 messages / 10 seconds` with burst `60` in [service.go](/C:/Users/James/Downloads/Messages/ohmf/services/gateway/internal/messages/service.go:2679). The observed `419` accepted requests over `120s` closely matches that limiter envelope, which explains why the run failed the `120 accepted msg/sec` objective even though accepted messages reconciled cleanly downstream.

## Raw files

- Raw driver summary: [driver-summary.raw.md](/C:/Users/James/Downloads/Messages/benchmarks/results/2026-06-08-sustained-120msgsec/driver-summary.raw.md)
- Raw driver JSON: [driver-summary.raw.json](/C:/Users/James/Downloads/Messages/benchmarks/results/2026-06-08-sustained-120msgsec/driver-summary.raw.json)
- Environment capture: [env.json](/C:/Users/James/Downloads/Messages/benchmarks/results/2026-06-08-sustained-120msgsec/env.json)
- Driver stderr: [driver.stderr.log](/C:/Users/James/Downloads/Messages/benchmarks/results/2026-06-08-sustained-120msgsec/driver.stderr.log)
- Observation snapshots: [observations](/C:/Users/James/Downloads/Messages/benchmarks/results/2026-06-08-sustained-120msgsec/observations)

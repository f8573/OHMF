# OHMF — Local Kubernetes (k3s) deployment profile

Minimal, plain-Kubernetes (Kustomize, no Helm) manifests for deploying a slice
of OHMF to a **single-node local cluster** (k3s, k3d, kind, or Docker Desktop
Kubernetes). This exists to back a resume-level claim with visible, runnable
repo evidence — not to be a production deployment.

> **Read this first — scope.** This is a *stage-1, single-node* profile. It is
> deliberately small. The honest claims and non-claims are spelled out below;
> please don't read more into it than is written here.

---

## What this proves

- OHMF services build into images and **run on Kubernetes**, not just Docker
  Compose.
- A real application pod (**`apps`**) starts, connects to an **in-cluster
  Postgres**, applies its migrations, and reports healthy via an HTTP probe.
- The **`gateway`** edge service runs on Kubernetes and serves a working
  `/healthz` / `/readyz` (in smoke mode) behind a ClusterIP Service.
- The manifests follow Kubernetes basics: a dedicated namespace, common labels,
  a ConfigMap for non-secret config, a Secret for secret-shaped values,
  readiness + liveness probes, conservative resource requests/limits, and
  ClusterIP services.
- A one-command smoke script (`scripts/smoke-k3s.sh`) applies the overlay, waits
  for rollout, and verifies the health endpoint end to end.

## What this does NOT prove

- **Not production-grade.** No Ingress/TLS, no NetworkPolicies, no PodSecurity
  hardening, no real secret management (Vault/KMS/sealed-secrets), no backups.
- **Not highly available / multi-node.** Everything is `replicas: 1` on one
  node. Storage is `emptyDir` (ephemeral) — Postgres data is lost on restart.
- **No autoscaling.** There is no HPA/VPA and no autoscaling claim.
- **Not benchmark-validated.** No throughput/latency numbers are produced or
  implied by this profile.
- **Not the full event pipeline.** Kafka, Cassandra, and the message processors
  are **not** deployed here (see *Staged deployment path*). The full gateway
  (WebSockets + Kafka + Cassandra) is not exercised — the gateway runs in
  **smoke mode**.

If you need the full stack (Kafka, Cassandra, processors, Prometheus/Grafana),
use the existing **Docker Compose** workflow under `ohmf/infra/docker/` — it is
unchanged and remains the way to run everything together.

---

## What gets deployed (stage-1 profile)

| Workload            | Image                          | Role                                    | Notes |
| ------------------- | ------------------------------ | --------------------------------------- | ----- |
| `gateway`           | `ohmf-gateway:dev`             | API / realtime edge (**smoke mode**)    | serves `/healthz`,`/readyz`,`/metrics`; proxies `/v1/{apps,contacts,media}` |
| `apps`              | `ohmf-apps:dev`                | mini-app registry backend               | Postgres-backed; waits for DB via init container |
| `postgres`         | `postgres:16-alpine`           | dev database                            | **ephemeral** (`emptyDir`) |
| `redis`            | `redis:7-alpine`               | dev cache/presence dependency           | deployed but not exercised in smoke mode |
| `messages-processor`| `ohmf-messages-processor:dev`  | Kafka worker                            | **`replicas: 0`** — disabled, needs Kafka + Cassandra |

`contacts` and `media` backends are intentionally not in this profile; the
gateway's proxies to them will return 502, but the gateway's own health does not
depend on them.

---

## Staged deployment path (why Kafka/Cassandra are not here)

A full in-cluster Kafka + Cassandra setup is heavy for a single 16 GB dev node
and would add significant operational surface (StatefulSets, persistent volumes,
init/topic jobs) for little additional credibility at this stage. Rather than
ship something fragile or claim more than is true, this profile is staged:

- **Stage 1 (this profile, committed):** validate that OHMF *application*
  workloads deploy on Kubernetes with lighter dependencies (Postgres, Redis) and
  that health/readiness wiring works on a single node.
- **Stage 2 (future work):** in-cluster Kafka + Cassandra (likely as
  StatefulSets) so the `messages-processor` / `delivery-processor` workers and
  the full (non-smoke) gateway can run. Until then, **full event-store
  validation stays on Docker Compose** (`ohmf/infra/docker/`).

The `messages-processor` manifest is committed at `replicas: 0` so its shape
(probes, resources, env) is reviewable today without CrashLooping.

---

## Prerequisites

- A single-node Kubernetes cluster, e.g. one of:
  - [k3s](https://k3s.io/), [k3d](https://k3d.io/), [kind](https://kind.sigs.k8s.io/),
    or Docker Desktop's built-in Kubernetes.
- `kubectl` (with built-in Kustomize; `kubectl kustomize` must work).
- `docker` to build the local images.
- `curl` and `bash` for the smoke script (on Windows: Git Bash or WSL).

---

## 1. Build the local images

From the repository root:

```bash
# gateway (build context = the gateway module; Dockerfile builds ./cmd/api)
docker build -t ohmf-gateway:dev ohmf/services/gateway

# apps (build context = repo root; Dockerfile copies the ohmf/ tree)
docker build -t ohmf-apps:dev -f ohmf/services/apps/Dockerfile .

# only needed for a future stage-2; harmless to build now
docker build -t ohmf-messages-processor:dev ohmf/services/messages-processor
```

## 2. Load the images into the cluster

The manifests use `imagePullPolicy: Never` — there is no registry in this
profile, so the cluster must already have the images. **This step is
cluster-specific:**

```bash
# k3d
k3d image import ohmf-gateway:dev ohmf-apps:dev

# kind
kind load docker-image ohmf-gateway:dev ohmf-apps:dev

# k3s (bare, using its containerd)
docker save ohmf-gateway:dev | sudo k3s ctr images import -
docker save ohmf-apps:dev    | sudo k3s ctr images import -

# Docker Desktop Kubernetes: images built with `docker build` are already
# visible to the cluster — no load step needed.
```

## 3. (Optional) provide a real secret

`base/secrets.example.yaml` ships **dev-only placeholder** values so the profile
works out of the box. For anything beyond local play, copy it, change the
values, keep your copy untracked, and reference that instead.

## 4. Apply the manifests

```bash
kubectl apply -k deploy/k8s/overlays/local-k3s
```

## 5. Check pod status

```bash
kubectl -n ohmf get pods,svc
kubectl -n ohmf rollout status deploy/gateway
kubectl -n ohmf logs deploy/apps
```

## 6. Run the smoke health check

```bash
deploy/k8s/scripts/smoke-k3s.sh
```

This applies the overlay, waits for `postgres`, `redis`, `apps`, and `gateway`
to roll out, port-forwards the gateway, curls `/healthz`, prints pods/services,
and exits non-zero on failure. Record a real run under
[`results/`](results/README.md).

## 7. Tear down

```bash
# delete just this profile's namespace and resources
deploy/k8s/scripts/smoke-k3s.sh --down
# or directly:
kubectl delete -k deploy/k8s/overlays/local-k3s
```

---

## Why this is not a production deployment

Production messaging infrastructure needs durable/replicated storage, the full
Kafka + Cassandra event store, multi-node scheduling and anti-affinity,
Ingress/TLS, NetworkPolicies, real secret management, autoscaling, backups, and
tested failure/rollback procedures. **None of those are present here**, by
design. This profile's single job is to make the statement *"the services run on
Kubernetes on a single node"* concretely true and reproducible. Treat every
broader claim (HA, autoscaling, performance, production-readiness) as **not
supported** until there are committed manifests and recorded results that prove
it.

## Layout

```
deploy/k8s/
  README.md                     # this file
  base/                         # environment-agnostic manifests
    namespace.yaml
    configmap.yaml              # non-secret config
    secrets.example.yaml        # secret-shaped placeholders (dev only)
    postgres-dev.yaml           # ephemeral dev Postgres (Deployment + Service)
    redis-dev.yaml              # ephemeral dev Redis (Deployment + Service)
    api-deployment.yaml         # apps backend (Deployment + Service)
    gateway-deployment.yaml     # gateway edge (smoke mode)
    gateway-service.yaml        # gateway ClusterIP Service
    worker-deployment.yaml      # messages-processor (replicas: 0, staged)
    kustomization.yaml
  overlays/
    local-k3s/
      kustomization.yaml        # references base
      resource-patches.yaml     # conservative sizing for a 16GB/8-core laptop
  scripts/
    smoke-k3s.sh                # apply + wait + health-check + teardown
  results/
    README.md                   # what a recorded smoke artifact should contain
```

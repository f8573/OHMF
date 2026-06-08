# OHMF - Local Kubernetes profiles

Plain-Kubernetes (Kustomize, no Helm) manifests for **single-node local**
validation of OHMF. These profiles exist to create honest, repo-visible
evidence for statements like "the services run on Kubernetes" and
"the async message path can be exercised on a local cluster."

They do **not** claim production readiness.

## Profiles

### `overlays/local-k3s`

Stage-1 smoke profile:

- gateway in **smoke mode**
- `apps`
- Postgres
- Redis
- `messages-processor` present but `replicas: 0`

What it proves:

- the manifests apply on a real local cluster
- `apps` starts against in-cluster Postgres and applies migrations
- `gateway /healthz` and `/readyz` work behind a ClusterIP Service

What it does not prove:

- Kafka/Cassandra pipeline behavior
- autoscaling
- production deployment

Run it with:

```bash
kubectl apply -k deploy/k8s/overlays/local-k3s
deploy/k8s/scripts/smoke-k3s.sh
```

### `overlays/local-k3s-full`

Local full-pipeline baseline:

- full gateway mode (`APP_SMOKE_MODE=0`)
- `apps`
- Postgres
- Redis
- Kafka (single broker, KRaft, local-only)
- Kafka topic init job
- Cassandra (single node, local-only)
- `messages-processor` enabled

What it proves when the smoke passes:

- gateway/API accepts a real authenticated send
- the gateway produces to Kafka
- `messages-processor` consumes and persists the message
- the message lands in Postgres and Cassandra on a live local cluster

What it still does not prove:

- production Kafka/Cassandra operations
- multi-node scheduling or HA
- durable storage
- ingress/TLS, NetworkPolicy, secret management, backups
- throughput, latency, or large-scale performance claims

Run it with:

```bash
kubectl apply -k deploy/k8s/overlays/local-k3s-full
deploy/k8s/scripts/smoke-k3s-full.sh
```

### `overlays/local-k3s-full-hpa`

Optional autoscaling baseline layered on top of `local-k3s-full`.

Current scope:

- HPA targets the **gateway** only
- depends on Metrics Server being available
- tuned for visible behavior on a local single-node cluster
- uses synthetic in-cluster load against `gateway /metrics`

This is a **local HPA smoke**, not production tuning.

Run it with:

```bash
kubectl apply -k deploy/k8s/overlays/local-k3s-full-hpa
deploy/k8s/scripts/hpa-smoke-k3s-full.sh
```

### `overlays/local-k3s-resilience`

PVC-backed local resilience profile layered on top of `local-k3s-full`.

Current scope:

- swaps Postgres, Kafka, and Cassandra from `emptyDir` to `local-path` PVCs
- keeps the same single-broker and single-node honesty
- supports local restart/recovery validation on a k3s/k3d cluster

What it proves when a restart artifact passes:

- local PVC-backed state can survive a pod restart
- the single-node stack can return to service after local restart stabilization

What it still does not prove:

- HA
- broker failover
- durable production storage
- zero-loss across restart windows

Run it with:

```bash
kubectl apply -k deploy/k8s/overlays/local-k3s-resilience
```

## Prerequisites

- A reachable local Kubernetes cluster:
  - `k3s`
  - `k3d`
  - `kind`
  - Docker Desktop Kubernetes
- `kubectl`
- `docker`
- `curl`
- `python`
- `bash` for the provided smoke scripts

## Local images

Build from the repo root:

```bash
docker build -t ohmf-gateway:dev ohmf/services/gateway
docker build -t ohmf-apps:dev -f ohmf/services/apps/Dockerfile .
docker build -t ohmf-messages-processor:dev ohmf/services/messages-processor
```

Load them into the cluster when needed:

```bash
# k3d
k3d image import ohmf-gateway:dev ohmf-apps:dev ohmf-messages-processor:dev

# kind
kind load docker-image ohmf-gateway:dev ohmf-apps:dev ohmf-messages-processor:dev

# bare k3s
docker save ohmf-gateway:dev | sudo k3s ctr images import -
docker save ohmf-apps:dev | sudo k3s ctr images import -
docker save ohmf-messages-processor:dev | sudo k3s ctr images import -
```

## Resource posture

These profiles are intentionally bounded for a stronger developer workstation,
not sized for production:

- Postgres, Redis, gateway, and `apps` stay small
- Kafka and Cassandra are single-node and use `emptyDir`
- `local-k3s-full` increases requests/limits enough to run the real async path
- the HPA profile relies on those requests for CPU-based scaling
- `local-k3s-resilience` keeps local-path PVC semantics and still does not imply
  production durability

## Recorded evidence

Real, dated run artifacts belong in [`results/`](results/README.md).

Supported claims must match those artifacts exactly.

## Unsupported claims

These profiles do **not** support claims of:

- production readiness
- Helm support
- HA or multi-node resilience
- ingress/TLS
- NetworkPolicy or PodSecurity hardening
- durable storage
- backup/restore validation
- benchmark throughput or latency
- "zero loss" or large-client-count load-test results

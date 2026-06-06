# 17 — Infrastructure Overview

Mapping: OHMF spec section 17 (Infrastructure & Deployment)

> NOTE: This page mirrors the spec's *design intent*. Most items below (Helm,
> Terraform, Vault/KMS, mTLS, ELK/OTel, IaC-in-CI) are **not implemented in this repo**.
> Local Docker Compose under `infra/docker` is the full local stack. A minimal
> **local single-node k3s** Kubernetes profile (Kustomize, app + Postgres/Redis,
> gateway in smoke mode) now exists under `deploy/k8s/` — it is *not* production-grade,
> *not* autoscaling, and *not* benchmark-validated. Read the rest as a target, not as
> shipped capability.

Purpose
- Document infra components, environment setups, and deployment patterns for local dev and production.

Expected behavior
- Provide reproducible Docker Compose for local dev. (Present today.)
- Provide a minimal local single-node Kubernetes (k3s) profile for app-deployment
  credibility. (Present today under `deploy/k8s/`; single-node only, not production.)
- Design intent only, NOT in this repo: Helm charts, production/multi-node Kubernetes,
  and Terraform. None are committed.

Key elements
- Local compose: Postgres, Redis, Kafka (or nats), gateway, auth, users, messages.
- Secrets management: Vault or KMS recommended.
- Networking: mTLS for service-to-service.

Implementation constraints
- IaC must be idempotent and tested in CI.

Security considerations
- Use least privileged IAM roles; rotate secrets.

Observability and operational notes
- Centralized logs (ELK/Opensearch), metrics (Prometheus), traces (OTel collector).
- Local Prometheus and Grafana assets now live under `infra/observability`.

Testing requirements
- Infrastructure integration tests (smoke tests) in CI.

References
- infra/docker for compose, infra/docker/README.md for examples, infra/observability for metrics and dashboards.

# Production Readiness Gap

Status: Stage A evidence exists, but production-readiness remains unsupported.

This document exists to make the evidence boundary explicit. Even after local
M4 validation, the following remain outside the supported claim boundary unless
separately implemented and proven:

- production-ready Kubernetes operations
- HA or failover for Kafka, Cassandra, Postgres, or the gateway tier
- ingress, TLS, NetworkPolicy, PodSecurity, secret rotation
- backup and restore
- durable production storage classes
- end-to-end p95 latency claims
- large-client concurrency claims beyond committed artifacts

Rows will be filled during Stage E with concrete artifact links and gap notes.

Current Stage A evidence does support only this narrower boundary:

- local single-cluster deployment and restart validation
- client-observed accept latency only
- run-scoped reconciliation by fresh test conversation where possible
- explicit documentation of the single-broker Kafka availability gap during restart

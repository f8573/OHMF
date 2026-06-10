# Production Readiness Gap

Status: M4 local validation complete. Production-readiness remains unsupported and out of scope.

This document makes the evidence boundary explicit. Even after M4 local Kubernetes validation,
the following remain outside the supported claim boundary unless separately implemented and proven:

- production-ready Kubernetes operations (no Helm, no ingress/TLS, no NetworkPolicy, no PodSecurity)
- HA or failover for Kafka, Cassandra, Postgres, or the gateway tier
- multi-node scheduling or multi-host resilience
- durable production storage classes and backup/restore
- end-to-end p95/p99 delivery latency claims
- large-client concurrency claims beyond committed artifacts
- zero-loss claims generally

The M4 evidence supports only this bounded boundary:

- **Local single-node cluster** — normal load, processor pod deletion/Kafka consumer-group rebalance, and processor backlog recovery at `120 msg/sec`
- **Exact Kafka/Postgres/Cassandra reconciliation** for the scaling-matrix and backlog-recovery committed artifacts
- **Kafka consumer group rebalance** confirmed after pod deletion (exact reconciliation was not established for that run; Redis outage caused gateway failures)
- **Client-observed HTTP accept latency** only — no server-internal latency claims
- Explicit documentation of single-broker Kafka availability gap during restart (resilience overlay artifact)

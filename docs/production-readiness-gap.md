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

- **Local single-node cluster** — normal scaled load, processor pod deletion/Kafka consumer-group rebalance, and processor backlog recovery at `120 msg/sec`
- **Exact Kafka/Postgres/Cassandra reconciliation** established for the scaling-matrix (4 replicas @ 120 msg/sec), the 2026-06-10 processor pod-deletion/rebalance run, and the 2026-06-10 backlog-recovery run
- **Diagnostic-only artifact preserved**: the initial 2026-06-09 pod-deletion run at 120 msg/sec was invalidated by a coincident Redis ack outage (4,520 gateway 500s); exact reconciliation was not established for that run; see [benchmarks/results/2026-06-09-processor-pod-deletion-120msgsec/summary.md](../benchmarks/results/2026-06-09-processor-pod-deletion-120msgsec/summary.md)
- **Client-observed HTTP accept latency** only — no server-internal latency claims
- Explicit documentation of single-broker Kafka availability gap during restart (resilience overlay artifact)

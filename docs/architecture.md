# OHMF Architecture

This document describes how OHMF is put together: its components, how a message flows through the
system, the concurrency model, what failures it assumes, and the trade-offs that follow. It is
written to match what is actually in the repository. Where something is design intent rather than
implemented-and-tested behavior, it is labeled as such.

For the correctness-hardening work and the specific test backing each fix, see
[reliability-hardening.md](reliability-hardening.md).

## Components

| Component | Path | Responsibility |
| --- | --- | --- |
| Gateway (API) | `ohmf/services/gateway/cmd/api` | HTTP + WebSocket entry point: auth, request validation, idempotency claiming, message send (sync/async), realtime fan-out to connected clients. |
| Gateway worker | `ohmf/services/gateway/cmd/worker` | Replication / fan-out worker that processes domain events in per-conversation order. |
| messages-processor | `ohmf/services/messages-processor/cmd/processor` | Consumes `msg.ingress.v1` from Kafka, persists to Postgres (authoritative) and Cassandra (shadow write), publishes `msg.persisted.v1`, with at-least-once retry semantics. |
| delivery-processor | `ohmf/services/delivery-processor/cmd/processor` | Consumes persisted events, writes idempotent delivery receipts to `message_deliveries`, emits delivery notifications. |
| sms-processor | `ohmf/services/sms-processor/cmd/processor` | Consumes the SMS dispatch topic for off-platform delivery. |
| Auxiliary services | `ohmf/services/{contacts,apps,media,auth,users,...}` | Supporting domain services (contacts, mini-apps, media, etc.). |
| PostgreSQL | compose `db` | Authoritative store for messages, conversation ordering, and delivery receipts. |
| Cassandra | compose `cassandra` | Secondary message store. Shadow-written today; reads default off (`APP_USE_CASSANDRA_READS=false`). |
| Kafka | compose `kafka` (+ `kafka-init`) | Event backbone. Partitioned topics (e.g. `msg.ingress.v1` at 96 partitions) plus dead-letter queues. |
| Redis | compose `redis` | Presence reference-counting, typing/user-event fan-out, async-send ack wake-ups. |
| Observability | `ohmf/infra/observability` | Prometheus scraping + Grafana dashboards; processors expose `/metrics`. |

## Data flow

Send path (async, the primary path):

1. A client sends a message over WebSocket/HTTP to the **gateway**.
2. The gateway authenticates, validates (membership, blocks, encryption policy, device ownership,
   reply targets — shared by sync and async paths), and **claims the idempotency key** before any
   canonical persistence.
3. The gateway produces to Kafka topic `msg.ingress.v1`.
4. **messages-processor** consumes the event, persists the message to **Postgres** (source of truth)
   and shadow-writes to **Cassandra**, then publishes `msg.persisted.v1`. On recoverable
   post-persistence failures it leaves the Kafka offset uncommitted and retries in place.
5. The gateway's async-send wait path observes the persistence ack via Redis pub/sub (with a final
   authoritative key read), then returns the canonical message. On timeout it returns a provisional
   queued response (`server_order = 0`) to be reconciled later.
6. **delivery-processor** consumes persisted events and records delivery receipts idempotently in
   `message_deliveries`, then emits delivery notifications.
7. The gateway fans delivery/realtime events back out to connected clients via Redis pub/sub and the
   WebSocket layer.

Read/sync path: clients list/sync from Postgres-backed history; per-conversation ordering is
provided by a monotonic `server_order`.

## Persistence path

- **PostgreSQL is authoritative.** Messages, conversation ordering (`server_order` /
  `next_server_order`), and delivery receipts (`message_deliveries`) live here. Uniqueness
  constraints — e.g. the delivered-state guard on `(message_id, recipient_user_id)` — are what make
  duplicate suppression correct rather than advisory.
- **Cassandra is a secondary store**, currently shadow-written. The intent is a scalable message
  read path; today it is not the live serving path.
- **Kafka and Redis are best-effort distribution**, not sources of truth. They reduce latency and
  decouple producers from consumers, but durable correctness always falls back to Postgres.

## Concurrency model

- **Stateless gateway, horizontally scalable.** Multiple gateway instances can run; client WebSocket
  connections are local to a pod, while shared state (presence, acks) lives in Redis.
- **Idempotency before persistence.** Concurrent sync sends with the same idempotency key are
  serialized by claiming the key first, then resolving duplicates to the stored canonical response,
  so only one row is created and `next_server_order` advances once.
- **Per-conversation ordered fan-out.** The replication worker claims only the earliest unprocessed
  domain event per conversation, so multiple workers cannot publish later events before earlier ones
  for the same conversation. Order is preserved per conversation, not globally.
- **At-least-once processing with dedupe.** Processors may see the same Kafka record more than once
  (retry/redelivery). Correctness comes from idempotent writes and tracking completed downstream
  side effects in the idempotency payload, not from assuming each record is seen once.
- **Cross-pod presence refcounting.** Presence is reference-counted across sessions in Redis, so one
  pod disconnecting only clears global presence when the last live session for a user disappears.

## Failure assumptions

- A persisted message may be **redelivered** by Kafka; handlers must be idempotent.
- A processor may **crash mid-pipeline**. Postgres commit is the durability boundary; recoverable
  post-commit steps (Cassandra write, Redis ack, downstream publish) are retried.
- Redis pub/sub is a **wake-up optimization** and may be missed; the durable ack key is authoritative
  for the gateway wait path.
- Redis presence/typing state is **TTL-backed**; correctness depends on session refresh and cleanup
  continuing to run.
- Kafka/Redis notification publishes are **best-effort** around authoritative Postgres state.

## Trade-offs

- **At-least-once over exactly-once.** Exactly-once across Postgres + Redis + Cassandra + Kafka is not
  attempted. The system favors durable authoritative records plus idempotent, retryable side effects.
  The cost is that a crash between a side effect and its marker can duplicate that side effect.
- **Postgres-authoritative + Cassandra-secondary.** Keeps correctness reasoning simple (one source of
  truth) while leaving room to grow a scalable read path. The cost is dual-write complexity and that
  Cassandra is not yet exercised as the serving path.
- **Best-effort realtime over guaranteed realtime.** Redis/Kafka give low-latency UX; durability and
  recovery always fall back to Postgres rather than to the realtime layer.
- **Per-conversation ordering over global ordering.** A global total order would be far more
  expensive; per-conversation monotonicity is what consumers actually need for resume/sync.

## Known limitations

- Delivery semantics are **at-least-once**, not exactly-once.
- **Kubernetes is local-single-node only.** `deploy/k8s/` ships Kustomize manifests for a lighter smoke profile (`local-k3s`), a fuller local pipeline profile (`local-k3s-full`), and an optional local gateway HPA layer (`local-k3s-full-hpa`). Recorded results now show those profiles working on a real local single-node cluster, including a gateway HPA smoke where replicas increased under synthetic load. They still do **not** establish production readiness, Helm, multi-node/HA, durable storage, ingress/TLS, network policy, or benchmark validation. All services expose `/healthz`, `/readyz`, and `/metrics` so they *can* be orchestrated.
- **No substantiated load-test results** live in the repo yet; see
  [../benchmarks/README.md](../benchmarks/README.md).
- Cassandra read path is **off by default** (shadow-write only).
- Ordering guarantees are **per conversation**, not a global total order.



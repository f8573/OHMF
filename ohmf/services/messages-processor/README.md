# Messages Processor

Consumes `msg.ingress.v1`, validates metadata against Postgres, persists canonical timeline records to Cassandra, updates idempotency state, and publishes:

- `msg.persisted.v1`
- `microservice.events.v1`
- `msg.sms.dispatch.v1` (for SMS-intent events)

Also writes the gateway ack correlation payload into Redis key `msg:ack:{event_id}` and publishes the same payload on `msg:ack:notify:{event_id}` so the gateway can wait for persistence without high-frequency Redis polling.

## Retry and DLQ Semantics

- The processor provides at-least-once retry behavior with idempotency suppression for ordinary redelivery.
- `msg.ingress.dlq.v1` is a terminal quarantine topic for malformed or non-retryable ingress records. It is not used as a retry queue.
- If Postgres commit has already succeeded and a later Cassandra, Redis ack, or Kafka publish step fails, the processor now leaves the Kafka offset uncommitted so the record is retried in-place instead of being quarantined and acknowledged.
- Redis sender ack writes use `SET` on a stable key and Cassandra writes are deterministic upserts, so duplicate redelivery repeats those side effects safely.
- Downstream Kafka publishes (`msg.persisted.v1`, `microservice.events.v1`, and `msg.sms.dispatch.v1`) are tracked in the idempotency payload after each successful publish so duplicate Kafka redelivery after a later offset-commit failure does not re-emit already published downstream records.
- Redis recipient fanout publish is also marked after a full successful fanout pass to suppress duplicate realtime publishes on ordinary Kafka redelivery.
- The processor does not provide exactly-once cross-system side effects. There is still no transactional outbox across Postgres, Redis, Cassandra, and Kafka, so a process crash after a downstream side effect succeeds but before its idempotency marker update commits can still produce duplicate downstream emission on retry.

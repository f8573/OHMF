# Reliability and correctness hardening

This note summarizes distributed-systems hardening work that is already implemented in OHMF. It is intentionally narrow: the goal is to describe correctness improvements and their boundaries, not to claim finished production readiness.
OHMF is not presented here as exactly-once distributed messaging; it uses durable database records as the source of truth, best-effort realtime notifications for low-latency UX, and targeted idempotency/retry logic to make ordinary redelivery and partial-failure paths safer.

## Semantics and limits

- `message_deliveries` is the authoritative store for delivery receipts and duplicate delivery suppression.
- Kafka and Redis realtime notifications are best-effort distribution paths around that authoritative state. They improve latency, but they are not the source of truth.
- `messages-processor` now has at-least-once retry behavior with redelivery suppression for ordinary retries. It does not provide exactly-once cross-system side effects across Postgres, Redis, Cassandra, and Kafka.

## 1) Sync send idempotency race

Problem
- Two concurrent sync sends with the same idempotency key could race and each try to create a message and advance conversation ordering.

Why it mattered
- That risks duplicate messages, skipped or double-consumed `server_order` values, and inconsistent client-visible history.

Fix
- The sync path now claims idempotency before canonical persistence, then resolves duplicates back to the stored canonical response instead of allowing a second logical send to proceed.

Test/evidence
- `TestSendSyncConcurrentSameIdempotencyKeyCreatesOneMessage` and `TestSendToPhoneSyncConcurrentSameIdempotencyKeyCreatesOneMessage` in `ohmf/services/gateway/internal/messages/send_sync_idempotency_race_integration_test.go` verify that two concurrent callers get the same canonical message while only one row is persisted and `next_server_order` advances once.

Remaining limitation, if any
- This is scoped to the gateway/database idempotency boundary. It does not make downstream cross-system side effects exactly once.

## 2) Shared sync/async send validation

Problem
- Sync and async send flows can drift if they validate membership, blocks, encryption policy, device ownership, or reply targets differently.

Why it mattered
- Divergent validation creates mode-dependent behavior: a request rejected on one path might be accepted on the other.

Fix
- Both send modes now share the same validation invariants before persistence or enqueueing, so the transport path does not change the acceptance rules.

Test/evidence
- `TestSendValidationParitySyncAndAsync` in `ohmf/services/gateway/internal/messages/send_validation_parity_test.go` exercises blocked-recipient, encryption-policy, sender-device-ownership, and reply-target cases against both modes.

Remaining limitation, if any
- Parity is only as complete as the shared validation surface. Future send features still need to stay on that shared path.

## 3) Multi-worker fanout ordering

Problem
- Multiple fanout workers can process pending conversation events concurrently and publish later events before earlier ones for the same conversation.

Why it mattered
- Even if message storage is correct, a recipient inbox stream that regresses in `server_order` is a correctness failure for sync/resume consumers.

Fix
- The replication worker now claims only the earliest unprocessed domain event per conversation, preventing same-conversation reordering across concurrent workers.

Test/evidence
- `TestConcurrentFanoutWorkersPreserveConversationOrderInUserInbox` in `ohmf/services/gateway/internal/messages/fanout_ordering_integration_test.go` sends 40 messages and processes fanout with 4 workers, then verifies the recipient inbox stream stays monotonic by `server_order`.
- The worker-side ordering guard is in `ohmf/services/gateway/internal/replication/store.go`.

Remaining limitation, if any
- This preserves per-conversation order, not a global total order across different conversations.

## 4) Delivery processor idempotency

Problem
- Delivery processing can see the same persisted message more than once because of redelivery or retry, and naive handling would emit duplicate delivery rows and duplicate notifications.

Why it mattered
- Delivery receipts are user-visible state. Duplicates would corrupt receipt history and create noisy or contradictory realtime updates.

Fix
- Delivery writes now use `message_deliveries` as the authoritative receipt store with a unique delivered-state guard on `(message_id, recipient_user_id)`, and duplicate processing is treated as a no-op.

Test/evidence
- `TestProcessMessageSkipsDuplicateDeliveryEvent`, `TestProcessMessageSkipsDuplicateKafkaRedelivery`, `TestProcessMessageRetryAfterPartialRecipientFailureOnlyPublishesRemainingRecipient`, and `TestPGDeliveryRecorderTreatsConflictAsDuplicate` in `ohmf/services/delivery-processor/cmd/processor/main_test.go` verify row-level dedupe and suppression of duplicate Kafka/Redis notifications.
- The unique delivered-state index is created in `ohmf/services/gateway/migrations/000051_message_deliveries_delivered_idempotency.up.sql` and reflected in `ohmf/packages/protocol/sql/ohmf_schema.sql`.

Remaining limitation, if any
- Kafka and Redis notification publishes are still best-effort around the authoritative `message_deliveries` table.

## 5) Message processor retry semantics

Problem
- After Postgres persistence succeeds, later failures in Cassandra, Redis ack publication, or downstream Kafka publication are recoverable, but treating them as terminal can lose work or force unsafe replay behavior.

Why it mattered
- A messaging system has to distinguish retryable post-commit failures from malformed input. Otherwise it either drops persisted messages too early or duplicates downstream side effects unnecessarily.

Fix
- `messages-processor` now leaves Kafka offsets uncommitted on recoverable post-persistence failures, retries in place, and tracks completed downstream side effects in the idempotency payload so ordinary redelivery does not re-emit them.

Test/evidence
- `TestHandleFetchedMessageRetriesCassandraFailureAfterPostgresPersistence`, `TestHandleFetchedMessageRetriesRedisAckFailureAfterPersistence`, `TestHandleFetchedMessageRetriesKafkaPublishFailureAfterPersistence`, and `TestHandleFetchedMessageSkipsUnsafeDuplicateDownstreamPublishesOnRedelivery` in `ohmf/services/messages-processor/cmd/processor/main_test.go` cover the main retry paths.
- The behavior is also documented in `ohmf/services/messages-processor/README.md`.

Remaining limitation, if any
- This is still at-least-once, not exactly-once. A crash between a successful side effect and its side-effect marker update can still duplicate that side effect on retry.

## 6) Processor health/readiness/metrics

Problem
- Background processors are hard to operate safely if they only log failures and expose no health, readiness, lag, or duplicate/error counters.

Why it mattered
- Without explicit probes and metrics, orchestration cannot distinguish live from ready, and operators cannot see lagging consumers or retry-heavy behavior early.

Fix
- Both processors now expose `/healthz`, `/readyz`, and `/metrics`, including counters for processed items, errors, duplicates, DLQ publishes, and consumer lag.

Test/evidence
- `TestObservabilityHandlers` in `ohmf/services/delivery-processor/cmd/processor/main_test.go` verifies health, readiness failure, and metrics output.
- `ohmf/services/messages-processor/cmd/processor/observability_test.go` covers the same surface for `messages-processor`.

Remaining limitation, if any
- These endpoints improve operability, but they do not by themselves prove cluster-level autoscaling, alert tuning, or sustained production capacity.

## 7) Cross-pod presence refcounting

Problem
- Presence tracked only per-process or per-connection can be cleared incorrectly when one pod disconnects even though another live session for the same user still exists elsewhere.

Why it mattered
- In a multi-pod realtime system, presence must reflect aggregate live sessions, not the last local event seen by a single pod.

Fix
- Presence now uses Redis session keys plus per-user session membership so disconnecting one pod/session only removes global presence when the last live session disappears.

Test/evidence
- `TestDisconnectOnePodKeepsGlobalPresenceWhileAnotherSessionIsActive` and `TestTouchConnectionCleansUpStaleSessions` in `ohmf/services/gateway/internal/realtime/ws_test.go` verify refcount-style behavior and stale-session cleanup.
- `TestGetUserPresenceMarksOnlineWhenLiveSessionExists` in `ohmf/services/gateway/internal/presence/service_test.go` covers the read path.

Remaining limitation, if any
- This is still TTL-backed session bookkeeping in Redis, so correctness depends on session refresh and cleanup continuing to run.

## 8) Typing pubsub cleanup

Problem
- Legacy typing pubsub paths and disconnect leaks can leave stale typing state behind or emit redundant channels that do not match the newer user-event fanout model.

Why it mattered
- Typing is ephemeral, but stale typing indicators are a visible correctness bug and unnecessary legacy pubsub can amplify duplicate or inconsistent signals.

Fix
- Typing events now flow through the replication/user-event path, the legacy Redis typing channel is no longer published, and disconnect unregister cleans up typing keys.

Test/evidence
- `TestHandleTypingSignalDoesNotPublishLegacyRedisChannel`, `TestHandleTypingSignalBroadcastsToOtherMembers`, and `TestTypingStateIsCleanedUpOnDisconnect` in `ohmf/services/gateway/internal/realtime/ws_test.go` cover the new behavior.

Remaining limitation, if any
- Typing remains intentionally ephemeral and best-effort; it is not durable state.

## 9) Async ack wait improvement

Problem
- The async gateway send path originally depended too heavily on polling and could either return too early, wait inefficiently, or miss a persistence ack that arrived near the timeout boundary.

Why it mattered
- Async send needs to return either a canonical persisted message or an honest provisional response, without creating avoidable Redis load or hiding a just-arrived ack.

Fix
- The gateway now waits on Redis pubsub as a wake-up signal, uses sparse fallback reads instead of busy polling, and performs a final authoritative key read before timing out. If no ack arrives in time, it returns a provisional queued response with `server_order = 0`.

Test/evidence
- `TestAsyncPipelineWaitAckReturnsPublishedAck`, `TestAsyncPipelineWaitAckTimeoutDoesNotBusyPoll`, `TestSendAsyncReturnsCanonicalMessageWhenAckArrives`, and `TestSendAsyncTimeoutReturnsProvisionalAndLateAckRemainsReadable` in `ohmf/services/gateway/internal/messages/async_test.go` cover the success, timeout, and late-ack cases.
- The behavior is summarized in `ohmf/services/gateway/internal/messages/README.md`.

Remaining limitation, if any
- Redis pubsub is only a wake-up optimization. The durable ack key is authoritative for the gateway wait path, and a timeout still returns a provisional result that must be reconciled through normal sync/list behavior.

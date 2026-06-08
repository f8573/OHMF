# Processor stage instrumentation diagnostic - 2026-06-08

## Outcome

The instrumented ladder did not show an under-reconciliation failure in these runs.

## Failure semantics

Kafka offsets commit only after handler success. PostgreSQL commits can happen before Cassandra/Redis/downstream side effects, so partial persistence is possible inside an attempt, but retryable failures should leave the Kafka offset uncommitted.

## Stage counter snapshot

- `45 msg/sec`: accepted=`6750`, consumed=`6750`, pg_ok=`6750`, cass_ok=`6750`, commits=`6750`, handler_error=`0`, pg_exact=`6750`, cass_exact=`6750`, mode=`no failure detected`
- `60 msg/sec`: accepted=`9000`, consumed=`9000`, pg_ok=`9000`, cass_ok=`9000`, commits=`9000`, handler_error=`0`, pg_exact=`9000`, cass_exact=`9000`, mode=`no failure detected`

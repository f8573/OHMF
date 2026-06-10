# Processor stage instrumentation diagnostic - 2026-06-08

## Conclusion

The instrumented ladder did not show an under-reconciliation failure in these runs.

Kafka offsets do not commit after a retryable partial-persistence failure. The code commits offsets only after `processMessage` returns success; PostgreSQL can commit before Cassandra/Redis/downstream side effects, but those later failures leave the offset uncommitted for retry.

## Stage counters

| Rate | Accepted | Consumed | Decoded | Deduped | PG ok | PG fail | Cassandra ok | Cassandra fail | Redis ack ok | Redis ack fail | Persisted topic ok | Microservice topic ok | Offset commits | Handler success | Handler error | PG exact | Cassandra exact | Failure mode |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | --- |
| 45 msg/sec | 6750 | 6750 | 6750 | 0 | 6750 | 0 | 6750 | 0 | 6750 | 0 | 6750 | 6750 | 6750 | 6750 | 0 | 6750 | 6750 | no failure detected |
| 60 msg/sec | 9000 | 9000 | 9000 | 0 | 9000 | 0 | 9000 | 0 | 9000 | 0 | 9000 | 9000 | 9000 | 9000 | 0 | 9000 | 9000 | no failure detected |

## Artifacts

- `45 msg/sec`: `benchmarks/results/2026-06-08-processor-stage-instrumentation/45msgsec`
- `60 msg/sec`: `benchmarks/results/2026-06-08-processor-stage-instrumentation/60msgsec`
- Combined summary JSON: `benchmarks/results/2026-06-08-processor-stage-instrumentation/summary.json`

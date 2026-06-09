# Processor scaling matrix - 2026-06-09

## Outcome

- Question answer: Yes. Adding processors made `120 msg/sec` pass strict full-pipeline reconciliation.
- Hypothesis result: `validated`
- Supported claim: Validated 120 msg/sec local Kubernetes ingress with exact full-pipeline reconciliation after scaling the messages-processor consumer group across Kafka partitions.
- Evidence boundary: With 4 `messages-processor` replicas, OHMF sustained 120 msg/sec in the local Kubernetes full-pipeline profile: `74,700/74,700` accepted, `74,700` Postgres rows, `74,700` Cassandra rows, and Kafka lag returned to zero in `103.145s`.

## Matrix

- `1 replicas @ 105 msg/sec`: status=`passed`, requested/accepted=`15075`/`15075`, consumed=`15075`, pg_ok=`15075`, cass_ok=`15075`, commits=`15075`, handler_errors=`0`, pg_exact=`15075`, cass_exact=`15075`, lag_zero=`410.278s`, lag_end=`0`, assigned_members=`1/1`
- `1 replicas @ 120 msg/sec`: status=`failed`, requested/accepted=`74700`/`74700`, consumed=`38990`, pg_ok=`38990`, cass_ok=`38990`, commits=`38989`, handler_errors=`0`, pg_exact=`74700`, cass_exact=`74700`, lag_zero=`602.316s`, lag_end=`37742`, assigned_members=`1/1`
- `2 replicas @ 105 msg/sec`: status=`failed`, requested/accepted=`15075`/`15075`, consumed=`12651`, pg_ok=`12650`, cass_ok=`12650`, commits=`12649`, handler_errors=`0`, pg_exact=`0`, cass_exact=`0`, lag_zero=`2.231s`, lag_end=`0`, assigned_members=`2/2`
- `2 replicas @ 120 msg/sec`: status=`failed`, requested/accepted=`74700`/`74700`, consumed=`70702`, pg_ok=`70701`, cass_ok=`70701`, commits=`70700`, handler_errors=`0`, pg_exact=`74700`, cass_exact=`74700`, lag_zero=`600.481s`, lag_end=`7897`, assigned_members=`2/2`
- `4 replicas @ 105 msg/sec`: status=`passed`, requested/accepted=`15075`/`15075`, consumed=`15075`, pg_ok=`15075`, cass_ok=`15075`, commits=`15075`, handler_errors=`0`, pg_exact=`15075`, cass_exact=`15075`, lag_zero=`2.021s`, lag_end=`0`, assigned_members=`4/4`
- `4 replicas @ 120 msg/sec`: status=`passed`, requested/accepted=`74700`/`74700`, consumed=`74700`, pg_ok=`74700`, cass_ok=`74700`, commits=`74700`, handler_errors=`0`, pg_exact=`74700`, cass_exact=`74700`, lag_zero=`103.145s`, lag_end=`0`, assigned_members=`4/4`

Note: the standalone 2-replica 105 msg/sec rerun passed, but the strict six-cell matrix rollup marks the cell failed because the original rung required recovery after runner interruption and did not satisfy the matrix's exact evidence rules. This does not affect the 120 msg/sec conclusion, which depends on the clean 4-replica 120 msg/sec passing cell.

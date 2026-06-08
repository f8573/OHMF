# Stage B1 throughput rerun - 2026-06-08

## Outcome

- Highest passing sustained rate: `105`
- `120 msg/sec` passed: `False`
- Supported claim: Validated 105 msg/sec through the local Kubernetes full pipeline with exact reconciliation across Kafka consumption, Postgres persistence, Cassandra persistence, downstream publishes, and offset commits.

## Rollout discipline

- Unique image rollout verified: `True`
- `stage_events_total` present before ladder: `True`

## Rungs

- `75 msg/sec`: status=`passed`, requested/sent/accepted=`11025`/`11025`/`11025`, consumed=`11025`, pg_ok=`11025`, cass_ok=`11025`, commits=`11025`, handler_errors=`0`, pg_exact=`11025`, cass_exact=`11025`, lag_zero=`122.472s`, restarts=`0`
- `90 msg/sec`: status=`passed`, requested/sent/accepted=`13050`/`13050`/`13050`, consumed=`13050`, pg_ok=`13050`, cass_ok=`13050`, commits=`13050`, handler_errors=`0`, pg_exact=`13050`, cass_exact=`13050`, lag_zero=`213.191s`, restarts=`0`
- `105 msg/sec`: status=`passed`, requested/sent/accepted=`15075`/`15075`/`15075`, consumed=`15075`, pg_ok=`15075`, cass_ok=`15075`, commits=`15075`, handler_errors=`0`, pg_exact=`15075`, cass_exact=`15075`, lag_zero=`303.511s`, restarts=`0`
- `120 msg/sec`: status=`failed`, requested/sent/accepted=`74700`/`74700`/`74685`, consumed=`46568`, pg_ok=`46568`, cass_ok=`46568`, commits=`46567`, handler_errors=`0`, pg_exact=`50881`, cass_exact=`51755`, lag_zero=`903.001s`, restarts=`0`

A prior 60 msg/sec under-reconciliation result was traced to stale container image deployment; subsequent unique-tag rollout with processor stage instrumentation reconciled exactly.

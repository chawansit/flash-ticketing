# ADR0282: Bound hourly terminal relationship audit work

## Status

Accepted for independent recovery of failed ADR0281; cloud validation executed. No capacity qualification or application change.

## Context

The hourly run dispatched 300,228 of 302,400 journeys and missed its in-hour ticket gate. Its immutable result remains failed. Original topology restoration was interrupted after candidate restoration failed; independent recovery restarted the exact saved topology. The canonical full-cohort financial cardinality query completed, but the relationship query over 1,008 shows timed out at 60 seconds. Repeating that monolithic query does not establish correctness.

## Decision

Retain the original counts, post-TTL, payment, booking, ticket, inventory, customer ownership and global duplicate checks. Evaluate the same full-cohort relationship predicates within one repeatable-read, read-only transaction, with the cohort and scoped-booking CTEs marked NOT MATERIALIZED so PostgreSQL can push join predicates into indexed base relations. Execute the seven unchanged relationship expressions as separate bounded statements inside that same snapshot; keep all seven relationship counters and reconcile the actual dispatched cohort, separately from the unchanged scheduled-load gate. Only statement decomposition and the two CTE planning hints differ; no relationship predicate is removed. Keep three-minute per-statement and ten-minute whole-operation bounds; reduce each statement timeout to the remaining whole-operation budget. Capture the current predicate index and safe error type, so failures can be attributed instead of replayed blindly. The sixty-second full-predicate limit also proved insufficient after decomposition against the existing large inventory. Bound each statement and the independent recovery operation. No payment, ticket or booking row is modified.

## Alternatives

- Increase the full-query timeout indefinitely: unbounded audit work and uncertain completion.
- Skip relationships or accept counts alone: cannot establish payment and inventory consistency.
- Split the whole relationship query into show batches: repeats global inventory/hold scans and exceeded the recovery timeout.
- Change application indexes or persistence while auditing: confounds the measured run and is unnecessary for recovery.

## Consequences

The audit still examines every fixture show and every relationship predicate. Explicit CTE planning hints avoid scanning unindexed materialized intermediate relations in repeated correlated lookups. This is recovery tooling, not evidence of increased service capacity. Cross-show inconsistencies remain failures through the unchanged scoped booking and inventory joins; the global duplicate audit remains independent.

## Failure and recovery behavior

Keep the original report, customer failures, drops, timeouts and gates immutable. Unknown coverage, any relationship error, missing payment, duplicate seat or incomplete queue prevents recovery completion. Remove only owner-bound resources and credentials after independent verification. A recovered failed run remains FAILED_RESTORED, never passed.

## Supersession

Supersedes the single full-cohort relationship execution only for ADR0281 independent recovery. Does not supersede financial guarantees or original qualification gates.

## Validation evidence

A twelve-show batching attempt exceeded its 225-second process timeout and produced no accepted financial receipt. A read-only EXPLAIN identified a 2,781,893-row inventory sequential scan and a costly holds scan repeated per batch; batching was rejected. The old exact audit-process hash was checked and no matching process remained. The decision was revised before executing the planning-hint variant. The full-cohort planning-hint variant also timed out at sixty seconds. EXPLAIN showed a plan over an approximately 29.7-million-row inventory and large merge/nested joins; disabling nested loops increased estimated cost and was rejected. Separate the seven predicates within the same snapshot before the next execution. Executed full-cohort cardinality and seven unchanged predicates in one read-only, repeatable-read snapshot: 300,228 unique paid tickets, zero errors across every relationship counter, zero duplicate booking and zero pending financial work. Both safety fixtures and global queues passed. Runtime restored/stable, namespace/helpers absent, generator idle, secondary empty, private inputs removed and 1,010 fixture events retired. Final independent recovery completed in 152.59 seconds. Original failed load report preserved; ledger remains FAILED_RESTORED.

[Terminal recovery receipt](../capacity/cce/status-reuse-hourly-terminal-recovery-2026-10-11.json) records the exact generated audit program SHA-256. [Hourly result](../capacity/cce/status-reuse-hourly-result-2026-10-11.json) retains drops, errors, failed diagnostics and in-hour counts. No new customer load is authorized by this ADR.

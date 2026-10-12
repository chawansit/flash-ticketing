# ADR0288: Collect statistics before variable cohort work

## Status

Accepted for newly source-bound ADR0285 verification scopes. Supersedes only the cohort-before-statistics ordering within ADR0285 for those new scopes. Legacy non-independent observation keeps its existing order. Historical experiments remain unchanged, including the failed full qualification in ADR0287.

## Context

The latest hour verified 302,290 unique paid-and-issued tickets within the hour and passed customer, financial, queue and restoration gates. Full qualification failed on one 2.318296-second statistics gap. That combined sample took 1,508.283 ms; statistics collection itself took 33.252 ms. Adjacent cohort work varied substantially.

An executed local reproduction uses the existing sampler, a .19-second cohort call followed by a 1.51-second call, and the unchanged two-second statistics gate. Collection has no errors but the gate fails: collecting statistics after the variable work shifts successive statistics timestamps by over two seconds.

Inspection corrects an earlier assumption: there is one PostgreSQL diagnostic connection shared by cohort and statistics queries. The other observer connection is a PgBouncer administration connection, not an available PostgreSQL statistics connection. Fully independent concurrent SQL streams would require another connection or removal of an existing observation. Neither is needed for this isolated correction.

## Decision

For fresh sealed independent-sampling scopes, collect the existing statistics queries at the start of each cycle, before cohort work. Preserve the sampler's start-to-start scheduling, existing single diagnostic PostgreSQL connection, exclusive thread ownership, PgBouncer observation and all query predicates, timeouts, workload, application image and connection budgets.

Seal the ADR0288 ordering and helper source hashes in the verification inventory before execution. Reject missing or incompatible ordering markers. The old non-independent path remains cohort-first.

Retain the completed statistics record if subsequent cohort work fails; record the cohort exception type and incomplete cohort explicitly, without synthesizing ticket counts or marking the pipeline healthy. Preserve rollback failures as types. No reconnect, hidden retry or fallback credentials.

Retain the two-second statistics coverage gate, collection-overhead gate, counter/reset checks, ten-second cohort freshness and full financial audits. Add bounded timestamp-gap and counter-discontinuity counts to reports so subsequent failures identify their category.

This is a cooperative scheduling correction, not full isolation. A cohort query blocking this one connection for over two seconds can still fail the statistics gate. Do not claim arbitrary slow queries are harmless. Any such measured recurrence requires a separately recorded decision, not relaxed gates.

## Alternatives

- Add a dedicated PostgreSQL statistics connection: stronger isolation, but changes the controlled observation connection budget.
- Remove PgBouncer observation and reuse its budget: loses required pool evidence and cannot directly reuse its administration connection.
- Shorten cohort query timeout or skip failed slices: changes workload/coverage and may discard legitimate cohort evidence.
- Relax the two-second gate or regrade the previous hour: rejected.
- Rebuild the runner or application image: unnecessary for this observer ordering correction.

## Consequences

Variable cohort execution no longer shifts statistics timestamps to the end of otherwise one-second cycles. SQL work and connection count remain unchanged. Slow work exceeding the cadence budget still fails honestly. Payment handling and ticket throughput are unchanged; no application capacity improvement is claimed.

## Failure and recovery behavior

Each connection remains owned by the existing bounded sampler and is cancelled/joined before close. Statistics errors, stale/incomplete cohort counts, timestamp gaps, counter resets and file failures retain their existing failure semantics. Newly retained statistics alongside a failed cohort cannot qualify the customer or pipeline gates. Failed cloud scopes remain consumed; only a fresh registered scope can run after local verification.

## Validation evidence

Executed before implementation: delayed-cohort reproduction passed (one test), demonstrating an actual greater-than-two-second gap with no statistics collection errors.

Candidate validation will replay the same timing variation and check the unchanged gate, retained cohort errors, absence of synthetic counts, budget/source binding and cleanup. A deliberately longer stall must still fail. Cloud rerun has not started; production qualification and maximum capacity remain unclaimed.

Executed candidate verification: 198 targeted cadence, connection ownership, verification, metric capture, entry, hourly, profile, retention and legacy pipeline tests passed. The recorded identical-delay replay measured a 2.317436-second gap with cohort-first ordering and 1.510625 seconds with statistics-first ordering; only the candidate passed the unchanged two-second gate. A deliberately longer stall still failed; cohort errors and stale counts remained explicit. See [local replay](../capacity/cce/statistics-order-local-replay-2026-10-12.json). No new cloud capacity result exists yet.

Under the standing Continue authorization, execute one fresh 84/s five-minute validation with unchanged runtime/workload and mandatory restoration. Only if every gate and restoration passes may a separately registered fresh 84/s hour follow, within the previously approved 5,400-second experiment ceiling. Do not increase load or automatically replay a spent scope.

Executed fresh cloud short validation adr0151-9e72366d7ec8: PASSED_RESTORED, all ten measurement gates passed. All25,200 scheduled journeys dispatched and completed paid-and-issued; zero first-attempt errors, retries, final failures, double-booking or payment loss. Statistics capture covered the offered window with354 samples, no collection errors/discontinuities, maximum interval1.017462s and maximum collection37.836ms. Payment HTTP p9586.257ms; total journey p952663.116ms (worst shard). All queues drained, post-TTL financial checks passed, native resources and transport credentials were removed, and exact normal runtime restoration/integrity passed. See [short result](../capacity/cce/statistics-order-paid-result-2026-10-12.json). This qualifies the short diagnostic correction, not maximum production or flash-opening capacity.

The subsequent fresh hour adr0151-d301c0c07d23 was FAILED_RESTORED before paid dispatch: stale transition inventory stopped the pipeline observer, not the statistics sampler. Zero paid load runs started. Retained startup evidence and exact restoration passed. See [aborted hourly evidence](../capacity/cce/statistics-order-hourly-abort-2026-10-12.json) and [ADR0289](0289-prepare-fixtures-before-fresh-admission.md). The short remains passed; no additional hourly capacity result or upgraded historical qualification is claimed.

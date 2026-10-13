# ADR0300: Compare cohort observer query cost

## Status

Accepted for a bounded read-only query comparison; no paid-run activation.

## Context

ADR0299 exposed existing query counters without a restart or resetting them. Historical paid-cohort monitoring queries rank among the largest consumers of cumulative execution time (two role-specific entries of the current join query average about 657 and 461 ms). The complete cohort counting query runs repeatedly during load. Cumulative values do not prove the latest four payment errors' cause.

ADR0247 already supplies an alternative observer query using cohort ID arrays. It has not been activated in the frozen paid runner. Expected expiry, dispatch and outbox indexes are present. Repeating load without measuring the monitoring query's overhead would leave an avoidable attribution gap.

## Decision

Read the retained ADR0298 candidate fixture identity and compare the exact current observer query with the existing ADR0247 alternative against its completed 25,200-order cohort on unchanged RDS. Use one protected, verified-TLS diagnostic connection while the generator is idle and no owned experiment is active. Both reads use one statement snapshot, read-only transactions and a two-second statement deadline. No financial rows, indexes, settings or fixture ownership records change.

Execute two alternating-order rounds per query, recording the nine exact counters, latency, EXPLAIN ANALYZE with buffers, observed relation scans, fixture digest and statistics epoch. Equality is mandatory. Preserve timeouts/errors explicitly. No adoption is permitted merely from historical average costs or planner estimates.

If one query reduces observed cost materially, prepare its existing-runner integration as a separate decision before paid testing. Keep post-TTL, independent financial audits, diagnostic sampling frequency and paid profile quality gates unchanged.

## Alternatives

Delete old data, disable diagnostics, change sampling frequency, add connections or resize RDS: rejected for this comparison. Directly activate the alternative before checking snapshot/counter equivalence: rejected.

## Consequences

This measures idle monitoring-query cost on a completed short cohort. It cannot prove performance under contention, hourly-cohort behavior or increased paid-ticket capacity. Warm-cache order affects results, so alternate query order and retain each measurement. Runtime application images and transaction design remain unchanged.

## Failure and recovery behavior

Reject missing or mismatched fixture evidence, active load, endpoint/certificate mismatch or unequal counters. Each read-only transaction rolls back on error; close its connection and clear credentials. Preserve private raw plans and public aggregate evidence. Never retry timed-out SQL as successful or reset cumulative statistics. No application restoration is needed because no deployed component changes.

## Validation evidence

Executed read-only comparison below. Historical statistics are cumulative since 2026-09-13 and shared across prior tests; they are used only to select the query to measure.

Executed two alternating-order rounds on the retained 25,200-ticket cohort. All nine counters matched exactly, including 25,200 fulfilled orders, delivered callbacks and issued tickets, with zero pending queues. EXPLAIN execution means were 228.556 versus 203.464 ms (10.98% reduction); buffer hits were 203,204 versus 142,578 (29.84% reduction), with zero physical reads in these plans. Evidence: [cohort observer comparison](../capacity/cce/cohort-observer-query-comparison-2026-10-13.json). A first draft attempt incorrectly used dictionary rows for duplicate unnamed count columns; excluded and rerun with all nine positional counters. No deployment, paid-load stage or capacity increase claimed.

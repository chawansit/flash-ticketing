# ADR 0045: Bounded API pool wait through intermittent RDS commit stalls

Status: Proposed for controlled validation; ADR 0037 and ADR 0039 remain in force

## Context

The 20 September 2026 post-keep-alive 750-RPS safety stage completed 450,000 no-retry requests with one hold `DATABASE_UNAVAILABLE`. The API logged `PoolTimeout` after 152 ms at 10:22:40 UTC. All four APIs had commit stalls in the same interval, with maxima 369–419 ms; the RDS observer sampled WALWrite and WalSync waits. There were zero generator drops, read errors, admission rejections, double bookings, broken links or undrained queues. The exact cause of the RDS WAL stall is unresolved. Increasing the database connection count could increase simultaneous stalled commits. A 150-ms local checkout deadline guarantees an error when all three per-replica connections remain occupied for a roughly 400-ms stall.

## Decision

Expose the application database-pool checkout timeout as a validated setting with the current 150-ms default. In the opt-in Huawei benchmark overlay only, test a 500-ms timeout for API replicas. Keep three application DB connections and five admitted holds per replica, PgBouncer backend pool 24, workload and Nginx retries disabled, and all transaction/lock/statement timeouts unchanged. This is bounded queuing for a measured transient, not a WAL fix. Count pool waits and hold tail latency as independent gates. Start with a fresh-fixture no-retry 750-RPS 10-minute stage. A 30-minute confirmation and then 800 RPS remain blocked unless the preceding stage passes every strict gate.

## Alternatives considered

- Keep 150 ms: preserves a fast failure, but the measured 400-ms commit stall makes the strict availability target unattainable in that interval.
- Raise the per-replica DB pool from three to four: may add WAL concurrency and alters the fixed connection budget, so defer until isolation tests show benefit.
- Retry `DATABASE_UNAVAILABLE`: rejected because it hides the failed attempt and changes offered load.
- Increase admission above five: rejected because more requests can accumulate behind stalled commits.
- Change RDS WAL parameters or storage again: defer until provider-level evidence identifies a causal setting; Extreme SSD did not eliminate the stalls.

## Consequences

A request may wait up to 500 ms for a local connection and can therefore have a longer tail latency during WAL stalls. The pool and backend connection budgets do not increase. Ordinary deployments retain the 150-ms default. A future sustained stall can still exhaust the bounded wait and fail the strict gate; this change is not permission to mask errors or declare production capacity from one successful sample.

## Failure and recovery behavior

Reject invalid timeout settings during startup. Fail preflight if rendered/live API settings or readiness differ from the candidate. Any unexpected response, transport error, generator drop, latency breach, durability mismatch, overlapping hold, or undrained queue fails the stage and blocks promotion. Restore the 150-ms benchmark setting if the candidate fails; retain evidence for diagnosis. No retry is introduced and a failed hold remains failed unless the client makes a new, independent request.

## Validation evidence

At decision time the 750-RPS stage failed strict availability on one 152-ms `PoolTimeout` while RDS WAL-path commit stalls reached 419 ms. The post-TTL audit confirmed all 22,499 acknowledged holds, zero overlaps and drained queues. Rendered configuration, unit tests, live deployment and candidate load results are pending and must be recorded separately after execution.

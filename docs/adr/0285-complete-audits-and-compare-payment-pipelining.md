# ADR0285: Complete bounded audits and compare payment pipelining

## Status

Accepted for implementation, verification and two bounded five-minute comparisons under the user's standing envelope. The user authorized steps 1 through 5 on 12 October 2026. The hourly qualification is outside this authorization. Historical failures stay failed.

## Context

ADR0283 exceeded the hourly ticket target but failed its canonical terminal audit and one database-statistics continuity interval of 2.012074 seconds. The same financial predicates passed the independent ADR0282 audit. Database statistics currently share the broader loop with metric collection. ADR0284 implements a default-off payment pipeline and verifies a candidate derived from the exact measured image.

## Decision

Integrate the existing ADR0282 relationship audit recipe: one read-only repeatable-read snapshot, unchanged cardinality and seven relationship predicates, NOT MATERIALIZED cohort/scoped-booking CTEs, and sequential predicate statements. Keep a bounded total deadline and retain every failed predicate. Do not weaken correctness or change application data.

Move cohort SQL and database statistics onto one bounded background sampling loop, owning the existing diagnostic data connection exclusively. The metrics loop receives explicitly aged snapshots. Save the independent SQL/diagnostics stream separately and qualify its original statistics continuity, overhead, counter-reset and visibility gates. Use one diagnostic data connection and the existing pooler administration connection; no additional application or diagnostic connection budget. Join sampling before closing the connection, and retain SQL failures and stale snapshots.

Publish and verify the ADR0284 candidate by immutable registry manifest and configuration digests. Use the same shared candidate image in both comparison arms, with PAYMENT_WRITE_PIPELINE=0 for control and 1 for candidate APIs; keep it 0 on background roles. Preserve the measured image's payment queries and all worker behavior, resources, gateway delays, recovery and connection budgets. Seal source/configuration and observation contracts before starting each fresh experiment.

Native worker CPU and progress summaries must derive projection membership from the validated admitted roles, rather than a historical ADR label. All admitted workers, continuity and positive-progress gates remain mandatory. A verification-only failure keeps its original result failed; independent terminal recovery closes ownership and finances, then a fresh control is required.

Run control first at 84 offered journeys/s for 300 seconds. Candidate starts only after all control gates, financial checks, queue drain and exact restoration pass. Compare initial errors, recoveries, final errors, undispatched journeys, unique paid tickets, total latency, database slot occupancy, waits and commit time. An off/on comparison qualifies this isolated factor; it does not establish maximum capacity or eliminate the cause of unobserved commit stalls.

## Alternatives

- Increase diagnostic or application connections: changes the controlled budget.
- Relax the two-second statistics gap gate: hides missing evidence.
- Retry the failed monolithic audit or increase its timeout alone: preserves expensive shared intermediate plans.
- Include the repository-only payment query optimization: confounds the pipeline comparison.
- Increase offered load before control passes: combines unresolved correctness/evidence failures with additional demand.

## Consequences

Independent sampling removes metric-fetch delays from SQL/statistics scheduling while keeping the same connection count. Snapshots in the metrics stream are explicitly asynchronous and may repeat; the independent diagnostic stream is the authority for continuity. SQL collection that itself exceeds the bound still fails. Local tests and read-only cloud checks precede paid traffic. Pipeline performance may be neutral or insufficient; retain and report that outcome.

## Failure and recovery behavior

Stop progression when control or candidate gates fail. Preserve both raw streams and failure-time slot evidence. Complete financial/duplicate/post-TTL checks, queue drain and exact restoration even after customer or observer failures. Bound shutdown and cancel stuck sampling; never silently replace missing observations with zero. Financial predicate timeouts preserve the failed index and original exception class. Rollback is the original image/configuration plus default-off pipeline, verified by exact runtime identity. Reuse existing customer idempotency/recovery without changing retry budgets.

## Validation evidence

Implemented the bounded audit, exclusively owned independent SQL sampler, separate diagnostic transport and explicit same-image comparison binding. Executed PostgreSQL clean/corrupt audits and pinned observer tests (6 passed), Linux SIGTERM cleanup, pipeline drift/admission checks (15 passed), and the wider runner suite (294 passed, one skipped, two initial failures). Corrected the short-test fixture and resealed the explicit current orchestration overlay; reruns passed (63 transaction-profile checks and 18 verification/resource checks). Lint passed.

The integrated read-only audit passed the existing ADR0283 cohort: 302,400 unique paid-and-issued tickets, all relationship checks, hold expiry and empty queues in 91.422 seconds, with unchanged runtime. This is recovered financial evidence, not a new paid run or a retrospective qualification pass. See [verification evidence](../capacity/cce/complete-verification-result-2026-10-12.json).

Published, pulled by immutable digest and verified all 22 imported source modules and bytecode in the ADR0284 image. See [publication receipt](../capacity/cce/payment-write-pipeline-image-2026-10-12.json). The fresh short control/candidate comparison is complete below; no capacity improvement is claimed.

The initial short control (`adr0151-b5eb09e537ce`) completed all 25,200 journeys and issued 25,200 unique paid tickets with zero drops, initial errors and final errors. Financial, independent database statistics, API CPU and projection ownership checks passed. It remains failed because the worker CPU summarizer omitted projection membership for the new ADR0285 identity. Independent recovery verified financial relationships, both safety cohorts, empty queues and exact restoration in 77.141 seconds. The worker summary now derives membership from admitted roles (six regression checks passed); admission proofs are also retained locally through cleanup. A fresh control remains required.

The fresh control (`adr0151-84a625ee08fa`) passed all ten measurement gates and exact restoration: 25,200 scheduled, dispatched, customer-confirmed and unique paid-issued tickets; zero generator drops, first-attempt errors, final customer failures, duplicate bookings and payment loss. Independent diagnostics cover the entire offered window without errors. Payment HTTP p95 was 63.941ms, total journey p95 3,189.197ms; 25,081 tickets were issued inside the 300-second window. The same-image pipeline-on candidate subsequently completed and restored. See [fresh control](../capacity/cce/payment-pipeline-control-result-2026-10-12.json).

The pipeline-on candidate (`adr0151-157d143e0e4e`) also passed all ten gates and restoration: 25,200 terminal tickets and zero drops, first-attempt errors, retries, final failures, duplicate bookings and payment loss. Both diagnostic streams cover the offered window with no collection errors or slot-failure records. Payment HTTP p95 changed from 63.941ms to 60.318ms (-5.67%); maximum-shard journey p95 changed from 3,189.197ms to 2,558.627ms (-19.77%). However, API database connection-hold mean rose from 23.214ms to 24.056ms (+3.63%), commit mean from 3.230ms to 3.548ms (+9.84%), and worker CPU from 1.340 to 1.485 cores (+10.82%). Inside-window tickets declined from 25,081 to 25,030. These all-operation means and one sequential pair do not isolate every source of variation.

Decision after comparison: do not adopt pipeline-on as the baseline; retain default off. The mixed result does not demonstrate increased ticket throughput or reduced database occupancy. Audits and independent diagnostics are qualified for these fresh short runs; the intermittent commit-stall cause and hourly qualification remain unresolved. Terminal ticket totals include completions after the offered window, so neither five-minute result certifies 84 successful tickets/s sustained for an hour. See the [controlled comparison](../capacity/cce/payment-pipeline-comparison-2026-10-12.json). All temporary cloud test resources and credentials were cleaned up, with exact original topology restored.

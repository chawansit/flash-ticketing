# ADR0142: Pin the passing baseline before diagnosing the cloud regression

- Status: Executed; baseline reproduction failed qualification; no capacity promotion
- Date: 2026-10-04

## Context

ADR0141 failed at60buyers/s300s with host CPU99.397%, API acquisition mean81.083ms and late callback refunds. Backend runtime differences were limited to cache.py, but fixture/generator/orchestration scripts also differed from the historical passing deb330e revision. Distributed paths appear to retain the same settings; this is not proof of identical test behavior. All cloud source/images/settings were restored to deb330e and payments accounted for by tickets or simulated refunds; queues/Kafka drained. Initiating cause remains unresolved.

## Decision

Reproduce exactly one historical ADR0133 control on deb330e using baseline-revision fixture, generator, journey, observer and orchestration scripts. Export pinned scripts into an ignored local directory without changing the shared working tree. Verify all normalized Git hashes and actual upload bytes before dispatch. Keep original Windows Python CRLF upload convention; record both raw and normalized hashes. All backend/generator checkouts must remain clean deb330e and API/worker runtime hashes must match it. No candidate source sync.

Use the same60buyers/s300s/60shows/300seats/18000buyers, two generator shards/eight clients each,500journey concurrency,1s polling and zero customer retry. Keep four API instances/pool4/shared acquisition12/payment2/cache0, six consumers/pool8, three writers/batch4, simulator refill8 and PgBouncer24. Reuse ADR0040/0090 and ADR0141 bounded lifecycle guards; keep20 existing gates plus baseline deployment and pinned harness identity. A missing gate fails. No additional stage follows.

## Alternatives and consequences

Retrying the optimized candidate or enabling caching now changes the factor and cannot distinguish runtime regression from current conditions. Comparing only HTTP RPS omits customer tickets and late refunds. The five-minute reproduction can diagnose a discrepancy; it does not prove sustained capacity or300000paid-issued/hour. Fresh data and cloud scheduling remain uncontrolled variation and must be recorded.

## Persistence, locking, messaging, idempotency, TTL and scaling

No application/runtime, authority, lock, transaction, idempotency, Kafka, hold TTL, host size, connection-budget or cache-pattern choice changes. Existing accepted decisions remain applicable; none are superseded. This ADR records diagnostic orchestration scope only.

## Failure and recovery

Use the previously authorized freshly generated restricted45minute root key on verified known hosts; compensate partial installation and remove exactly owned entries and local files afterward. Keep passwords memory-only. Stop on failed gates; collect postTTL financial/uniqueness/global queue/Kafka evidence and restore normal service configuration/readiness, generator idle and private scratch. Preserve failed evidence and later recovery separately. Cloud checkouts and all service images remain deb330e. Never overwrite local qualified candidate/tests. No automatic extra load or infrastructure changes.

## Validation evidence

Recorded before this reproduction's key installation or load. Require pinned source/syntax/CLI guards and fresh source/settings/global queue/generator preflight. Then record all executed customer/financial/queue/source/CPU/cleanup gates and evidence. If baseline fails too, investigate current conditions/CPU/callback occupancy; if it passes, isolate the projection candidate with the same pinned harness before choosing a fix. Architectural fixes require their own ADR and validation.

[Exact profile and source manifest](../capacity/flash-sale-opening/pinned-baseline-reproduction-plan-2026-10-04.json).

Preparation executed:78 baseline script files and11 runtime-module hashes verified; helper/CLI syntax and fixed budgets checked; seven offline synthetic reporting guard cases passed. First preflight stopped before load because the host resource helper incorrectly assumed Docker on the native generator. Both cloud hosts remained baseline; normal source/settings/idle/scratch recovery and exact key removal passed. The read-only diagnosis confirmed API4CPUs, no API container quota,13.78% idle sample and no sampled steal ticks; generator Docker absence is expected. Guard repaired while preserving the no-load attempt evidence; runs_started remains0.

Second preparation attempt also dispatched no customer load: the pinned runner's local output parent was missing. The parent was created and the pinned import/CLI check passed. Baseline recovery and exact key removal passed again. These are two retained preparation failures, not additional load experiments.

## Executed reproduction and interpretation

Run `checkout-20261004T153639Z-c4a192` executed the single authorized 60 buyers/s, 300-second control. The 78 prepared baseline scripts, uploaded harness bytes and 11 runtime-module hashes matched deb330e. Configuration differences from the historical control were empty. Nineteen of 22 gates passed; customer completion, post-TTL financial cohort equality and the inherited compound `hold_deadlines_elapsed` gate failed. The actual deadlines elapsed: the compound gate failed because payment/ticket cohort equality failed.

Of 18,000 scheduled journeys, 17,958 dispatched and 42 dropped. Only 1,095 confirmed a ticket by the client deadline; 16,863 journeys failed (93.902% of dispatched journeys, not an HTTP error percentage). Post-TTL evidence showed 16,483 successful simulated payments and 13,204 unique tickets, with zero duplicate booked seats or multi-booking orders. All callbacks completed, global queues and Kafka lag were zero. The existing audit does not classify the remaining 3,279 successful payments by refund state. A separate read-only post-restoration accounting snapshot was attempted twice, but SSH password authentication failed; their refund accounting remains unverified for this run. Do not infer missing money or claim refunds completed from the prior run's result.

During the generator-anchored offered sample, host CPU averaged 99.192%, versus 81.358% historically; API CPU rose from 1.295 to 1.960 cores. API pool acquisition averaged 74.942 ms versus 2.881 ms. Status checks rose from 36,212 to 118,580, or approximately 2.01 to 6.60 per dispatched journey. Callback backlog reached 6,816 at the sampled offered-window end; post-TTL payment-due-to-callback p95 was 124.340 seconds. Callback-to-ticket p95 for issued tickets was 263.261 ms. These measurements locate substantial delay before callback completion and show read amplification under saturation. They do not establish which factor initiated the regression. The separate 240-second whole CPU sample averaged 96.489%; do not mix the two windows.

The simulator's offered-window mean successful delivery was 203.996 ms, claim 18.705 ms and acknowledgment 15.730 ms. Eight slots divided by their approximately 238 ms combined mean gives a rough 33.6 deliveries/s service-time estimate before errors and other overhead. This is a diagnostic approximation, not measured production capacity. Writer connection holding increased from 87.140 to 147.395 ms; writer commit mean was 4.702 versus 4.423 ms. There is no evidence here establishing WAL or RDS storage as the initiating cause.

Both servers were restored to baseline source/images and normal service settings. Readiness, generator idle and private scratch cleanup passed. Exact temporary public-key entries were removed from both servers, and local key files were deleted. No second load, source optimization, cache activation, infrastructure change, GitHub push or main merge followed.

Baseline failure means the projection candidate alone cannot explain this regression; it does not prove the candidate harmless. Keep it unqualified. Before another paid-stage qualification, obtain the missing read-only refund accounting. Next engineering work should isolate API CPU/status-read amplification and callback occupancy, then select and record one mitigation in a new ADR. Preserve the total connection budget, payment authority, hold TTL, uniqueness and full financial/queue gates.

[Executed report](../capacity/flash-sale-opening/pinned-baseline-reproduction-2026-10-04.json). Raw local evidence is retained under the run directory in ignored `tmp/`; the report contains compact summaries only.

### Follow-up recovery verification

On resume, password-only read-only access succeeded at 2026-10-04T15:54:48Z. The fresh fixture snapshot verified 13,204 FULFILLED orders, 3,279 REFUNDED orders/refund requests, 1,475 EXPIRED unpaid orders and zero REFUND_PENDING. All 16,483 successful simulated payments are accounted as 13,204 fulfilled plus 3,279 refunded. Global queues and Kafka lag remained zero. Owned follow-up fixture/queue scratch was removed in finally. No temporary SSH key was reinstalled, runtime changed or new load dispatched. This resolves the accounting uncertainty retained above; it does not retroactively pass the failed original control or validate a real payment/refund provider.

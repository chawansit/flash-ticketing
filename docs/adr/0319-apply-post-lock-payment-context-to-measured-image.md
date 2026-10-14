# ADR0319: Apply post-lock payment context to the measured shared image

## Status

Accepted for isolated implementation and local validation under the standing work envelope. Cloud comparison and capacity improvement are not yet measured. Extends ADR0248/ADR0249 onto the ADR0298 shared image; does not adopt ADR0317/ADR0318 partitions or the rejected read/payment pipeline candidates.

## Context

The shared-pool ADR0315 reference issued 30,370 tickets within five minutes, but 71 scheduled journeys were undispatched and 39 initial errors recovered. Failure captures frequently observed payment transactions in commit; the exact underlying IO stall remains unconfirmed. Both static backend partitions reduced throughput or customer quality and are rejected.

Inspection of the exact immutable ADR0298 image on 14 October confirms payment initiation still separately reads existing payment, hold and database clock after locking the authorized order. ADR0248 already implemented and locally tested a combined post-lock context lookup, but that change never entered this measured image. Its earlier read-only RDS profiling measured lookup means of 4.1415 versus 1.3683 ms, excluding checkout, locks, writes and commit. This is a bounded correction hypothesis, not proof that it removes commit stalls.

## Decision

Transplant only the historically reviewed ADR0248 payment method into the complete hash-verified ADR0298 source map. Match the parent method to the historical control AST before replacement; reject any unrelated module or method change. Preserve explicit BEGIN, default-off pipelining, authorized order lock and a fresh subsequent READ COMMITTED context query. Retain all committed-status, atomic hold, writer, messaging and recovery behavior. Build from the parent's immutable digest with unchanged dependency inputs and verify copied, installed and imported source identities.

For a future comparison retain the shared 24 backend pool, four APIs with 2+2 client slots, four reservation writers/batch four, all other worker counts/resources, gateway, recovery and 102 offered journeys/s for 300 seconds. Use a fresh experiment identity and compare against the retained shared-pool reference; do not reopen consumed failed scopes. First-attempt errors, recovered journeys, final customer failures, generator drops and inside-window unique tickets remain separate. Hourly qualification needs a passing short experiment.

Also retain per-endpoint metrics error class and bounded HTTP status without response bodies, URLs or exception messages. Keep successfully sampled endpoints in a failed sample, but fail completeness whenever any endpoint is missing. Do not retry collection, increase the sampling budget, infer zero or discard missing samples. Report endpoint absence distinctly before offered-window distribution validation. These diagnostic changes do not alter application traffic or gate limits.

## Alternatives

Retest static backend splits: both failed. Enable status/payment pipelining: earlier comparisons did not establish a throughput benefit. Increase connection or retry budgets: confounds this comparison and can hide overload. Combine context with the locking SELECT: risks stale visibility. Change durability or asynchronous payment patterns: outside this isolated correction. Claim the post-lock lookup fixes WAL: unsupported by current evidence.

## Consequences

New payment initiation removes two statements and sequential round trips. Existing payment replay remains one context query, now also reading its indexed hold. Commit count and write volume are unchanged. Expected benefit is shorter payment-slot occupancy; no RPS or hourly capacity increase is claimed. The source overlay prevents unrelated repository changes from entering the candidate image.

## Failure and recovery behavior

Keep actor authorization, NOWAIT order locking, expiry, idempotency and unique payment-per-order constraints. Any failure rolls back the whole existing transaction; success is returned after commit. Existing attempts still replay after expiry/completion. Lost responses recover the same operation and key; never create a second charge. Stop load escalation on failed gates, preserve evidence, reconcile all actual payments/tickets and expired unpaid orders, drain queues and restore exact original runtime. Roll back to the parent digest. No new backend aliases or budgets are introduced.

## Validation evidence

Executed before this decision: verified the local immutable parent source against its publication receipt and inspected its payment method. Historical ADR0248 evidence is retained separately and is not a current-image test. New overlay, regression tests, image verification, cloud comparison and hourly qualification are pending; append only executed results.


## Executed implementation and local validation

The exact parent image's payment method matched the historically reviewed control AST. The candidate changes only PostgresReservations.initiate_payment; all other definitions and the other 21 modules remain identical. Built the candidate from the parent's digest without dependency changes. Verified all 22 copied, installed and imported modules and eight default role settings locally and after an actual SWR pull on ECS. Candidate digest is recorded in the publication receipt; it has not been deployed as an application service.

Executed 47 focused unit checks and 86 integration checks against owned PostgreSQL17.6/Redis using the candidate's exact imported 22-module source. Coverage includes the reduced lookup count, actor authorization, expiry, NOWAIT locks, concurrent payment keys, rollback, idempotent replay, reservation atomicity and duplicate financial effects. Owned databases/verification containers and temporary registry credentials were removed. Lint passed. This does not substitute for RDS18 cloud validation.

The actual frozen observer is still hash-verified before loading. Its adapter replaces exactly one known API collection AST statement, retaining the existing main loop, sampling budget and database/worker collection. New tests verify the overlay on the actual historical observer, safe per-endpoint HTTP metadata, preserved successful readings, no collector retry and failed completeness on missing endpoints. Test-only historical fixture isolation was extended to the metric-admission suites so they do not inherit the operator's consumed goal; an initial run failed because that stale goal referenced previous helper hashes, and the isolated rerun passed. No live authorization was rewritten.

[Executed image and local proof](../capacity/cce/measured-payment-context-image-2026-10-14.json). Next: integrate this receipt into a fresh opt-in shared-pool runner scope, then run the fixed-budget five-minute comparison with full financial, queue and restoration checks. No cloud paid load or capacity improvement is claimed in this update.

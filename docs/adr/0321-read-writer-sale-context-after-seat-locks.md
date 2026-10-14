# ADR0321: Read writer sale context after seat locks

## Status

Implementation and customer-quality checks passed; not adopted as a qualified100/s configuration because the conservative short-window gate failed. Supersedes the rejected ADR0320 outer-pipeline experiment as the next performance hypothesis; ADR0225 inner-write pipelining remains unchanged. No cloud throughput gain or new qualification is claimed.

## Context

Reservation writers retain multiple sequential dependent reads per command. The separate event lookup and post-lock clock lookup can be combined, provided the clock is still evaluated after all sorted seat locks. ADR0319 did not improve throughput; its payment method is excluded. ADR0320 passed correctness tests but worsened local batch occupancy and was rejected before cloud load. The underlying source of periodic database/WAL-related stalls remains unconfirmed. Reducing a serial writer lookup directly reduces work without changing resources or retry budgets.

## Decision

Change only PostgresReservations._persist_reservation_command on the exact ADR0298 shared-image source. Keep command replay and idempotency checks first, then acquire existing sorted NOWAIT seat locks, then query event fields and clock_timestamp together. Evaluate event existence, sale window, seat count, hold expiry and ownership using that post-lock context. Do not cache event metadata, move the clock before locking, change SQL isolation or expand pipelining. Keep the same writes, bounded batch/savepoints and outer commit.

Compare local protocol and correctness against the unchanged parent. Before a cloud test, verify the complete immutable source/dependency map and integrate only this image factor into the existing runner. Keep shared24 backends, four APIs2+2 slots, four writers/batch4, all other resources and generator500 unchanged. Fresh bounded experiment only; no load increase or hourly qualification until quality gates pass. Retain the existing missing diagnostic-history failure; do not bundle a larger ring into this candidate.

## Alternatives

Outer transaction pipeline: rejected by ADR0320 local measurements. Increase batch/partition slots: ADR0316/0317/0318 rejected. Cache sale windows: introduces freshness concerns. Read event/clock with the locking statement: clock could precede lock acquisition. Remove idempotency/replay or durability: rejected for correctness. Scale compute: does not remove this serial lookup and would confound attribution.

## Consequences

One fewer dependent SQL statement per new reservation command. Event metadata is read later in the same READ COMMITTED transaction; concurrent sale-window changes can be observed after seat locking. Missing events still produce EVENT_NOT_FOUND and the transaction rolls back. No database writes, commit counts, financial decisions, messaging patterns or limits change. An immutable experimental image selects the correction; rollback restores the parent image.

## Failure and recovery behavior

Preserve Redis atomic intake, actor/request identity checks, command replay, sorted seat locks, post-lock fresh clock and hold expiry. Domain failures retain savepoint isolation; database or commit failure rolls back the whole batch. No durable marker or stream ACK before commit. Replay after commit/response loss returns the same order, with no duplicate financial effects. Stop escalation on failed quality or financial gates, reconcile actual cohort, drain queues and restore exact runtime.

## Validation evidence

Decision recorded before implementation. Retained writer/database evidence and rejected pipeline results motivated this hypothesis. New source correction, local protocol/correctness checks and cloud comparison are pending; append only executed results.


## Executed local validation and immutable publication

Changed only the writer persistence method in the exact parent image. All other21 modules and dependency inputs remain unchanged; the rejected payment-context and outer-pipeline experiments are excluded. All22 copied/installed/imported modules and eight role defaults were verified locally and after an actual ECS/SWR digest pull. Temporary registry credentials and owned verification resources were removed; running cloud services remained unchanged.

Executed70 existing reservation/payment/Redis-first integration checks and two new concurrency checks on PostgreSQL17.6/Redis with the exact candidate imported source. The new checks commit a sale-window closure after seat locking and verify SALE_CLOSED with no persistent hold, order, idempotency, command or outbox effects. Seventeen focused source/default/connection checks passed. Lint passed. These local checks do not replace live RDS validation.

The Linux image protocol comparison used eight fresh four-command batches per arm, control first then candidate. Median local batch occupancy decreased61.823to54.006ms(12.64%). ReadyForQuery messages decreased36to32; Sync20 and explicit Flush4 were unchanged. This supports the removed serial lookup hypothesis but does not establish production capacity or cure periodic WAL stalls. A probe summary initially had an empty-arm calculation error; the corrected probe completed and owned databases were removed. [Local evidence](../capacity/cce/writer-sale-context-local-2026-10-14.json), [immutable image proof](../capacity/cce/writer-sale-context-image-2026-10-14.json).

Existing source admission, native worker transition, inventory and observer paths now have explicit ADR0321 branches; ADR0319 pins remain immutable. The pending102/s comparison retains the established shared-pool budgets and quality gates, including diagnostic history completeness. No cloud paid traffic has started for this candidate at this checkpoint.


## Executed controlled cloud comparison

Fresh run adr0151-7b637d7b8a0f completed102 offered journeys/s for300 seconds with unchanged resources, shared24 backends, four APIs2+2 slots, four writers/batch4 and generator concurrency500. All30,600 scheduled journeys were dispatched and fulfilled. Initial errors, recovery attempts and final customer failures were zero. All actual30,600 paid bookings/tickets reconciled; payment durability, zero double-booking/duplicate financial effects, full queue drain, native/ingress/source/CPU admission and exact restoration passed. Namespace/helpers/temporary credentials were removed and no active experiment remains. The ledger is FAILED_RESTORED because ticket throughput qualification failed; preserve that verdict.

The timestamp window contains30,221 unique paid-issued tickets(100.737/s). The unchanged two-second conservative boundary guard proves29,981 tickets per300 seconds(99.937/s), nineteen below the30,000 gate. Do not round this into a100/s qualification or loosen the guard. The retained reference had30,370 timestamp-window tickets and30,155 conservative tickets, but71 drops and39 recovered initial errors. This run improved observed customer quality without establishing a throughput increase. Worst-shard durable p95 fell2,022.464to1,104.099ms, while ticket-wait p95 rose2,027.750to6,330.010ms and total journey p95 rose3,353.261to7,433.793ms. Full-trace writer batch mean was79.045ms versus78.687ms; local protocol savings did not translate into a meaningful cloud batch-mean gain.

Remaining delay is in post-payment completion. Fulfillment Kafka partition1 lag peaked661 records and ended the sampled window at636, while other peaks were20to62. Its incoming sampled record count was19,563, below the other partitions'20,206to20,521. Six members/six partitions and exact assignments stayed stable. Consumer CPU means were0.091to0.120 cores against0.25-core limits; averages cannot rule out burst throttling, network or database delays. Five generic consumer errors were seen at startup by2.362s, whereas partition1 lag grew later; do not attribute the later backlog to those errors without further evidence. Three reservation batch errors and fourteen simulator delivery errors recovered internally; zero customer errors is not zero internal errors.

A separate read-only TLS query with a two-second statement limit joined all30,600 retired-cohort orders to scheduled payment eligibility and ticket insertion timestamps. Server due-to-issued p95 was6,039.310ms(p50 173.003ms); therefore the long tail is not supported as solely a client status-visibility issue. These are server row timestamps, not commit timestamps, and cannot be subtracted from worst-shard customer percentiles. A further callback-only join timed out; preserve that missing attribution and do not synthesize its result. Exact consumer cause remains unconfirmed. Keep hourly qualification and load escalation gated, and investigate partition1's serial processing before one further correction.

Executed runner regression:61 image/profile/inventory/writer cases passed; a separate broader158-case run passed with one skipped existing case. Counts overlap and must not be added. Seventy existing plus two new real PostgreSQL/Redis integration cases and seventeen focused source/default/connection checks passed. Lint and public evidence validation passed before deployment. [Controlled result](../capacity/cce/writer-sale-context-comparison-2026-10-14.json).

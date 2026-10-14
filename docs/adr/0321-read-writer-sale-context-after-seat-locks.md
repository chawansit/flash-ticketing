# ADR0321: Read writer sale context after seat locks

## Status

Accepted for isolated implementation and validation. Supersedes the rejected ADR0320 outer-pipeline experiment as the next performance hypothesis; ADR0225 inner-write pipelining remains unchanged. No cloud throughput gain or new qualification is claimed.

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

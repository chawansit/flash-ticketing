# ADR0253: Independently reconcile the failed simulator hourly run

## Status
Accepted for read-only recovery. Capacity qualification remains failed.

## Context
ADR0252 issued 301843 unique paid tickets inside the one-hour window, including 301652 inside a conservative two-second guard. Customer quality failed: 372 of 302400 scheduled journeys were undispatched, and 104 dispatched journeys returned errors (67 payment 503, 37 status 503). Terminal audit found 301961 successful payments/tickets among 302028 orders, with 67 expired unpaid orders, zero duplicate bookings and queues at zero. The original result remains failed and requires recovery verification; the normal runtime was restored. Slot diagnostics exceeded the retained ring and bounded pod logs reached their cap, preventing complete attribution.

## Decision
Reuse the existing scoped repeatable-read, read-only financial/relationship recovery with an exact new run, ledger, original report digest, fixture digest and simulator binding. Verify all 1008 fixture shows, all 302028 dispatched orders, all 301961 successful payments and tickets, and the 67 expired unpaid orders. Independently verify both safety payments, post-TTL integrity, normal runtime stability, absent owned CCE namespace/helpers, secondary cleanup, generator idle, removal of protected inputs, retired owned sale windows and global queue drain. Only then mark the consumed reservation FAILED_RESTORED. Do not rewrite the original customer, diagnostic or qualification gates; do not replay paid traffic.

## Supersession
Extends ADR0235 and ADR0250 scoped failed-run recovery to this exact consumed hourly cohort. No financial, transaction, locking, idempotency, TTL, scaling or SLO decision changes.

## Alternatives
Treat terminal target mismatch as payment loss: does not distinguish unpaid failures. Clear state without an independent audit: unsafe. Rerun or relabel the failed scope: invalid evidence. Relax customer gates: outside this correction.

## Consequences
The ticket-count target was measured, but this run is not fully qualified. Recovery proves durable financial outcomes and cleanup, not the reason for 503 errors. The retained observer trace may support partial diagnosis; missing bounded events remain missing.

## Failure and recovery behavior
Any missing fixture identity, changed digest/binding, pending queue, duplicate, broken relationship or remaining owned resource keeps recovery blocked. All verification queries remain read-only and bounded. Preserve unsuccessful verification attempts and their duration.

## Validation evidence
Original run adr0151-ada3471665ef; ledger bounded_cce_hourly_qualification__17b2158eb19b; original result SHA256 338b88f4716c777240eac9f1a1ec6295121ecce4107698463c87e9aaf2dafb18. Recovery tests and independent cloud checks are pending.

Executed 27 scoped recovery tests; all passed, including forged binding, missing payment/ticket, duplicate booking, invalid relationships, pending queue, wrong namespace, wrong resource partition and replay rejection. Ruff passed. Independent cloud verification remains pending.

The first independent relationship audit hit its 60-second SQL timeout. Retain that failed attempt. Partition the identical scoped relationship query into disjoint batches of at most 84 shows within the same repeatable-read, read-only snapshot, and sum every error/unique-ticket count. Keep the existing 20-second per-statement timeout and exact aggregate cohort totals. No financial rows, gates or original evidence are changed. Cross-event relationships remain tested by the same union and joins; any cross-batch inconsistency prevents closure.

Executed hourly run adr0151-ada3471665ef for 3600 offered seconds. Database timestamps identify 301843 unique paid-and-issued tickets inside the hour (301652 within the two-second inner guard), exceeding the 300000 numerical target. Customer quality remains failed: 104 errors among 302028 dispatched journeys (0.034434 percent), and 372 undispatched of 302400 scheduled (0.123016 percent). Terminal totals are 301961 successful payments/tickets and 67 expired unpaid orders; duplicate bookings and queues are zero. Original observer collection failed after bounded slot/log capture became incomplete. No original gate is relabeled. Independent ADR0253 recovery passed financial/relationship/safety/cleanup checks; the failed first SQL-timeout attempt is retained. Executed 30 recovery tests including late-batch corruption and complete disjoint show coverage, and real read-only RDS verification. Status is FAILED_RESTORED with no active load/recovery. See [simulator-hourly-qualification-2026-10-10.json](../capacity/flash-sale-opening/simulator-hourly-qualification-2026-10-10.json). Full capacity and production qualification remain failed/pending.

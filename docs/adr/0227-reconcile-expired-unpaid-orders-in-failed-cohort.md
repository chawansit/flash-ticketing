# ADR0227: Reconcile expired unpaid orders in a failed cohort

Status: Accepted for the exact ADR0226 failed cohort

## Context

The exact ADR0226 scope bounded_generator_completion_probe__8d5499f7111a, run adr0151-7cd8ed0b7083, arm adr0151-arm-7157e6875382 dispatched 23,545 of 25,200 journeys and dropped 1,655. Customers confirmed 23,435 tickets; 98 status reads and 12 payment submissions returned 503. The retained database audit shows 23,533 succeeded payments and unique tickets, 12 expired unpaid orders, zero duplicate bookings, complete drain and restored runtime. A customer error does not establish whether payment committed. ADR0221 currently accepts only cohorts where every dispatched journey succeeded.

## Decision

Extend ADR0221 only for this immutable case, with separate exact dispatched (23,545), confirmed (23,435), paid (23,533), expired unpaid (12) and outcome counts. Independently reconcile all owned orders and holds after TTL in a read-only snapshot. Require all succeeded payments to have exactly one valid booking and ticket; expired orders must have no payment attempt, booking, ticket or held/booked inventory. Retain global duplicate checks, queue/Kafka drain, generator idle, original runtime and identical valid index verification. Preserve all original failed gates and journal entries. The receipt clears recovery uncertainty only; it neither qualifies throughput nor permits replay of the consumed scope.

## Alternatives

- Treat all customer 503 responses as unpaid: rejected; 98 status failures already correspond to additional committed tickets.
- Count expired unpaid orders as payment loss: rejected without evidence of a succeeded payment.
- Lower the original expected paid volume: rejected; the 25,200 requirement remains failed.
- Ignore the cohort and run again: rejected until independent correctness and restoration are verified.

## Consequences

Recovery can account explicitly for unpaid expirations without hiding customer failures. Existing all-success cases retain their exact requirements. Unknown scopes, altered counts or missing expired-order relationship proof remain rejected. No application transaction, payment, generator or infrastructure behavior changes.

## Failure and recovery behavior

Any unexpected payment status, missing ticket, duplicated booking, expired order retaining financial/inventory state, altered source/index/runtime, nonempty queue, stale evidence or changed retained hash blocks the recovery receipt and further load. Preserve raw evidence and account for verification time. Read-only verification performs no dispatch, deployment, DDL or data repair.

## Validation evidence

Planned: positive exact-case reconciliation and negative tests for paid loss, unpaid count/status drift, expired inventory/financial contamination, missing proof, index change, runtime drift and original result preservation. Then one bounded independent live read-only audit. No verification pass is claimed before execution.


Executed 89 recovery/envelope tests in 8.28 s, including existing all-success compatibility and exact mixed-cohort positive/negative checks. Ruff and repository naming passed before live verification. Independent live verification passed in 157.719 s: 23,533 unique paid-and-issued tickets; 12 expired unpaid orders with no payment attempts, bookings or held/booked inventory; valid relationships after TTL; zero global duplicates; complete queues/Kafka drain; restored, unchanged runtime; identical valid index; idle generator. [Append-only receipt](../capacity/flash-sale-opening/generator-completion-recovery-2026-10-08.json). The original 25,200 expectation, 110 customer failures, 1,655 drops and all failed gates remain preserved. No new traffic or infrastructure changes were made by recovery.

Final receipt-resolution and recovery/envelope regression suite passed 90 tests in 6.47 s, including altered public customer counts and retained-artifact tampering. The earlier 89-test suite remains separately accounted.

# ADR0276: Reconcile the mixed terminal worker-placement cohort

## Status
Accepted exact-scope independent recovery of run adr0151-df0b0a270c9d, ledger bounded_cce_paid_comparison__eddb1a47d450. Extends ADR0269 independent recovery to a cohort with terminal unpaid expirations.

## Context
The original report9b1d487a42b5b77bf56fab18f4cf0fb59956e9ce0792b7e550bd07e4da44cd29 failed because25200 scheduled journeys were not all dispatched/paid. It reports21218 committed orders,21161 successful payments/tickets and57 unpaid expired orders;20772 customers confirmed a ticket. Original runtime and cleanup completed, but integrity against the scheduled25200 expectation failed. ADR0275 records the retry-profile mismatch. A terminal-cohort audit must not reinterpret this as a passing customer test.

## Decision
Independently verify the retained84-show fixture identity and its original digest/binding, actual21218 orders,21161 paid-and-issued tickets,57 expired orders with no payment/booking/ticket, exact callback counts, all relational invariants and elapsed holds. Check both safety tickets with three callbacks, all86 exact fixtures retired, global zero duplicate bookings, empty queues, absent namespace/helpers, generator idle/private input removal, secondary empty and unchanged restored runtime. On complete proof only, append an independent recovery receipt and mark FAILED_RESTORED. Preserve original report, failed gates, customer errors/drops and one consumed paid stage. No financial rows, reservations, offsets or fixture contents are rewritten.

## Alternatives
Use scheduled25200 as the terminal cohort: confuses undispatched traffic with missing payments. Use only20772 customer confirmations: omits389 other issued tickets. Mark qualification passed from paid totals: violates customer outcomes and workload comparability. Ignore integrity: unsafe.

## Consequences
Separates failed customer experience from payment durability and permits safe future engineering. Aggregates do not prove each status503 customer's ticket was retrieved; that recovery still needs a correctly configured test. No capacity qualification or additional paid permission results.

## Failure and recovery behavior
Fail closed on changed report/binding/fixture or any orphan, pending financial operation, unmatched booking, live hold, duplicate booking or incomplete cleanup. Keep RECOVERY_REQUIRED and evidence on failure. Independent verification may exceed experiment duration; record actual elapsed time. No new paid traffic.

## Validation evidence
Independent read-only audit and closure pending. Original report remains failed.

Executed independent read-only recovery:21218 committed orders,21161 successful payments/unique tickets,57 unpaid expired orders with no payment/refund/booking. All relational checks passed, both safety tickets and three callback deliveries durable, global duplicate count0, every queue empty, all86 fixture sales closed, namespace/helpers absent, generator idle/private inputs removed, secondary empty and restored runtime stable.20696 tickets were issued within the300-second offered window. Recovery took70.406seconds. Ledger FAILED_RESTORED, original failed report unchanged and one paid stage consumed. [Independent recovery evidence](../capacity/cce/worker-placement-terminal-recovery-2026-10-10.json).

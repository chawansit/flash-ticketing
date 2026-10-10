# ADR0274: Close the refunded worker-migration safety scope

## Status
Accepted exact-scope recovery of ADR0271 run adr0151-6a2c764994ca. Extends the independent recovery protocol from ADR0269 to a terminal simulated refund; no safety or performance gate is weakened.

## Context
Kafka connectivity now passes and fourteen CCE workers reached readiness. The CCE safety journey timed out at ticket confirmation before any capacity stage. Cleanup restored the original runtime but failed the safety ticket expectation, leaving ledger bounded_cce_paid_comparison__f1b7bbcd25a6 in RECOVERY_REQUIRED. Read-only audit finds the original ECS safety ticket fulfilled, and the CCE safety order REFUNDED with one successful payment, one durable simulated refund, three acknowledged duplicate callback deliveries, no booking or ticket, and elapsed hold TTL. Global queues are empty.

## Decision
Independently verify the exact report digest 3829f2234ce00434788382e68b5973271d27d6614e63d765e8c3f3e1a104254d, binding, retained two-fixture identities, financial/refund relationships, expiry, restored unchanged runtime, empty queues, absent namespace/helpers, idle generator and empty secondary host. Retire only the still-open native safety fixture using its exact producer title and UUID. Mark the ledger FAILED_RESTORED only after all checks pass, preserving the original failed report and zero paid-stage consumption. Do not mark a refunded safety customer as ticket-confirmed or qualification-passed. Then diagnose the simulator callback route before another attempt.

## Alternatives
Treat the refund as a successful ticket: incorrect customer outcome. Force a booking after expiry or edit financial rows: breaks correctness. Leave a proven terminal scope indefinitely active: prevents bounded diagnosis. Delete the fixture and financial evidence: unnecessary.

## Consequences
Proves safe terminal financial handling while retaining the failed safety/customer outcome. This is simulated refund durability, not proof of real bank settlement. No capacity benefit is claimed.

## Failure and recovery behavior
Fail closed on any changed run, report, fixture, binding, count, pending payment/refund, duplicate booking or incomplete callback. Never modify payment, booking, ticket or refund rows or reset offsets. Only the exact fixture sale window and recovery bookkeeping may change. Preserve independent evidence and actual recovery duration; cleanup may exceed the experiment ceiling.

## Validation evidence
Read-only audit executed: original safety FULFILLED with one successful payment/ticket; native safety REFUNDED with one successful payment and durable refund, three of three callbacks, zero bookings/tickets, hold expired. Queues empty. Independent closure and callback-path diagnosis pending. No paid load started.

Executed independent closure: both safety orders/payment relations and elapsed holds verified; original safety has one ticket, native safety has one durable simulated refund and no ticket/booking. All duplicate callbacks completed, global queues empty, Kafka member count1, namespace/helpers absent, generator idle and secondary empty. Exact runtime IDs/start times/execution identities stayed unchanged. Native safety sale retired by exact UUID and producer title. Ledger FAILED_RESTORED; original failure report digest preserved, zero paid stages consumed. Recovery took60.609seconds. [Independent evidence](../capacity/cce/worker-migration-safety-recovery-2026-10-10.json).

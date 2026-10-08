# ADR0229: Reconcile failed CCE paid and refunded cohort

Status: Accepted for exact failed-run recovery; capacity remains unqualified.

## Context

ADR0228 run adr0151-5db81bfa8ec2 dispatched 25,200 journeys but failed customer and observation gates. The original fixed expectation requires every journey to produce a ticket. Terminal recovery instead contains 2,250 fulfilled orders, 9,638 refunded orders and 13,312 expired unpaid orders. Independent read-only recovery verifies these relationships, no unaccounted successful payment, full queue drain, exact restoration and owned namespace removal.

## Decision

Add a separate, exact-result-bound paid recovery classifier. Require the consumed one-stage scope, unchanged original failed report digest, exact fixture identity, all independent recovery gates, post-TTL terminal financial relationships and both safety payments. Preserve original failure, ticket counts and paid-stage counters. Mark only recovery complete. This extends ADR0228 recovery behavior; it does not supersede or relax capacity/customer gates. Refund completion means the existing durable simulation record, not a real banking refund.

## Alternatives

Keep recovery unresolved despite complete independent proof, or reuse pre-dispatch recovery. The former loses the verified distinction between qualification and recovery; the latter misclassifies dispatched financial work. Changing the original test expectation would conceal a failed capacity test.

## Consequences

The run remains failed and cannot be replayed. Successful payments must reconcile to exactly one ticket or one durable simulated refund. The fixed comparison's paid-stage allowance remains consumed. Further scaling requires a separately declared qualified profile.

## Failure and recovery

Reject changed result/binding/fixture identities, unresolved payment/refund relationships, incomplete callbacks, pending work, duplicates, missing cleanup or restoration gates, symlinked/foreign evidence and repeat closure. No writes to customer financial records occur during classification. Preserve private evidence and publish only sanitized aggregate receipts.

## Validation evidence

Executed cloud read-only evidence: tmp/adr0151-5db81bfa8ec2/independent-paid-recovery.private.json. Every successful payment accounted for, zero duplicate bookings, all queues zero, exact runtime stable and temporary namespace/helpers absent. Local classifier tests must execute before closure; results recorded in the checkpoint. No hourly or throughput qualification claimed.

The runner retains current_run and active_run when original integrity qualification fails. Recovery may clear only markers equal to the exact stopped run after independent generator-idle, financial, queue, restoration and resource-absence proof. Reject any foreign marker or existing runner lock. Original integrity qualification remains false.

Validation completed: 63 local recovery tests passed in 0.42s and Ruff passed. Independent final audits verified both safety payments, all 86 owned sale windows closed, all queues drained, zero doubles and all successful payments accounted for. The exact stopped markers were cleared; original failed result and one consumed paid stage remain unchanged.

## Exact second recovery case (2026-10-09)

The ADR0230 bridge correction run adr0151-73e82b8d5210 delivered 25,200 distinct paid-and-issued tickets and passed all nine measurement gates. The original runner nevertheless failed its intermediate ECS candidate readiness check. Final original ECS restoration passed, including financial integrity. Independent read-only recovery subsequently verified all 25,200 payments and tickets, both safety payments, all 86 fixture sale windows closed, stable restored runtime, idle generator, absent namespace/helpers and complete queue drain.

Extend this same exact-result-bound recovery classifier with a second immutable case: ledger bounded_cce_paid_comparison__33bd91855dcc, original result digest d99b0754805fe5a3df109d1fbfddde367bcf62a4e66891052773e8a835810a02. Require the original nine passing measurement gates, customer pass with every scheduled journey fulfilled, financial counts of 25,200, no refunds or expired orders, and only the recorded candidate_restore failure. Retain the original report's pass=false and its consumed stage. Mark only recovery complete; publish the measured short-stage result separately from full-run or hourly qualification. No additional customer load is needed to verify final restoration. The intermediate readiness timeout's cause remains unresolved; it must be diagnosed before hourly qualification. This addendum does not relax or supersede any customer, durability, queue or restoration gate.

Validation evidence: tmp/adr0151-73e82b8d5210/independent-paid-recovery.private.json. Local fault tests for both immutable cases must pass before ledger closure; executed results will be recorded in the checkpoint.

Second-case validation: 81 recovery fault tests passed in 0.67 s; Ruff passed. The initial closure test lacked an elapsed-time fixture field (80 passed, 1 failed in 1.62 s); corrected before closure. The second ledger is FAILED_RESTORED. Its original report and one consumed paid stage remain unchanged.

# ADR0369: Reconcile the clean-customer observer failure

## Status
Accepted2026-10-19 for operational recovery only. No capacity promotion.

## Context
ADR0368 runadr0151-4163debac412 completed54000/54000 journeys with0drops/initial/recovered/final errors, guarded53415tickets and p952564.43ms. Financial and terminal-log capture passed. Native worker progress validation failed because the new batch delivery path bypassed the existing simulate_one operation counter (0throughout despite delivered callbacks). Original rawhashd5f7ecb029af526d74a279e3031e2d29ea2f919f0adc3e6110b5a608959e346a remains failed/recovery required. No200traffic.

## Decision
Use the existing independent bounded read-only database auditor for exactly54000orders/payments/tickets/callbacks. Verify all relationships/postTTL, zero pending/duplicate work, stable normal runtime, generator idle, drained queues and owned-resource/credential cleanup. Register only this exact stopped scope FAILED_RESTORED. Keep the original observer failure and raw hash immutable; a fresh fully validated180 remains required before200.

## Alternatives
Promote the old result after fixing the counter: rejected. Assume the original successful financial report is a fresh reconciliation: rejected. Reset the run: rejected.

## Consequences
Operational closure is separate from qualification. No application data, persistence, locks, messaging, idempotency, TTL, scaling or acceptance criteria change.

## Failure and recovery
Exact raw/binding/fixture hashes, counts, clock freshness, cleanup and audit predicates remain mandatory. Incomplete evidence leaves RECOVERY_REQUIRED. No new traffic or infrastructure resize.

## Validation evidence
Nine focused validation cases and Ruff passed. Actual independent bounded read-only database reconciliation passed for54000orders/payments/tickets/callbacks,0pending/duplicates/relationship errors and postTTL. Stable runtime/generator idle/queues clear/owned cleanup passed. Registered FAILED_RESTORED; rawhash unchanged. Receipt docs/capacity/cce/claim-batch-180-observer-recovery-2026-10-19.json.

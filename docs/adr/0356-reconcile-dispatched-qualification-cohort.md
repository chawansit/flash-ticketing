# ADR0356: Reconcile the dispatched qualification cohort

## Status

Accepted for the exact stopped ADR0355 180/s run only. Uses existing read-only bounded financial audit and prior terminal recovery registration pattern. Does not supersede any performance or correctness gate, authorize200/s, reset the failed attempt or classify it as passed.

## Context

Run adr0151-ccb85bad6425 scheduled54000, dispatched/fulfilled53998, dropped2, recovered15 initial errors, zero final customer failures. Conservative issuance53181=177.27/s, journey p955696.82ms. Original financial target audit expected54000 and failed; counts show53998 paid/tickets with zero relationship errors. Native resources and normal runtime restored, but integrity requires independent actual-cohort reconciliation before any further traffic.

## Decision

Pin exact original run, ledger, report hash, fixture and counts. Reuse unchanged cce_bounded_financial_audit.program with expected/paid53998 and callback multiplicity1 in a fresh read-only repeatable-read database snapshot. Preserve all seven relationship predicates, expiry, uniqueness and pending/dead-letter checks. Verify current normal replacement-RDS runtime twice, generator idle and global queues zero; retain original verified namespace/resource/credential cleanup. Only after every gate passes append a recovery receipt and change operational status to FAILED_RESTORED, retaining original report/hash, target audit and failed measurements. Clear only this consumed active scope, never human pause or other recovery state.

## Alternatives

Treat target-count failure as payment loss without reconciliation: unsupported. Count missing dispatched journeys as paid: rejected. Rewrite original expected count/report or mark performance passed: rejected. Run more traffic before reconciliation: rejected.

## Persistence, locking, messaging, idempotency, TTL and scaling

No application writes, lock, message, idempotency, TTL, resource or topology changes. Only append recovery evidence and existing journal/state terminal classification. Existing RR READ ONLY audit retains its bounded timeouts.

## Failure and recovery

Fail closed on mismatched run/hash/fixture/counts, any financial predicate, runtime drift, nonzero queue or incomplete owned cleanup. Retain RECOVERY_REQUIRED and prohibit load until resolved. The original experiment remains failed even if operations recover.

## Validation evidence

Eight focused exact-case/negative tests passed; Ruff passed. Fresh existing bounded RR READ ONLY database audit actually executed on new RDS and reconciled53998 dispatched orders/payments/bookings/tickets, all seven relationship predicates and expiry checks. Current normal runtime verified twice, generator idle and global queues zero. Exact registered receipt appended to journal/state as FAILED_RESTORED; original result/hash/quality untouched. [Recovery](../capacity/cce/rate-180-dispatched-recovery-2026-10-19.json).

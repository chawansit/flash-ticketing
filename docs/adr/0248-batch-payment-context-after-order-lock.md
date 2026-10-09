# ADR0248: Batch payment context after locking the order

## Status
Accepted for isolated implementation and regression validation. Not deployed or capacity-qualified. ADR0245's failed control remains consumed; its candidate cannot proceed.

## Context
The latest 84 journeys/s control failed with 13 payment-request errors and 82 status-read errors. Retained samples show higher acquisition waits and longer connection holds across several services; a common stall cause remains unproven. The payment intake implementation locks its authorized order, then reads the existing payment, hold and database clock in separate statements. Payment gateway delay occurs outside these transactions.

## Decision
Keep idempotency processing and the authorized order's FOR UPDATE NOWAIT statement unchanged. Only after that lock succeeds, read the existing payment ID, hold expiry and database clock in one indexed SELECT. A new payment therefore needs two fewer database statements and sequential round trips. Preserve insertion, due-time calculation, idempotent response persistence, transaction boundaries and the original existing-payment replay behavior.

Do not join payment discovery into the initial order-lock statement: its earlier READ COMMITTED snapshot could miss a payment committed while the order row is being acquired. The post-lock SELECT retains the fresh snapshot of the existing implementation. Do not lock the joined payment/hold rows or change the financial lock order.

## Alternatives
Combine everything into the order-lock SELECT: rejected because of snapshot/concurrent-payment visibility. Increase payment pools: cannot justify taking capacity from status reads after the failed control. Change all transaction setup: the prior explicit-BEGIN candidate failed and was rejected. Remove idempotency or expiry checks: violates correctness. Change observer SQL first: its isolated benefit does not establish customer-path improvement.

## Consequences
Reduce only new-payment lookup work. Existing-payment lookup remains one SELECT, now also reading the indexed hold. Missing orders/other actors remain indistinguishable; expired or non-pending orders without an attempt remain unpayable. No machine size, connection budget, payment settings, callback semantics, holds, retries, schema or indexes change. The expected benefit is fewer round trips, not a proven RPS gain.

## Failure and recovery behavior
Any lookup, lock, insert or response-persistence failure rolls back the same transaction and idempotency row. Concurrent requests preserve NOWAIT behavior and the unique payment-per-order constraint. Existing attempts still return their original ID after hold expiry or payment completion. Restore the original lookup block to roll back this correction. Never reopen ambiguous/consumed test scopes; use a fresh matched control and stop progression if it fails.

## Validation evidence
Pending execution: real PostgreSQL tests for replay, authorization, expiry, rollback, concurrent order locking and duplicate financial effects. Verify exactly two fewer lookup statements on new attempts and preserve expiry checks after the lock snapshot. No paid load or hourly capacity is claimed. Diagnostics and immutable source/image qualification remain required before any future controlled deployment.

Executed validation: 24 payment/reservation tests passed. Sixty-nine financial regression tests then passed with the accepted control image's explicit-BEGIN transaction method, including payment pool isolation, callback lock order, asynchronous receipt durability, replay and ticket issuance. The initial broader attempt passed 66 tests and failed three because the local Redis URL was absent; after supplying an owned Redis, the same suites passed. Both owned databases were removed. Ruff and repository naming checks passed.

Bounded read-only RDS profiling preserved runtime identity and compared twenty alternating pairs on one retired unpaid order. The post-lock lookup mean changed from 4.1415 to 1.3683 ms (67.0% lower), with matching payment/expiry context. This excludes locks, writes, checkout, commit and the customer response. The accepted image's original payment method matches the pre-change AST; the isolated 22-module candidate source changes only that method, with all other method ASTs unchanged. No candidate image, paid load or capacity qualification occurred. [Sanitized lookup evidence](../capacity/cce/payment-context-query-profile-2026-10-09.json).

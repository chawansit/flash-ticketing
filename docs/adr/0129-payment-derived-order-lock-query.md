# 0129 - Payment-derived order lock in one query

- **Status:** Accepted for implementation and local validation; cloud comparison pending.
- **Date:** 2026-10-04

## Context

The saved ADR0127 profile attributed 33.26% of recognized database-context weight to payment callbacks (7.59% of all sampled weight). This is historical stack context, not charged CPU, query duration or a new profile. Callback source is unchanged in the passing ADR0128 runtime. The latest passing baseline is checkout-20261004T025114Z-e8b884 at application revision 7bbfaa6, with cache disabled, 60 buyers/s for 300 seconds and all 16 gates passed.

Callback currently reads a payment without a row lock solely to find its order, then reads and locks that order in a second statement. It subsequently locks and re-reads the payment. The discovery round trip can be removed without removing that essential fresh payment read.

## Decision

Replace only callback's initial discovery and order-lock statements with a parameterized join:
SELECT o.* FROM orders o JOIN payment_attempts p ON p.order_id=o.id
WHERE p.id=%s FOR UPDATE OF o NOWAIT.
Bind the payment ID, not the callback's asserted order ID. Lock only the order. Keep the subsequent SELECT of payment_attempts FOR UPDATE NOWAIT and its fresh state unchanged.

The non-null foreign key from payment_attempts.order_id to orders guarantees a committed payment has an order. No joined row returns PAYMENT_NOT_FOUND404, as the old missing-payment branch did. Application paths do not delete or reassign payments or orders. This does not introduce such a lifecycle.

## Persistence, locking, messaging and idempotency

No schema, index or migration changes. Existing payment and order primary keys support the join. The explicit OF o is required: locking both joined tables would change the order-before-payment lock hierarchy. Retain order, payment, hold and sorted-seat locking, all NOWAIT behavior, booking uniqueness, expiry authority, refund intent and atomic callback/outbox writes. Preserve ADR0004 rather than superseding it.

Keep payment/callback mismatch checks, callback ID hash deduplication, already-successful payment handling and consumer replay behavior. Keep ADR0117 transaction setup, READ COMMITTED isolation, lock75ms/statement1500ms/idle3s, rollback and pool return. The later payment read must observe a committed state change after discovery. Provider calls remain outside the transaction.

Keep ADR0128 order reads and every other method unchanged. Redis writer fairness, cache0, hold/command TTLs, inbox/outbox, worker counts, pool/admission budgets and load remain unchanged. No accepted architectural authority, messaging, idempotency, TTL or scaling decision is superseded.

## Alternatives and consequences

Keeping two initial reads preserves existing behavior but incurs an extra domain round trip on every callback, including duplicate deliveries. Locking the payment first or locking both joined tables would change the hierarchy and was rejected. Using the payload order ID would trust an unverified association. Reusing a payment snapshot from the join would weaken replay/concurrency handling. Batching bookings is a different factor and does not reduce calls for the one-seat comparison.

The join adds server planning work; fewer calls alone do not establish a CPU, latency or capacity win. The later payment state read and common transaction overhead remain. Normal payment/order deletion is outside current scope; a future lifecycle must revisit this query and concurrent mapping guarantees.

## Failure and recovery

Missing payment returns404; order/payment/hold/seat contention follows existing NOWAIT failure and full rollback. Invalid association, amount or currency must still roll back without callback, booking, refund or outbox writes. Same/distinct callback ID replay must never double book or duplicate tickets. No retry or timeout relaxation is introduced.

Local validation uses owned isolated PostgreSQL17.6 and Redis7.4.5 containers. A cloud comparison is limited to one unchanged 60 buyers/s, 300 seconds, cache0 run against ADR0128, with all customer, durability/financial, zero-double-booking, post-TTL, full-keyspace queue, Kafka, observer, CPU, source identity, configuration restoration, idle and private-cleanup gates. Use the existing ADR0040 orchestration and ADR0090 generator. Paid orchestration product extensions remain future work.

On any failed gate retain evidence, complete correctness/drain/restoration audits, restore baseline source/images and stop. No load increase, GitHub publication or main merge. Temporary root access, if needed, must be restricted, expiring and removed after audits; prior keys are already removed.

## Validation evidence

No new test or cloud result is claimed at decision time. Planned real-PostgreSQL coverage: one fewer domain SELECT; payment-derived association; unknown payment404; invalid payload rollback; order-only first lock and subsequent fresh payment read; NOWAIT contention and lock release; identical/distinct-ID concurrent replay; multiseat uniqueness and fulfillment replay. Existing expiry/reclamation, transaction safety and writer fairness regressions remain mandatory.

- [Historical database context](../capacity/flash-sale-opening/order-read-database-investigation-2026-10-04.json)
- [Identified passing baseline](../capacity/flash-sale-opening/order-read-single-statement-control-2026-10-04.json)
- [Payment idempotency authority](0004-payment-idempotency.md)
- [Transaction-local timeout setup](0117-consolidated-transaction-local-timeouts.md)
- [PostgreSQL17 locking clauses](https://www.postgresql.org/docs/17/sql-select.html#SQL-FOR-UPDATE-SHARE)
- [PostgreSQL17 READ COMMITTED snapshots](https://www.postgresql.org/docs/17/transaction-iso.html#XACT-READ-COMMITTED)

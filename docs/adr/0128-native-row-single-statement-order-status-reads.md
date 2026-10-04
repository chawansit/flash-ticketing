# 0128 — Native-row single-statement order-status reads

- **Status:** Accepted for local implementation and controlled validation on 2026-10-04. Cloud qualification is pending.
- **Context:** The passing ADR0127 diagnostic at 60 buyers/s for 300 seconds with cache0 recorded 36,165 order GETs. Of 487 database-context observations, order-status reads account for 19.71% of database-context weight (4.50% of all sampled weight). Callback contexts account for 33.26%, payment initiation for 19.71%, and 27.10% have no route owner. These are stack contexts, not charged CPU or SQL durations.
- **Prior decisions:** ADR0089 and ADR0094 tested single-snapshot reads with different connection setup and JSON aggregation; both failed customer gates and were reverted. Their historical rejections remain valid. This candidate reopens only the domain read query, retaining ADR0117's common transaction helper and ADR0099's booking-order access index. No accepted authority, durability, locking, messaging, idempotency, TTL or scaling decision is superseded.

## Decision

Replace only PostgresReservations.get_order's two domain SELECTs with one parameterized actor-scoped SELECT. Select the order and left join its issued tickets through bookings, preserving an order with no tickets. Sort by seat ID. Reconstruct the existing dictionary and ticket-list shape from native PostgreSQL rows and remove internal projection aliases before returning.

Keep Postgres.transaction unchanged: explicit BEGIN, consolidated transaction-local lock timeout75ms, statement timeout1500ms and idle timeout3s, commit/rollback, diagnostics and pool return. Preserve ORDER_NOT_FOUND404 for missing or non-owned orders. Preserve UUID/datetime Python values and HTTP representation.

A single statement reads status and tickets from one MVCC snapshot. If fulfillment commits concurrently, the response describes either the earlier state or the fully committed state. It must not combine a pre-fulfillment status with a later ticket list.

## Persistence, locking and messaging

No migrations or new indexes. Existing orders primary key, bookings_order_id index and unique ticket booking key supply lookups. Read without row locks; all mutation lock ordering and seat/booking uniqueness remain unchanged. No payment/callback, consumer inbox, transactional outbox, replay or notification changes.

Cache0, cache authority/age policy and hold/command TTLs remain unchanged. No retry, resource-budget, worker-count or generator change.

## Alternatives and consequences

Retaining two SELECTs keeps an avoidable domain round trip and separate READ COMMITTED snapshots. JSON aggregation changes native ticket identifiers and adds server aggregation; lateral arrays add query/adapter complexity. Changing to a separate read-only connection lifecycle, removing BEGIN/setup or using pipeline mode would change a second factor. Increasing cache lifetime, connections or load changes this comparison's scope.

The flat join repeats order columns for each issued ticket. The API admits up to eight seats; multi-seat tests must check shape, order and uniqueness. The joined query can add planning/server work, so fewer calls do not prove a performance win. The retained setup and commit costs remain. This optimization does not qualify production capacity.

## Failure and recovery

SQL errors, statement/lock timeout and connection failures follow the unchanged transaction rollback and pool lifecycle. No partial response, stale financial acknowledgement or automatic retry is introduced. Missing/wrong-actor rows return the same404.

Validate with isolated local PostgreSQL17.6 and Redis7.4.5 and remove only owned test containers. Before any cloud promotion, require a single identified-baseline comparison at the same60buyers/s300s/cache0 configuration and all customer, financial/durability, zero-double-booking, post-TTL, full-keyspace queue/Kafka, observer, CPU, source, restoration, idle and private-cleanup gates. Do not profile the candidate when comparing against the unprofiled ADR0126 baseline; profiling would add a second factor.

On a failed gate, retain evidence, finish exact correctness/drain/restoration audits, revert this candidate's runtime source, verify all service source hashes and stop further cloud tests. No higher load or main merge. A fresh temporary root key needs its own explicit access approval if required; prior ADR0127 access was removed.

## Validation evidence

[Saved database-path attribution](../capacity/flash-sale-opening/order-read-database-investigation-2026-10-04.json) conserves all487 database-context observations. No measured query-duration claim is made. The saved backend_statement_timings field contains journey-phase timestamps, not SQL execution measurements.

Planned regression coverage: pending/paid/fulfilled/expired state and native response values; missing and wrong actor; multi-seat sorting and callback/event replay; one domain statement without removing transaction setup; concurrent fulfillment across the read boundary; preserved pool/timeout recovery and financial uniqueness. Execute the single-snapshot race against the old implementation before changing it, then the candidate. Retain actual commands/counts and skipped or failed checks. No new test or cloud result is claimed at decision time.

- [Prior ADR0089 rejection](0089-single-statement-order-status-read.md)
- [Prior ADR0094 rejection](0094-reevaluate-single-snapshot-order-reads-with-bounded-timeouts.md)
- [Booking-order access index](0099-proposed-booking-order-lookup-index.md)
- [Existing transaction setup](0117-consolidated-transaction-local-timeouts.md)
- [Passing unprofiled cache0 baseline](../capacity/flash-sale-opening/cache-disabled-control-2026-10-04.json)

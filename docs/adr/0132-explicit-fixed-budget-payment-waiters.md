# ADR 0132: Explicit financial waiter allocation within the fixed API budget

- Status: Accepted for local implementation; cloud qualification pending
- Date: 2026-10-04

## Context and evidence

ADR0131's two-slot control failed the customer gate: 17999/18000 paid-issued tickets and one payment initiation HTTP503. All other19 gates passed; shared ADR0129 source/images/settings were restored and access removed. The previous report incorrectly said the exception type was unavailable. Reanalysis of the existing observer counters found one TooManyRequests, one payment-initiation503 and one acquisition error on the same replica and interval. The sampled general pool was entirely idle; the financial pool had no available connections. Sparse queue gauges do not capture the instantaneous rejection boundary.

A real PostgreSQL/HTTP control reproduced that boundary: hold two financial connections, queue six payment initiations, then the seventh fails TooManyRequests before any payment write while general reads work. Release and explicit same-key retry complete all seven; initiation/callback/consumer replay leaves exactly seven payments/bookings/tickets and zero double booking. One test passed in5.29s. Its one-second test-only acquisition fence makes the boundary observable; it does not alter the production150ms deadline or establish cloud capacity.

## Decision and supersession

Reuse ADR0131's opt-in route-specific pools, lifecycle/readiness and metrics. Keep general2/payment2 connections, total4, and total12 waiters. Add API_PAYMENT_POOL_MAX_WAITING: default0 preserves the proportional policy; a positive value assigns that many existing waiter slots to the financial pool and the remainder to general. The next candidate selects payment11/general1 waiters. This supersedes ADR0131's proportional6/6 candidate allocation, whose failed result remains unqualified. ADR0103/0083 total ceilings and shared default remain active until qualification.

Validate all budgets before opening resources. Reject negative values, explicit allocation in shared mode, and any allocation leaving either enabled pool with zero waiters (zero would mean unbounded in psycopg_pool). No borrowing, new queue, fallback or automatic retry. The bounded candidate runner accepts only shared0/0 or partition2/11 and verifies every replica before dispatch; rollback restores both options to0.

## Persistence, locking, messaging, idempotency and TTL

No schema, persistence authority, transaction/connection helper, financial query or lock-order change. Retain ADR0129 order lock and fresh payment lock, order/payment/hold/sorted-seat locking, transactional outbox, consumer inbox, callback signatures and replay keys. Preserve ADR0122 writer fairness, ADR0128 reads, existing transaction/acquisition deadlines and all TTLs. No messaging or worker scheduling change.

## Alternatives, consequences and scaling

Keeping6/6 already failed the zero-error gate. Increasing total waiters, connections, replicas, server budgets, load or deadlines changes the established budget. Giving payments three connections changes the general connection ceiling. A work-conserving priority scheduler needs a larger synchronization policy. Recovery retries are a separate future decision and must not hide the original failures.

An explicit fixed-budget allocation addresses the observed immediate financial queue overflow with a small change. General requests can now overflow a single waiter; payment requests may still time out or overflow eleven. Neither protection nor throughput is assumed. General queue peaks were0 in the two saved controls, but samples do not prove future headroom. PgBouncer/RDS servers, locks, workers and CPU remain shared. No sustained300000/hour claim.

## Failure and recovery

Invalid configuration fails before creating pools. Preserve partial-initialization cleanup, unique-pool readiness, pool closure, explicit HTTP503/cause/Retry-After, rollback and connection return. Overflow and timeout remain visible, with no implicit retry. Keep both failed allocation reports.

Run local real-DB concurrency, overflow, timeout, isolation, signed HTTP authorization, callback/consumer replay, exact financial counts and zero-double-booking checks, plus configuration/metrics/lifecycle/CLI controls. The historical six-waiter reproduction remains alongside the eleven-waiter case.

A later comparison requires fresh approval for one60buyers/s300s/cache0 run and restricted45-minute root-key installation/removal on both ECS hosts. Compare against passing ADR0129 and retain ADR0131 as a failed reference. Reuse ADR0040 orchestration and ADR0090 bounded controls. Retain all20 financial/durability/postTTL/zero-double-booking/full-keyspacequeue/Kafka/observer/CPU/source/budget/partition/restoration/idle/private-cleanup gates. On any failed gate finish audits, restore baseline source/images/settings, remove access, retain errors and stop. No higher load, additional run, push or main merge.

## Validation evidence

At decision time only the saved-evidence reanalysis and executed one-case local reproduction above have run. Implementation and eleven-waiter validation are pending; no new cloud access or load.

- [ADR0131 failed comparison](../capacity/flash-sale-opening/payment-pool-two-slot-control-2026-10-04.json)
- [Overflow diagnosis and reproduction](../capacity/flash-sale-opening/payment-pool-queue-overflow-diagnosis-2026-10-04.json)
- [Passing ADR0129 comparison](../capacity/flash-sale-opening/callback-order-lock-control-2026-10-04.json)
- [Superseded candidate](0131-two-slot-api-payment-pool-allocation.md)

Executed local candidate validation:636 unit/integration cases passed93.54s, no skips, two dependency deprecations. Both overflow controls, eleven-waiter FIFO progress, signed route isolation, concurrent financial transactions and exact replay/zero-double-booking passed. Budget/settings/lifecycle/metrics and before-transport checks passed. Linux shell/inlinePython/Compose checks passed; owned services removed. Financial/worker code and Postgres constructor/transaction/connection/cursor/close AST match passing ADR0129. [Local validation](../capacity/flash-sale-opening/payment-pool-waiter-allocation-local-validation-2026-10-04.json). No cloud access/load this turn.

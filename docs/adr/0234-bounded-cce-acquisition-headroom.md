# ADR0234: Bounded CCE acquisition headroom with unchanged connections

Status: Accepted after passing the bounded short comparison; hourly capacity remains unqualified.

## Context

ADR0233 control adr0151-11edb3936dcc dispatched 25,200 journeys and confirmed 25,178. Twenty status reads and two payments returned HTTP503. Retained snapshots prove four general-role global_limit rejections at shared occupancy 12 (general 7, payment 5), retained timeout slots zero and general native waiters 7. Log byte ceilings were reached; these snapshots do not establish the cause of all 22 errors. Financial reconciliation found 25,198 unique successful payments and tickets, two unpaid expired orders, no duplicates or lost payments and complete queue drain. API compute capacity alone would not remove this configured rejection.

## Decision

Compare one CCE-only configuration factor: DB_POOL_MAX_WAITING 12 to 20. Preserve four API pods, four database connections per API (two general and two payment), 24 PgBouncer server connections, a 500-ms acquisition deadline, customer retries zero, frozen application images, background placement, payment simulator and all gates. The existing frozen formula also changes each role's finite acquisition ceiling from 10 to 18; this consequence must be reported. No unbounded waiting or additional database connections are introduced.

Keep the retained baseline contract immutable. Generate candidate API environment/Secret and startup hashes with an explicit bounded override. Bind the chosen budget to the reservation, exact manifests, startup environment proof and declared goal. Reject arbitrary ceilings and configuration drift before namespace creation. ECS safety and restored runtime remain at their baseline settings; only native CCE pods use the candidate. Supersede ADR0228's fixed waiting-12 requirement only for this source-bound candidate, retaining its source/image, pool, correctness and workload requirements.

Use the immediately preceding diagnostic control as the identified baseline. Offer 84 journeys/s for 300 seconds once, with unchanged generator concurrency 500, eight clients per shard, polling and no customer retries. Require all customer, latency, payment durability, zero-double-booking, queue, observer and restoration gates before hourly progression. Preserve and independently reconcile failure. No throughput gain is assumed from local tests.

## Alternatives

Scale pods, add database connections, change payment architecture, increase the acquisition timeout, or leave the configuration unchanged. The captured failure is a bounded admission rejection, so test modest queue headroom first. Separate global and role limits would require an application/source change; the existing configuration is the smaller initial correction. Scaling remains available if measurements identify compute saturation.

## Consequences

Brief coincident read/payment acquisition bursts can wait for existing connections instead of being rejected immediately. More waiters can increase latency or hit the unchanged timeout; the experiment must fail if this merely converts immediate failures into delayed failures. The four snapshots are enough to identify one mechanism, not complete error attribution. Capture truncation remains explicit. A passing short run does not qualify an hour or a simultaneous single-concert opening.

## Failure and recovery

Reject unbound budgets, altered Secrets or manifests, missing startup proofs and consumed scopes. Stop progression on failed controls or candidate gates. Audit every successful payment, all exact fixture identities and post-TTL state, drain queues, remove owned CCE resources and restore original ECS settings. Roll back by removing candidate pods; no persistent schema or application change is made. Keep original reports and consumed paid stages unchanged.

## Validation evidence

Private snapshots: tmp/adr0151-11edb3936dcc/candidate/admission-failures.private.json. Independent recovery passed all ten gates. Execute environment-diff, budget-bound, source/manifest-drift and normal cleanup tests before the candidate. Capacity benefit remains unmeasured.

Local qualification: 223 affected adapter, entry, transition, lifecycle, failure-capture and headroom tests passed in 5.30 s; Ruff passed. An initial syntax/import issue was fixed before test execution and before any cloud mutation. Benefit remains unmeasured.

## Executed comparison

Run adr0151-555e1c2469d4 passed all nine measurement gates, final financial integrity and restoration. It dispatched and confirmed all 25,200 journeys, with 25,200 unique successful payments and issued tickets, zero customer errors, drops, retries, expired unpaid orders or duplicate bookings, and complete queue drain. Hold-to-ticket p95 changed from 3,647.51 to 2,628.48 ms; payment-to-ticket p95 was 1,112.45 ms. API process CPU averaged 1.082 cores, and successful acquisition mean was 2.346 ms. All owned CCE resources were removed and original ECS services restored. Ledger bounded_cce_paid_comparison__74652cc2c71f is PASSED_RESTORED; original result digest 8d53302985cc0cd072edb62eb1f6caf4cda4507d65171728c235a839a16f2def.

Sanitized evidence: docs/capacity/flash-sale-opening/cce-acquisition-headroom-comparison-2026-10-09.json. The broader local CCE/envelope cohort passed 627 tests with one platform-specific skip in 20.09 s. Adopt the 20-slot configuration for subsequent controlled qualification; do not claim an hourly capacity limit from one short run. All log byte ceilings were reached; zero retained snapshots does not establish complete log coverage. Full customer outcomes and financial audits are independently passing. No hour-long load has started.

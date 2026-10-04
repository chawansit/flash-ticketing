# 0130 - Fixed-budget API payment pool partition

- **Status:** Accepted for opt-in implementation and local validation; cloud comparison pending.
- **Date:** 2026-10-04
- **Roadmap:** Step4, protect payment/webhook database capacity.

## Context and reproduced failure

The passing ADR0129 control at60buyers/s300s had no customer errors and mean API pool acquisition0.267ms. It does not establish protection under read saturation. An isolated real-PostgreSQL test held four genuine actor-owned order reads after their query with all four shared pool connections occupied. Both payment initiation and callback then raised PoolTimeout under the unchanged50ms test bound. After releasing reads, callback and replay produced exactly one booking/callback record. The reproduction passed in0.50s; this is a controlled local resource-exhaustion case, not an observed cloud outage.

All API methods currently share one Postgres pool. Raising connection or waiter totals would change resource budgets. A nonblocking read semaphore could preserve a slot but reject otherwise healthy read bursts, while a second semaphore wait would add a queue/timeout policy.

## Decision

Introduce an opt-in API_PAYMENT_POOL_MAX setting, default0 (the current shared pool). When enabled, partition the existing per-API DB_POOL_MAX into general and payment pools. Candidate allocation is total4 = general3 + payment1. Only resolved POST payment-initiation and payment-webhook routes receive a service backed by the payment pool. All order/hold/browse/readiness/general operations keep the general pool. Verify both pools for readiness and close both on shutdown or failed setup.

Partition the existing total DB_POOL_MAX_WAITING as well: payment waiters = max(1, floor(total_waiters * payment_max / total_connections)); general gets the remainder. Candidate total12 = general9 + payment3. Require positive general/payment maxima and positive waiter limits for both before opening any connections. Zero max_waiting means unlimited in psycopg_pool and must never arise in enabled partition mode. Retain the existing acquisition deadline for each pool, with no fallback, borrowing, retries or second admission queue.

Both pools use the existing Postgres transaction helper. API pool state metrics must aggregate both pools into the original total gauges and expose fixed general/payment state labels to verify the partition. Unpartitioned API/worker pools preserve their existing semantics. The shared pool default remains unchanged.

## Prior decisions and persistence authority

This supersedes ADR0103's single-pool allocation only when the new opt-in partition is enabled; its per-API total connection ceiling and fixed PgBouncer server budget remain. It extends ADR0083's bounded per-API waiter ceiling to the sum of the two queues, not an independent ceiling on each. Shared mode still implements the prior allocation. No authority decision is superseded.

No migrations, query, lock-order, payment/booking/refund, callback hash/idempotency, consumer inbox or transactional outbox changes. Retain ADR0129's payment-derived order lock and fresh payment read; ADR0128 native-row order snapshots; ADR0122 writer fairness; transaction timeouts/rollback/pool return. Hold/command/cache TTLs and cache0 remain unchanged.

The isolation guarantee is process-local: general reads cannot take payment-pool client connections or its waiter slots. This does not reserve PgBouncer/RDS server connections or protect from PostgreSQL locks, CPU saturation, worker pressure, event-loop/thread exhaustion or provider failures. Those remain measured limits and later roadmap work.

## Alternatives and consequences

Separate unbounded/additional payment pools violate the fixed client budget. Shared-pool priority scheduling would require changing pool internals. A hard read rejection gate can sacrifice confirmation availability under ordinary bursts. Removing pooling or increasing timeouts hides occupancy. Opt-in static partition uses supported pool mechanisms and keeps errors explicit.

Idle payment capacity cannot serve reads, and payment bursts cannot borrow general capacity. One payment connection per API may limit throughput or overflow its smaller queue. Both effects must be measured under the unchanged workload. Two enabled pools each keep min_size1, so idle minimum connections increase from1 to2 per API while the maximum total stays fixed. No production promotion or capacity gain is assumed.

## Failure and recovery

Misconfigured partition/queues fail before pool creation. If the second pool or later API setup fails, close all created resources. Any pool acquisition, lock, statement or connection failure retains the original error classification, HTTP response and rollback behavior. No automatic fallback to the other pool or caller retry is introduced.

Test against isolated local PostgreSQL/Redis: general-read saturation must leave payment initiation/callback usable; payment saturation must remain bounded and visible; replay and fulfillment remain exact; authorization/signatures unchanged; settings, route selection, aggregate/purpose gauges and all lifecycle cleanup must pass.

A proposed cloud comparison is one60buyers/s300s/cache0 run against passing ADR0129, with API total4/waiters12 and PgBouncer24 unchanged, partition1 enabled only for the candidate. Existing orchestration/generator from ADR0040/0090 remains. Require all customer, exact financial/durability/post-TTL, zero-double-booking, full-keyspace queue/Kafka, observer/CPU/source/budget/partition/restoration/idle/private-cleanup gates. This cloud scope and required fresh temporary root access must be concrete and approved before dispatch.

On failed gates retain evidence, complete financial/drain/recovery audits, restore the shared baseline source/images/configuration, remove temporary access and stop. No higher load, publication or main merge. No retry, burst-scaling or one-hour capacity proof is claimed here.

## Validation evidence

Executed: shared-pool reproduction1passed in0.50s, two expected PoolTimeout exceptions under four paused reads, exactly one booking/callback after recovery, owned local test services removed. No candidate implementation/test/cloud result exists at decision time.

- [Identified passing ADR0129 baseline](../capacity/flash-sale-opening/callback-order-lock-control-2026-10-04.json)
- [Bounded API waiter decision](0083-bounded-api-db-pool-waiters.md)
- [Four-connection allocation](0103-api-pool-four-fixed-server-budget.md)
- [Psycopg pool bounds and queue semantics](https://www.psycopg.org/psycopg3/docs/api/pool.html)

Executed candidate validation: all604unit/integration cases passed82.98s with no skips and two dependency deprecations, including33new cases. The real API completed payment initiation and a signed callback while all three general connections were held by real order reads. Payment-pool overflow/timeout remained visible without fallback; callback/fulfillment replay, actor authorization, signatures and exact bookings/tickets passed. Shared and partitioned lifecycle cleanup/readiness and aggregate/purpose gauges passed.

The initial collection failure from duplicate test basenames and one incomplete fake-adapter fixture were corrected; failed runs retained privately. Clean full rerun passed. Linux inline-probe Python/shell syntax and API-only Compose0/1 mapping passed after LF-byte preflight correction. Ownedlocalservices removed. Postgres transaction/connection/constructor/cursor/close are AST-identical to baseline; durable reservation, workers and seven other runtime modules unchanged. API function changes are lifespan/service/readiness only.

[Local validation](../capacity/flash-sale-opening/payment-pool-partition-local-validation-2026-10-04.json). Cloud source remains passing ADR0129f3f511d; no deployment/load/access yet. Local protection does not establish that the one-connection payment allocation sustains the normal cloud workload.

The [prepared unchanged-load comparison](../capacity/flash-sale-opening/payment-pool-partition-control-plan-2026-10-04.json) specifies candidate f421a76, the ADR0129 baseline, all20 mandatory gates and exact source/budget checks. The local source bundle and restricted temporary public key are prepared; helper syntax and bounded-scope checks passed. Cloud deployment, access installation, comparison and failure-path restoration have not been executed or live validated.

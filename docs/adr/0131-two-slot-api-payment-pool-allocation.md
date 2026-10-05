# 0131 - Two-slot payment allocation within the fixed API pool budget

- **Status:** Unqualified after a failed cloud comparison; candidate source restored to ADR0129 baseline.
- **Date:** 2026-10-04
- **Roadmap:** Step 4, protect payment/webhook database capacity.

## Context and evidence

ADR0130's general3/payment1 allocation failed its only unchanged60buyers/s300s/cache0 cloud comparison:17397/18000 journeys completed and603 payment initiations returned HTTP503(3.35%). All other19gates passed. PostTTL counts were17397 payments/bookings/tickets/callbacks and603expiredorders, with no pending orders/payments, double booking or queue/Kafka backlog. Source and service settings were restored to passing ADR0129; temporary access was removed.

Sampled payment queues reached their3waiter limit on all4replicas while general queue peaks were0. Aggregate API acquisition errors rose0to987; mean successful acquisition0.267to15.118ms. These are sampled gauges and aggregate counters, including callbacks and observers, not per-purpose exception attribution. They support changing the allocation hypothesis; they do not establish the cause of every503 or prove that two slots will sustain the workload.

## Decision and supersession

Reuse ADR0130's opt-in pool factory, route selection, aggregate/purpose state gauges, readiness and lifecycle cleanup. Select API_PAYMENT_POOL_MAX=2 for the next candidate with total DB_POOL_MAX=4 and DB_POOL_MAX_WAITING=12: general2/payment2 connections and general6/payment6 waiter slots. Shared mode remains the default0. No connection, waiter, PgBouncer server, replica, load or timeout increase.

This supersedes ADR0130's one-slot candidate allocation. Its source was reverted; reintroducing its mechanisms implements this new allocation decision, not a promotion of its failed result. ADR0103's shared default and total ceiling and ADR0083's bounded total waiter ceiling remain. When enabled, this decision supersedes the single-pool allocation, with those totals retained.

The bounded checkout runner will accept allocation0 or2; it will reject failed allocation1 before transport. Generic configuration continues to validate positive partition maxima against the total budget; lower-level one-slot cases remain useful isolation/recovery controls, not cloud capacity qualification.

## Persistence, locking, messaging, idempotency and TTL

Use the existing Postgres transaction/connection helpers without changes. Preserve ADR0129 payment-derived order lock and fresh payment read, order-to-payment-to-hold-to-sorted-seat lock order, financial writes, callback hash/idempotency, consumer inbox and transactional outbox. No migrations or authority change. Retain ADR0128 reads and ADR0122 writer fairness. No hold, command, cache or callback TTL changes; cache0 stays fixed. No fallback, borrowing, added queue, automatic retry or message delivery change.

## Alternatives, scaling and consequences

Keeping one payment slot already failed the unchanged workload. Increasing total resources or queue deadlines would change budgets and can hide saturation. Priority scheduling or elastic borrowing requires a larger synchronization policy and can weaken the reserved-capacity guarantee. A two-slot static allocation reuses tested mechanisms and is the smallest measured allocation adjustment.

It doubles the candidate payment connection ceiling while reducing general connections3to2. General reads may now saturate; payment bursts may still overflow. Static idle slots cannot be borrowed. Both pools retain min_size1, so enabled idle minimum remains2 rather than shared1. This reserves process-local client/queue capacity only: PgBouncer/RDS servers, locks, CPU, workers, event loop/thread resources and provider failures remain shared limits. No throughput improvement, production protection or300000/hour qualification is assumed.

## Failure and recovery

Invalid partition/queue budgets fail before resource creation. Failed setup and shutdown close both pools. Pool overflow, acquisition timeout and transaction errors preserve their existing HTTP classification, rollback and connection return; no silent fallback or retry. Verify both pools for readiness.

Validate locally against isolated real PostgreSQL/Redis. Hold all general connections and prove payment initiation and callback progress, including real signed HTTP routes. Prove two distinct critical transactions can be in flight together, exact replay/fulfillment and zero double booking. Exhaust both payment connections and all six waiter slots; overflow and timeout must stay explicit while general reads still work. Check total/purpose metrics, authorization, signatures, lifecycle and bounded runner validation.

Any later cloud comparison is separately approved and limited to one60buyers/s300s/cache0 run against passing ADR0129. Retain every financial/durability/postTTL/zero-double-booking/full-keyspacequeue/Kafka/observer/CPU/source/budget/partition/restoration/idle/private-cleanup gate and restrict/45-minute fresh root access with removal. Reuse ADR0040 orchestration and ADR0090 bounded controls. Paid-stage orchestration extensions remain future work.

On any failed gate preserve evidence, finish financial/drain/recovery audits, restore shared baseline source/images/settings, remove temporary access and stop. No higher load, extra run or main merge on failed gates.

## Validation evidence

Decision-time evidence is the executed ADR0130 comparison and its verified recovery. No ADR0131 implementation or test result exists yet; cloud testing has not run.

- [ADR0130 failed comparison and pool evidence](../capacity/flash-sale-opening/payment-pool-partition-control-2026-10-04.json)
- [Passing ADR0129 baseline](../capacity/flash-sale-opening/callback-order-lock-control-2026-10-04.json)
- [Prior partition decision](0130-fixed-budget-api-payment-pool-partition.md)

Executed candidate validation:all614 unit/integration cases passed85.2s,no skips andtwo dependency deprecations. Real initiation/callback transactions ran concurrently while both general connections were occupied;real signed HTTP routes,six-waiter overflow/timeouts,no borrowing,exact replay/fulfillment andzero double booking passed. Initial611passed/3failed run is retained;pool warmup andtest-only timing/snapshot fixtures were corrected beforeclean full rerun. Production deadlines andfinancial/worker code are unchanged. Linux shell/inlinePython andAPI-only Compose0/2mapping passed;owned services removed. Runtime matches ADR0130 candidate byte-for-byte,with transaction/connection helpers unchanged frombaseline. [Local validation](../capacity/flash-sale-opening/payment-pool-two-slot-local-validation-2026-10-04.json). At the local checkpoint no cloud deployment/load/access had run.

The [prepared comparison](../capacity/flash-sale-opening/payment-pool-two-slot-control-plan-2026-10-04.json) identifies candidate7ba8fca,passing ADR0129baseline,one-run60/300/cache0 limits andall20gates. Sourcebundle andfreshrestrictedpublickey areprepared;15helper syntax/scope checks passed. At preparation no cloudaccess/deployment/comparison had run.

Executed cloud comparison: checkout-20261004T050752Z-12380e,60buyers/s300s/cache0 against passing ADR0129. Failed gates retained;candidate reverted andbaseline source/images/servicebudgets restored. Both temporary root keys/localkeyfiles removed;generator idle andprivate cleanup verified. [Comparison evidence](../capacity/flash-sale-opening/payment-pool-two-slot-control-2026-10-04.json). No higherload,push/mainmerge orhourlyproduction capacityclaim.

The customer gate failed:17999/18000 journeys completed,withonepaymentHTTP503 andzero drops/retries. All19othergates passed. PostTTL counts were17999payments/bookings/tickets/callbacks andoneexpiredorder;no pendingorders/payments,doublebooking orqueue/Kafka backlog. Local614-case validation remains historical evidence at7ba8fca;baseline source/tests were restored exactly,not newlyrerun.

Against the failed ADR0130 allocation,onlythe partition setting changed andall10runtime hashes matched. Observed payment503s fell603to1 andaggregateacquisitionerrors987to1;mean successfulacquisition15.118to1.293ms. Bothallocations remain unqualified. The singleacquisitionerror took0.098ms,consistentwithfail-fast rejection (an inference);exceptiontype/purpose were notrecorded andsparse queue samples cannot exclude a brief overflow. Classify/reproduce thisfailure beforeselectinganotherallocation orrecovery policy;do notwaive thecustomer gate. ADR0103/0083 shared allocation remainsactive.

Correction2026-10-04: saved cause/route counters did capture one TooManyRequests on payment initiation. The earlier summary missed them. The local held-two/six-waiter HTTP control reproduced immediate rejection before writes and exact recovery/replay. Historical qualification remains FAILED/reverted. ADR0132 supersedes the candidate waiter allocation for local implementation; cloud shared baseline remains active.

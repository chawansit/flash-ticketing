# ADR 0133: Shared bounded acquisition budget for isolated API pools

- Status: Accepted for local implementation; cloud qualification pending
- Date: 2026-10-04

## Context

ADR0131 rejected a payment initiation at its six-waiter boundary. ADR0132 moved eleven of twelve waiter slots to payments and failed five order-status reads at the remaining single general waiter. Its post-TTL audit confirmed all 18000 payments, bookings and tickets exactly once, but the customer gate still failed. Both candidates were reverted. One-second general queue gauges remained zero despite those read bursts; sampled idle capacity cannot establish burst headroom.

The existing real-DB controls reproduce the six-waiter payment rejection and single-general-waiter rejection. No further cloud experiment is authorized. Installed psycopg_pool3.3.1 source shows FIFO native waiting lists, and expired waiters can remain in those lists until a connection return removes them. A shared logical budget must account for these expired entries as well as live acquisitions.

## Decision and supersession

Keep two physically isolated API pools with general2/payment2 connections and the existing total12 acquisition/waiter budget. Add opt-in API_POOL_SHARED_WAITING, requiring an enabled payment partition and at least as many total acquisition slots as total connections. Shared mode and the proportional legacy partition remain defaults when the option is disabled.

A process-local lock protects a common acquisition budget. Before native getconn, claim one slot without adding an admission wait queue. Either purpose may use unused slots, up to total12 minus the other purpose's connection ceiling: for2+2 pools each purpose can acquire up to10 slots, leaving at least2 for the other. Per-purpose ceilings overlap; they must not be summed as a configured total. Native queues stay positive and bounded at10 each; the common guard enforces at most12 combined live or retained expired acquisition slots.

The guard includes immediately available connection acquisitions, so its bound is conservative. Release the slot when checkout succeeds or an error has no retained native queue entry. A timeout with retained native waiting entries keeps a budget debt until that purpose's native queue is empty. Refresh debt on acquisition, return, metrics and closure. This prevents new attempts from reusing slots still occupied by expired native entries, without touching private library state. No pool connection borrowing, retry, fallback or new waiting scheduler. Native per-purpose FIFO order remains authoritative; cross-purpose progress follows dedicated connections and protected acquisition headroom.

Wrap the public getconn/putconn/connection/close surface used by Postgres. Keep its transaction, constructor, connection and cursor behavior unchanged. Deduct wrapper elapsed time from the native acquisition deadline. Count guard rejections visibly as TooManyRequests and retain existing HTTP503/cause/Retry-After behavior. Expose the true common limit, live acquisition count, retained expired slots and fixed purpose ceilings in pool-state metrics. Default shared-mode/worker metrics retain their original semantics.

This supersedes ADR0132's one/eleven fixed waiter allocation for the next opt-in candidate, without qualifying its failed result. ADR0103/0083 shared defaults and total resource limits remain active in the cloud. No increased connections, replicas, PgBouncer servers, load, deadlines or TTLs.

## Persistence, locking, messaging and idempotency

No schema or authority change. Keep ADR0129 order/payment discovery and locking, fresh payment read, order/payment/hold/sorted-seat lock order, all financial writes, outbox/inbox, signatures and replay keys. Retain ADR0122 reservation-writer fairness and ADR0128 order reads. No worker/message dispatch, hold/cache/command/callback TTL or automatic failure recovery change. The new lock protects only process-local acquisition counts; it is never held across native getconn or SQL.

## Alternatives and consequences

Another static split is simpler but reserves idle waiter slots and has already shifted failures between routes. Increasing budgets or deadlines changes the control. Sharing all slots without purpose ceilings lets one saturated purpose deny the other's dedicated connections. A priority connection scheduler or borrowing physical connections changes synchronization and isolation more broadly. Retrying customer reads would mask the present failure and needs its own recovery decision.

This design shares idle acquisition headroom while retaining physical isolation and a hard combined bound. It can still reject mixed bursts at12 or a single-purpose burst at10, including brief ready-connection attempts. Retained timeout debt deliberately limits admission until native queue cleanup; recovery must prove that it clears. Native ceiling sums are20, but the effective combined live-plus-expired bound is12; metrics and comparison gates must validate that effective bound explicitly. PgBouncer/RDS, CPU and workers remain shared. No throughput or sustained300000/hour claim.

## Failure and recovery

Reject invalid budgets before creating resources. Close partial factory resources and both pools on shutdown; check both for readiness. Release a claimed slot exactly once on success or non-retaining error, preserve timeout debt until queue cleanup, and preserve original exceptions. Closed pools must unblock waiters and clear debt. No stale lease may survive after native queues drain or closure.

Before any cloud comparison, execute controls for static failure versus shared general7/payment5 and general5/payment7 bursts, both directions of protected headroom, combined12 overflow, single-purpose10 overflow, timeouts/retained entries/recovery, FIFO progress, close/setup failure, signed HTTP authorization, callback/consumer replay, exact counts and zero double booking. Keep original failure controls and full financial integration checks.

Any later comparison needs fresh approval for one unchanged60buyers/s300s/cache0 run with restricted45-minute root access and removal. Reuse ADR0040 orchestration and ADR0090 controls; paid-stage orchestration extensions remain future work. Retain all20 financial/durability/postTTL/zero-double-booking/full-keyspacequeue/Kafka/observer/CPU/source/budget/isolation/restoration/idle/private-cleanup gates, validating shared12 and purpose10 ceilings on every replica. On failure finish audits, restore passing ADR0129 source/images/settings, remove access and stop. No additional run, higher load, push or main merge.

## Validation evidence

At decision time only the linked executed static controls and installed native pool source inspection support this choice. The shared guard is not implemented or tested yet. Cloud baseline remains unchanged, with no temporary access.

- [ADR0132 failed comparison and cause counters](../capacity/flash-sale-opening/payment-pool-waiter-allocation-control-2026-10-04.json)
- [ADR0132 executed local validation](../capacity/flash-sale-opening/payment-pool-waiter-allocation-local-validation-2026-10-04.json)
- [ADR0131 failure reproduction](../capacity/flash-sale-opening/payment-pool-queue-overflow-diagnosis-2026-10-04.json)
- [Passing ADR0129 control](../capacity/flash-sale-opening/callback-order-lock-control-2026-10-04.json)
- [Superseded allocation](0132-explicit-fixed-budget-payment-waiters.md)

Executed local candidate validation:645 unit/integration cases passed106.51s, no skips, two dependency deprecations. Focused22 cases passed9.70s after a retained collection-name collision was corrected. Mixed signed HTTP bursts, both directions of protected headroom, combined/per-purpose overflow, retained timeout debt and recovery, FIFO progress, concurrent critical transactions, closure and exact financial replay/zero double booking passed. Offline shell/Python/Compose checks passed and owned services were removed. Financial/worker code and Postgres constructor/transaction/connection/cursor/close AST match passing ADR0129. [Local validation](../capacity/flash-sale-opening/shared-acquisition-budget-local-validation-2026-10-04.json). No cloud access/load.

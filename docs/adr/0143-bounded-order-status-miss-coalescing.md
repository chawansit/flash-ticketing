# ADR0143: Bounded coalescing of advisory order-status cache misses

- Status: Implemented and locally validated; cloud deployment/qualification pending
- Date: 2026-10-04

## Context

ADR0142 reproduced CPU saturation and callback delay on the pinned historical baseline: 118,580 order-status requests, 74.942 ms mean API pool acquisition and 99.192% offered sampled host CPU. It did not establish the initiating cause. The 2026-10-04T15:54:48Z read-only recovery snapshot now accounts for all 16,483 successful simulated payments as 13,204 fulfilled plus 3,279 refunded, with no refund pending and empty global queues/Kafka. This does not pass the original failed customer/financial gates.

ADR0097 already provides an optional actor-scoped Redis read-through snapshot, default disabled, with Redis server-time absolute freshness deadlines and bounded PostgreSQL fallback. Simultaneous misses can all execute the same authorized PostgreSQL read. Coalescing targets this duplicate work; distinct orders still need independent reads and this change alone cannot solve unique-buyer polling or callback contention.

## Decision

Before implementing, select process-local bounded coalescing around a cache miss/refill. Keep the existing initial validated cache hit fast path. On a miss with a usable Redis timestamp, join or create an in-flight entry keyed by the existing hashed actor/order cache key. Leaders and followers recheck Redis before falling back to the existing authorized database read. A leader wakes followers and removes its owned entry in finally, including database/cache errors. Followers share no raw result or exception: every cache response still passes the existing owner/schema/server-time/TTL validation.

Bound each cache adapter to 128 active entries, eight waiting followers per key, 128 waiting followers overall and 100 ms maximum follower wait. Limits or timeout fall back to the existing bounded database path; they do not acquire additional connections, retry mutations or fabricate success. No background threads, distributed locks, additional Redis keys or retained response objects. Redis lookup errors retain the direct bounded database fallback without coalescing or another Redis attempt. An extra Redis recheck on healthy misses prevents a just-completed fill race; retain the original earliest timestamp for conservative freshness accounting.

Only the existing ORDER_STATUS_CACHE_MS option enables the cache and this behavior. Default remains zero. No cloud cache activation or load stage is part of this local implementation. Add bounded metric outcomes without actor/order labels. Preserve the current HTTP response and polling contract; event-driven cache publication and client jitter are separate future decisions.

## Alternatives

- Cache misses with independent database reads: current behavior can duplicate authorized reads.
- Redis lease/distributed single-flight: coordinates across APIs but adds failure ownership and lease complexity; not needed for this local guard.
- Share a leader's raw row/exception: can bypass existing freshness checks and introduces shared mutable payloads.
- Unbounded waiting or per-key locks: can retain arbitrary entries and occupy every request worker.
- Enable the cache, extend freshness/hold TTL, raise connections or change callback slots simultaneously: confounds qualification.
- Prioritize callbacks or move background services: credible separate measured mitigations, not this factor.

## Consequences

Duplicate misses in one process may avoid database transactions; every healthy miss adds a Redis recheck. Cross-process and distinct-order misses are not coalesced. Bounded followers occupy synchronous request threads temporarily, so this is a locally validated candidate, not demonstrated cloud capacity. Pool admission, thread scheduling and CPU saturation remain relevant. Timeout/overflow/Redis outage can still generate bounded PostgreSQL pressure and explicit 503 responses.

## Persistence, locking, messaging, idempotency, TTL and scaling

PostgreSQL remains the sole payment/booking/ticket authority. No transaction, schema, seat lock, atomic Redis hold, outbox/Kafka delivery or callback idempotency changes. No hold, command, callback or advisory snapshot TTL increase; hits never extend the absolute deadline. Process-local metadata coordinates display reads only. No machine, API worker, pool, acquisition or simulator capacity changes. Extend ADR0097 only for bounded process-local miss coordination; retain its default-disabled freshness/ownership/fallback contract and ADR0124's qualification limits. No other accepted decision is superseded.

## Failure and recovery behavior

Leader failure or cancellation always removes its own entry and wakes followers; errors/negative authorization are not cached. Followers recheck Redis and independently use the existing authorized store when no fresh snapshot exists. Slow leader work may outlive the bounded wait, but waiting does not hold a database connection and all metadata is bounded. Process restart loses only advisory in-flight metadata. Redis errors cannot release seats or authorize payments. Cache disablement restores the unchanged direct database path.

## Validation evidence

This decision is recorded before code changes. Planned local checks: deterministic concurrent cold reads use one authorized database query/fill; actor/order isolation; entry and follower caps; deadline timeout; leader exception/cancellation cleanup; stale/failed fill cannot be shared; hits/clock/freshness validation and disabled behavior remain intact. Execute real PostgreSQL/Redis integration tests for coalesced reads, committed payment-to-ticket state after expiry, ownership rejection, replay and atomic seat contention. Run the focused checks and appropriate regression suite on isolated owned native-Linux containers; verify source/locked dependencies, retain logs and remove only owned resources. No cloud test, higher RPS, production capacity or GitHub publication is implied by local validation. Executed results will be appended.

### Executed local validation

Windows focused tests: 43 passed in 0.37 seconds after correcting two exception/cancellation test assertions (initial run: 41 passed, two failed). Changed-file lint passed. The first generated native validation helper had an indentation error and was rejected before containers or tests; it was repaired and syntax-checked.

Verified source and locked dependencies on isolated native Linux: 121 focused tests passed in 26.26 seconds, then all 739 unit/integration tests passed in 96.31 seconds, with no skips. The focused real PostgreSQL/Redis cold-read test confirms eight simultaneous owner/order readers use exactly one authorized store read and cache fill, release the database connection before cache use and reject another actor. Atomic hold contention, reservation replay, payment commit recovery, bounded payment/general pools and shared acquisition-budget regressions passed. This is correctness evidence for the controlled local case, not production throughput evidence.

Owned PostgreSQL, Redis and runner containers/network/context were removed and absence verified. Existing local services were untouched. No cloud source/settings/load, cache activation, SSH-key installation, GitHub push or main merge occurred. The fresh password-only ADR0142 accounting snapshot is recorded separately in that failed control's evidence; all successful simulated payments are tickets or refunds, but its failed customer/financial ticket gates remain failed.

Before any cloud comparison, isolate the factor: both control and candidate need the same enabled-cache age and underlying runtime/harness; the latest ADR0142 cloud control had cache disabled. Do not bundle this with the retained cloud-unqualified seat-projection implementation. Keep all connection/acquisition/simulator budgets, workload, client deadlines and financial/uniqueness/full queue/Kafka gates. Event-driven committed cache publication, polling jitter, callback priority and moving workers remain separate future factors.

[Local validation evidence](../capacity/flash-sale-opening/order-status-coalescing-local-validation-2026-10-04.json).

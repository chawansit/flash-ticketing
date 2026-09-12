# ADR 0030: Keep-seatmap-hot-path reads alive by TTL touch

Status: Accepted

## Context

During long and concentrated availability workloads, `SEATMAP_WARMING` rose while traffic was still present. Investigation showed some seat-map keys were being garbage-collected by TTL and then re-generated only by background reconciliation windows. Meanwhile, read-heavy traffic continues to hit the same seat-maps every few seconds, so evicting those keys adds avoidable cold misses and load spikes.

Existing pattern relies on periodic full or delta reconciliation to refresh the Redis TTL. This is correct for correctness, but it is too rigid under high read QPS when reconciliation is briefly delayed by DB pool contention.

## Decision

Use a read-side TTL refresh path (`TOUCH`) and update seat-map metadata on successful reads:

- Add a Redis-side Lua script that verifies the map has valid metadata (`version` present, `updating` absent) and then `EXPIRE`s the hash key for the configured seat-map TTL.
- Refresh TTL inside the existing atomic `BROWSE` Lua operation for successful HTTP reads (`200` and `304`), so the high-volume path still uses one Redis round trip. `RedisSeats.read()` uses the guarded touch helper after a valid internal read.
- Keep update behavior unchanged: full rebuilds refresh TTL; delta patches deliberately do not, so they cannot postpone bounded full reconciliation.
- Keep 30-second semantic default so reconciliation interval still guarantees a hard freshness bound.

This keeps hot maps in Redis under steady polling without weakening write semantics.

## Alternatives considered

1. **Immediate DB fallback on every `SEATMAP_WARMING` request**

   Rejected because the API contract is intentionally advisory and DB fallback would shift reads to synchronous query latency and add contention on hold-heavy load.

2. **Disable TTL on seat-map keys**

   Rejected because it weakens stale-data recovery signals during partial-cache corruption and increases memory pressure for stale events.

3. **Enlarge TTL globally without read-touch**

   Rejected because it delays recovery after partial corruption and does not protect against delayed reconciliation when nodes restart or memory pressure forces evictions.

## Consequences

- **Pros**: fewer cold reads under steady polling, reduced likelihood of `SEATMAP_WARMING` bursts when background reconciliation is delayed.
- **Cons**: each successful browse mutates key expiry, increasing Redis command-side write work and replication/AOF traffic even for `304` responses. It does not add another network round trip to the HTTP read path.
- **Failure behavior**:
  - If `TOUCH` fails (Redis transient), read still succeeds on current map data.
  - If map is missing/dirty, `SEATMAP_WARMING` behavior remains unchanged.

## Failure/recovery behavior

- If a read comes with missing metadata, API still returns `503` and should be re-attempted by the client workload.
- If map is marked `updating`, read still blocks on that transition to avoid exposing partial state.
- Redis-side errors in touch/read still degrade to `503` only when actual read cannot be returned.

## Validation evidence

- Unit/integration tests for cache invariants and touch-insensitive correctness to be covered by existing seatmap integration tests plus load-suite behavior:
  - Lower `missing_or_stale` count during prewarm phases when traffic continuously reads warm maps.
  - Lower `generator_drops` with similar seat-polling distribution and no added warm failures.
  - Observer `minimum_ttl` should remain positive during stable reads.

Local implementation validation on 2026-09-12: the focused TTL/reconciliation regression set passed
(3 tests), the full PostgreSQL/Redis integration suite passed (57 tests), the unit suite passed
(68 tests), and Ruff passed. Production-style read-load comparison is still pending and must not be
inferred from these correctness checks.

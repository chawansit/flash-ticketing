# ADR 0075: Bind seat-map delta cursors to cache incarnation

Status: Proposed

## Context

ADR 0071 introduced bounded versioned seat-map deltas using an integer `since` cursor. Redis already assigns each seat-map hash an `incarnation`, but the availability response does not expose it and the delta request does not send or validate it. A cache loss and durable rebuild can therefore lower the numeric aggregate version while creating a new history chain. An old client cursor can be greater than the rebuilt version (`ahead`) or can numerically fall inside a later range from the new incarnation (`history_overlap`). In the second case the server cannot tell that equal-looking version numbers belong to different histories.

The isolated 60-second Huawei diagnostic `20260928T145309Z-01eafd08` at 1,000 offered RPS recorded 5,860 `history_overlap`, zero `history_missing`, 4,628 `ahead` and 1,393 `tail_gap` resets. A read-only audit of all 800 current Redis histories found zero internal overlap or missing edges. All 3,579 acknowledged holds were durable, overlapping booking intervals were zero and queues drained. The defect is therefore cursor identity across rebuilds rather than missing durable reservations or an internally broken current history chain.

## Decision

Bind every delta cursor to both the Redis seat-map incarnation and its integer version.

1. Availability snapshots and delta responses expose the current `incarnation` alongside `version`.
2. Delta requests accept the snapshot incarnation with `since`. The Redis Lua read validates incarnation before comparing versions or walking history.
3. A missing or different incarnation returns the existing fail-safe full reset response with reason `incarnation_mismatch`. The response carries the current incarnation and version, which the client must use for its next request.
4. The development load generator stores cursors per viewer and show as `(incarnation, version)` and always sends both after snapshot bootstrap.
5. The existing integer version and bounded delta history remain unchanged within one incarnation. PostgreSQL remains authoritative and Redis rebuilds remain allowed to select a lower aggregate durable version.

The incarnation query is optional at the HTTP parsing layer for migration safety, but omission deliberately produces a full reset. This preserves a successful response for older clients while making their performance degradation visible until they adopt the composite cursor.

This decision supersedes ADR 0071 only where it defines the client cursor as an integer version alone. Its bounded history, full-reset recovery and per-client state model remain accepted. ADR 0074 remains proposed; a fair retention comparison must be repeated after cursor ambiguity is removed.

## Alternatives considered

### Preserve a globally monotonic numeric version in Redis

Rejected as the primary identity mechanism. A separate counter can also be lost during a managed cache restore or flush, and keeping it durable would add a new database write or coordination dependency to the read projection path. Incarnation already models exactly the history boundary that clients need.

### Encode incarnation and version into one opaque cursor string

Deferred. It provides a cleaner future API and freedom to change cursor internals, but replacing the existing `since` contract would enlarge this correction. Two explicit fields are sufficient for the MVP and remain clear in OpenAPI.

### Keep integer cursors and treat every overlap as a reset

Rejected because that is the current behavior. It is safe for correctness but cannot distinguish expected rebuild recovery from a corrupt current history and causes repeated full-map responses when old and new version ranges collide.

### Prevent cache expiry or rebuild

Rejected. Redis keys can still disappear through failover, flush, operator action or capacity failure. Recovery must remain correct across a new cache incarnation rather than relying on uninterrupted cache lifetime.

## Consequences

- A client receives at most one required full reset when its map incarnation changes, then resumes bounded deltas on the new history.
- Old clients that omit incarnation continue to receive correct full snapshots but cannot obtain the delta-path performance benefit.
- Response payloads and OpenAPI gain one stable string field. Delta requests gain one optional query parameter.
- Metrics can classify an incarnation transition directly rather than inferring it from `ahead`, overlap or tail gaps.
- Numeric versions remain meaningful only within their incarnation.

## Failure and recovery behavior

If a map is missing or marked as updating, the endpoint keeps returning `SEATMAP_WARMING`. After reconciliation rebuilds it, the new incarnation causes a full reset even when its numeric version overlaps the old range. If Redis preserves the map and history, matching-incarnation requests continue through the bounded delta chain. If a client loses its cursor incarnation, omission fails safe to a full reset and supplies a new composite cursor. No change is made to atomic holds, PostgreSQL persistence, outbox delivery, expiry or reconciliation recovery.

## Validation evidence

Before implementation, the following evidence is established:

- Run `20260928T145309Z-01eafd08` completed 59,672 of 60,000 scheduled requests, with 328 generator drops, read p95 430.933 ms and hold p95 1,021.358 ms.
- It had no transport errors or admission rejections; 3,579 holds were durable, overlap was zero and all queues drained.
- Reset classification was 5,860 `history_overlap`, zero `history_missing`, 4,628 `ahead` and 1,393 `tail_gap`.
- A post-run audit of 800 Redis histories found zero anomalous histories, zero internal overlap edges and zero missing edges.

Acceptance requires unit and integration coverage for matching, missing and changed incarnation; unchanged delta continuity inside an incarnation; a short cloud diagnostic with zero `history_overlap` and zero `ahead` for composite-cursor clients; exact durability, zero booking overlap and drained queues. A later full capacity run remains required before accepting a production capacity claim.

### Local implementation evidence

The availability and delta response schemas now expose `incarnation`; delta reads validate it before numeric version continuity, and the load generator carries `(incarnation, version)` per viewer and show. Missing or changed incarnation returns the existing full-reset response and the current composite cursor. Focused API, bootstrap, browse and Redis-first coverage passed 31 tests. After correcting one stale mock discovered by the first full run, the complete unit/integration suite passed 257 tests with two dependency deprecation warnings. Repository-wide Ruff passed with cache disabled and the documented Windows executable-bit artifact ignored. Cloud validation remains pending, so the ADR remains Proposed.

### First cloud validation

Run `20260928T151516Z-5e6057c2` at revision `7bdabe0` exercised composite cursors at 1,000 offered RPS for 60 seconds. It completed 59,999 of 60,000 scheduled requests with one bounded generator drop, no transport errors and no admission rejections. Worst-worker read p95 was 354.453 ms and hold p95 was 767.069 ms. All 3,600 acknowledged holds became durable, overlapping hold intervals were zero and all queues drained.

The run did not satisfy this ADR's reset acceptance condition. Clients observed 12,193 reset snapshots. Scrape-aligned counters recorded 6,152 `history_overlap`, zero `history_missing`, 4,577 `ahead` and 1,464 `tail_gap`; these categories account for every observed reset, so `incarnation_mismatch` was zero. The result confirms that the generator and server carried matching incarnations, but it also proves there is a second same-incarnation cursor/range concurrency problem. Composite cursor identity remains necessary for rebuild correctness but is insufficient to meet the performance gate by itself.

The run therefore leaves this ADR Proposed. No production-capacity increase is claimed. The next measurement must capture map version, incarnation and delta range edges while the load is live; the retained history keys had expired by the time a post-run detailed audit was attempted. Any decision to relax strict range equality requires a separate ADR because it changes delta replay semantics.

### Live chain diagnostics after the first cloud validation

A second 60-second diagnostic, run `20260928T153734Z-fcf61515` at revision `0caa087`, observed 12,417 reset snapshots out of 56,025 delta responses. It dropped 396 scheduled requests; read/hold p95 were 421.422/952.746 ms, so the capacity gate failed. All 3,579 acknowledged holds were durable, booking overlap was zero and queues drained. A bounded rotating Redis observer sampled recent delta edges during the run and found repeated overlapping ranges, map-version regressions under an unchanged incarnation, and delta tails beyond the map version. These are repeated sample observations, not counts of unique corrupt keys.

To rule out non-atomic observer reads, run `20260928T155410Z-c8f6469b` used an atomic Redis Lua sample for each map and separately compared a later plain read. Atomic samples still observed 5,150 overlapping edges, 538 same-incarnation regressions and 5,950 tail mismatches; no plain read was behind its immediately preceding atomic sample. A separate isolated DCS read-after-write probe returned zero stale reads in 1,000 iterations, both at rest and during this stage, and reported the master role. This narrows the issue to state mutation, managed failover/rollback or another server-side consistency path; it does not yet prove which. The load itself completed all 30,000 requests with zero drops and 1,800 durable holds, zero booking overlap and drained queues, but the orchestrator lost its transient launcher PID and marked the stage failed. Therefore its 61.270/118.581 ms read/hold p95 values are diagnostic only and cannot certify capacity. ADR 0076 addresses the generator supervision race.

Before changing delta replay semantics, a future controlled run must correlate atomically sampled map/history regressions with DCS server identity changes and confirm the generator process lifecycle. ADR 0075 remains Proposed.

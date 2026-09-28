# ADR 0079: Omit zero-length bootstrap seat-map delta entries

Status: Accepted; short cloud observation passed, sustained capacity remains unproven

## Context

After ADR 0078 deployed a coherent reconciler image, a controlled 1,000 RPS, 30-second cloud stage observed zero aggregate-version regressions, history overlaps and tail mismatches. Its atomic Redis observer still counted 25,967 repeated observations of nonpositive delta ranges. Inspection of the current `PUT` Lua shows that initial full snapshots of untouched seats publish `from_version=0, version=0`: the seat-map body changes from absent to present, but its aggregate cursor does not advance because all PostgreSQL source versions are zero. The observer samples the same inert entry repeatedly, so the count is not the number of distinct corrupt writes.

ADR 0071 requires contiguous versioned changes. An edge that does not advance the cursor cannot be used as a delta. It occupies bounded history memory and makes chain diagnostics noisy. Initial clients already obtain a full `/availability` snapshot.

## Decision

In the atomic Redis `PUT` script, append a delta-history entry only when at least one selected seat state changed **and** the resulting aggregate version is greater than the prior version. Continue publishing the full snapshot, layout, incarnation and TTL atomically as today. A zero-version bootstrap leaves no delta key; `since=version=0` correctly yields an empty change set. The first later advancing patch creates the `0→N` edge. This narrows ADR 0071's “every atomic mutation” wording to mutations that advance the aggregate cursor; all other ADR 0071 decisions remain in force.

## Alternatives considered

- Keep the `0→0` entry and exclude it from diagnostic counts: rejected because it still occupies history and is not a valid advancing edge.
- Assign a synthetic version 1 to every initial snapshot: rejected because this changes cursor semantics and every existing source-version comparison.
- Filter zero-length edges on reads: rejected because it leaves unnecessary writes and memory in Redis.

## Consequences

Untouched maps have no delta-history key until the first advancing mutation. This saves one initial ZSET entry per show. The initial snapshot contract and client cursor remain unchanged. A map whose cursor remains unchanged but whose seat body is repaired must be consumed through an initial or reset snapshot, not as an invisible delta; the current source-version fence makes that case exceptional and validation must cover it.

## Failure and recovery behavior

Redis Lua still updates the map and any advancing delta in one atomic invocation. If Redis rejects the script, no partial map/history state is published. After a cache loss, a new incarnation and full snapshot still force a client reset. PostgreSQL remains authoritative for seat ownership, and reconciliation can rebuild the map. No reservation, TTL, Kafka, payment or durability behavior changes.

## Validation evidence

The coherent-reconciler cloud stage at revision `c8b3963` passed 30,000/30,000 requests, 1,800/1,800 durable holds, zero booking overlap, queue drain and rollback. Its observer saw 427 samples, zero regressions/overlaps/tail mismatches, and 25,967 repeated nonpositive bootstrap-edge observations. The real-Redis test now proves a zero-version bootstrap has no delta entry and a later advancing patch creates exactly the `0→2` edge returned without a reset. The complete incremental and browse integration suites passed: 21 tests. The focused deployment plus bootstrap suite passed: 16 tests. Ruff passed for changed Python files (ignoring the Windows executable-bit artifact). A controlled Huawei RDS/DCS rerun at revision `8020a05` delivered 30,000/30,000 scheduled requests at 1,000 RPS for 30 seconds with zero drops, transport errors, retries or admission rejections. Worst-worker read/hold p95 were 11.557/24.630 ms. All 1,800 acknowledged holds were durable, booking overlap was zero, all fixed-audit queues drained and rollback passed. The atomic DCS observer collected 426 samples with zero nonpositive ranges, version regressions, overlaps, gaps or tail mismatches; DCS server identity did not change. The 800 synthetic sale windows were retired without row deletion. Sustained capacity remains unverified. The compact result appears in [`docs/capacity/huawei-rds/2026-09-28-1000-coherent-reconciler`](../capacity/huawei-rds/2026-09-28-1000-coherent-reconciler/README.md).

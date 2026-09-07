# ADR 0009: Measured database work and incremental seat projections

Status: Accepted; implemented and validated locally on 2026-09-07.

## Context
ADR 0008 coalesces notifications but still rebuilds every seat for each refresh.
Transaction timing does not distinguish pool acquisition from SQL execution.

## Decision
Instrument SQL execution with bounded command labels, pool acquisition including
failures, pool occupancy, and worker operation busy time. Sample database connections,
blockers and lock waits under increasing load. Query duration includes network and
PgBouncer wait; it is not server-only execution. Samples miss short NOWAIT conflicts,
so retain SQLSTATE error counters too. No SQL literals or customer IDs in metrics.

Carry changed seat IDs in the durable coalesced request. Read only those rows and
merge into a Redis hash atomically using per-seat source versions. Map version is
the sum of cached source versions; changed seats receive its new aggregate marker.
Keep full reconciliation every five seconds and the 30-second TTL, extended only
by full reconciliation. Missing cache or legacy messages force a full snapshot.
Use a new v2 cache key; migrate then restart API/workers together and allow warming.
Partially supersedes ADR 0008 full-snapshot-per-dirty-event behavior. PostgreSQL
ownership, hold TTL, payment idempotency, outbox and Kafka semantics are unchanged.

## Alternatives
Full snapshots retain O(inventory) refresh work; JSON patching still decodes and
rewrites the whole blob; CDC adds operational scope. Hash writes are proportional
to changed seats. Full map reads and periodic reconciliation remain O(inventory).

## Consequences
Hash memory overhead and accumulated dirty-seat IDs. Worker busy time includes I/O,
not CPU utilization. Compare fixed inventories and identical offered rates. Inventory
deletion is unsupported and requires an explicit cache rebuild. Mixed old/new cache
workers are not a supported rolling deployment.

## Failure and recovery
Intent and inbox commit together. Redis failure leaves work dirty under a finite
lease. Crash after Redis write is safe to replay. Token-fenced acknowledgements cannot
clear newer work. Source-version checks prevent duplicate/out-of-order regression,
including racing full snapshots. Patches cannot create incomplete maps after expiry.
Versions after cache loss rebuild from SQL; clients refetch on warming/invalid-version.
Redis remains advisory. Lua errors can leave partial writes even though scripts exclude
other commands while running. An in-progress hash marker therefore makes interrupted
writes unreadable and rejects patches; full reconciliation recomputes the version
from merged source rows and clears the marker only after completing all writes. Rollback application together to old cache format; leave
additive migration installed and let periodic snapshots warm the old key.

## Validation evidence
The final container suite passed 52 tests (11.66 seconds, two dependency warnings).
New coverage includes concurrent/out-of-order patches, stale full snapshots, cache
expiry, overlapping durable generations, crash after Redis write, partial-write
repair, missed-notification reconciliation, actual SQL lock conflicts and exhausted
connection pools. The existing real HTTP 100-contender test still has one winner.
Partial Lua failure is modeled by writing its interrupted state, not an induced OOM.
Nine 30-second reservation stages at 10/50/100 offered RPS and isolated 3k/10k/50k
inventory probes were executed. Final 3k refresh p95: full 47.97 ms, patch 7.14 ms.
Large full-refresh timeouts and noisy end-to-end results are retained, not declared
passes. See [report](../capacity/incremental/README.md) for evidence and limitations.

Partially superseded by [ADR 0011](0011-bounded-reconciliation-scheduler.md): "keep full
reconciliation every five seconds" is replaced by a bounded per-event schedule over active
events. Changed-seat patching, per-seat source versions, the `seatmap:v2` hash and the
30-second TTL extended only by full reconciliation all remain unchanged.

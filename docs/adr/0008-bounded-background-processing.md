# ADR 0008: Bounded background work and durable coalesced cache refresh

Date: 2026-09-07
Status: Accepted; implemented

Shared-loop reconciliation placement is superseded by [ADR 0016](0016-isolated-reconciliation-worker.md); ownership, fencing and bounded-work decisions remain accepted.

## Context
The measured 50-RPS independent-seat run left 2,091 unconsumed events. Each SeatsChanged message rebuilds a full seat map. Serial simulated callbacks also build backlog at five checkout journeys/sec. PostgreSQL ownership remains authoritative; improving HTTP acceptance alone is insufficient.

## Decision
1. Replace synchronous per-message cache rebuilds with a durable PostgreSQL refresh request keyed by event ID. The consumer commits the request generation and inbox entry together. Maintenance leases dirty rows, commits the lease, reads current inventory, writes the Redis versioned snapshot, and acknowledges only the captured generation with the matching lease token. Changes arriving during refresh remain dirty.
2. Apply a 250-ms per-event refresh cooldown after a successful queued refresh. Multiple changes combine into one snapshot. Keep the existing five-second periodic rebuild as a recovery/warmup path; Redis snapshots retain their 30-second TTL. These intervals are scheduling targets, not guaranteed freshness SLOs.
3. Publish up to 32 leased outbox rows per iteration. Enqueue sends before waiting for acknowledgements, with a shared ten-second acknowledgement budget. Mark only acknowledged rows published, fenced by the lease token. Unsent, uncertain and failed rows retain the finite 30-second lease and are retried with stable IDs.
4. Dispatch simulated callbacks with four bounded threads sharing the existing pool. Wait for every submitted job before starting the next batch. Existing per-payment 15-second leases and idempotent callbacks protect against races/restarts. No provider call runs inside a SQL transaction. Keep one worker container per role and existing API/pool limits.

## Supersession
Partially supersedes ADR 0003's synchronous Redis refresh before inbox acknowledgement: inbox now acknowledges durable refresh intent, not completion of the Redis projection. Partially supersedes ADR 0002's one-row publication scheduling and ADR 0006's serial simulator execution. Their transactional outbox, at-least-once delivery, PostgreSQL seat locking and uniqueness decisions remain in force. ADR 0005's business hold TTL is unchanged.

## Alternatives
Incremental per-seat Redis hashes require changing the read/delta representation and version contract; defer them. In-memory debouncing alone loses work on crash. Kafka batch consumption complicates offset recovery; retain one-record polls and existing commit/DLQ behavior. Unbounded threads/queues or merely adding API replicas can overload the shared database.

## Failure and recovery
A Redis write failure cannot acknowledge the refresh. A crash after the write but before SQL acknowledgement repeats a version-fenced snapshot after lease expiry. A newer request cannot be erased by an older completion. A stale owner cannot acknowledge another lease. Periodic rebuilds can race queued rebuilds safely under Redis version checks. Dead letters still cover failure to persist consumer intent; Redis outages leave durable dirty work rather than falsely marking the cache current.
Partial Kafka acknowledgement may cause redelivery but never publication marking without acknowledgement. Expired leases can overlap workers; stable event IDs, inbox deduplication and token fencing preserve effects.
Concurrent callback dispatch preserves one active lease per payment. On HTTP or acknowledgement uncertainty, retry after lease expiry; delivery counters cannot advance with a stale token.

## Consequences and measurement
Seat-map freshness is eventual. Add pending-refresh count and oldest-dirty age metrics. Benchmarks must observe this queue and compare Redis snapshot version against SQL; an empty inbox backlog is no longer evidence of cache completion. Evaluate the same 3,000-seat/50-RPS/60-second reservation workload and 300-seat/5-checkout-per-second/60-second workload before and after, with an idle queue at stage start, on the same machine. Retain failed results. Do not infer production capacity from these tests.

## Validation plan
Test coalescing, duplicate intent, rollback, changes during refresh, failed Redis writes, stale leases, partial publisher acknowledgement, simulator lease exclusivity and callback failure recovery. Run existing concurrency/payment/expiry tests plus full HTTP/Kafka checkout. Record executed results and any limits here after measurement.


Refresh-age refinement: after acknowledging a generation while newer changes remain, reset the dirty-age lower bound to the lease claim time. Newer generations arrived after that claim; completed work must not inflate the age or monopolize oldest-first scheduling. The concurrent-update test checks this advancement.

## Executed validation

46 container tests passed with no skips; 12 targeted integration cases also passed after the refresh-age refinement. Matched 5-checkout/sec ticket p95 improved from 20.33 s to 0.381 s. The 50-RPS projection drain changed from a failed 60-second window to 0.781 s. The final build completed 1,200 checkouts at 10/sec with 0.313 s ticket p95. The real Kafka outage drill passed and final queues were empty. See [full evidence and limits](../capacity/optimized/README.md). These are local measurements, not production capacity guarantees.

Partially superseded by [ADR 0009](0009-incremental-seat-projection.md): routine refreshes use versioned changed-seat updates; durable generations and leases remain.

Decision 2's five-second periodic rebuild is partially superseded by
[ADR 0011](0011-bounded-reconciliation-scheduler.md): proactive reconciliation is now a
bounded, leased, per-event schedule over events inside their sale window. The 250-ms refresh
cooldown, durable generations, lease-token fencing and the 30-second TTL remain in force.

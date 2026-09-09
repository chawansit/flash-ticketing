# ADR 0011: Bounded, fair reconciliation scheduling for seat availability

Integration note: numerical results below were supplied by Claude on a different host and the original patch, not measured on this computer. The integrated implementation checks deadlines before each snapshot and counts successful acknowledgements separately. Throughput multiplied by TTL is not a safe active-show ceiling: the supplied 6,000-show run already breached TTL.

Date: 2026-09-08
Status: Accepted; implemented and validated locally on 2026-09-08

Shared-loop reconciliation placement is superseded by [ADR 0016](0016-isolated-reconciliation-worker.md); ownership, fencing and bounded-work decisions remain accepted.

## Context

Maintenance rebuilt every retained event's entire seat map roughly every five seconds:
`SELECT id FROM events` with no filter, then a full `snapshot()` per row, executed inline
in the maintenance loop. Three problems follow.

1. The work is O(total retained inventory), not O(active inventory). It grows forever as
   past shows accumulate.
2. It is unbounded and inline. A long pass blocks hold expiry and dirty-seat refresh for its
   whole duration. Measured at 6,000 shows of 300 seats: **19.75 seconds per pass** against
   a five-second target period. A statement timeout part-way through abandons the remaining
   events, and the next pass restarts from the beginning, so the same early events are
   refreshed repeatedly while later ones may never be refreshed at all.
3. It has no notion of due work, ownership or failure. Multiple maintenance instances
   duplicate the entire sweep, and one slow or failing event delays every event behind it.

Target deployment is 200 locations, 4–10 screens each, ~300 seats per screen, with separate
availability per scheduled show. The schema models scheduled inventory as `events` only; it
has no location, screen, seat-geometry or show-end column.

Constraint that shapes everything: the Redis seat map carries a 30-second TTL, and per
ADR 0009 **only a full snapshot extends it**. Proactive reconciliation is therefore both a
correctness mechanism and, incidentally, the thing keeping maps alive.

## Decision

Replace the periodic full-inventory sweep with a durable, bounded, per-event schedule.

1. **Additive schedule table.** `event_reconciliation(event_id PK, next_due_at,
   last_reconciled_at, consecutive_failures, lease_until, lease_token, claimed_at)` in
   migration 004. It is advisory scheduling state derived from `events`, never an authority
   for seats, holds or orders, and is safe to truncate.

2. **Active-event definition using existing fields only.** An event is proactively
   reconciled from `sale_starts - RECONCILE_WINDOW_SECONDS` until
   `sale_ends + RECONCILE_WINDOW_SECONDS` (default 300 s each side). No show-end, screen or
   location field is invented. The lead window warms a map before sale opens; the trailing
   window lets late hold expiry converge after sale close. Events outside the window are not
   proactively reconciled and are still repaired reactively through `seat_refresh_requests`
   when a SeatsChanged notification arrives.

3. **Bounded seed, prune and sampling.** `maintain_schedule` runs at most once per second:
   it inserts up to `RECONCILE_SEED_BATCH` missing active rows, deletes up to the same number
   of unleased rows whose event has left the window, and samples backlog, tracked count and
   overdue age with capped counts. Seeding converges over iterations rather than in one scan.

4. **Bounded, token-fenced claims.** `claim_due_events` selects at most
   `RECONCILE_BATCH_SIZE` (default 8) due, unleased rows ordered by `next_due_at, event_id`
   under `FOR UPDATE OF r SKIP LOCKED`, stamps a fresh lease token and
   `RECONCILE_LEASE_SECONDS` expiry, and commits. Ordering by oldest deadline is the fairness
   rule; `SKIP LOCKED` plus the lease is the mutual-exclusion rule for multiple instances.

5. **No SQL lock held during Redis I/O.** The claim transaction commits before any
   `snapshot()` call. Each acknowledgement opens its own short transaction and is fenced by
   `lease_token`, so a worker that lost its lease cannot move another owner's deadline.

6. **Per-event isolation.** Every event in a batch is projected inside its own try/except.
   A failure records `consecutive_failures + 1`, applies bounded exponential backoff
   (`RECONCILE_BACKOFF_MS * 2**min(failures, 5)`, default 1 s base capped at 32 s), releases
   the lease and continues to the next event. One slow or failing event cannot abort a batch
   or block anything behind it.

7. **Wall-clock budget.** `reconcile_pass` claims batches until `RECONCILE_BUDGET_MS`
   (default 500 ms) is spent, checking the deadline between batches so no lease is orphaned
   mid-batch. Overrun is bounded by one in-flight batch. The maintenance loop keeps its
   existing order — dirty-seat refresh, then hold expiry, then reconciliation — so
   reconciliation cannot starve either.

8. **Interval below the TTL.** `RECONCILE_INTERVAL_SECONDS` defaults to 20, under the
   30-second TTL, leaving margin. This is a scheduling target, not a freshness guarantee.

9. **Bounded metric labels.** `ticketing_reconciliation_backlog`,
   `_overdue_seconds`, `_tracked_events`, `_events_total{outcome}`,
   `_failures_total{stage}`, `_seconds{outcome}` and `_recovered_leases_total`. Label
   vocabularies are fixed (`ok|error`, `snapshot|defer|acknowledge|schedule`). No event,
   seat or actor identifiers appear in any label.

## Supersession

Partially supersedes **ADR 0008 §2** and **ADR 0009**'s "keep full reconciliation every five
seconds": proactive reconciliation is now a bounded per-event schedule over active events,
not a periodic sweep of all retained inventory. Everything else in both ADRs remains in
force — durable coalesced refresh generations, lease-token fencing, changed-seat patching,
per-seat source versions, the `seatmap:v2` hash representation and the 30-second TTL.

ADR 0001 (PostgreSQL authority, Redis advisory), ADR 0002 (outbox), ADR 0003 (at-least-once
delivery), ADR 0004 (payment idempotency), ADR 0005 (hold TTL) and ADR 0006 (horizontal
scaling) are unchanged. Reservation locking, API contracts and Kafka semantics are untouched.

## Alternatives

**Reuse `seat_refresh_requests` as the only queue.** Enqueue due active events as full
refresh requests. Rejected: reconciliation would compete with dirty-seat work in one
oldest-first ordering, so a reconciliation flood could starve user-visible change
propagation (requirement 6); an enqueued full request would also erase pending changed-seat
IDs and discard ADR 0009's incremental benefit; and the table has no deadline column, only a
retry cooldown, so "overdue" would be unmeasurable.

**Advisory locks or a Redis-held leader.** Rejected: not durable across restart, and a
Redis-based lease would make Redis an availability dependency of PostgreSQL-side scheduling.
Redis is advisory here by ADR 0001.

**Hash-partition events across workers by ID.** Rejected: static partitioning does not
rebalance when a worker dies, and a partition owner that is slow starves only its own shard
with no recovery path. Leases with `SKIP LOCKED` self-balance.

**Keep the sweep but add a statement timeout and resume cursor.** Rejected: still O(retained
inventory), still inline, and a cursor over a mutating set gives no ownership or failure
isolation.

**Raise or remove the 30-second TTL so reconciliation is only a repair mechanism.** Not
rejected on merit — this is the right long-term answer, but the TTL lives in
`src/ticketing/infrastructure/cache.py`, owned by the concurrent HTTP-reads work. Raised as
a coordination request instead of implemented here. See "Unresolved".

**Drop proactive reconciliation entirely and rely on notifications.** Rejected: it is the
only repair path for lost notifications and evicted or lost Redis state (requirement 3).

## Failure and recovery

- **Worker crash mid-projection.** The lease expires after `RECONCILE_LEASE_SECONDS`; the
  row is claimable again and counted in `_recovered_leases_total`. `snapshot()` is idempotent
  under per-seat source versions, so replay is safe.
- **Crash after Redis write, before acknowledgement.** Redis is already current; the event is
  reconciled once more after lease expiry. No incorrect state, only duplicated work.
- **Stale worker returning after its lease was reclaimed.** Both the success and failure
  acknowledgements match on `lease_token`, so a stale worker updates zero rows and cannot
  reset another owner's deadline or failure count.
- **Persistent per-event failure.** Bounded exponential backoff pushes that event's deadline
  out to at most 32 s while every other event continues at its normal interval.
  `_failures_total{stage="snapshot"}` and `consecutive_failures` expose it.
- **Redis outage.** Every projection fails and backs off. Nothing is marked reconciled, so no
  map is falsely reported fresh. PostgreSQL remains authoritative and booking is unaffected.
- **Schedule maintenance failure.** Seeding and pruning failures are caught, counted under
  `stage="schedule"` and logged; already-scheduled reconciliation continues.
- **Event leaves the active window mid-lease.** Pruning skips leased rows, so no row is
  deleted from under a worker. The acknowledgement then targets a row that still exists.
- **Schedule table lost entirely.** It is derived state. Seeding rebuilds it within
  `ceil(active_events / RECONCILE_SEED_BATCH)` seconds; no booking data is involved.
- **Demand above capacity.** Deadlines slip uniformly rather than some events being starved,
  because claims are oldest-deadline-first. `_overdue_seconds` and `_backlog` are the alert
  signals. See the measured overload case below.

## Consequences

- Proactive reconciliation now covers active events only. A map for a closed or far-future
  event is not kept warm; a read may return the existing `SEATMAP_WARMING` 503 until a
  reactive refresh or the next active-window entry populates it. This is a behaviour change
  for reads of inactive events.
- Bounded fairness costs throughput: two extra short transactions per event reduced raw
  projection rate from ~304/s (legacy sweep) to 209–238/s on the same inventory, about 30%.
  That is the price of ownership, isolation and recovery, and is not a speedup.
- The 30-second TTL, not the schedule, is the binding freshness constraint. Above the
  measured ceiling, maps expire and are rebuilt lazily. The system does not claim otherwise.
- One more table, one more index on `events`, and seven new settings to operate.
- Deleting an `events` row still requires clearing the schedule row first, matching the
  existing `seat_refresh_requests` foreign-key pattern.

## Validation

Full suite: **62 passed** (50 pre-existing plus 12 new) against real PostgreSQL 16.15 and
real Redis 7.0.15, no skips, no mocked infrastructure. The existing 100-contender same-seat
test still yields exactly one winner, one hold and one order against a real HTTP API.
`ruff` clean.

New integration coverage in `tests/integration/test_reconciliation.py`: active-window
selection and pruning; backlog and overdue gauges; two concurrent workers claiming disjoint
events with no duplicate processing; failure isolation within a batch; backoff, bound and
recovery; stale-token acknowledgement rejection; abandoned-lease reclaim; proof that no SQL
lock is held during Redis I/O, with a companion test proving that assertion can actually
fail; cache-loss repair through the schedule; wall-clock budget bounding; oldest-deadline
ordering under an inverted insertion order.

Measured with 300-seat shows, one vCPU, single runs:

| Configuration | Result |
| --- | --- |
| Legacy sweep, 6,000 shows | 19.75 s inline per pass, 0 failures |
| Scheduler, 1,000 shows, 1 worker | 237.96 events/s saturated; peak staleness 20.12 s |
| Scheduler, 1,000 shows, 2 workers | split 3,728 / 3,696, no duplicates, no scaling gain |
| Scheduler, 6,000 shows, 1 worker | 209.06 events/s; peak staleness **45.28 s**, TTL breached |

Conservative derived ceiling: roughly **4,200 active 300-seat shows per worker** at the
20-second interval on this hardware. At 6,000 shows a single worker cannot hold the TTL, and
this is reported as a limit rather than tuned away. Fairness held under that overload: every
one of the 6,000 shows was reconciled at least once in the window.

Full evidence, method and limitations: [reconciliation measurements](../capacity/reconciliation/README.md).
These are local, single-run, single-core measurements and are not production capacity.

## Rollout

1. Apply migration `004_reconciliation_schedule.sql`. It is additive and creates no
   constraint on existing rows. `CREATE INDEX events_sale_window` is **not** `CONCURRENTLY`
   because the migration runner executes each file in one transaction; on a large `events`
   table build that index concurrently by hand first, then apply the file (the
   `IF NOT EXISTS` guard makes it a no-op).
2. Deploy the maintenance worker. No API, consumer or publisher change is required.
3. Watch `ticketing_reconciliation_tracked_events` rise as seeding converges, then
   `ticketing_reconciliation_backlog` and `_overdue_seconds` settle.
4. Alert on `_overdue_seconds` approaching the TTL minus the interval, and on any sustained
   `_failures_total{stage="snapshot"}`.

## Rollback

Revert the worker image. Leave migration 004 installed: the old code ignores the table
entirely, and retaining it avoids a destructive down-migration. The previous periodic sweep
resumes and re-warms every retained event. If the table must be removed, drop it only after
all maintenance instances running this version have stopped, so no worker is holding a lease.

## Unresolved

- **Coordination request to the cache owner (`infrastructure/cache.py`, ADR 0010):** the
  30-second TTL is the binding constraint on active-show count. Raising it, or extending it
  on changed-seat patches as well as full snapshots, would decouple "keep the map alive" from
  "repair drift" and raise the ceiling substantially. Not changed here; the file is out of
  scope for this branch.
- Reads of events outside the active window may see `SEATMAP_WARMING` more often. Whether the
  layout/availability endpoints should trigger an on-demand schedule entry is a question for
  the API owner.
- Scaling across maintenance instances is unmeasured; the test host has one core.
- Reconciliation was not measured under simultaneous HTTP reservation load.

## Integration amendment (2026-09-08, before implementation)
Pruning will lock candidates with SKIP LOCKED and recheck eligibility before deletion.
Pass deadlines will be checked before each snapshot; unstarted claimed work is released
with its token. One already-running snapshot may overrun; this is not preemptive I/O.
Projection and SQL acknowledgement outcomes will be separate; benchmark throughput must
count successful acknowledgements, not claims. Compose will pass all scheduler settings.
Active-window comparisons will use statement_timestamp and direct indexed columns;
LIMIT bounds results, not all scan work. No constant-time discovery guarantee is made.
External throughput arithmetic is retained as historical evidence, not a safe TTL ceiling.
The integrated container suite passed 71 tests in 14.29 seconds, zero skipped, with two existing dependency warnings. The scheduler-specific suite passed 15 cases. Added tests cover prune/lease locking, per-event deadline release and stale-ack accounting. Mixed local HTTP validation is recorded separately in local-integration.md. Intervals of 30 seconds or more are rejected against the existing cache TTL.

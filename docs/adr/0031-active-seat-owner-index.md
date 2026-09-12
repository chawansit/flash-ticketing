# ADR 0031: Partial index for active seat ownership

Status: Accepted

## Context

The 400 RPS cloud soak used about 86.3% of one CPU core in PostgreSQL and the
500 RPS soak used about 99.1%, while neither run recorded a lock waiter and mean
commit latency remained close to 2 ms. The expiry path selects and then updates
`event_seats` by `hold_id`, but the table has no index beginning with that column.

The retained Huawei database contained 240,300 seats and 126,264 completed
holds. Its cumulative statistics reported 252,530 sequential scans of
`event_seats`, close to the two hold-owner scans performed by each release or
expiry. `EXPLAIN` confirmed sequential scans with estimated cost 5,112 for both
the locking select and update. The expiry claim itself already uses
`holds_expiry` and `orders_hold_id_key` indexes.

## Decision

Add a partial B-tree index on `(hold_id, seat_id)` for rows where `hold_id IS NOT
NULL`. The index supports the ordered owner lock and the owner update while
remaining proportional to currently held seats rather than total historical
inventory. Keep PostgreSQL as the ownership authority and keep the existing
order, hold and sorted-seat lock order unchanged.

Deploy the additive index through the existing checksum-protected migration
runner. For this MVP deployment the migration runs before the API and workers
start. A later online migration system must build an equivalent index
concurrently when upgrading a live, large production inventory.

Measure this change alone before selecting transaction batching, worker polling
changes or additional indexes. Compare query plans, PostgreSQL CPU, transaction
latency, admission outcomes and expiry backlog on the same topology and workload.

## Alternatives considered

- Scanning the primary key `(event_id, seat_id)` cannot help because expiry is
  driven by `hold_id` and does not know the event before locating the owner.
- An unfiltered `hold_id` index would retain an entry for every available or sold
  seat, increasing memory and write maintenance for no lookup benefit.
- Rewriting expiry as a broader batch before indexing would change transaction
  and lock behavior at the same time, preventing attribution and increasing
  rollback scope.
- Moving hold expiry to Redis would make an advisory cache responsible for
  durable ownership and conflict with ADR 0001.

## Consequences

Each hold creation inserts up to eight small index entries and expiry, release or
booking removes them. Owner lookup becomes dependent on the number of seats in
one hold instead of total inventory. The index adds storage and write
maintenance, but its partial predicate bounds both to active holds.

The migration takes a write-conflicting table lock while the index is built.
Compose runs migrations before application startup, which is acceptable for the
measured MVP. A production upgrade with uninterrupted writes requires a future
non-transactional/concurrent migration decision.

## Failure and recovery behavior

Migration failure prevents application startup and leaves the previous schema
authoritative. PostgreSQL rolls back an unsuccessful transactional index build.
The migration is idempotent and checksum recorded. Dropping or losing the index
reduces performance but does not weaken row locking, uniqueness constraints or
zero-double-booking guarantees.

Redis failure, an interrupted API request and ambiguous commit acknowledgement
retain the recovery behavior in ADR 0001. No Redis, Kafka, idempotency, TTL or
payment semantics change.

## Validation evidence

Before implementation, PostgreSQL 17.6 planned sequential scans for both
`event_seats WHERE hold_id = $1` statements. Cumulative table statistics and the
cloud CPU measurements are retained in the Huawei long-run report. After migration, both statements use `event_seats_active_hold`; estimated lookup cost fell
from 5,112 to 8.14 in the local PostgreSQL plan. Ruff passed, 68 unit tests passed and
57 PostgreSQL/Redis integration tests passed, with two existing dependency warnings.
Matched cloud-load evidence remains pending and will be appended after execution.
### Huawei matched load on 2026-09-12

At commit `152414d`, the same 400 RPS / 30-minute workload reduced PostgreSQL
mean CPU from 86.34% to 30.78%, API transaction mean from 17.28 ms to 11.54 ms,
read p95 from 13.05 ms to 9.94 ms and hold p95 from 45.35 ms to 37.27 ms.
Across 72,000 seat updates, the `event_seats` sequential-scan delta was zero.
All 36,000 acknowledged holds passed durability and overlap verification, and
all queues drained. One of 684,000 reads ended in a connection reset, so the
strict workload gate failed and the planned 500 RPS stage was not run. See the
[retained report](../capacity/postgres-owner-index/README.md).
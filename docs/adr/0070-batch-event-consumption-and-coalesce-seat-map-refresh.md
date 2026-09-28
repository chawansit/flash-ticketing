# ADR 0070: Batch event consumption and coalesce seat-map refresh work

Date: 2026-09-28
Status: Proposed

## Context

The accepted three-writer reservation topology can sustain a three-minute burst at
1,000 total requests per second with a six-percent reservation mix. During a
15-minute run, one Kafka consumer and one combined maintenance worker preserved
request latency but accumulated Kafka and seat-map refresh backlog. Splitting the
background lanes and adding a second consumer drained every queue and preserved
correctness, but increased PostgreSQL and host concurrency enough to miss the
request latency gates: seat-map read p95 was 161.799 ms and hold p95 was 343.946
ms.

The workload produces more durable database transactions than its logical work
requires. Consumer inbox writes, event handling, refresh claims and refresh
completion are performed in small units. This increases connection occupancy,
commit frequency and WAL synchronization. Adding workers moves the bottleneck
instead of increasing sustained capacity.

Order, payment and other business events must retain individual delivery and
idempotency semantics. Seat-map refresh requests are different: for a given show,
applying the newest committed inventory generation makes older pending refresh
generations redundant.

## Decision

Introduce bounded micro-batching in event consumption and seat-map refresh.

1. A Kafka consumer may gather at most 100 records or wait at most 10 ms after
   receiving the first record, whichever occurs first.
2. Consumer inbox/idempotency rows are inserted with one PostgreSQL statement.
   Duplicate event identifiers remain safe and do not fail the batch.
3. Business handlers still process order, payment and other non-refresh events
   individually and retain their existing idempotency boundaries.
4. Pending seat-map refresh work is coalesced by event/show identifier. The
   existing generation fence retains the newest pending work and never moves a
   completed generation backwards.
5. A refresh worker claims a bounded batch of 16 items, updates Redis, and
   acknowledges successful items with one bounded PostgreSQL operation.
6. Expiry remains a separate lane with its existing worker count.
7. Kafka offsets are committed only after durable inbox and handler state commit.
   Delivery remains at least once; database idempotency makes replay safe.
8. Batch size and collection time are bounded configuration values. Admission
   and database connection budgets remain bounded.

This decision extends ADR 0003 and ADR 0054. It supersedes ADR 0052 only where
that decision relies on added consumer concurrency as the primary way to increase
refresh ingress capacity. It does not alter payment idempotency, reservation
authority, hold TTL or Redis-first reservation intake.

## Alternatives considered

### Add more consumers and refresh workers

This drained queues in the 15-minute experiment, but raised database waiters and
caused both public request latency gates to fail.

### Keep per-event transactions and scale PostgreSQL vertically

This can buy headroom, but preserves avoidable commit and WAL amplification and
makes capacity depend on a larger database instance.

### Coalesce every event type

Rejected because payment, order and other business events may carry transitions
that cannot be discarded. Coalescing is limited to versioned seat-map refresh
intent.

### Acknowledge Kafka before PostgreSQL persistence

Rejected because a process failure could lose events after the broker considers
them consumed.

## Consequences

- PostgreSQL transactions, commits and WAL synchronization per logical event
  should decrease.
- The bounded collection window adds up to 10 ms of background-event latency.
- Seat-map cache freshness includes batching, queue and refresh execution time.
- Partial batch failure and replay require explicit tests.
- Scaling decisions can use useful work per transaction rather than worker count.

## Failure and recovery behavior

- If the consumer stops before database commit, Kafka re-delivers the batch.
- If the database commits but offset commit fails, replay encounters inbox
  duplicates and remains idempotent.
- A failing non-refresh handler prevents committing its unprocessed offset;
  successfully committed inbox rows are replay-safe.
- If Redis is unavailable, refresh work remains retryable and its durable target
  generation is retained.
- If a worker stops after updating Redis but before completion, repeating the same
  or newer generation is safe.
- Coalescing always retains the latest generation, preventing stale replay from
  overwriting newer availability.
- Batches are bounded so failure cannot monopolize the connection pool or create
  unbounded retry work.

## Validation evidence

This ADR is proposed. Required evidence before acceptance:

1. Unit tests for duplicate inbox IDs, mixed event batches, generation
   coalescing, and replay.
2. Integration tests for restart before/after database commit and Redis failure
   during refresh.
3. Complete local test and lint suites.
4. A 1,000 RPS, six-percent-write, three-minute cloud control run.
5. If control passes, a 15-minute run with zero double booking, zero lost durable
   reservations and all queues drained.
6. Only after those stages pass, three-minute 1,250 and 1,500 RPS discovery.

Targets at 1,000 RPS are fewer than 20-30 PostgreSQL event transactions per
second, no more than five to six interesting RDS waiters, seat-map read p95 below
100-150 ms, and hold p95 below 200-300 ms. These are validation targets, not
confirmed capacity.
### Local implementation evidence

The proposed design is implemented locally. Kafka polling is bounded to 100
records with a 10 ms broker fetch window. Consecutive SeatsChanged records use
one inbox transaction, duplicate event IDs are ignored, seat IDs are coalesced
per event, and critical event handlers retain individual transactions. Successful
refresh projections are acknowledged with one PostgreSQL update per batch.
Prometheus histograms expose consumer and refresh-ack batch sizes.

The final complete unit and integration run passed 243 tests with two dependency
deprecation warnings. Ruff passed for every changed Python file; the
repository-wide Docker invocation was not used as evidence because Windows
mounts mark all source files executable and trigger unrelated EXE002 findings.

Cloud control, sustained validation and discovery stages remain future work.
No higher production capacity is claimed by this local evidence.

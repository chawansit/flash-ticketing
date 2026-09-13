# ADR 0036: Bounded PgBouncer backend-pool headroom

Status: Rejected; pool-only candidate failed the strict availability gate

## Context

The managed-RDS topology keeps PgBouncer on the backend ECS in transaction mode.
API connection pools are bounded to twelve connections in aggregate, but the
publisher, consumer, maintenance, reconciliation, simulator and diagnostic clients
also share PgBouncer's twelve-server default pool. Huawei RDS PostgreSQL 17.11 has
4 vCPU, 16 GiB memory and an effective 768-connection limit.

Corrected application metrics show local pool checkout averaging about 0.03 ms,
so the earlier `db_enter` measurement was mostly connection hold time rather than
application-pool wait. In a two-API 750 RPS control, PgBouncer sampled all twelve
servers active, eight clients waiting and an oldest wait of 146.685 ms. In the
four-API fixed-budget repeat, a 93.666 ms PgBouncer wait at 15:12:18 UTC coincided
with admission rejections on all four API replicas. Raising only API admission or
its timeout would move more waiting behind the same pooler limit.

Provider evidence around the earlier failure showed RDS CPU no higher than 10.19%,
write latency no higher than 0.77 ms and connection usage 2.37%. Those one-minute
samples do not prove transient-free storage, but they leave measured headroom for
a bounded pool comparison.

## Decision

Run a controlled RDS candidate with PgBouncer `default_pool_size` increased from
12 to 24. Keep four API replicas with pool three and hold admission three each, so
the aggregate API budgets remain twelve. Keep `max_client_conn=160`,
`reserve_pool_size=0`, `query_wait_timeout=1`, transaction pooling, workload mix,
fixture, keep-alive policy and no-retry generator behavior unchanged.

The additional twelve server slots are shared headroom for API and background
roles; they do not change PostgreSQL seat ownership, transaction boundaries,
idempotency, TTL or Kafka delivery behavior. Validate 750 RPS for ten minutes
first. Continue to a 30-minute stage only if request accounting, availability and
correctness gates pass. Record PgBouncer client waits, application pool timing,
connection hold time, event-loop lag, RDS activity and post-expiry durability.

Accept 24 as the RDS benchmark default only if the 30-minute stage has zero
unexpected HTTP or transport errors, zero generator drops, documented latency
targets, exact acknowledged persistence, zero overlapping hold intervals and
drained queues. Treat the result as a topology-specific bound rather than a
production maximum.

## Alternatives considered

- Increase API pool or admission budgets. Rejected for this comparison because it
  increases client-side concurrency while the measured queue is at PgBouncer.
- Increase the 150 ms application timeout. Rejected because it hides saturation
  and allows longer admission occupancy without increasing service capacity.
- Enable a PgBouncer reserve pool. Deferred because its activation timeout is a
  second-scale overload mechanism and does not fit the observed sub-150 ms queue.
- Connect API processes directly to RDS. Rejected because it removes transaction
  pooling and changes more than the measured constraint.
- Create role-specific poolers or database users. Deferred until a bounded shared
  pool is measured; isolation adds configuration and connection budgets per role.
- Scale API replicas further. Rejected as the immediate next step because four
  replicas reproduced a pooler wait and simultaneous admission rejection.

## Consequences

PgBouncer may open up to twelve additional RDS server connections for this
database/user pool. Together with existing direct and administrative sessions,
the expected total remains a small fraction of the RDS connection limit. More
concurrent transactions can raise RDS CPU, WAL, IOPS and lock contention, so these
must be measured rather than inferred from the configured limit.

Application pool checkout remains a separate metric from time holding a checked-
out connection. PgBouncer statistics require the configured read-only stats user;
no credential values or private manifests are retained in evidence.

## Failure and recovery behavior

If PgBouncer fails readiness after recreation, do not send load. Restore
`RDS_PGBOUNCER_POOL_SIZE=12`, recreate PgBouncer and verify API readiness. Existing
transactions may fail during the deliberate pooler restart and are outside the
measured window; Redis shields are released on failed persistence and PostgreSQL
remains the only durable seat authority.

If the candidate creates RDS saturation, lock growth, connection failures,
unexpected responses or incomplete queues, stop escalation, retain the failed
evidence and restore twelve. After each measured stage, wait beyond hold TTL and
verify every acknowledged hold plus global queue drain before continuing.

## Validation evidence

The ten-minute candidate completed all 450,000 scheduled requests with zero
generator drops or transport errors. Worst-worker read p95 was 8.143 ms and hold
p95 was 51.238 ms. PgBouncer recorded zero waiting samples in the exact window,
server activity peaked at 18 of 24 and average wait was 1.436 microseconds per
assignment. RDS connections peaked at 21, active connections at 17 and sampled
lock waiters remained zero.

The run still returned 22 `ADMISSION_FULL` responses across all four API
replicas. All 22,478 acknowledged holds matched durable records after expiry,
held-seat interval overlap was zero and outbox, refresh and dead-letter queues
drained. Increasing only PgBouncer backend headroom therefore removed the
measured pooler queue but did not pass availability. Do not adopt it as a
standalone change. ADR 0037 uses this result to test bounded admission headroom
without increasing the twelve-connection API DB budget.

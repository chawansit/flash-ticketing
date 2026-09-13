# ADR 0037: Fixed DB-pool admission headroom for four API replicas

Status: Accepted for the measured four-API RDS benchmark topology

## Context

ADR 0034 fixes the four-replica aggregate PostgreSQL pool and hold-admission
budgets at twelve, or three per replica. The first 750 RPS, ten-minute run with a
PgBouncer backend pool of twelve produced twelve `ADMISSION_FULL` responses and a
correlated PgBouncer wait. ADR 0036 tested backend-pool headroom of 24 while
keeping each API at pool three and admission three.

The ADR 0036 candidate completed all 450,000 scheduled requests with zero
generator drops or transport errors and met latency targets, but 22 holds returned
`ADMISSION_FULL`. PgBouncer recorded zero waiting samples across 3,000 samples,
server activity peaked at 18 of 24 and average wait per assignment was 1.436
microseconds. RDS connections peaked at 21, active connections at 17, and lock
waiters remained zero. Application pool acquisition averaged about 0.02 ms while
connection hold and transaction time averaged about 27 ms. The hard process-local
admission limit is now the measured failing boundary.

Because Nginx connection affinity and request timing distribute short hold bursts
imperfectly, a per-replica limit equal to its DB pool can reject on one process
while aggregate database and pooler capacity remains available.

## Decision

Run a controlled candidate with four API replicas, DB pool three per replica and
hold admission four per replica. The aggregate DB connection budget remains
twelve while aggregate hold admission rises from twelve to sixteen. Retain the
PgBouncer backend pool of 24 from the diagnostic candidate so background clients
do not queue behind the API budget. Keep transaction pooling, the 150 ms DB pool
timeout, workload, fixture, keep-alive policy and no-retry generator behavior
unchanged.

This is bounded admission headroom, not a retry. At most one request per replica
may wait for its local three-connection pool. PostgreSQL remains the durable seat
authority and Redis remains the atomic hold shield; no transaction, TTL,
idempotency or Kafka semantics change.

Validate 750 RPS for ten minutes first. Continue to a 30-minute stage only if all
requests complete with zero unexpected responses, zero generator drops, latency
targets pass, acknowledged records are exact, held-seat intervals do not overlap
and every queue drains. Accept both the PgBouncer-24 and admission-16 benchmark
settings only after the 30-minute stage passes. Treat the result as specific to
this topology and workload.

## Alternatives considered

- Keep admission three per replica. Rejected for the next candidate because two
  controlled runs reproduced hard-limit rejection while backend headroom remained.
- Add automatic retry for `ADMISSION_FULL`. Rejected because it changes offered
  traffic and hides overload evidence.
- Add a timed middleware queue. Deferred because the existing database-pool wait
  already provides a bounded 150 ms queue for the one extra admitted request; a
  second queue would add code and another timeout policy.
- Increase each DB pool above three. Rejected because it changes the aggregate API
  database connection budget and is unnecessary given the measured acquisition
  and RDS headroom.
- Increase admission without a bound. Rejected because it can amplify a database
  slowdown and break predictable overload behavior.

## Consequences

Short bursts can use a fourth hold slot on each process while database execution
remains capped at three connections. A request in that slot may spend part of the
existing 150 ms timeout waiting for a local connection, so pool-acquire latency and
`DATABASE_UNAVAILABLE` must be retained as independent failure gates.

Aggregate admitted hold work increases by four. PgBouncer and RDS activity,
application pool waits, transaction time, event-loop lag and connection
distribution must be measured. Uneven load balancing remains visible and must not
be hidden by proxy or client retries.

## Failure and recovery behavior

If readiness fails after the override, restore admission three before traffic.
If the ten-minute candidate has any unexpected response, generator drop, latency
failure, durability mismatch, overlapping interval or undrained queue, stop the
30-minute escalation and restore the accepted aggregate admission budget of
twelve. If PgBouncer-24 is not accepted by a complete 30-minute gate, restore its
backend pool to twelve as well.

## Validation evidence

The first ten-minute admission-four run completed all 450,000 requests with zero
admission rejection, generator drop or transport error. It returned one
`RESOURCE_BUSY` response for a previously used synthetic seat. The retained
result failed the strict gate. A repeat using a fresh disjoint seat range passed:
all 450,000 requests completed, 22,500 holds returned 201, read/hold p95 was
8.118/48.721 ms and post-expiry durability, overlap and queue checks passed.

The subsequent 30-minute stage completed all 1,350,000 requests with zero
admission rejection, generator drop or client transport error, but two reads
received HTTP 502 at the Nginx-to-API boundary. ADR 0038 records that separate
transport decision and its validation.

With ADR 0038 applied, the final 750 RPS, 30-minute run completed all 1,350,000
requests with zero unexpected HTTP or transport errors and zero drops. Read/hold
p95 was 8.251/51.050 ms. Corrected local pool acquisition averaged
0.028–0.035 ms, while transaction time averaged 26.662–26.784 ms. Every one of
67,500 acknowledged holds matched its idempotency, hold and order records after
expiry; broken links and overlapping intervals were zero and all queues drained.

Admission four per replica is accepted together with pool three per replica and
PgBouncer backend pool 24 for this measured four-API benchmark. This does not
authorize unbounded admission or establish a production maximum.

# ADR 0039: Admission headroom for repeatable four-API bursts

Status: Proposed for controlled validation

## Context

ADR 0037 accepted four API replicas with a database pool of three and hold
admission of four per replica after a clean 750 RPS, 30-minute run. A fresh
750 RPS control on 2026-09-14 did not reproduce that zero-error result. One API
returned one `ADMISSION_FULL` response about three seconds after measured load
began, so the run was stopped under the strict gate.

The failing replica reached four admitted holds while its three database
connections were in use. A PgBouncer sample 0.607 seconds before the rejection
showed one waiting client, fourteen active servers, no idle server and a
47.323 ms maximum wait. Across the partial run, application pool acquisition
averaged 0.037--0.156 ms and transaction time averaged 27.976--28.484 ms.
PostgreSQL connections peaked at eighteen, active connections at eight and lock
waiters at zero. The failure is therefore a short distribution and pooler burst,
not evidence of sustained RDS saturation.

The existing one-request admission margin per replica is too narrow to make the
previous 750 RPS result repeatable. Retrying the workload, smoothing its start or
ignoring the single failure would weaken the production-style strict gate.

## Decision

Run a controlled candidate with four API replicas, database pool three per
replica and hold admission five per replica. This raises aggregate hold admission
from sixteen to twenty while keeping the aggregate application database
connection budget at twelve and the PgBouncer backend pool at 24.

At most two admitted hold requests per replica can wait for its local database
pool. Keep the 150 ms application pool timeout, Nginx `least_conn`, five-second
Nginx upstream keep-alive, ten-second Uvicorn keep-alive, four-worker no-retry
load, fixture shape, transaction boundaries and PostgreSQL, Redis and Kafka
semantics unchanged.

Validate 750 RPS for ten minutes first. Proceed to a fresh 30-minute 750 RPS
confirmation only if the safety run has zero unexpected responses, transport
errors, admission rejections and generator drops; latency passes; acknowledged
holds are durable; held-seat intervals do not overlap; and all queues drain.
Only after that confirmation may 800 RPS be attempted under the same gates.

## Alternatives considered

- Keep admission four and rerun. Rejected because the fresh control has already
  shown that the accepted setting is not repeatable under the strict gate.
- Smooth or ramp the generator start. Rejected because it removes a realistic
  synchronized burst and breaks comparison with the accepted workload.
- Retry `ADMISSION_FULL` at the client or proxy. Rejected because retries hide
  the failed attempt and change offered backend traffic.
- Increase the application database pool above three. Rejected because sampled
  acquisition time was small and this would change the fixed aggregate database
  connection budget.
- Change load-balancer routing. Deferred because admission headroom addresses the
  observed per-process burst without changing request affinity or proxy behavior.
- Remove the admission bound. Rejected because an unbounded queue can amplify a
  database slowdown and make overload latency unpredictable.

## Consequences

Each process may retain two hold requests while all three local database
connections are occupied. This should absorb the observed short burst without
increasing concurrent database transactions. It can also keep requests in the
process longer during a database slowdown, so pool wait, end-to-end hold latency,
event-loop lag and `DATABASE_UNAVAILABLE` remain independent failure gates.

The setting is specific to the measured four-API topology and workload. A clean
candidate would establish repeatability for that topology, not a production
maximum or permission to scale admission independently of database evidence.

## Failure and recovery behavior

Validate all four APIs after recreation and send no traffic if readiness fails or
the rendered value differs from five. Stop a load stage at the first strict-gate
failure. Restore admission four per replica if the ten-minute or 30-minute
candidate fails, then retain the evidence and classify 750 RPS as demonstrated
once but not repeatable. Database pool, PgBouncer capacity and data-authority
settings remain unchanged during deployment and rollback.

## Validation evidence

The triggering 2026-09-14 control warmed all 800 shows on the first attempt. Four
workers reached a combined 225,000 scheduled requests with zero reported drops
before operator termination. The strict gate failed on one admission rejection.
The partial post-TTL audit found 11,621 linked idempotency, hold and order records,
zero broken links, zero overlapping held-seat intervals and drained outbox,
refresh and dead-letter queues.

The first admission-five safety candidate completed 450,000 requests with zero
admission rejections, transport errors or generator drops and met both latency
targets. It failed the strict gate on three `RESOURCE_BUSY` responses from seats
within a previously exercised fixture. All 22,497 acknowledged holds passed the
exact-run durability, expiry and overlap audit and all queues drained. Admission
was restored to four.

This run does not accept or reject admission five because historical fixture
state was not isolated. The next candidate must use newly created shows and
seats. This ADR remains Proposed until a fresh-fixture ten-minute safety run and
30-minute confirmation pass every gate.

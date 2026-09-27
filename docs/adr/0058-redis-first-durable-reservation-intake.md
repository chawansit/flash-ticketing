# ADR 0058: Redis-first durable reservation intake

Date: 2026-09-27
Status: Accepted for implementation; production activation requires the gates below

## Context

The synchronous hold path in ADR 0001 acquires a Redis contention shield and then performs
the complete PostgreSQL hold, order, item, idempotency and outbox transaction before replying.
It preserves strong durability, but every admitted request occupies an API thread, a database
pool slot and a PostgreSQL transaction through commit. The verified five-minute 1,000 RPS run
at revision `62cb50d` delivered all 300,000 scheduled requests, but three admission rejections
remained. A matched single-retry run still ended with two failed requests. Successful API
commit phases also reached 254.560 ms during transient WAL waits.

A short local admission wait can hide some micro-spikes but does not increase sustained write
capacity. Blind retries increase physical traffic and do not remove the shared commit stall.
The system needs a bounded intake that decides seat ownership without holding an HTTP request
open on PostgreSQL, while preserving idempotency, expiry, recovery and the database uniqueness
backstop.

## Decision

Add an opt-in `redis-first` reservation mode alongside the existing `postgres` mode. The
existing mode remains the deployment default until every activation gate is satisfied.

In `redis-first` mode, one Redis Lua operation validates a pre-warmed seat map, sale metadata,
seat availability and the actor/idempotency key; assigns pre-generated hold and order IDs;
updates all requested seats; stores the immutable response; schedules expiry; and appends a
stable command to a Redis Stream. The API returns `202 Accepted` with
`persistence_status=PENDING`. A replay of the same actor, key and request returns the same IDs
and deadline; a changed payload returns `IDEMPOTENCY_MISMATCH`.

A dedicated reservation writer consumes the stream through a consumer group. It persists the
hold, pending order, items, PostgreSQL idempotency response and transactional outbox event in
one PostgreSQL transaction, using the IDs, prices and deadline fixed by intake. PostgreSQL row
locks and uniqueness constraints remain the final booking backstop. After commit, the writer
marks the Redis command `DURABLE` and acknowledges it. A payment or checkout is allowed only
after the PostgreSQL hold exists, so financial processing never acts on an unpersisted command.

The writer is at-least-once. The command ID is unique in PostgreSQL and the existing
idempotency key is unique, making replay safe. A writer crash before commit leaves the stream
entry pending. A crash after commit but before acknowledgement replays the same command and
discovers the durable result.

Redis-first intake uses the existing per-event hash tag and a per-event command stream so all
keys touched by the Lua operation share one Redis Cluster slot. Writers discover stream keys
from a bounded registry and periodically scan as a recovery backstop. Production Redis must
use `noeviction`, AOF persistence and at least one acknowledged replica. Startup rejects
`redis-first` outside development when the configured acknowledgement requirement is below
one. The API calls `WAIT` after intake and treats insufficient acknowledgements as an unknown
outcome: clients must replay the same idempotency key to discover the stored response.

This decision supersedes ADR 0001 only for reservation intake while `redis-first` mode is
enabled. PostgreSQL remains authoritative for payments, bookings, tickets and long-term audit.
ADR 0001 remains fully active in the default `postgres` mode. ADR 0002's PostgreSQL
transactional outbox and ADR 0003's Kafka at-least-once processing remain unchanged; Kafka is
not placed in the intake acknowledgement path.

## Alternatives considered

- Keep immediate rejection or wait 25-50 ms in each API process. This smooths short bursts but
  leaves the PostgreSQL commit path as the sustainable-capacity limit.
- Add more retries. This increases request amplification and the matched experiment still left
  failures, so it is not a capacity mechanism.
- Publish directly to Kafka after a Redis hold. Redis and Kafka cannot commit atomically; a
  crash between them can leave ownership without a durable command.
- Make a global Redis Stream part of every Lua operation. It works on one Redis node but causes
  cross-slot failures in Redis Cluster and creates one ingestion hotspot.
- Return a confirmed `201` before PostgreSQL persistence. This hides the provisional state and
  lets checkout race persistence. The explicit `202/PENDING` contract is safer.
- Replace PostgreSQL ownership immediately. A staged opt-in permits shadow validation and an
  immediate rollback to the already measured synchronous path.

## Consequences

The API no longer holds a database connection while accepting a provisional reservation, so
short RDS WAL or pool stalls become writer lag instead of HTTP admission errors. Intake is a
single atomic Redis operation plus replica acknowledgement. Capacity can scale by event and
Redis hash slot, and PostgreSQL writers can batch or scale independently in later ADRs.

The design introduces a visible provisional state and a second authoritative interval: Redis
owns active provisional leases until they are durable, while PostgreSQL owns durable commerce
records. Operations must monitor command age, pending-entry count, persistence failures,
replica acknowledgements and compensation. Redis sizing and HA now affect reservation
availability and acknowledged provisional ownership.

## Failure and recovery behavior

- Redis rejects missing, stale or updating event data with `SEATMAP_WARMING`; no command is
  acknowledged.
- If the Lua operation succeeds but the HTTP response is lost, replaying the same actor and
  idempotency key returns the stored response. A different payload is rejected.
- If `WAIT` does not reach the configured replica count, return an unknown-outcome 503. Do not
  release or create another hold; replay the same key checks the atomic record.
- A writer or PostgreSQL outage leaves commands pending in Redis. The bounded stream backlog
  applies backpressure before Redis memory can grow without limit.
- A deterministic persistence conflict marks the command `FAILED` and releases only seats
  still owned by that command. It never clears a later owner. The client observes failure when
  polling the command; payment remains blocked.
- A writer crash before stream acknowledgement is recovered with `XAUTOCLAIM`. PostgreSQL
  command uniqueness and idempotency make the replay harmless.
- Redis loss before replication/AOF recovery can lose provisional holds. Therefore production
  activation is prohibited without the durability gate; PostgreSQL's existing synchronous
  mode remains the rollback.
- Expiry is represented in the same Redis slot and must release only the matching hold token.
  PostgreSQL expiry remains responsible for already durable holds. Reconciliation repairs the
  read projection but must never invent ownership.

## Validation and activation evidence

Before production activation, all of the following are required:

1. Unit tests for Lua atomicity, replay mismatch, bounded backlog, `WAIT` failure and ownership-
   fenced compensation.
2. PostgreSQL/Redis integration tests with 100 concurrent attempts on one seat and exactly one
   provisional success, at-least-once writer replay, writer crash recovery, persistence
   conflict compensation, expiry/reclaim races and payment blocked before durability.
3. A shadow or isolated cloud test with fresh fixtures, no client retry, zero overlapping
   ownership, zero missing acknowledged commands, every stream pending entry drained, every
   durable response linked to one hold/order/idempotency row and rollback verified.
4. Redis AOF/replication/failover validation, including replay after primary loss. The measured
   environment must report the configured replica acknowledgement count.
5. A controlled 1,000 RPS comparison followed by higher stages only when latency, provisional
   age, durability, double-booking and drain gates all pass.

Implementation and local tests are pending at decision creation time. No capacity increase is
claimed by this ADR until cloud evidence is appended.




## Implementation validation on 2026-09-27

The opt-in path now includes the atomic Lua intake, per-event Redis Stream, bounded backlog,
connection-affine EVAL plus WAIT, explicit 202/PENDING API contract, command-status endpoint,
at-least-once reservation writer, PostgreSQL command receipt migration, token-fenced
compensation, AOF-enabled local Redis, separate Redis admission limit, structured logs and
Prometheus metrics. The synchronous PostgreSQL path remains the default.

- Ruff passed for src and tests.
- The complete containerized unit and PostgreSQL/Redis integration selection passed: 209 tests,
  with two existing dependency deprecation warnings.
- The dedicated Redis-first integration file passed nine tests. Its 100-thread same-seat test
  admitted exactly one provisional command. It also covered changed idempotency payloads,
  checkout before durability, deterministic conflict compensation, bounded backlog,
  expiry/reclaim ownership fencing, pending-message reclaim after a writer failure,
  actor-scoped command status, missing command metadata and fair reads across event streams.
- A local authenticated FastAPI check returned HTTP 202 and the writer advanced the command to
  DURABLE.
- A separate local HTTP race sent 100 concurrent actors to one seat and produced exactly one
  HTTP 202 winner and 99 409 SEAT_UNAVAILABLE responses.
- Local Compose rendered successfully. The local stack uses one Redis node, so it does not
  satisfy or test the production replica/failover gate.
- Redis primary-loss recovery, replicated WAIT behavior on the target service, distributed
  cloud load, final command drain and higher-RPS capacity remain untested. Production activation
  and any capacity claim remain blocked.


## Huawei DCS preflight evidence on 2026-09-27

A Redis 7 master/standby DCS instance was probed from the API ECS over its private endpoint.
The effective configuration reported AOF enabled, `appendfsync=everysec`,
`maxmemory-policy=noeviction`, one connected standby and master role. The application client
successfully executed Lua, Redis Stream group/read/ack/delete operations, and a connection-
affine write plus `WAIT 1 1000`. The standby acknowledged the write in approximately 3.1 ms.

This verifies command compatibility, private reachability and one live replica at the time of
the probe. It does not satisfy the primary-loss recovery gate: managed failover, reconnect,
acknowledged-command survival and post-failover stream drain remain to be tested.

## Isolated Huawei cloud smoke evidence on 2026-09-27

The four-API RDS topology was temporarily switched to `redis-first` with one reservation
writer after PgBouncer `verify-full` and DCS connectivity checks passed. The DCS endpoint
reported master role with one connected replica. A connection-affine temporary write received
one replica acknowledgement inside the configured 100 ms window and was deleted immediately.

One authenticated hold returned HTTP 202/PENDING, advanced to DURABLE, and replayed with the
same command, hold and order IDs. PostgreSQL contained exactly one matching command, active
hold, pending order, order item, seat owner and transactional `SeatsChanged` outbox event. The
event stream length and consumer-group pending count both returned to zero.

A separate no-retry race sent 100 actors to one seat. It produced exactly one HTTP 202 winner
and 99 HTTP 409 `SEAT_UNAVAILABLE` responses. The winner became DURABLE with exactly one
matching command, hold, order and seat owner; stream length and pending count again drained to
zero. The slowest response in this small ECS-local contention probe was 337.204 ms. This is
correctness evidence, not a capacity or latency qualification.

Every registered reservation stream was empty before rollback. The deployment was returned to
`postgres` mode, the reservation writer was stopped, all four API replicas were healthy with
zero restarts, readiness passed five consecutive checks and inspected runtime logs contained no
error-level entries. Credential-bearing manifests were deleted. The compact evidence is
retained in the [cloud smoke report](../capacity/huawei-rds/2026-09-27-redis-first-smoke/README.md).

This satisfies the isolated single-command and 100-way contention portions of activation gate
3. It does not satisfy DCS primary-loss recovery, writer-restart recovery in the managed
environment, distributed 1,000 RPS load, provisional-age bounds under load or production
activation. The measured deployment therefore remains in synchronous PostgreSQL mode.

## Distributed validation tooling on 2026-09-27

The distributed load path now carries an explicit `postgres` or `redis-first` mode from the
unattended stage through the coordinator to every generator. In Redis-first mode, HTTP 202 is
counted only as provisional intake. The backend audit requires one `DURABLE`
`reservation_commands` row for every acknowledged 202, complete hold/order/idempotency linkage,
zero overlapping ownership, and zero entries and pending messages across reservation streams.
The unattended deployment starts the reservation writer before the API cutover, verifies the
effective API mode and worker source, and restores the captured mode and writer replica count
on rollback.

Validation executed against the rebuilt container image:

- the focused generator, coordinator, audit and unattended-stage selection passed 25 tests;
- the complete unit suite passed 142 tests with two dependency deprecation warnings;
- Ruff passed for the changed Python files, excluding the existing Windows executable-bit
  warning, and both shell helpers passed Alpine `sh -n`;
- the audit executed against local PostgreSQL and Redis and reported matching zero provisional
  acknowledgements, zero reservation stream entries, zero pending stream messages and a passing
  durability/expiry gate. One unrelated pre-existing seat refresh request remained visible in
  the global queue snapshot and is not hidden by the audit.

This completes the tooling portion of activation gate 3. ADR 0059 subsequently records a
passing five-minute no-retry 1,000 RPS safety stage with two reservation writers and exact
15,000-command durability. Managed pending-command writer-restart recovery, DCS primary
failover and a longer confirmation remain unexecuted. The cloud deployment therefore remains
in synchronous PostgreSQL mode outside isolated tests, and no sustained capacity claim is made.

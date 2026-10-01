# ADR 0060: DCS primary-failover validation for Redis-first intake

Date: 2026-10-01
Status: Proposed; production activation requires the passing evidence below

## Context

ADR 0058 prohibits production activation of `redis-first` mode until Redis AOF, replication
and failover are validated, including replay after primary loss. ADR 0059 and the
[writer-restart drill](../capacity/huawei-rds/2026-09-27-redis-writer-restart/README.md)
proved acknowledged provisional holds survive a reservation-writer restart in the managed
Huawei topology. The remaining durability risk is loss of the DCS primary itself:

- Intake uses one connection-affine Lua write plus `WAIT` with
  `REDIS_RESERVATION_REPLICA_ACKS=1`. A primary that dies after acknowledging a client but
  before replication completes can lose acknowledged provisional holds. `WAIT` bounds but
  cannot eliminate this window; the drill must measure it, not assume zero.
- Managed failover moves the read/write endpoint to the promoted standby. During promotion,
  writes may fail or `WAIT` may time out, producing `RESERVATION_DURABILITY_UNKNOWN`
  responses whose replay behavior must be verified.
- The reservation writer's consumer group, pending entries and provisional seat ownership all
  live in Redis. Promotion must preserve them or the writer must be able to rebuild state
  from PostgreSQL; silent loss would break the acknowledged-202 durability contract.

No existing evidence covers this fault. The local Compose stack is single-node Redis and
cannot validate it. This must run against the managed DCS instance in the isolated Huawei
environment.

## Decision

Add `scripts/redis_failover_drill.py`, a bounded drill executed against the isolated
Redis-first cloud topology. The drill does not trigger failover itself; the operator starts a
managed DCS primary/standby switchover from the Huawei console while the drill observes,
measures and verifies. The drill:

1. Preflights the topology: API readiness, effective `redis-first` mode, DCS role `master`
   with at least one connected replica, `noeviction`, AOF enabled, and a passing
   connection-affine `WAIT 1` probe within the configured acknowledgement window.
2. Writes a bounded set of uniquely keyed holds (`--commands`, default 50), polls every
   command to `DURABLE`, and records the acknowledged command set. No client retry is used.
3. Enters the failover window: waits for the operator to confirm switchover start, then
   probes intake and Redis role at one-second intervals. It records the first and last write
   failure, any `RESERVATION_DURABILITY_UNKNOWN` responses, and the observed promotion time.
4. After the endpoint reports a (new) `master` with a connected replica, replays every
   unknown-outcome idempotency key exactly once to resolve stored responses, then re-verifies:
   every acknowledged 202 has exactly one PostgreSQL `DURABLE` reservation command with
   complete hold/order/idempotency linkage, zero overlapping seat intervals, and zero
   reservation stream entries or pending messages after the drain window.
5. Sends a fresh bounded hold set post-failover and requires it to become durable with
   replica acknowledgement on the new master.

The drill writes a single JSON verdict and exits non-zero on any failed gate. Nothing in the
drill restarts, scales or reconfigures the deployment; rollback remains the existing
unattended-stage restore path.

### Acceptance gates

- Zero lost acknowledged commands: every pre-failover HTTP 202 reaches exactly one `DURABLE`
  command with complete linkage after failover and drain.
- Every `RESERVATION_DURABILITY_UNKNOWN` resolves through same-key replay to its original
  stored response; no duplicate hold or order results.
- Intake write unavailability, measured first-failure to first post-promotion success, is
  within `--max-outage-seconds` (default 120; an engineering budget, not an agreed SLO).
- Post-failover intake succeeds with the configured replica acknowledgement on the new
  master, and reservation streams and consumer groups drain to zero.
- Zero overlapping seat ownership across the whole drill.

## Alternatives considered

- Trigger failover through the Huawei Cloud API from the drill: deferred. It requires
  credential scope beyond the drill's verification role; the console switchover is the
  supported managed path, and one-second observation is sufficient for the measured budget.
- Validate failover only with synthetic Redis writes, without HTTP intake: rejected because
  the contract under test is acknowledged HTTP 202 durability, including Lua intake, `WAIT`,
  the stream and the writer path end to end.
- Skip the drill and rely on `WAIT 1` plus AOF `everysec`: rejected. Those bound the loss
  window but do not prove promoted-standby state completeness, endpoint reconnection, stream
  consumer-group survival or same-key replay resolution.
- Run the drill under full 1,000 RPS load immediately: rejected for the first execution. A
  bounded command set isolates durability correctness; a loaded variant is a follow-up stage
  after the bounded drill passes.

## Consequences

Passing evidence satisfies the primary-loss portion of ADR 0058 activation gate 4 and the
acknowledged-hold survival portion of gate 2 in `docs/redis-first-reservations.md`. The
30-minute sustained confirmation remains outstanding. A failed gate keeps the deployment in
synchronous PostgreSQL mode and requires a diagnosis entry before any retry.

The drill creates real holds that expire naturally; it never deletes business data and never
acts outside the run-prefixed idempotency keys and fixture seats.

## Failure and recovery behavior

If the promoted instance loses acknowledged commands, intake must remain in `postgres` mode
and the loss window must be measured against `WAIT`/AOF settings before further Redis-first
stages. If failover does not complete within the outage budget, the drill fails and the
operator restores service per the RDS runbook; already-acknowledged commands remain
recoverable from the stream and are audited at the next opportunity. If the drill itself is
interrupted, its JSON report up to the last save identifies the acknowledged command set for
manual audit with `verify_cloud_holds.py`.

## Validation evidence

Drill implementation and the managed failover execution are pending at decision acceptance
time. No production activation follows until passing evidence is appended here and linked
from `docs/capacity/huawei-rds/`.

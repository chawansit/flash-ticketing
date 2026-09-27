# 0060: Bound Redis reservation-writer failover recovery

## Status

Accepted for implementation on 27 September 2026. This decision refines the failure and recovery behavior of [ADR 0058](0058-redis-first-durable-reservation-intake.md) and [ADR 0059](0059-scale-redis-reservation-writers.md). It does not authorize production activation of Redis-first reservations; activation remains blocked until the managed-DCS failover validation passes exact durability.

## Context

The first live Huawei DCS master/standby switchover drill ran at 500 RPS for ten minutes with two reservation writers and 800 event streams. A new connection to the managed endpoint recovered after one failed 250 ms probe, an observed window of about 500 ms. The long-lived writers nevertheless stopped producing durable PostgreSQL commands for about 129 seconds. They later consumed the commands, but the two-minute provisional holds had expired, so 1,980 of 14,612 HTTP 202 acknowledgements were compensated as expired instead of becoming durable.

The writer currently discovers as many as 5,000 streams and performs group and pending-entry work serially for every stream on every poll. During a managed failover, many individually bounded Redis operations can therefore add up to an unbounded iteration. The worker catches the final exception and sleeps, but it does not explicitly discard pooled connections or bound how many streams one iteration may inspect.

## Decision

Reservation writers will process a rotating, bounded subset of event streams per poll. Stream discovery will be cached briefly and refreshed on a bounded cadence; newly discovered streams remain reachable within that cadence. Consumer-group creation will be remembered per process and repeated only when a group is not yet known locally. Pending-entry reclamation will also rotate across bounded subsets rather than scan every stream before new work is read.

When a reservation writer sees a Redis connection error, it will explicitly disconnect the Redis connection pool, invalidate its local discovery/group state and retry through the existing bounded worker backoff. The writer will not retry the PostgreSQL transaction or acknowledge a stream entry unless the existing idempotent persistence path completes.

The enqueue path keeps its existing `EVAL` plus same-connection `WAIT` contract. It will continue to return an unknown/unavailable outcome when replica acknowledgement cannot be proven; it will not blindly replay a possibly committed Lua command inside the request.

## Alternatives considered

- **Keep the full serial scan and rely on redis-py reconnects.** Rejected because the live drill showed that per-command timeouts can compose into a stall longer than the hold TTL even when the endpoint itself recovers quickly.
- **Increase the hold TTL.** Rejected as the primary fix because it only widens the failure window and makes unavailable seats remain locked longer.
- **Use one global stream.** Deferred. It would simplify discovery but changes sharding, ordering and hot-key behavior and needs a separate capacity decision.
- **Retry every API enqueue automatically.** Rejected because a connection loss after `EVAL` can leave an ambiguous committed write. Client-visible idempotency/status resolution is the safe recovery contract.

## Consequences

One writer poll has bounded Redis work and cannot grow linearly across all event streams during a failover. Round-robin polling introduces a small, measurable delay before a quiet stream is revisited. The batch size and refresh cadence must therefore be chosen so their worst-case revisit time stays well below the hold TTL at the production event count. Local discovery state is advisory and can be rebuilt from the durable Redis registry/key scan after any error or restart.

## Failure and recovery behavior

On Redis connection failure, the current poll stops, pooled sockets are closed, local stream state is invalidated and the worker waits through its existing one-second error backoff. The next iteration reconnects through the managed endpoint and rebuilds bounded discovery state. Unacknowledged stream entries stay pending and are reclaimed by either writer after the idle threshold. PostgreSQL idempotency and uniqueness continue to make replay safe. If recovery cannot keep command age below the hold TTL, the command is compensated and the activation gate fails; no missing command may be reported as durable.

## Validation evidence

The triggering failure is recorded in `docs/capacity/huawei-rds/2026-09-27-dcs-failover/`. Implementation requires unit coverage for bounded round-robin scans and connection-state reset, the existing 100-contender and writer-reclaim integration tests, repository lint/tests, and a new live DCS switchover drill. The live gate requires exact correspondence between all HTTP 202 acknowledgements and durable PostgreSQL reservation commands, zero overlap, drained queues and a writer persistence gap comfortably below the two-minute hold TTL.

# ADR 0063: Bound reservation persistence batches and reject stale intake

Date: 2026-09-28
Status: Accepted for implementation as an isolated experiment; production activation pending validation

## Context

ADR 0058 introduced Redis-first provisional reservation intake and one PostgreSQL transaction
per command. ADR 0059 scaled the writer to two replicas and explicitly deferred transaction
batching because a wider transaction changes failure coupling and lock duration.

The verified 15-minute 1,000 RPS stage with a 3% write mix made all 27,000 acknowledged
commands durable with zero overlap. The matched 6% write stage acknowledged 53,997 provisional
commands but made only 34,781 durable before their fixed leases expired. Request delivery and
read capacity remained healthy, so the sustained limit is the Redis-to-PostgreSQL persistence
lane rather than Redis intake or the read path. The current writer performs a complete
PostgreSQL transaction and commit for every command and admits new provisional ownership based
only on stream length. A stream can therefore remain below its count limit while its oldest
command approaches the 120-second hold expiry.

Commit `525ec20` adds phase, command-age, failure-code and batch-size diagnostics plus redacted
stage summaries. Those diagnostics identify where time is spent in the next cloud run; they do
not themselves increase capacity.

## Decision

Add an opt-in bounded persistence batch to the reservation writer. A writer may collect at most
eight immediately available commands and persist them under one outer PostgreSQL transaction.
Each command runs inside a nested savepoint so a deterministic domain failure rolls back only
that command. Successful and deterministically failed outcomes are retained in memory until the
outer commit succeeds. Only after that commit may the writer mark Redis commands `DURABLE` or
`FAILED` and acknowledge/delete their stream entries.

If the outer transaction or commit fails, the writer changes no Redis command status and
acknowledges no entry. At-least-once stream recovery then retries the complete batch. Existing
`reservation_commands.command_id` and actor/idempotency uniqueness make successful replay safe.
The configured batch size is independent of the Kafka publisher batch size, is restricted to
1-8, and defaults to 1 until an isolated comparison promotes a larger value.

Extend the atomic Redis intake operation with event-local lag admission. After checking for an
idempotent replay, Lua reads the oldest remaining entry in that event's reservation stream. If
its Redis stream timestamp is older than the configured maximum command age, new holds are
rejected with `RESERVATION_PERSISTENCE_LAGGING`; existing idempotent replays remain readable.
The threshold must be lower than `HOLD_SECONDS` and leave at least 30 seconds for persistence
and client observation. It is disabled by default until the experiment supplies an explicit
value. The existing stream-length limit remains an independent memory bound.

This decision extends ADR 0058 and ADR 0059. It does not change Redis atomic seat ownership,
PostgreSQL authority, the 120-second hold TTL, payment gating, transactional outbox semantics,
Kafka delivery, or the zero-double-booking constraints.

## Alternatives considered

- Add more reservation writers only. Two writers pass 30 writes per second but not the 60 writes
  per second sustained stage; additional writers also add commits and can amplify WAL pressure.
- Increase the hold TTL. This delays visible failure and changes the customer contract without
  improving steady-state persistence throughput.
- Accept commands until the stream count limit. Count does not reveal whether the oldest
  command is close to expiry, especially across many event-local streams.
- Reject on a global queue-age value. A single hot or stalled event would reduce availability
  for unrelated events and would add a cross-slot coordination key to Redis Cluster.
- Persist the whole batch without savepoints. One deterministic bad command would roll back and
  repeatedly poison every other command in the batch.
- Mark Redis outcomes before PostgreSQL commit. A later commit failure could expose a durable
  status for data that does not exist in PostgreSQL.
- Use Kafka for reservation persistence. Redis ownership and Kafka publication cannot commit
  atomically, while the existing Redis Stream already provides replay in the same hash slot as
  the provisional command.

## Consequences

A batch amortizes PostgreSQL commit, pool acquisition and transaction setup across up to eight
commands. It retains the existing SQL validation and uniqueness backstops. The tradeoff is a
longer transaction, more locks held until the shared commit and a larger replay unit after a
transient failure. The strict size cap, immediate-only collection and current statement/lock
timeouts bound that exposure.

Lag admission stops creating provisional promises when an event's persistence lane no longer
has enough TTL margin. It can increase explicit 503 responses during overload, but prevents
those requests from becoming acknowledged holds that later end as `HOLD_EXPIRED`. Clients may
retry a rejected new request with bounded backoff; they must reuse the same idempotency key for
any ambiguous or previously acknowledged request.

Metrics must distinguish commands attempted, transaction commits, deterministic failures,
outer transaction failures, post-commit Redis status failures, batch size and oldest-command
age. Capacity is qualified by durable commands, not HTTP 202 intake alone.

## Failure and recovery behavior

- A writer crash before outer commit leaves all batch entries unacknowledged; PostgreSQL rolls
  back and another writer reclaims them.
- A crash after commit but before Redis status updates replays the batch. PostgreSQL returns the
  stored command responses and Redis is advanced idempotently.
- A deterministic command failure rolls back to its savepoint. After the outer commit succeeds,
  only that command is token-fenced as failed and only seats still owned by it are released.
- An outer commit failure publishes no Redis outcome. Every entry stays pending for retry.
- A Redis error after PostgreSQL commit leaves the stream entry pending. Replay discovers the
  durable PostgreSQL command and repairs Redis status before acknowledgement.
- Commands already accepted are never discarded by lag admission. Existing replays are served
  before the lag check, and pending entries remain recoverable.
- If the oldest stream entry exceeds the age threshold, only new commands for that event are
  rejected. Other events and seat-map reads continue.
- If batching increases deadlocks, lock waits or p99 transaction time beyond the gate, restore
  batch size 1 without a schema or data migration.

## Validation evidence and activation gates

Evidence available at decision time:

- The 2026-09-28 3% write stage passed for 15 minutes at 1,000 RPS with 27,000 of 27,000 durable
  commands, zero double-booking and drained queues.
- The matched 6% stage produced 34,781 durable commands from 53,997 provisional acknowledgements
  and failed the durability gate. This establishes a sustained persistence shortfall; it does
  not yet attribute the shortfall to a specific PostgreSQL phase.
- Diagnostic instrumentation at `525ec20` passed the complete isolated Compose suite: 224 tests,
  including end-to-end checkout and the 100-contender same-seat test. Two dependency
  deprecation warnings remain.

Before a batch size above 1 or lag admission is promoted:

1. Unit and PostgreSQL/Redis integration tests must cover mixed success/failure batches,
   savepoint isolation, outer commit rollback, crash-after-commit replay, post-commit Redis
   failure, oldest-entry rejection and replay while lagging.
2. Compare batch sizes 1, 4 and 8 with the same fixture, writer count, connection budget and
   no-retry workload. Record transaction body, commit, pool wait, batch size, command age,
   failure codes, RDS CPU/IO and WAL observations.
3. A short 1,000 RPS / 6% diagnostic must show zero overlap, exact linkage for every durable
   command and complete queue drain. It may be used to choose a candidate but cannot certify
   sustained capacity.
4. The selected candidate must pass 1,000 RPS / 6% writes for 15 minutes with no generator
   drops, no unexpected errors, every acknowledged command durable, zero double-booking and all
   reservation streams/pending entries drained within the existing recovery window.
5. Rollback to batch size 1 and disabled lag admission must be exercised and leave four healthy
   API replicas and the captured writer topology.

Implementation and higher write capacity are not claimed by this ADR at creation time.
## Implementation validation on 2026-09-28

The opt-in implementation now includes a writer-specific batch size, savepoint-isolated command
writes under one outer transaction, post-commit Redis status and acknowledgement, event-local
oldest-stream-entry admission, restored `hold_id` ownership in full snapshots, and unattended
cloud deploy/rollback controls for both candidate settings. Defaults remain batch size 1 and
lag admission disabled.

- The focused configuration and PostgreSQL/Redis integration selection passed 18 tests. It
  covers a mixed durable/failing batch, post-commit Redis failure and complete replay, pending
  recovery, oldest-command rejection and same-key replay while lagging.
- The unattended capacity-stage selection passed 13 tests, and the backend helper passed Alpine
  POSIX shell syntax validation.
- Ruff passed for the changed Python files with the repository's existing Windows executable-bit
  warning excluded.
- The final isolated Compose suite passed 229 tests, including end-to-end checkout and the
  100-contender same-seat test, with two dependency deprecation warnings. The preceding full
  run had one transient Redis socket timeout in the seat-map TTL test; that test passed alone
  and the complete confirmation run then passed.

No Huawei load has been run with batch size above 1 or lag admission enabled. The cloud
activation and capacity gates in this ADR therefore remain pending.

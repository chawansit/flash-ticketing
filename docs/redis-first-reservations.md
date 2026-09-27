# Redis-first reservation intake

This is an opt-in capacity path defined by [ADR 0058](adr/0058-redis-first-durable-reservation-intake.md).
The measured production default remains `RESERVATION_MODE=postgres` until Redis HA/failover and
cloud load gates pass.

## Request flow

1. `POST /v1/holds` validates authentication and rate limits the actor.
2. One Redis Lua operation checks the sale window, idempotency key and all requested seats;
   records provisional ownership; stores the response; schedules expiry; and appends a command
   to the event's Redis Stream.
3. Redis-first mode returns HTTP `202` with `persistence_status=PENDING`, a stable
   `command_id`, `hold_id`, `order_id` and deadline.
4. The `reservation-writer` service consumes the command at least once. One PostgreSQL
   transaction revalidates the event and seats and writes the hold, pending order, items,
   idempotency response, command receipt and transactional outbox event.
5. The writer marks the command `DURABLE` and acknowledges/deletes the stream entry. Clients
   poll `GET /v1/reservation-commands/{event_id}/{command_id}`.
6. A deterministic database conflict marks the command `FAILED`, releases only Redis seats
   still owned by that hold token, and rebuilds the affected snapshot.

Checkout and payment continue to use PostgreSQL. Before the writer commits, checkout returns
`HOLD_NOT_FOUND`; clients should keep polling the reservation command rather than start payment.

## Local activation

Local Compose uses single-node Redis with AOF for development. It cannot validate node-loss
durability.

```sh
RESERVATION_MODE=redis-first docker compose up -d --build api reservation-writer
docker compose --profile tools run --rm seed
```

The synchronous rollback is:

```sh
RESERVATION_MODE=postgres docker compose up -d --force-recreate api
docker compose stop reservation-writer
```

For the RDS deployment overlay, keep the DCS connection string only in the private
`.env.rds` file as `DCS_REDIS_URL`. Use the read/write hostname so managed failover can
move the endpoint; the read-only hostname must never serve reservation intake.

Relevant settings:

| Setting | Default | Purpose |
|---|---:|---|
| `RESERVATION_MODE` | `postgres` | Select synchronous or Redis-first hold intake |
| `REDIS_RESERVE_CONCURRENCY` | 128 | Per-API in-flight Redis intake limit |
| `REDIS_RESERVATION_REPLICA_ACKS` | 0 | Required replica acknowledgements; production Redis-first requires at least 1 |
| `REDIS_RESERVATION_WAIT_MS` | 100 | Maximum Redis `WAIT` time |
| `REDIS_RESERVATION_MAX_BACKLOG` | 10,000 | Maximum unacknowledged stream entries per event |

## Correctness and recovery

- Lua provides all-seat atomicity and one winner for a seat before PostgreSQL persistence.
- Actor, idempotency key and request hash replay the same command; changed payloads return
  `IDEMPOTENCY_MISMATCH`.
- `EVAL` and `WAIT` use one Redis connection. Insufficient replica acknowledgement or a
  connection failure after dispatch returns `RESERVATION_DURABILITY_UNKNOWN`; the write may
  exist, so make only a bounded retry with the identical actor, payload and idempotency key.
  The API supplies `Retry-After: 1`. Never replace the key to recover an unknown result.
- A database or writer outage leaves the stream item pending. `XAUTOCLAIM` transfers abandoned
  work to another writer after the lease interval.
- Writers create consumer groups for every discovered event, reclaim abandoned deliveries,
  then read all active event streams together. Continuous traffic on an earlier stream cannot
  starve commands from later events.
- PostgreSQL `reservation_commands.command_id` and the existing idempotency primary key make
  replay after a commit safe. A missing or already-failed Redis command cannot be marked
  durable, so its stream entry remains available for operator recovery instead of being
  silently acknowledged.
- Stream length bounds work per event. The Lua operation rejects before changing seats when the
  backlog is full.
- Redis provisional updates never advance PostgreSQL `source_version`. A committed database
  change can therefore repair stale availability without overwriting a newer provisional owner.
- Redis expiry permits a new provisional owner after the deadline. An old delayed command then
  fails as expired or conflicts with the newer owner; token-fenced compensation cannot clear
  that newer owner.

## Metrics and logs

- `ticketing_reservation_intake_total{outcome=...}`
- `ticketing_reservation_replica_acknowledgements`
- `ticketing_reservation_command_age_seconds`
- `ticketing_reservation_persistence_total{outcome=durable|failed}`
- `ticketing_worker_errors_total{role="reservation-writer"}`

Terminal business conflicts emit a structured `reservation_command_failed` log with command ID
and fixed error code. Infrastructure failures remain pending and are reported by worker error
metrics and logs.

## Distributed validation

The distributed tools require an explicit reservation mode so an HTTP 202 cannot be
mistaken for a durable reservation. Run the generator with:

```sh
python scripts/parallel_cloud_load.py \
  --manifest /private/load-manifest.json \
  --output /private/load-results \
  --rate 1000 \
  --seconds 600 \
  --workers 4 \
  --reservation-mode redis-first
```

After the hold TTL and worker drain window, run `verify_cloud_holds.py` with both
`TEST_DATABASE_URL` and `TEST_REDIS_URL`. The audit requires one PostgreSQL
`reservation_commands` row in `DURABLE` state for every acknowledged HTTP 202, complete
hold/order/idempotency linkage, zero overlapping seat intervals, and zero entries or pending
messages across registered reservation streams. HTTP 202 is provisional and is never enough
for a passing result by itself.

The unattended RDS stage propagates `--reservation-mode redis-first`, starts the bounded
`--reservation-writer-candidate` count before replacing the API replicas, verifies the effective
API mode, writer replica count and worker source, and restores the captured mode and writer
replica count during rollback.

## Activation gates still outstanding

The code and local tests do not authorize production activation. The remaining gates are:

1. Deploy Redis with `noeviction`, AOF, a replica, monitored replication lag and a tested
   primary-failover procedure.
2. Prove acknowledged provisional holds survive primary loss and writer restart.
3. Run a longer confirmation of the passing five-minute 1,000 RPS safety topology while command
   age, errors, overlap, PostgreSQL durability and final stream drain all remain within gates.

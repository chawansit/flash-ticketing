# Local operational runbook

## Startup and configuration

Run `docker compose up -d --build`, then `docker compose run --rm seed`.
Migrations are serialized and checksummed. Do not edit an applied migration; add the next SQL file.
Inspect `docker compose ps` and `docker compose logs --tail 100 api publisher consumer maintenance simulator`.
Configure non-default signing secrets before using an environment other than development.

## Observability

Prometheus scrapes the API and every worker. Start with HTTP latency/status, business outcome
counters, worker failures and oldest unpublished event age. Do not use customer or seat IDs as
metric labels. JSON request logs have a generated request ID and bounded route labels.

Suggested local alerts: sustained 503s, rising outbox age (>30 seconds), worker failures, and
any dead letters. Observe DB connections and locks using PostgreSQL system views; PgBouncer
caps backend connections. This MVP does not include a PostgreSQL exporter.

## Failure drills

The automated host-side Kafka drill is `python scripts/recovery_drill.py` from an activated
project environment. It always restarts Kafka, even when a check fails.

1. Stop Kafka with `docker compose stop kafka`, make a simulated payment, and confirm the order
   reaches PAID while its outbox remains unpublished. Restart with `docker compose start kafka`;
   confirm eventual FULFILLED. Do not delete Kafka volumes.
2. Stop the consumer, complete a payment, restart the consumer and confirm exactly one ticket.
3. Stop maintenance, let a hold expire, reserve the same seat as a new user, restart maintenance
   and verify that the new hold survives.
4. Stop Redis and verify reservations/seat maps fail quickly with 503, while payment callbacks
   can still finalize valid holds through PostgreSQL.

## Dead-letter replay

Inspect `dead_letters` in PostgreSQL and worker logs. After fixing the cause:

```sh
docker compose exec consumer python -m ticketing.cli replay --id DEAD_LETTER_UUID
```

Replay removes the dead letter only after processing succeeds. Inbox uniqueness makes an
already-completed event safe to replay. Invalid event payloads require correcting the upstream
producer; do not silently manufacture a successful outcome.

## Payment reconciliation

Unacknowledged simulated deliveries are retried after the 15-second dispatch lease. Outbox
publication leases last 30 seconds. Poll orders for REFUND_PENDING and inspect consumer status
if refunds do not settle. A real provider requires an idempotent gateway adapter and provider
status reconciliation; the local simulator performs neither a real charge nor real refund.

## Scaling

Pre-warm and pre-scale before opening sales. Count pools across **all** API and worker replicas
against PgBouncer's limits. Keep transactions short; do not increase lock timeouts to hide
contention. Add edge abuse/bot protection before exposing the service publicly. Redis outage
does not trigger a seat-map read stampede into PostgreSQL.

`RESERVE_CONCURRENCY` caps reservation requests before the synchronous thread pool. Excess
requests receive 503 ADMISSION_FULL immediately; this is overload rejection, not a waiting room.
The default is 8 per API process. Raising it requires a new latency and connection-budget test.

## Coalesced cache refresh operations

Apply migration 002 before starting the optimized consumer and maintenance workers. Do not remove
the new table during a rollback; stop writers and drain outstanding refresh work before reverting
workers. Existing periodic rebuilds remain a fallback, but do not count a leftover dirty queue as
successfully processed.

Monitor ticketing_cache_refresh_pending and ticketing_cache_refresh_oldest_seconds alongside
outbox/inbox backlog and cache version. The consumer acknowledges durable intent; cache availability
is still eventually consistent. If Redis fails, the dirty request remains and its lease can be
reclaimed after 30 seconds. Inspect the maintenance logs and Redis availability before retrying.
Never delete dirty rows to make a backlog graph look healthy.

PUBLISHER_BATCH_SIZE defaults to 32 (1â€“100), SIMULATOR_CONCURRENCY to 4 (1â€“DB_POOL_MAX),
REFRESH_COOLDOWN_MS to 250 (1â€“5000). Existing API admission and database pool sizes remain unchanged.

## Incremental projection rollout

Apply migration 003 before starting the new consumer or maintenance worker. Restart API
and workers together; the new hash key is `seatmap:v2:{event-id}`. Old JSON keys expire
naturally. Allow warming responses until a full snapshot completes. Mixed old/new
cache writers are not a supported rolling update. Rollback API/workers together; retain
the additive migration and let the old periodic snapshot warm its own key.

Refresh SQL acknowledgements remain lease-token fenced. Do not delete dirty work to
hide backlog. A hash with `updating` is unreadable after an interrupted write; full
reconciliation repairs it. Legacy messages without seat IDs request full reconciliation.
Cold full snapshots at 10,000 and 50,000 seats exceeded the 100 ms Redis timeout in the
local adapter probe. Large inventories need bounded reconciliation work before claiming
support at those sizes; simply raising the timeout does not remove Redis blocking.

See [metrics](observability.md) and [current measurements](capacity/incremental/README.md).

## Bounded reconciliation scheduling

Apply migration 004 before starting this maintenance worker. It is additive; keep it
installed across a rollback, because the previous periodic sweep simply ignores the table.
On a large `events` table build `events_sale_window` with `CREATE INDEX CONCURRENTLY` by
hand first: the migration runner wraps each file in one transaction and cannot use
`CONCURRENTLY`. The `IF NOT EXISTS` guard then makes the statement a no-op.

Maintenance no longer rebuilds every retained event's seat map. It keeps a durable schedule
in `event_reconciliation` for events inside their sale window
(`sale_starts - RECONCILE_WINDOW_SECONDS` to `sale_ends + RECONCILE_WINDOW_SECONDS`), claims
the oldest-due rows in bounded batches under token-fenced leases, and spends at most
`RECONCILE_BUDGET_MS` per loop so hold expiry and dirty-seat refresh are never starved.

`RECONCILE_INTERVAL_SECONDS` defaults to 20 (1-29; must remain below the 30-second map TTL), `RECONCILE_WINDOW_SECONDS` to 300
(0-86400), `RECONCILE_BATCH_SIZE` to 8 (1-100), `RECONCILE_BUDGET_MS` to 500 (50-5000),
`RECONCILE_LEASE_SECONDS` to 30 (5-300), `RECONCILE_BACKOFF_MS` to 1000 (100-60000) and
`RECONCILE_SEED_BATCH` to 200 (1-5000).

Watch `ticketing_reconciliation_tracked_events` converge after deploy, then
`ticketing_reconciliation_backlog` and `ticketing_reconciliation_overdue_seconds`. Alert when
overdue age approaches the 30-second seat-map TTL minus the interval, and on sustained
`ticketing_reconciliation_failures_total{stage="snapshot"}`. Only a full snapshot extends the
TTL, so a schedule that cannot keep up means maps expire and reads fall back to
`SEATMAP_WARMING` until a rebuild. Local measurement put the ceiling near 4,200 active
300-seat shows per worker; treat that as an order of magnitude, not a guarantee, and
re-measure on real hardware.

Deadlines slip uniformly under overload rather than starving individual events. Do not raise
`RECONCILE_BATCH_SIZE` or `RECONCILE_BUDGET_MS` to hide a backlog: that trades hold-expiry
latency for a flatter graph. Add maintenance instances instead, which is safe because claims
use `SKIP LOCKED` with per-event leases. Never delete schedule rows to clear a backlog.
Events outside their sale window are not proactively reconciled and rely on SeatsChanged
notifications; a stale map for a closed event is expected, not an incident.

See [metrics](observability.md) and [current measurements](capacity/reconciliation/README.md).

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

import argparse
import hashlib
import hmac
import json
import logging
import os
import signal
import time
import urllib.request
from concurrent.futures import ThreadPoolExecutor, as_completed
from uuid import uuid4

from kafka import KafkaConsumer, KafkaProducer, TopicPartition
from prometheus_client import start_http_server
from psycopg.types.json import Jsonb

from ticketing.application.reservations import Reservations
from ticketing.config import Settings
from ticketing.infrastructure.cache import RedisSeats
from ticketing.infrastructure.postgres import Postgres
from ticketing.infrastructure.reservations import PostgresReservations, event
from ticketing.observability import (
    CACHE_ROWS,
    OUTBOX_AGE,
    RECONCILE_BACKLOG,
    RECONCILE_EVENTS,
    RECONCILE_FAILURES,
    RECONCILE_OVERDUE,
    RECONCILE_RECOVERED,
    RECONCILE_SECONDS,
    RECONCILE_TRACKED,
    REFRESH_AGE,
    REFRESH_PENDING,
    WORKER_ERRORS,
    configure_logging,
    measured_work,
)

log = logging.getLogger("ticketing.worker")
running = True


def stop(*_):
    global running
    running = False


@measured_work("snapshot")
def snapshot(db, cache, event_id):
    # One statement gives a consistent snapshot. A global sequence is not used: allocation
    # order is not commit order. Sum of monotonically increasing row versions is monotonic.
    with db.transaction() as conn:
        rows = conn.execute(
            """SELECT seat_id,price,hold_id,reserved_until,booked_order_id,version
            FROM event_seats WHERE event_id=%s ORDER BY seat_id""",
            (event_id,),
        ).fetchall()
    CACHE_ROWS.labels("full").inc(len(rows))
    version = sum(r["version"] for r in rows)
    # Redis atomically preserves the last aggregate marker for unchanged seats.
    seats = [
        {
            "seat_id": r["seat_id"],
            "price": r["price"],
            "status": "SOLD" if r["booked_order_id"] else "HELD" if r["hold_id"] else "AVAILABLE",
            "reserved_until": r["reserved_until"],
            "version": version,
            "source_version": r["version"],
        }
        for r in rows
    ]
    cache.put(str(event_id), version, {"event_id": str(event_id), "version": version, "seats": seats})


@measured_work("publish_batch")
def publish_batch(db, producer, limit=32):
    token = uuid4()
    with db.transaction() as conn:
        age = conn.execute("""SELECT EXTRACT(EPOCH FROM clock_timestamp()-min(occurred_at)) AS age
            FROM outbox_events WHERE published_at IS NULL""").fetchone()["age"]
        OUTBOX_AGE.set(float(age or 0))
        rows = conn.execute(
            """SELECT * FROM outbox_events WHERE published_at IS NULL
            AND (lease_until IS NULL OR lease_until < clock_timestamp())
            ORDER BY occurred_at LIMIT %s FOR UPDATE SKIP LOCKED""",
            (limit,),
        ).fetchall()
        if not rows:
            return False
        conn.execute(
            """UPDATE outbox_events SET lease_until=clock_timestamp()+interval '30 seconds',lease_token=%s
            WHERE id=ANY(%s)""",
            (token, [row["id"] for row in rows]),
        )
    pending, confirmed, errors = [], [], []
    deadline = time.monotonic() + 10
    for row in rows:
        if time.monotonic() >= deadline:
            break
        envelope = {
            "event_id": str(row["id"]),
            "aggregate_id": str(row["aggregate_id"]),
            "event_type": row["event_type"],
            "schema_version": row["schema_version"],
            "occurred_at": row["occurred_at"].isoformat(),
            "payload": row["payload"],
        }
        try:
            future = producer.send("ticketing.events", key=str(row["aggregate_id"]).encode(), value=envelope)
            pending.append((row["id"], future))
        except Exception as exc:  # noqa: BLE001 - re-raised after preserving successful acknowledgements
            errors.append(exc)
            break
    for event_id, future in pending:
        try:
            future.get(timeout=max(0, deadline - time.monotonic()))
            confirmed.append(event_id)
        except Exception as exc:  # noqa: BLE001 - re-raised after preserving successful acknowledgements
            errors.append(exc)
    if confirmed:
        with db.transaction() as conn:
            conn.execute(
                """UPDATE outbox_events SET published_at=clock_timestamp(),lease_until=NULL
                WHERE id=ANY(%s) AND lease_token=%s""",
                (confirmed, token),
            )
    if errors:
        raise errors[0]
    return bool(confirmed)


def publish_one(db, producer):
    return publish_batch(db, producer, limit=1)


def request_refresh(conn, event_id, seat_ids=None):
    conn.execute(
        """INSERT INTO seat_refresh_requests(event_id,seat_ids) VALUES (%s,%s)
        ON CONFLICT(event_id) DO UPDATE SET generation=seat_refresh_requests.generation+1,
        seat_ids=CASE
            WHEN seat_refresh_requests.generation=seat_refresh_requests.completed_generation THEN EXCLUDED.seat_ids
            WHEN seat_refresh_requests.seat_ids IS NULL OR EXCLUDED.seat_ids IS NULL THEN NULL
            ELSE ARRAY(SELECT DISTINCT unnest(seat_refresh_requests.seat_ids || EXCLUDED.seat_ids)) END,
        requested_at=CASE
            WHEN seat_refresh_requests.generation=seat_refresh_requests.completed_generation
            THEN clock_timestamp() ELSE seat_refresh_requests.requested_at END""",
        (event_id, seat_ids),
    )


def changed_snapshot(db, cache, event_id, seat_ids):
    with db.transaction() as conn:
        rows = conn.execute(
            """SELECT seat_id,price,hold_id,reserved_until,booked_order_id,version
            FROM event_seats WHERE event_id=%s AND seat_id=ANY(%s) ORDER BY seat_id""",
            (event_id, seat_ids),
        ).fetchall()
    seats = [seat_state(row) for row in rows]
    CACHE_ROWS.labels("patch").inc(len(seats))
    if not cache.patch(str(event_id), seats):
        snapshot(db, cache, event_id)


def seat_state(row):
    return {
        "seat_id": row["seat_id"],
        "price": row["price"],
        "status": "SOLD" if row["booked_order_id"] else "HELD" if row["hold_id"] else "AVAILABLE",
        "reserved_until": row["reserved_until"],
        "source_version": row["version"],
    }


@measured_work("refresh_one")
def refresh_one(db, cache, cooldown_ms=250):
    token = uuid4()
    with db.transaction() as conn:
        state = conn.execute("""SELECT count(*) AS n,
            EXTRACT(EPOCH FROM clock_timestamp()-min(requested_at)) AS age
            FROM seat_refresh_requests WHERE generation>completed_generation""").fetchone()
        REFRESH_PENDING.set(state["n"])
        REFRESH_AGE.set(float(state["age"] or 0))
        row = conn.execute("""SELECT * FROM seat_refresh_requests
            WHERE generation>completed_generation AND next_attempt_at<=clock_timestamp()
            AND (lease_until IS NULL OR lease_until<clock_timestamp())
            ORDER BY requested_at LIMIT 1 FOR UPDATE SKIP LOCKED""").fetchone()
        if not row:
            return False
        claimed_at = conn.execute(
            """UPDATE seat_refresh_requests
            SET lease_token=%s,lease_until=clock_timestamp()+interval '30 seconds'
            WHERE event_id=%s RETURNING clock_timestamp() AS claimed_at""",
            (token, row["event_id"]),
        ).fetchone()["claimed_at"]
    # No SQL locks while writing Redis. The snapshot is fenced by inventory version.
    if row["seat_ids"] is None:
        snapshot(db, cache, row["event_id"])
    else:
        changed_snapshot(db, cache, row["event_id"], row["seat_ids"])
    with db.transaction() as conn:
        conn.execute(
            """UPDATE seat_refresh_requests SET completed_generation=%s,
            seat_ids=CASE WHEN generation=%s THEN NULL ELSE seat_ids END,
            lease_until=NULL,lease_token=NULL,
            next_attempt_at=clock_timestamp()+(%s * interval '1 millisecond'),
            requested_at=CASE WHEN generation>%s THEN %s ELSE requested_at END
            WHERE event_id=%s AND lease_token=%s""",
            (
                row["generation"],
                row["generation"],
                cooldown_ms,
                row["generation"],
                claimed_at,
                row["event_id"],
                token,
            ),
        )
    return True


# --- Bounded proactive reconciliation -------------------------------------------------
# The schema models scheduled inventory as `events` only. Proactive reconciliation is
# therefore defined purely by the sale window that exists today: an event is active from
# `sale_starts - window` until `sale_ends + window`. No show-end, screen or location field
# is invented. Events outside the window are still reconciled reactively through
# seat_refresh_requests when a SeatsChanged notification arrives.
ACTIVE_WINDOW = """e.sale_starts <= statement_timestamp() + (%s * interval '1 second')
    AND e.sale_ends > statement_timestamp() - (%s * interval '1 second')"""
BACKOFF_SHIFT_CAP = 5  # Retry delay tops out at base * 2**5.
BACKLOG_COUNT_CAP = 10000  # Backlog gauge saturates rather than scanning unbounded rows.
TRACKED_COUNT_CAP = 50000


def schedule_window(settings):
    return (settings.reconcile_window_seconds, settings.reconcile_window_seconds)


@measured_work("reconcile_schedule")
def maintain_schedule(db, settings):
    """Bounded seed, prune and metric sampling. Never runs inside a claim or Redis write."""
    window = schedule_window(settings)
    with db.transaction() as conn:
        seeded = conn.execute(
            f"""INSERT INTO event_reconciliation(event_id)
            SELECT e.id FROM events e
            WHERE {ACTIVE_WINDOW} AND NOT EXISTS (
                SELECT 1 FROM event_reconciliation r WHERE r.event_id=e.id)
            ORDER BY e.sale_starts LIMIT %s
            ON CONFLICT DO NOTHING""",
            (*window, settings.reconcile_seed_batch),
        ).rowcount
        # Leaving the active window drops the advisory row. It is derived state and is
        # re-seeded if the window reopens; a leased row is never removed underneath a worker.
        pruned = conn.execute(
            f"""DELETE FROM event_reconciliation WHERE event_id = ANY(ARRAY(
                SELECT r.event_id FROM event_reconciliation r JOIN events e ON e.id=r.event_id
                WHERE NOT ({ACTIVE_WINDOW})
                AND (r.lease_until IS NULL OR r.lease_until < clock_timestamp())
                ORDER BY r.event_id LIMIT %s FOR UPDATE OF r SKIP LOCKED))
                AND (lease_until IS NULL OR lease_until < clock_timestamp())""",
            (*window, settings.reconcile_seed_batch),
        ).rowcount
        row = conn.execute(
            f"""SELECT
            (SELECT count(*) FROM (SELECT 1 FROM event_reconciliation
                WHERE next_due_at<=clock_timestamp() LIMIT {BACKLOG_COUNT_CAP}) due) AS backlog,
            (SELECT count(*) FROM (SELECT 1 FROM event_reconciliation
                LIMIT {TRACKED_COUNT_CAP}) all_rows) AS tracked,
            (SELECT EXTRACT(EPOCH FROM clock_timestamp()-min(next_due_at))
                FROM event_reconciliation) AS overdue"""
        ).fetchone()
    RECONCILE_BACKLOG.set(row["backlog"])
    RECONCILE_TRACKED.set(row["tracked"])
    RECONCILE_OVERDUE.set(max(0.0, float(row["overdue"] or 0)))
    return seeded, pruned


def claim_due_events(db, settings):
    """Claim a bounded batch under one short transaction. Returns (token, event ids)."""
    token = uuid4()
    window = schedule_window(settings)
    with db.transaction() as conn:
        rows = conn.execute(
            f"""SELECT r.event_id, r.lease_until FROM event_reconciliation r
            JOIN events e ON e.id=r.event_id
            WHERE r.next_due_at<=clock_timestamp()
            AND (r.lease_until IS NULL OR r.lease_until<clock_timestamp())
            AND {ACTIVE_WINDOW}
            ORDER BY r.next_due_at, r.event_id
            LIMIT %s FOR UPDATE OF r SKIP LOCKED""",
            (*window, settings.reconcile_batch_size),
        ).fetchall()
        if not rows:
            return token, []
        recovered = sum(1 for row in rows if row["lease_until"] is not None)
        conn.execute(
            """UPDATE event_reconciliation
            SET lease_token=%s, lease_until=clock_timestamp()+(%s * interval '1 second'),
            claimed_at=clock_timestamp() WHERE event_id=ANY(%s)""",
            (token, settings.reconcile_lease_seconds, [row["event_id"] for row in rows]),
        )
    if recovered:
        RECONCILE_RECOVERED.inc(recovered)
    return token, [row["event_id"] for row in rows]


def complete_reconciliation(db, settings, event_id, token):
    with db.transaction() as conn:
        result = conn.execute(
            """UPDATE event_reconciliation SET last_reconciled_at=clock_timestamp(),
            next_due_at=clock_timestamp()+(%s * interval '1 second'),
            consecutive_failures=0, lease_until=NULL, lease_token=NULL
            WHERE event_id=%s AND lease_token=%s""",
            (settings.reconcile_interval_seconds, event_id, token),
        )
        return result.rowcount == 1


def defer_reconciliation(db, settings, event_id, token):
    """Bounded exponential backoff. A stale token cannot move another worker's deadline."""
    cap = settings.reconcile_backoff_ms * 2**BACKOFF_SHIFT_CAP
    with db.transaction() as conn:
        conn.execute(
            f"""UPDATE event_reconciliation SET consecutive_failures=consecutive_failures+1,
            next_due_at=clock_timestamp() + (LEAST(
                %s * power(2, LEAST(consecutive_failures, {BACKOFF_SHIFT_CAP})), %s)
                * interval '1 millisecond'),
            lease_until=NULL, lease_token=NULL
            WHERE event_id=%s AND lease_token=%s""",
            (settings.reconcile_backoff_ms, cap, event_id, token),
        )


@measured_work("reconcile_batch")
def reconcile_batch(db, cache, settings, deadline=None):
    """One bounded batch. No SQL lock is held while `snapshot` writes Redis, and one slow
    or failing event can neither abort the batch nor block another event's progress."""
    token, event_ids = claim_due_events(db, settings)
    processed = 0
    for index, event_id in enumerate(event_ids):
        if deadline is not None and time.monotonic() >= deadline:
            with db.transaction() as conn:
                conn.execute(
                    """UPDATE event_reconciliation SET lease_until=NULL,lease_token=NULL
                    WHERE event_id=ANY(%s) AND lease_token=%s""",
                    (event_ids[index:], token),
                )
            break
        processed += 1
        started = time.monotonic()
        try:
            snapshot(db, cache, event_id)
        except Exception:
            RECONCILE_SECONDS.labels("error").observe(time.monotonic() - started)
            RECONCILE_EVENTS.labels("error").inc()
            RECONCILE_FAILURES.labels("snapshot").inc()
            log.exception("reconciliation_failed")
            try:
                defer_reconciliation(db, settings, event_id, token)
            except Exception:
                RECONCILE_FAILURES.labels("defer").inc()
                log.exception("reconciliation_defer_failed")
            continue
        try:
            acknowledged = complete_reconciliation(db, settings, event_id, token)
        except Exception:
            RECONCILE_FAILURES.labels("acknowledge").inc()
            log.exception("reconciliation_acknowledge_failed")
            RECONCILE_EVENTS.labels("ack_error").inc()
            continue
        if not acknowledged:
            RECONCILE_EVENTS.labels("stale").inc()
            continue
        RECONCILE_SECONDS.labels("ok").observe(time.monotonic() - started)
        RECONCILE_EVENTS.labels("ok").inc()
    return processed


def reconcile_pass(db, cache, settings, deadline):
    """Claim batches until the wall-clock budget is spent. Overrun is bounded by one started snapshot plus database bookkeeping,
    so hold expiry and dirty-seat processing keep their turn in the maintenance loop."""
    done = 0
    while time.monotonic() < deadline:
        claimed = reconcile_batch(db, cache, settings, deadline)
        done += claimed
        if not claimed:
            break
    return done


@measured_work("consume_event")
def consume_event(db, cache, envelope):
    if envelope["schema_version"] != 1:
        raise ValueError("Unsupported event schema")
    kind, data = envelope["event_type"], envelope["payload"]
    with db.transaction() as conn:
        inserted = conn.execute(
            """INSERT INTO consumer_inbox VALUES ('fulfillment',%s)
            ON CONFLICT DO NOTHING RETURNING event_id""",
            (envelope["event_id"],),
        ).fetchone()
        if not inserted:
            return
        if kind == "SeatsChanged":
            if "seats" in data:
                request_refresh(conn, data["event_id"], data["seats"])
            else:
                request_refresh(conn, data["event_id"])
        elif kind == "OrderPaid":
            order = conn.execute(
                "SELECT * FROM orders WHERE id=%s FOR UPDATE", (data["order_id"],)
            ).fetchone()
            if order["status"] not in {"PAID", "FULFILLED"}:
                raise ValueError("OrderPaid has no paid order")
            bookings = conn.execute("SELECT id FROM bookings WHERE order_id=%s", (order["id"],)).fetchall()
            for booking in bookings:
                conn.execute(
                    "INSERT INTO tickets(id,booking_id) VALUES (%s,%s) ON CONFLICT DO NOTHING",
                    (uuid4(), booking["id"]),
                )
            conn.execute("UPDATE orders SET status='FULFILLED' WHERE id=%s", (order["id"],))
            event(conn, order["id"], "TicketsIssued", {"order_id": str(order["id"])})
        elif kind == "RefundRequested":
            # Simulator's financial effect is this durable row. A real gateway requires
            # a separate idempotent external dispatch/reconciliation adapter.
            conn.execute("SELECT id FROM orders WHERE id=%s FOR UPDATE", (data["order_id"],))
            conn.execute(
                "UPDATE refund_requests SET status='REFUNDED' WHERE payment_id=%s", (data["payment_id"],)
            )
            conn.execute(
                "UPDATE orders SET status='REFUNDED' WHERE id=%s AND status='REFUND_PENDING'",
                (data["order_id"],),
            )
        elif kind == "TicketsIssued":
            log.info("development_notification", extra={"fields": data})
        else:
            raise ValueError("Unknown event type")


@measured_work("simulate_one")
def simulate_one(db, settings):
    token = uuid4()
    with db.transaction() as conn:
        row = conn.execute("""SELECT p.*,o.total,o.currency FROM payment_attempts p JOIN orders o ON o.id=p.order_id
            WHERE p.deliveries<p.target_deliveries AND p.due_at<=clock_timestamp()
            AND (p.lease_until IS NULL OR p.lease_until<clock_timestamp())
            ORDER BY p.due_at LIMIT 1 FOR UPDATE OF p SKIP LOCKED""").fetchone()
        if not row:
            return False
        conn.execute(
            "UPDATE payment_attempts SET lease_until=clock_timestamp()+interval '15 seconds',lease_token=%s WHERE id=%s",
            (token, row["id"]),
        )
    # Same delivery ID repeated intentionally; tests also exercise different IDs for one payment.
    payload = {
        "callback_id": str(row["id"]),
        "payment_id": str(row["id"]),
        "order_id": str(row["order_id"]),
        "amount": row["total"],
        "currency": row["currency"],
        "outcome": row["outcome"],
    }
    raw = json.dumps(payload).encode()
    timestamp = str(int(time.time()))
    signature = hmac.new(
        settings.webhook_secret.encode(), timestamp.encode() + b"." + raw, hashlib.sha256
    ).hexdigest()
    request = urllib.request.Request(
        os.getenv("API_URL", "http://localhost:8000") + "/v1/webhooks/payments",
        data=raw,
        headers={
            "Content-Type": "application/json",
            "X-Payment-Timestamp": timestamp,
            "X-Payment-Signature": signature,
        },
    )
    with urllib.request.urlopen(request, timeout=5) as response:
        response.read()
    with db.transaction() as conn:
        conn.execute(
            """UPDATE payment_attempts SET deliveries=deliveries+1,lease_until=NULL
            WHERE id=%s AND lease_token=%s""",
            (row["id"], token),
        )
    return True


def simulate_batch(db, settings, executor):
    futures = [executor.submit(simulate_one, db, settings) for _ in range(settings.simulator_concurrency)]
    work = False
    for future in as_completed(futures):
        try:
            work = future.result() or work
        except Exception:
            WORKER_ERRORS.labels("simulator").inc()
            log.exception("callback_dispatch_failed")
    return work


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("role", choices=["publisher", "consumer", "maintenance", "reconciler", "simulator"])
    role = parser.parse_args().role
    configure_logging()
    settings = Settings()
    settings.validate()
    if role == "simulator" and settings.environment != "development":
        raise RuntimeError("Simulator is development only")
    signal.signal(signal.SIGTERM, stop)
    signal.signal(signal.SIGINT, stop)
    start_http_server(settings.worker_port)
    db, cache = Postgres(settings.database_url, settings.pool_max), RedisSeats(settings.redis_url)
    service = Reservations(PostgresReservations(db, cache, settings.hold_seconds))
    producer = consumer = executor = None
    try:
        if role == "simulator":
            executor = ThreadPoolExecutor(max_workers=settings.simulator_concurrency)
        if role == "publisher":
            producer = KafkaProducer(
                bootstrap_servers=settings.kafka_bootstrap,
                acks="all",
                value_serializer=lambda v: json.dumps(v).encode(),
                retries=3,
                max_block_ms=10000,
                request_timeout_ms=10000,
            )
        if role == "consumer":
            consumer = KafkaConsumer(
                "ticketing.events",
                bootstrap_servers=settings.kafka_bootstrap,
                group_id="ticketing-fulfillment-v1",
                enable_auto_commit=False,
                auto_offset_reset="earliest",
                max_poll_records=1,
            )
        next_warm = 0
        while running:
            try:
                work = False
                if role == "publisher":
                    work = publish_batch(db, producer, settings.publisher_batch_size)
                elif role == "simulator":
                    work = simulate_batch(db, settings, executor)
                elif role == "maintenance":
                    for _ in range(10):
                        if not refresh_one(db, cache, settings.refresh_cooldown_ms):
                            break
                        work = True
                    for _ in range(100):
                        if not service.expire_one():
                            break
                        work = True
                elif role == "reconciler":
                    if time.monotonic() >= next_warm:
                        # Seeding, pruning and metric sampling are bounded and infrequent;
                        # a failure here must not stop reconciliation already scheduled.
                        try:
                            maintain_schedule(db, settings)
                        except Exception:
                            RECONCILE_FAILURES.labels("schedule").inc()
                            log.exception("reconciliation_schedule_failed")
                        next_warm = time.monotonic() + 1
                    work = (
                        reconcile_pass(
                            db, cache, settings, time.monotonic() + settings.reconcile_budget_ms / 1000
                        )
                        > 0
                        or work
                    )
                else:
                    for messages in consumer.poll(timeout_ms=500, max_records=1).values():
                        for message in messages:
                            envelope = None
                            for attempt in range(5):
                                try:
                                    envelope = json.loads(message.value)
                                    consume_event(db, cache, envelope)
                                    break
                                except Exception:
                                    if attempt == 4:
                                        # Durable dead letter before committing Kafka offset.
                                        try:
                                            with db.transaction() as conn:
                                                conn.execute(
                                                    """INSERT INTO dead_letters(event_id,payload,error)
                                                    VALUES (%s,%s,%s)""",
                                                    (
                                                        uuid4(),
                                                        Jsonb(
                                                            {"raw": message.value.decode(errors="replace")}
                                                        ),
                                                        "Processing failed after 5 attempts; inspect worker logs",
                                                    ),
                                                )
                                        except Exception:
                                            consumer.seek(
                                                TopicPartition(message.topic, message.partition),
                                                message.offset,
                                            )
                                            raise
                                        log.exception("dead_letter")
                                    else:
                                        time.sleep(0.2 * 2**attempt)
                            consumer.commit()
                            work = True
                if not work:
                    time.sleep(0.1)
            except Exception:
                WORKER_ERRORS.labels(role).inc()
                log.exception("worker_iteration_failed", extra={"fields": {"role": role}})
                time.sleep(1)
    finally:
        if executor:
            executor.shutdown(wait=True)
        if producer:
            producer.close(timeout=5)
        if consumer:
            consumer.close()
        db.close()
        cache.redis.close()


if __name__ == "__main__":
    main()

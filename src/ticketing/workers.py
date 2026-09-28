import argparse
import hashlib
import hmac
import json
import logging
import os
import signal
import socket
import time
import urllib.request
from concurrent.futures import ThreadPoolExecutor, as_completed
from uuid import uuid4

from kafka import KafkaConsumer, KafkaProducer, TopicPartition
from prometheus_client import start_http_server
from psycopg.types.json import Jsonb
from redis.exceptions import RedisError

from ticketing.application.reservations import Reservations
from ticketing.config import Settings
from ticketing.infrastructure.cache import RedisSeats
from ticketing.infrastructure.postgres import Postgres
from ticketing.infrastructure.redis_reservations import RedisReservationIntake
from ticketing.infrastructure.reservations import PostgresReservations, event
from ticketing.observability import (
    CACHE_ROWS,
    EVENT_CONSUMER_BATCH_SIZE,
    OUTBOX_AGE,
    RECONCILE_BACKLOG,
    RECONCILE_EVENTS,
    RECONCILE_FAILURES,
    RECONCILE_OVERDUE,
    RECONCILE_RECOVERED,
    RECONCILE_SECONDS,
    RECONCILE_TRACKED,
    REFRESH_ACK_BATCH_SIZE,
    REFRESH_AGE,
    REFRESH_PENDING,
    RESERVATION_COMMAND_AGE_SECONDS,
    RESERVATION_PERSISTENCE,
    RESERVATION_PERSISTENCE_BATCH_SIZE,
    RESERVATION_PERSISTENCE_FAILURES,
    RESERVATION_PERSISTENCE_PHASE_SECONDS,
    WORKER_ERRORS,
    configure_logging,
    measured_work,
)

log = logging.getLogger("ticketing.worker")
running = True


def stop(*_):
    global running
    running = False


@measured_work("reservation_write")
def persist_reservation_batch(store, intake, consumer, limit=1):
    messages = list(intake.messages(consumer, count=limit))
    RESERVATION_PERSISTENCE_BATCH_SIZE.observe(len(messages))
    if not messages:
        return False

    commands = []
    for stream, message_id, fields in messages:
        payload = json.loads(fields["payload"])
        response = json.loads(fields["response"])
        created_at = float(fields.get("created_at_epoch", time.time()))
        command_age = max(0.0, time.time() - created_at)
        RESERVATION_COMMAND_AGE_SECONDS.observe(command_age)
        commands.append(
            {
                "stream": stream,
                "message_id": message_id,
                "payload": payload,
                "response": response,
                "command_age": command_age,
            }
        )

    started = time.perf_counter()
    try:
        outcomes = store.persist_reservation_commands(
            [(command["payload"], command["response"]) for command in commands]
        )
    except Exception:
        RESERVATION_PERSISTENCE_PHASE_SECONDS.labels("postgres_batch", "error").observe(
            time.perf_counter() - started
        )
        RESERVATION_PERSISTENCE.labels("transient_error").inc(len(commands))
        raise
    RESERVATION_PERSISTENCE_PHASE_SECONDS.labels("postgres_batch", "ok").observe(
        time.perf_counter() - started
    )
    if len(outcomes) != len(commands):
        RESERVATION_PERSISTENCE.labels("outcome_mismatch").inc(len(commands))
        raise RuntimeError("reservation persistence batch returned the wrong outcome count")

    failed_events = set()
    for command, (outcome, value) in zip(commands, outcomes, strict=True):
        payload = command["payload"]
        if outcome == "failed":
            error_code = str(value.code or "UNKNOWN")
            RESERVATION_PERSISTENCE.labels("failed").inc()
            RESERVATION_PERSISTENCE_FAILURES.labels(error_code).inc()
            compensate_started = time.perf_counter()
            try:
                intake.mark_failed(
                    payload["event_id"],
                    payload["command_id"],
                    payload["seat_ids"],
                    error_code,
                )
            except Exception:
                RESERVATION_PERSISTENCE_PHASE_SECONDS.labels(
                    "redis_compensate", "error"
                ).observe(time.perf_counter() - compensate_started)
                RESERVATION_PERSISTENCE.labels("compensation_error").inc()
                raise
            RESERVATION_PERSISTENCE_PHASE_SECONDS.labels(
                "redis_compensate", "ok"
            ).observe(time.perf_counter() - compensate_started)
            failed_events.add(payload["event_id"])
            log.warning(
                "reservation_command_failed",
                extra={"fields": {
                    "event": "reservation_command_failed",
                    "command_id": payload["command_id"],
                    "error_code": error_code,
                    "command_age_seconds": round(command["command_age"], 6),
                }},
            )
            continue

        if outcome != "durable":
            RESERVATION_PERSISTENCE.labels("outcome_invalid").inc()
            raise RuntimeError(f"unknown reservation persistence outcome: {outcome}")
        mark_started = time.perf_counter()
        try:
            marked = intake.mark_durable(payload["event_id"], payload["command_id"])
        except Exception:
            RESERVATION_PERSISTENCE_PHASE_SECONDS.labels(
                "redis_mark_durable", "error"
            ).observe(time.perf_counter() - mark_started)
            RESERVATION_PERSISTENCE.labels("mark_durable_error").inc()
            raise
        RESERVATION_PERSISTENCE_PHASE_SECONDS.labels(
            "redis_mark_durable", "ok"
        ).observe(time.perf_counter() - mark_started)
        if marked != 1:
            RESERVATION_PERSISTENCE.labels("metadata_expired").inc()
            log.error(
                "reservation_command_metadata_expired",
                extra={"fields": {
                    "event": "reservation_command_metadata_expired",
                    "command_id": payload["command_id"],
                    "command_age_seconds": round(command["command_age"], 6),
                    "mark_result": marked,
                }},
            )
            raise RuntimeError(
                "reservation command metadata expired before durable acknowledgement"
            )
        RESERVATION_PERSISTENCE.labels("durable").inc()

    for event_id in failed_events:
        snapshot(store.db, intake.cache, event_id)

    for command in commands:
        acknowledge_started = time.perf_counter()
        try:
            intake.acknowledge(command["stream"], command["message_id"])
        except Exception:
            RESERVATION_PERSISTENCE_PHASE_SECONDS.labels(
                "redis_acknowledge", "error"
            ).observe(time.perf_counter() - acknowledge_started)
            RESERVATION_PERSISTENCE.labels("acknowledgement_error").inc()
            raise
        RESERVATION_PERSISTENCE_PHASE_SECONDS.labels(
            "redis_acknowledge", "ok"
        ).observe(time.perf_counter() - acknowledge_started)
    return True

@measured_work("snapshot")
def snapshot(db, cache, event_id):
    # Event metadata and seats come from one transaction, then one Lua PUT publishes the
    # complete intake/read snapshot atomically.
    with db.transaction() as conn:
        sale = conn.execute(
            "SELECT sale_starts,sale_ends,currency FROM events WHERE id=%s",
            (event_id,),
        ).fetchone()
        rows = conn.execute(
            """SELECT seat_id,price,hold_id,reserved_until,booked_order_id,version
            FROM event_seats WHERE event_id=%s ORDER BY seat_id""",
            (event_id,),
        ).fetchall()
    CACHE_ROWS.labels("full").inc(len(rows))
    version = sum(r["version"] for r in rows)
    seats = [
        {
            "seat_id": r["seat_id"],
            "price": r["price"],
            "status": "SOLD" if r["booked_order_id"] else "HELD" if r["hold_id"] else "AVAILABLE",
            "hold_id": str(r["hold_id"]) if r["hold_id"] else None,
            "reserved_until": r["reserved_until"],
            "reserved_until_epoch": (
                r["reserved_until"].timestamp() if r["reserved_until"] is not None else None
            ),
            "version": version,
            "source_version": r["version"],
        }
        for r in rows
    ]
    cache.put(
        str(event_id),
        version,
        {
            "event_id": str(event_id),
            "version": version,
            "sale_starts_epoch": sale["sale_starts"].timestamp(),
            "sale_ends_epoch": sale["sale_ends"].timestamp(),
            "currency": sale["currency"],
            "seats": seats,
        },
    )

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
        "reserved_until_epoch": (
            row["reserved_until"].timestamp() if row["reserved_until"] is not None else None
        ),
        "source_version": row["version"],
    }

@measured_work("refresh_batch")
def refresh_batch(db, cache, limit=2, cooldown_ms=250):
    token = uuid4()
    with db.transaction() as conn:
        state = conn.execute(
            """SELECT count(*) AS n,
            EXTRACT(EPOCH FROM clock_timestamp()-min(requested_at)) AS age
            FROM seat_refresh_requests WHERE generation>completed_generation"""
        ).fetchone()
        REFRESH_PENDING.set(state["n"])
        REFRESH_AGE.set(float(state["age"] or 0))
        rows = conn.execute(
            """SELECT * FROM seat_refresh_requests
            WHERE generation>completed_generation AND next_attempt_at<=clock_timestamp()
            AND (lease_until IS NULL OR lease_until<clock_timestamp())
            ORDER BY requested_at LIMIT %s FOR UPDATE SKIP LOCKED""",
            (limit,),
        ).fetchall()
        if not rows:
            return 0
        claimed_at = conn.execute("SELECT clock_timestamp() AS claimed_at").fetchone()["claimed_at"]
        conn.execute(
            """UPDATE seat_refresh_requests
            SET lease_token=%s,lease_until=clock_timestamp()+interval '30 seconds'
            WHERE event_id=ANY(%s)""",
            (token, [row["event_id"] for row in rows]),
        )
    # No SQL locks while writing Redis. Every snapshot is fenced by inventory version.
    completed, errors = [], []
    for row in rows:
        try:
            if row["seat_ids"] is None:
                snapshot(db, cache, row["event_id"])
            else:
                changed_snapshot(db, cache, row["event_id"], row["seat_ids"])
            completed.append(row)
        except Exception as exc:  # noqa: BLE001 - preserve successes, then re-raise
            errors.append(exc)
    if completed:
        REFRESH_ACK_BATCH_SIZE.observe(len(completed))
        acknowledgements = [
            {
                "event_id": str(row["event_id"]),
                "generation": row["generation"],
                "claimed_at": claimed_at.isoformat(),
            }
            for row in completed
        ]
        with db.transaction() as conn:
            conn.execute(
                """WITH acknowledged AS (
                    SELECT * FROM jsonb_to_recordset(%s::jsonb)
                    AS item(event_id uuid,generation bigint,claimed_at timestamptz)
                )
                UPDATE seat_refresh_requests AS request
                SET completed_generation=acknowledged.generation,
                    seat_ids=CASE
                        WHEN request.generation=acknowledged.generation THEN NULL
                        ELSE request.seat_ids END,
                    lease_until=NULL,lease_token=NULL,
                    next_attempt_at=clock_timestamp()+(%s * interval '1 millisecond'),
                    requested_at=CASE
                        WHEN request.generation>acknowledged.generation
                        THEN acknowledged.claimed_at ELSE request.requested_at END
                FROM acknowledged
                WHERE request.event_id=acknowledged.event_id
                AND request.lease_token=%s""",
                (Jsonb(acknowledgements), cooldown_ms, token),
            )
    if errors:
        raise errors[0]
    return len(completed)


@measured_work("refresh_one")
def refresh_one(db, cache, cooldown_ms=250):
    return bool(refresh_batch(db, cache, limit=1, cooldown_ms=cooldown_ms))


@measured_work("expire_batch")
def expire_batch(service, limit=8):
    return service.expire_batch(limit)


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


@measured_work("consume_refresh_batch")
def consume_refresh_batch(db, envelopes):
    if not envelopes:
        return 0
    for envelope in envelopes:
        if envelope["schema_version"] != 1:
            raise ValueError("Unsupported event schema")
        if envelope["event_type"] != "SeatsChanged":
            raise ValueError("Only SeatsChanged events can be coalesced")

    with db.transaction() as conn:
        inserted = conn.execute(
            """INSERT INTO consumer_inbox(consumer,event_id)
            SELECT 'fulfillment',event_id FROM unnest(%s::uuid[]) AS event_id
            ON CONFLICT DO NOTHING RETURNING event_id""",
            ([envelope["event_id"] for envelope in envelopes],),
        ).fetchall()
        inserted_ids = {str(row["event_id"]) for row in inserted}
        refreshes = {}
        handled_ids = set()
        for envelope in envelopes:
            envelope_id = envelope["event_id"]
            if envelope_id not in inserted_ids or envelope_id in handled_ids:
                continue
            handled_ids.add(envelope_id)
            data = envelope["payload"]
            target = data["event_id"]
            seats = data.get("seats")
            if target not in refreshes:
                refreshes[target] = None if seats is None else set(seats)
            elif refreshes[target] is not None:
                if seats is None:
                    refreshes[target] = None
                else:
                    refreshes[target].update(seats)
        for event_id, seats in refreshes.items():
            request_refresh(conn, event_id, None if seats is None else sorted(seats))
    return len(inserted_ids)


def consume_events(db, cache, envelopes):
    pending_refresh = []

    def flush_refresh():
        if pending_refresh:
            consume_refresh_batch(db, pending_refresh)
            pending_refresh.clear()

    for envelope in envelopes:
        if envelope.get("event_type") == "SeatsChanged":
            pending_refresh.append(envelope)
            continue
        flush_refresh()
        consume_event(db, cache, envelope)
    flush_refresh()


def dead_letter_message(db, message):
    with db.transaction() as conn:
        conn.execute(
            """INSERT INTO dead_letters(event_id,payload,error)
            VALUES (%s,%s,%s)""",
            (
                uuid4(),
                Jsonb({"raw": message.value.decode(errors="replace")}),
                "Processing failed after 5 attempts; inspect worker logs",
            ),
        )


def consume_kafka_messages(db, cache, messages):
    EVENT_CONSUMER_BATCH_SIZE.observe(len(messages))
    for attempt in range(5):
        try:
            consume_events(db, cache, [json.loads(message.value) for message in messages])
            return
        except Exception:
            if attempt < 4:
                time.sleep(0.2 * 2**attempt)
                continue
            log.exception("consumer_batch_failed")

    # Isolate a poison message after the bounded batch retries. Already committed
    # inbox rows make replay of valid records inexpensive and safe.
    for message in messages:
        try:
            consume_event(db, cache, json.loads(message.value))
        except Exception:
            dead_letter_message(db, message)
            log.exception("dead_letter")

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
    parser.add_argument(
        "role",
        choices=["publisher", "consumer", "reservation-writer", "maintenance", "refresh", "expiry", "reconciler", "simulator"],
    )
    role = parser.parse_args().role
    configure_logging()
    settings = Settings()
    settings.validate()
    if role == "simulator" and settings.environment != "development":
        raise RuntimeError("Simulator is development only")
    signal.signal(signal.SIGTERM, stop)
    signal.signal(signal.SIGINT, stop)
    start_http_server(settings.worker_port)
    db, cache = Postgres(settings.database_url, settings.pool_max), RedisSeats(settings.redis_url, seatmap_ttl_seconds=settings.seatmap_ttl_seconds)
    store = PostgresReservations(db, cache, settings.hold_seconds)
    service = Reservations(store)
    intake = RedisReservationIntake(
        cache,
        hold_seconds=settings.hold_seconds,
        replica_acks=settings.redis_reservation_replica_acks,
        wait_ms=settings.redis_reservation_wait_ms,
        max_backlog=settings.redis_reservation_max_backlog,
        max_command_age_seconds=settings.redis_reservation_max_command_age_seconds,
    )
    reservation_consumer = f"{socket.gethostname()}-{os.getpid()}"
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
                max_poll_records=settings.consumer_batch_size,
                fetch_max_wait_ms=settings.consumer_batch_wait_ms,
            )
        next_warm = 0
        while running:
            try:
                work = False
                if role == "publisher":
                    work = publish_batch(db, producer, settings.publisher_batch_size)
                elif role == "reservation-writer":
                    work = persist_reservation_batch(
                        store, intake, reservation_consumer, settings.reservation_writer_batch_size
                    )
                elif role == "simulator":
                    work = simulate_batch(db, settings, executor)
                elif role == "maintenance":
                    work = (
                        refresh_batch(db, cache, settings.refresh_batch_size, settings.refresh_cooldown_ms) > 0
                    ) or work
                    work = expire_batch(service, settings.expiry_batch_size) > 0 or work
                elif role == "refresh":
                    work = (
                        refresh_batch(db, cache, settings.refresh_batch_size, settings.refresh_cooldown_ms) > 0
                    ) or work
                elif role == "expiry":
                    work = expire_batch(service, settings.expiry_batch_size) > 0 or work
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
                    batches = consumer.poll(
                        timeout_ms=500, max_records=settings.consumer_batch_size
                    )
                    starts = {
                        TopicPartition(topic_partition.topic, topic_partition.partition): messages[0].offset
                        for topic_partition, messages in batches.items()
                        if messages
                    }
                    try:
                        for messages in batches.values():
                            if messages:
                                consume_kafka_messages(db, cache, messages)
                    except Exception:
                        for topic_partition, offset in starts.items():
                            consumer.seek(topic_partition, offset)
                        raise
                    if starts:
                        consumer.commit()
                        work = True
                if not work:
                    time.sleep(0.1)
            except Exception as exc:
                WORKER_ERRORS.labels(role).inc()
                if role == "reservation-writer" and isinstance(exc, RedisError):
                    intake.reset_connection_state()
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

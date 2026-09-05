import argparse
import hashlib
import hmac
import json
import logging
import os
import signal
import time
import urllib.request
from uuid import uuid4

from kafka import KafkaConsumer, KafkaProducer, TopicPartition
from prometheus_client import start_http_server
from psycopg.types.json import Jsonb

from ticketing.application.reservations import Reservations
from ticketing.config import Settings
from ticketing.infrastructure.cache import RedisSeats
from ticketing.infrastructure.postgres import Postgres
from ticketing.infrastructure.reservations import PostgresReservations, event
from ticketing.observability import OUTBOX_AGE, WORKER_ERRORS, configure_logging

log = logging.getLogger("ticketing.worker")
running = True


def stop(*_):
    global running
    running = False


def snapshot(db, cache, event_id):
    # One statement gives a consistent snapshot. A global sequence is not used: allocation
    # order is not commit order. Sum of monotonically increasing row versions is monotonic.
    with db.transaction() as conn:
        rows = conn.execute(
            """SELECT seat_id,price,hold_id,reserved_until,booked_order_id,version
            FROM event_seats WHERE event_id=%s ORDER BY seat_id""",
            (event_id,),
        ).fetchall()
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


def publish_one(db, producer):
    token = uuid4()
    with db.transaction() as conn:
        age = conn.execute("""SELECT EXTRACT(EPOCH FROM clock_timestamp()-min(occurred_at)) AS age
            FROM outbox_events WHERE published_at IS NULL""").fetchone()["age"]
        OUTBOX_AGE.set(float(age or 0))
        row = conn.execute("""SELECT * FROM outbox_events WHERE published_at IS NULL
            AND (lease_until IS NULL OR lease_until < clock_timestamp())
            ORDER BY occurred_at LIMIT 1 FOR UPDATE SKIP LOCKED""").fetchone()
        if not row:
            return False
        conn.execute(
            "UPDATE outbox_events SET lease_until=clock_timestamp()+interval '30 seconds',lease_token=%s WHERE id=%s",
            (token, row["id"]),
        )
    envelope = {
        "event_id": str(row["id"]),
        "aggregate_id": str(row["aggregate_id"]),
        "event_type": row["event_type"],
        "schema_version": row["schema_version"],
        "occurred_at": row["occurred_at"].isoformat(),
        "payload": row["payload"],
    }
    producer.send("ticketing.events", key=str(row["aggregate_id"]).encode(), value=envelope).get(timeout=10)
    with db.transaction() as conn:
        conn.execute(
            """UPDATE outbox_events SET published_at=clock_timestamp(),lease_until=NULL
            WHERE id=%s AND lease_token=%s""",
            (row["id"], token),
        )
    return True


def consume_event(db, cache, envelope):
    if envelope["schema_version"] != 1:
        raise ValueError("Unsupported event schema")
    kind, data = envelope["event_type"], envelope["payload"]
    if kind == "SeatsChanged":
        # Redis cannot share a DB transaction. Rebuild first; repeating it is safe.
        snapshot(db, cache, data["event_id"])
    with db.transaction() as conn:
        inserted = conn.execute(
            """INSERT INTO consumer_inbox VALUES ('fulfillment',%s)
            ON CONFLICT DO NOTHING RETURNING event_id""",
            (envelope["event_id"],),
        ).fetchone()
        if not inserted:
            return
        if kind == "OrderPaid":
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
        elif kind != "SeatsChanged":
            raise ValueError("Unknown event type")


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


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("role", choices=["publisher", "consumer", "maintenance", "simulator"])
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
    producer = consumer = None
    try:
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
                    work = publish_one(db, producer)
                elif role == "simulator":
                    work = simulate_one(db, settings)
                elif role == "maintenance":
                    for _ in range(100):
                        if not service.expire_one():
                            break
                        work = True
                    if time.monotonic() >= next_warm:
                        with db.transaction() as conn:
                            events = conn.execute("SELECT id FROM events").fetchall()
                        for item in events:
                            snapshot(db, cache, item["id"])
                        next_warm = time.monotonic() + 5
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
        if producer:
            producer.close(timeout=5)
        if consumer:
            consumer.close()
        db.close()
        cache.redis.close()


if __name__ == "__main__":
    main()

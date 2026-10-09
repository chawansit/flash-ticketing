"""Integrated reviewed roles: provisional hold to unique tickets and drained owned work.

Gateway and broker transports are simulated here; PostgreSQL and Redis are real.
Full-stack CI separately exercises actual HTTP and Kafka.
"""
import hashlib
import hmac
import json
import os
from dataclasses import replace
from unittest.mock import Mock

import pytest

from ticketing import workers
from ticketing.application.reservations import Reservations
from ticketing.config import Settings
from ticketing.infrastructure.cache import RedisSeats
from ticketing.infrastructure.payment_confirmation import PostgresPaymentConfirmation
from ticketing.infrastructure.redis_reservations import RedisReservationIntake
from ticketing.infrastructure.reservations import PostgresReservations


@pytest.mark.parametrize("pipeline", [False, True], ids=["sequential", "pipelined"])
@pytest.mark.parametrize("queued", [False, True], ids=["sync", "async"])
def test_combined_roles_replay_expiry_and_owned_queues_drain(system, pipeline, queued):
    _svc, db, show = system
    url = os.getenv("TEST_REDIS_URL")
    if not url:
        pytest.skip("TEST_REDIS_URL not configured")
    cache = RedisSeats(url)
    intake = RedisReservationIntake(cache, hold_seconds=120)
    store = PostgresReservations(db, cache, 120, persist_write_pipeline=pipeline)
    svc = Reservations(store)
    settings = replace(Settings(), payment_confirmation_async=queued)
    receipts = PostgresPaymentConfirmation(db, settings)
    workers.snapshot(db, cache, show)
    try:
        accepted = intake.enqueue("buyer", show, ["A", "B"], "hold")
        assert intake.enqueue("buyer", show, ["B", "A"], "hold") == accepted
        assert workers.persist_reservation_batch(store, intake, "integrated-audit", 4)
        durable = intake.status(show, accepted["command_id"], actor="buyer")
        assert durable["persistence_status"] == "DURABLE"
        assert not workers.persist_reservation_batch(store, intake, "integrated-audit", 4)
        order = accepted["order_id"]
        first = svc.initiate_payment("buyer", order, "payment", "SUCCEEDED", 0, 3)
        assert svc.initiate_payment("buyer", order, "payment", "SUCCEEDED", 0, 3) == first

        class Gateway:
            def post(self, raw, headers):
                headers = {key.lower(): value for key, value in headers.items()}
                signed = headers["x-payment-timestamp"].encode() + b"." + raw
                expected = hmac.new(settings.webhook_secret.encode(), signed, hashlib.sha256).hexdigest()
                assert hmac.compare_digest(expected, headers["x-payment-signature"])
                payload = json.loads(raw)
                return receipts.receive(payload) if queued else svc.callback(payload)

        for _ in range(3):
            assert workers.simulate_one(db, settings, Gateway())
        assert not workers.simulate_one(db, settings, Gateway())
        if queued:
            with db.transaction() as conn:
                assert conn.execute("SELECT count(*) AS n FROM bookings").fetchone()["n"] == 0
            assert receipts.process_one(store)
            assert not receipts.process_one(store)

        delivered = []
        class Broker:
            def send(self, _topic, *, key, value):
                delivered.append(value)
                return Mock(get=Mock(return_value=None))

        for _ in range(6):
            if not workers.publish_batch(db, Broker(), 32):
                break
            batch, delivered = delivered[:], []
            workers.consume_events(db, cache, batch)
            workers.consume_events(db, cache, batch)  # Lost offset acknowledgement/redelivery.
        else:
            pytest.fail("Owned outbox did not converge")
        for _ in range(4):
            if not workers.refresh_batch(db, cache, 8, cooldown_ms=0):
                break
        assert svc.get_order("buyer", order)["status"] == "FULFILLED"
        # Advance durable hold expiry after payment; consumed ownership cannot be recycled.
        with db.transaction() as conn:
            conn.execute("UPDATE holds SET expires_at=clock_timestamp()-interval '1 second'")
        assert svc.expire_batch(8) == 0
        with db.transaction() as conn:
            assert conn.execute("SELECT count(*) AS n FROM bookings").fetchone()["n"] == 2
            assert conn.execute("SELECT count(*) AS n FROM tickets").fetchone()["n"] == 2
            assert conn.execute("SELECT count(*) AS n FROM event_seats WHERE booked_order_id=%s", (order,)).fetchone()["n"] == 2
            assert conn.execute("SELECT count(*) AS n FROM (SELECT event_id,seat_id FROM bookings "
                                "GROUP BY event_id,seat_id HAVING count(*)>1) duplicate").fetchone()["n"] == 0
            for query in (
                "SELECT count(*) AS n FROM outbox_events WHERE published_at IS NULL",
                "SELECT count(*) AS n FROM seat_refresh_requests WHERE generation>completed_generation",
                "SELECT count(*) AS n FROM seat_refresh_requests WHERE lease_token IS NOT NULL",
                "SELECT count(*) AS n FROM payment_attempts WHERE deliveries<target_deliveries",
                "SELECT count(*) AS n FROM payment_webhook_receipts WHERE status!='COMPLETED'",
                "SELECT count(*) AS n FROM payment_receipt_capacity WHERE outstanding!=0",
                "SELECT count(*) AS n FROM dead_letters",
            ):
                assert conn.execute(query).fetchone()["n"] == 0, query
        assert cache.redis.xlen(intake.stream_key(show)) == 0
        assert cache.redis.xpending(intake.stream_key(show), intake.group)["pending"] == 0
    finally:
        keys = list(cache.redis.scan_iter(match=f"*{{{show}}}*"))
        if keys:
            cache.redis.delete(*keys)
        cache.redis.srem("reservation-stream-registry", intake.stream_key(show))
        cache.redis.close()

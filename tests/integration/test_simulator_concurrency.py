"""Real SQL: twelve HTTP waits cannot retain a two-connection database pool."""
import json
import threading
from concurrent.futures import ThreadPoolExecutor
from dataclasses import replace
from uuid import uuid4

import pytest

from ticketing.application.reservations import Reservations
from ticketing.config import Settings
from ticketing.infrastructure.postgres import Postgres
from ticketing.infrastructure.reservations import PostgresReservations
from ticketing.workers import consume_event, simulate_one

pytestmark = pytest.mark.integration


@pytest.mark.parametrize("uncertainty", ["none", "lost_response", "stale_token"])
def test_twelve_deliveries_release_sql_and_preserve_replay(system, uncertainty):
    svc, original, event_id = system
    with original.transaction() as conn:
        for i in range(12):
            conn.execute("INSERT INTO event_seats(event_id,seat_id,price) VALUES (%s,%s,100)",
                         (event_id, f"D{i}"))
    attempts = []
    for i in range(12):
        order = svc.reserve(f"actor-{i}", event_id, [f"D{i}"], str(uuid4()))
        attempts.append(svc.initiate_payment(f"actor-{i}", order["order_id"], str(uuid4()),
                                            "SUCCEEDED", 0, 1)["payment_id"])
    db = Postgres(original.pool.conninfo, maximum=2, wait_ms=3000, maximum_waiting=12)
    db.pool.wait(timeout=5)
    all_entered, release = threading.Event(), threading.Event()
    guard = threading.Lock()
    calls = active = peak = 0
    uncertain_payment = attempts[0]
    stale_token = uuid4()
    store = Reservations(PostgresReservations(db, svc.store.cache, 120))

    class Gateway:
        def post(self, raw, _headers):
            nonlocal calls, active, peak
            payload = json.loads(raw)
            with guard:
                calls += 1
                active += 1
                peak = max(peak, active)
                if active == 12:
                    all_entered.set()
            try:
                if not release.wait(5):
                    raise RuntimeError("Test gateway was not released")
                store.callback(payload)
                if payload["payment_id"] == uncertain_payment:
                    if uncertainty == "lost_response":
                        raise ConnectionError("Committed callback response was lost")
                    if uncertainty == "stale_token":
                        with db.transaction() as conn:
                            conn.execute("UPDATE payment_attempts SET lease_token=%s WHERE id=%s",
                                         (stale_token, uncertain_payment))
            finally:
                with guard:
                    active -= 1

    settings = replace(Settings(), simulator_concurrency=12, pool_max=2)
    settings.validate()
    try:
        with ThreadPoolExecutor(max_workers=12) as executor:
            futures = [executor.submit(simulate_one, db, settings, Gateway()) for _ in range(12)]
            try:
                assert all_entered.wait(5)
                # All twelve HTTP requests are parked. SQL remains available to another borrower.
                conn = db.pool.getconn(timeout=.5)
                try:
                    assert conn.execute("SELECT 1 AS value").fetchone()["value"] == 1
                    conn.commit()
                    assert db.pool.get_stats()["pool_size"] <= 2
                finally:
                    db.pool.putconn(conn)
            finally:
                release.set()
            for future in futures:
                if uncertainty == "lost_response" and isinstance(future.exception(timeout=5), ConnectionError):
                    continue
                assert future.result(timeout=5) is True
        assert peak == calls == 12 and active == 0
        with db.transaction() as conn:
            records = conn.execute("SELECT id,deliveries,lease_token FROM payment_attempts").fetchall()
            assert len(records) == 12
            assert sum(r["deliveries"] for r in records) == (12 if uncertainty == "none" else 11)
            if uncertainty == "stale_token":
                assert next(r for r in records if str(r["id"]) == uncertain_payment)["lease_token"] == stale_token
            if uncertainty != "none":
                conn.execute("UPDATE payment_attempts SET lease_until=clock_timestamp()-interval '1 second' WHERE id=%s",
                             (uncertain_payment,))

        if uncertainty != "none":
            class Replay:
                def post(self, raw, _headers):
                    payload = json.loads(raw)
                    assert payload["callback_id"] == payload["payment_id"] == uncertain_payment
                    store.callback(payload)
            assert simulate_one(db, settings, Replay()) is True
        assert simulate_one(db, settings, Gateway()) is False
        with db.transaction() as conn:
            assert conn.execute("SELECT count(*) AS n FROM payment_attempts WHERE status='SUCCEEDED' AND deliveries=1").fetchone()["n"] == 12
            assert conn.execute("SELECT count(*) AS n FROM payment_callbacks").fetchone()["n"] == 12
            paid_events = conn.execute("SELECT id,payload FROM outbox_events WHERE event_type='OrderPaid'").fetchall()
        assert len(paid_events) == 12
        for row in paid_events:
            envelope = {"event_id": str(row["id"]), "schema_version": 1,
                        "event_type": "OrderPaid", "payload": row["payload"]}
            consume_event(db, None, envelope)
            consume_event(db, None, envelope)
        with db.transaction() as conn:
            assert conn.execute("SELECT count(*) AS n FROM tickets").fetchone()["n"] == 12
            assert conn.execute("SELECT count(*) AS n FROM orders WHERE status='FULFILLED'").fetchone()["n"] == 12
            assert conn.execute("SELECT count(*) AS n FROM (SELECT event_id,seat_id FROM bookings GROUP BY event_id,seat_id HAVING count(*)>1) b").fetchone()["n"] == 0
    finally:
        release.set()
        db.close()

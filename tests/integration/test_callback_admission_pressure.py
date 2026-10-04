"""Real native FIFO pressure and financial replay through protected callback admission."""
import time
from concurrent.futures import ThreadPoolExecutor
from contextlib import ExitStack
from unittest.mock import Mock
from uuid import uuid4

import pytest
from psycopg_pool import TooManyRequests

from ticketing.application.reservations import Reservations
from ticketing.infrastructure.postgres import create_api_databases
from ticketing.infrastructure.reservations import PostgresReservations
from ticketing.workers import consume_event, publish_batch

pytestmark = pytest.mark.integration


def wait_queue(pool, count, deadline):
    while pool.get_stats()["requests_waiting"] != count:
        assert time.monotonic() < deadline, "Native queue did not reach controlled boundary"
        time.sleep(.001)


@pytest.mark.parametrize("readers", [0, 2])
def test_callback_progress_duplicate_safety_and_drain_under_submission_pressure(system, readers):
    svc, db, show = system
    holds = [svc.reserve("owner", show, [seat], "hold-"+seat) for seat in ("A", "B")]
    bodies = []
    for index, hold in enumerate(holds):
        attempt = svc.initiate_payment("owner", hold["order_id"], "pay-"+str(index), "SUCCEEDED", 0, 1)
        bodies.append({"callback_id": str(uuid4()), "payment_id": attempt["payment_id"],
                       "order_id": hold["order_id"], "amount": hold["total"], "currency": "THB", "outcome": "SUCCEEDED"})
    general, payment = create_api_databases(db.pool.conninfo, 4, 1000, 12, 2, True, 2)
    callback = payment.callback_database
    financial = Reservations(PostgresReservations(callback, svc.store.cache, 120))

    def query(adapter):
        with adapter.transaction() as conn:
            return conn.execute("SELECT 1 AS n").fetchone()["n"]

    try:
        for adapter in (general, payment):
            adapter.pool.resize(2, 2)
            adapter.pool.wait(timeout=10)
        with ExitStack() as held, ThreadPoolExecutor(max_workers=12) as executor:
            for adapter in (general, payment):
                held.enter_context(adapter.transaction())
                held.enter_context(adapter.transaction())
            general_work = [executor.submit(query, general) for _ in range(readers)]
            submission_work = [executor.submit(query, payment) for _ in range(8)]
            deadline = time.monotonic() + .7
            wait_queue(general.pool, readers, deadline)
            wait_queue(payment.pool, 8, deadline)
            for adapter in ((general, payment) if readers else (payment,)):
                with pytest.raises(TooManyRequests):
                    adapter.pool.getconn()
            callbacks = [executor.submit(financial.callback, body) for body in bodies]
            wait_queue(payment.pool, 10, deadline)
            state = general._shared_acquisition_budget.snapshot()
            assert state["counts"] == {"general": readers, "payment": 8, "callback": 2}
            assert state["used"] == readers + 10
            with pytest.raises(TooManyRequests):
                callback.pool.getconn()
            held.close()
            assert [f.result(timeout=3) for f in general_work + submission_work] == [1] * (readers + 8)
            assert [f.result(timeout=3)["status"] for f in callbacks] == ["book", "book"]
        assert general._shared_acquisition_budget.snapshot()["used"] == 0
        # Existing NOWAIT financial locking can reject simultaneous duplicates.
        # Replay after commit checks idempotency without concealing a load error.
        for hold, body in zip(holds, bodies):
            assert financial.callback(body)["status"] == "duplicate"
            assert financial.callback({**body, "callback_id": str(uuid4())})["status"] == "duplicate"
            event = {"event_id": str(uuid4()), "schema_version": 1, "event_type": "OrderPaid",
                     "payload": {"order_id": hold["order_id"]}}
            consume_event(db, None, event)
            consume_event(db, None, event)
            assert svc.get_order("owner", hold["order_id"])["status"] == "FULFILLED"
        producer = Mock()  # Acknowledged local broker stub; no Kafka capacity claim.
        for _ in range(10):
            if not publish_batch(db, producer):
                break
        else:
            pytest.fail("Outbox did not drain within bounded local passes")
        with db.transaction() as conn:
            for table in ("payment_attempts", "bookings", "tickets"):
                assert conn.execute(f"SELECT count(*) AS n FROM {table}").fetchone()["n"] == 2
            # Each distinct callback ID is retained, while tickets remain unique.
            assert conn.execute("SELECT count(*) AS n FROM payment_callbacks").fetchone()["n"] == 4
            assert conn.execute("SELECT count(*) AS n FROM outbox_events WHERE published_at IS NULL").fetchone()["n"] == 0
            assert conn.execute("SELECT count(*) AS n FROM (SELECT event_id,seat_id FROM bookings "
                                "GROUP BY event_id,seat_id HAVING count(*)>1) duplicates").fetchone()["n"] == 0
        callback.close()
        assert not payment.pool.closed
    finally:
        general.close()
        payment.close()
    assert general.pool.closed and payment.pool.closed

"""Controlled pool exhaustion is separate from cloud capacity qualification."""
from concurrent.futures import ThreadPoolExecutor
from contextlib import contextmanager
from threading import Barrier, Event
from uuid import uuid4

import pytest
from psycopg_pool import PoolTimeout

from ticketing.application.reservations import Reservations
from ticketing.infrastructure.postgres import Postgres
from ticketing.infrastructure.reservations import PostgresReservations
from conftest import NoShield

pytestmark = pytest.mark.integration


def make_payment(svc, show):
    hold = svc.reserve("owner", show, ["A"], str(uuid4()))
    payment = svc.initiate_payment("owner", hold["order_id"], str(uuid4()), "SUCCEEDED", 0, 1)
    payload = {"callback_id": str(uuid4()), "payment_id": payment["payment_id"],
               "order_id": hold["order_id"], "amount": 100, "currency": "THB", "outcome": "SUCCEEDED"}
    return hold, payload


class PausedReads:
    def __init__(self, db, count):
        self.db = db
        self.arrived = Barrier(count + 1)
        self.release = Event()

    @contextmanager
    def transaction(self):
        with self.db.transaction() as conn:
            owner = self

            class Connection:
                def execute(self, query, params=None):
                    cursor = conn.execute(query, params)
                    if "FROM orders" in query:
                        owner.arrived.wait(timeout=2)
                        assert owner.release.wait(timeout=2), "Read boundary was not released"
                    return cursor

            yield Connection()


def test_shared_pool_reads_can_exhaust_payment_capacity(system):
    svc, db, show = system
    hold, payload = make_payment(svc, show)
    small = Postgres(db.pool.conninfo, maximum=4, wait_ms=50, maximum_waiting=4)
    small.pool.resize(4, 4)
    small.pool.wait(timeout=10)
    reads = PausedReads(small, 4)
    reader = Reservations(PostgresReservations(reads, NoShield(), 120))
    financial = Reservations(PostgresReservations(small, NoShield(), 120))
    try:
        with ThreadPoolExecutor(max_workers=4) as executor:
            futures = [executor.submit(reader.get_order, "owner", hold["order_id"]) for _ in range(4)]
            try:
                reads.arrived.wait(timeout=2)
                assert small.pool.get_stats()["pool_available"] == 0
                with pytest.raises(PoolTimeout):
                    financial.initiate_payment("owner", hold["order_id"], str(uuid4()), "SUCCEEDED", 0, 1)
                with pytest.raises(PoolTimeout):
                    financial.callback(payload)
            finally:
                reads.release.set()
            assert all(f.result(timeout=2)["status"] == "PENDING" for f in futures)
        assert financial.callback(payload)["status"] == "book"
        assert financial.callback(payload)["status"] == "duplicate"
        with db.transaction() as conn:
            assert conn.execute("SELECT count(*) AS n FROM bookings").fetchone()["n"] == 1
            assert conn.execute("SELECT count(*) AS n FROM payment_callbacks").fetchone()["n"] == 1
        assert small.pool.get_stats()["pool_available"] == 4
    finally:
        small.close()

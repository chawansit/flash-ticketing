"""Real database contracts for native-row, single-snapshot order reads."""

from concurrent.futures import ThreadPoolExecutor
from contextlib import contextmanager
from threading import Event
from uuid import UUID, uuid4

import pytest

from ticketing.domain import Failure
from ticketing.workers import consume_event

pytestmark = pytest.mark.integration


def make_order(svc, db, show, state="PENDING"):
    hold = svc.reserve("owner", show, ["C", "A", "B"], str(uuid4()))
    payload = envelope = None
    if state in {"PAID", "FULFILLED"}:
        attempt = svc.initiate_payment("owner", hold["order_id"], str(uuid4()), "SUCCEEDED", 0, 1)
        payload = {
            "callback_id": str(uuid4()),
            "payment_id": attempt["payment_id"],
            "order_id": hold["order_id"],
            "amount": 300,
            "currency": "THB",
            "outcome": "SUCCEEDED",
        }
        assert svc.callback(payload)["status"] == "book"
        envelope = {
            "event_id": str(uuid4()),
            "schema_version": 1,
            "event_type": "OrderPaid",
            "payload": {"order_id": hold["order_id"]},
        }
        if state == "FULFILLED":
            consume_event(db, None, envelope)
    elif state == "EXPIRED":
        with db.transaction() as conn:
            conn.execute(
                "UPDATE holds SET expires_at=clock_timestamp()-interval '1 second' WHERE id=%s",
                (hold["hold_id"],),
            )
        assert svc.expire_one()
    return hold, payload, envelope


def expected_order(db, order_id):
    with db.transaction() as conn:
        row = conn.execute("SELECT * FROM orders WHERE id=%s", (order_id,)).fetchone()
        row["tickets"] = conn.execute(
            "SELECT t.id,b.seat_id FROM tickets t JOIN bookings b ON b.id=t.booking_id "
            "WHERE b.order_id=%s ORDER BY b.seat_id",
            (order_id,),
        ).fetchall()
        return row


@pytest.mark.parametrize("state", ["PENDING", "PAID", "FULFILLED", "EXPIRED"])
def test_order_read_preserves_entire_native_row_and_ticket_contract(system, state):
    svc, db, show = system
    hold, _, _ = make_order(svc, db, show, state)
    result = svc.get_order("owner", hold["order_id"])
    assert result == expected_order(db, hold["order_id"])
    assert result["status"] == state
    assert isinstance(result["id"], UUID)
    assert len(result["tickets"]) == (3 if state == "FULFILLED" else 0)
    if state == "FULFILLED":
        assert [t["seat_id"] for t in result["tickets"]] == ["A", "B", "C"]
        assert all(isinstance(t["id"], UUID) for t in result["tickets"])
    assert not any(key.startswith("_ticket_") for key in result)


@pytest.mark.parametrize("missing", [False, True])
def test_order_read_missing_and_wrong_actor_are_indistinguishable(system, missing):
    svc, db, show = system
    hold, _, _ = make_order(svc, db, show, "FULFILLED")
    with pytest.raises(Failure, match="ORDER_NOT_FOUND") as error:
        svc.get_order("owner" if missing else "other", str(uuid4()) if missing else hold["order_id"])
    assert error.value.status == 404
    assert svc.get_order("owner", hold["order_id"]) == expected_order(db, hold["order_id"])


def test_order_read_preserves_callback_and_fulfillment_replay_uniqueness(system):
    svc, db, show = system
    hold, payload, envelope = make_order(svc, db, show, "FULFILLED")
    before = svc.get_order("owner", hold["order_id"])
    assert svc.callback(payload)["status"] == "duplicate"
    consume_event(db, None, envelope)
    consume_event(db, None, {**envelope, "event_id": str(uuid4())})
    assert svc.get_order("owner", hold["order_id"]) == before
    with db.transaction() as conn:
        assert conn.execute("SELECT count(*) AS n FROM bookings").fetchone()["n"] == 3
        assert conn.execute("SELECT count(*) AS n FROM tickets").fetchone()["n"] == 3
        assert conn.execute("SELECT count(*) AS n FROM payment_callbacks").fetchone()["n"] == 1


def test_order_read_has_one_domain_select_and_releases_pool(system):
    svc, db, show = system
    hold, _, _ = make_order(svc, db, show, "FULFILLED")
    statements = []

    class RecordingConnection:
        def __init__(self, conn):
            self.conn = conn

        def execute(self, query, params=None):
            statements.append(query)
            return self.conn.execute(query, params)

    class RecordingDatabase:
        @contextmanager
        def transaction(self):
            with db.transaction() as conn:
                assert conn.execute(
                    "SELECT current_setting('lock_timeout') AS lock, "
                    "current_setting('statement_timeout') AS statement, "
                    "current_setting('idle_in_transaction_session_timeout') AS idle"
                ).fetchone() == {
                    "lock": "75ms",
                    "statement": "1500ms",
                    "idle": "3s",
                }
                yield RecordingConnection(conn)

    svc.store.db = RecordingDatabase()
    assert len(svc.get_order("owner", hold["order_id"])["tickets"]) == 3
    assert len(statements) == 1
    assert db.pool.get_stats()["pool_available"] == db.pool.get_stats()["pool_size"]


def test_order_read_keeps_one_snapshot_when_fulfillment_commits_after_fetch(system):
    svc, db, show = system
    hold, _, envelope = make_order(svc, db, show, "PAID")
    fetched, committed = Event(), Event()

    class PausedCursor:
        def __init__(self, cursor):
            self.cursor = cursor

        def finish_fetch(self, rows):
            fetched.set()
            assert committed.wait(timeout=5), "Writer did not commit across the read boundary"
            return rows

        def fetchone(self):
            return self.finish_fetch(self.cursor.fetchone())

        def fetchall(self):
            return self.finish_fetch(self.cursor.fetchall())

    class PausedConnection:
        def __init__(self, conn):
            self.conn = conn

        def execute(self, query, params=None):
            cursor = self.conn.execute(query, params)
            # Pause after the first statement that reads the order. The old
            # two-query path can then fetch tickets from a different snapshot.
            return PausedCursor(cursor) if "FROM orders" in query else cursor

    class PausedDatabase:
        @contextmanager
        def transaction(self):
            with db.transaction() as conn:
                yield PausedConnection(conn)

    svc.store.db = PausedDatabase()
    try:
        with ThreadPoolExecutor(max_workers=1) as executor:
            future = executor.submit(svc.get_order, "owner", hold["order_id"])
            try:
                assert fetched.wait(timeout=5), "Reader did not reach the first result"
                consume_event(db, None, envelope)
            finally:
                committed.set()
            result = future.result(timeout=5)
    finally:
        svc.store.db = db
    assert result["status"] == "PAID" and result["tickets"] == []
    after = svc.get_order("owner", hold["order_id"])
    assert after["status"] == "FULFILLED"
    assert [t["seat_id"] for t in after["tickets"]] == ["A", "B", "C"]

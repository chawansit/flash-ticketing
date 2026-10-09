"""Real PostgreSQL lock-order and replay contracts for callback discovery."""
from concurrent.futures import ThreadPoolExecutor
from contextlib import contextmanager
from threading import Barrier
from uuid import uuid4

import pytest
from psycopg.errors import LockNotAvailable

from ticketing.domain import Failure
from ticketing.workers import consume_event

pytestmark = pytest.mark.integration


def make_payment(svc, show, seats=("A",), actor="owner"):
    hold = svc.reserve(actor, show, list(seats), str(uuid4()))
    attempt = svc.initiate_payment(actor, hold["order_id"], str(uuid4()), "SUCCEEDED", 0, 1)
    return hold, {
        "callback_id": str(uuid4()), "payment_id": attempt["payment_id"],
        "order_id": hold["order_id"], "amount": hold["total"],
        "currency": hold["currency"], "outcome": "SUCCEEDED",
    }


class ObservedDatabase:
    def __init__(self, db, statements, after_order=None):
        self.db, self.statements, self.after_order = db, statements, after_order

    @contextmanager
    def transaction(self):
        with self.db.transaction() as conn:
            owner = self

            class Connection:
                def execute(self, query, params=None):
                    owner.statements.append(query)
                    cursor = conn.execute(query, params)
                    if "FROM orders" in query and "FOR UPDATE" in query and owner.after_order:
                        owner.after_order()
                    return cursor

            yield Connection()


def assert_no_effects(db):
    with db.transaction() as conn:
        for table in ("bookings", "payment_callbacks", "refund_requests"):
            assert conn.execute(f"SELECT count(*) AS n FROM {table}").fetchone()["n"] == 0
        assert conn.execute("SELECT count(*) AS n FROM outbox_events WHERE event_type='OrderPaid'").fetchone()["n"] == 0
        assert conn.execute("SELECT status FROM payment_attempts").fetchone()["status"] == "PENDING"


def test_callback_discovery_saves_one_select_with_common_timeouts(system):
    svc, db, show = system
    _, payload = make_payment(svc, show, ("C", "A", "B"))
    statements = []

    class CheckedDatabase(ObservedDatabase):
        @contextmanager
        def transaction(self):
            with super().transaction() as conn:
                settings = conn.execute(
                    "SELECT current_setting('lock_timeout') AS lock, "
                    "current_setting('statement_timeout') AS statement, "
                    "current_setting('idle_in_transaction_session_timeout') AS idle"
                ).fetchone()
                assert settings == {"lock": "75ms", "statement": "1500ms", "idle": "3s"}
                statements.clear()
                yield conn

    svc.store.db = CheckedDatabase(db, statements)
    assert svc.callback(payload) == {"status": "book"}
    selects = [q for q in statements if q.lstrip().startswith("SELECT")]
    assert len(selects) == 6
    assert "FOR UPDATE OF o NOWAIT" in selects[0]
    assert "FROM payment_attempts" in selects[1] and "FOR UPDATE NOWAIT" in selects[1]
    assert db.pool.get_stats()["pool_available"] == db.pool.get_stats()["pool_size"]


def test_callback_missing_payment_is_404_and_has_no_effects(system):
    svc, db, show = system
    _, payload = make_payment(svc, show)
    with pytest.raises(Failure) as error:
        svc.callback({**payload, "payment_id": str(uuid4())})
    assert (error.value.code, error.value.status) == ("PAYMENT_NOT_FOUND", 404)
    assert_no_effects(db)


@pytest.mark.parametrize("field,value", [("order_id", None), ("amount", 101), ("currency", "USD")])
def test_callback_invalid_claim_rolls_back_before_financial_writes(system, field, value):
    svc, db, show = system
    _, payload = make_payment(svc, show)
    with pytest.raises(Failure) as error:
        svc.callback({**payload, field: str(uuid4()) if value is None else value})
    assert (error.value.code, error.value.status) == ("PAYMENT_MISMATCH", 422)
    assert_no_effects(db)
    assert svc.callback(payload)["status"] == "book"


def test_callback_locks_payment_derived_order_before_touching_locked_payment(system):
    svc, db, show = system
    hold, payload = make_payment(svc, show)
    statements = []
    svc.store.db = ObservedDatabase(db, statements)
    with db.transaction() as order_holder, db.transaction() as payment_holder:
        order_holder.execute("SELECT id FROM orders WHERE id=%s FOR UPDATE", (hold["order_id"],))
        payment_holder.execute("SELECT id FROM payment_attempts WHERE id=%s FOR UPDATE", (payload["payment_id"],))
        with pytest.raises(LockNotAvailable):
            svc.callback(payload)
    assert "FROM orders" in statements[-1] and "FOR UPDATE" in statements[-1]
    assert not any("FROM payment_attempts" in q and "FOR UPDATE" in q for q in statements)
    assert_no_effects(db)
    assert svc.callback(payload)["status"] == "book"


def test_callback_payment_contention_rolls_back_order_lock_and_pool(system):
    svc, db, show = system
    hold, payload = make_payment(svc, show)
    with db.transaction() as holder:
        holder.execute("SELECT id FROM payment_attempts WHERE id=%s FOR UPDATE", (payload["payment_id"],))
        with pytest.raises(LockNotAvailable):
            svc.callback(payload)
        with db.transaction() as probe:
            assert probe.execute(
                "SELECT id FROM orders WHERE id=%s FOR UPDATE NOWAIT", (hold["order_id"],)
            ).fetchone()
    assert_no_effects(db)
    assert svc.callback(payload)["status"] == "book"
    assert db.pool.get_stats()["pool_available"] == db.pool.get_stats()["pool_size"]


def test_callback_first_join_locks_only_order_and_later_payment_read_is_fresh(system):
    svc, db, show = system
    hold, payload = make_payment(svc, show)
    inspected = []

    def change_payment_after_order_lock():
        with db.transaction() as other:
            # The first query must leave the payment unlocked. This committed
            # state change deliberately models the later-read boundary only.
            other.execute(
                "SELECT id FROM payment_attempts WHERE id=%s FOR UPDATE NOWAIT",
                (payload["payment_id"],),
            )
            other.execute("UPDATE payment_attempts SET status='SUCCEEDED' WHERE id=%s", (payload["payment_id"],))
        inspected.append(True)
        with db.transaction() as probe, pytest.raises(LockNotAvailable):
            probe.execute("SELECT id FROM orders WHERE id=%s FOR UPDATE NOWAIT", (hold["order_id"],))

    statements = []
    svc.store.db = ObservedDatabase(db, statements, change_payment_after_order_lock)
    assert svc.callback(payload) == {"status": "duplicate"}
    assert inspected == [True]
    with db.transaction() as conn:
        assert conn.execute("SELECT count(*) AS n FROM bookings").fetchone()["n"] == 0
        assert conn.execute("SELECT count(*) AS n FROM payment_callbacks").fetchone()["n"] == 1
        assert conn.execute("SELECT count(*) AS n FROM refund_requests").fetchone()["n"] == 0


def test_callback_order_contention_does_not_block_an_unrelated_order(system):
    svc, db, show = system
    first, first_payload = make_payment(svc, show, ("A",), "first")
    _, second_payload = make_payment(svc, show, ("B",), "second")
    with db.transaction() as holder:
        holder.execute("SELECT id FROM orders WHERE id=%s FOR UPDATE", (first["order_id"],))
        assert svc.callback(second_payload)["status"] == "book"
        with pytest.raises(LockNotAvailable):
            svc.callback(first_payload)
    assert svc.callback(first_payload)["status"] == "book"
    with db.transaction() as conn:
        assert conn.execute("SELECT count(*) AS n FROM bookings").fetchone()["n"] == 2
        assert conn.execute("SELECT count(DISTINCT seat_id) AS n FROM bookings").fetchone()["n"] == 2


@pytest.mark.parametrize("distinct_ids", [False, True])
def test_callback_concurrent_replay_keeps_exact_bookings_tickets_and_outbox(system, distinct_ids):
    svc, db, show = system
    hold, payload = make_payment(svc, show, ("C", "A", "B"))
    callbacks = [{**payload, "callback_id": str(uuid4())} if distinct_ids else payload for _ in range(8)]
    start = Barrier(8)

    def attempt(item):
        start.wait(timeout=5)
        try:
            return svc.callback(item)
        except LockNotAvailable:
            return None

    with ThreadPoolExecutor(max_workers=8) as executor:
        results = list(executor.map(attempt, callbacks))
    assert sum(result == {"status": "book"} for result in results) == 1
    for item in callbacks:
        assert svc.callback(item) == {"status": "duplicate"}
    event = {
        "event_id": str(uuid4()), "schema_version": 1, "event_type": "OrderPaid",
        "payload": {"order_id": hold["order_id"]},
    }
    consume_event(db, None, event)
    consume_event(db, None, event)
    consume_event(db, None, {**event, "event_id": str(uuid4())})
    with db.transaction() as conn:
        for table in ("bookings", "tickets"):
            assert conn.execute(f"SELECT count(*) AS n FROM {table}").fetchone()["n"] == 3
        assert conn.execute("SELECT count(*) AS n FROM outbox_events WHERE event_type='OrderPaid'").fetchone()["n"] == 1
        assert conn.execute("SELECT count(*) AS n FROM payment_callbacks").fetchone()["n"] == (8 if distinct_ids else 1)
    assert svc.get_order("owner", hold["order_id"])["status"] == "FULFILLED"


def test_callback_hash_mismatch_replay_has_no_additional_effect(system):
    svc, db, show = system
    _, payload = make_payment(svc, show)
    assert svc.callback(payload)["status"] == "book"
    with pytest.raises(Failure) as error:
        svc.callback({**payload, "outcome": "FAILED"})
    assert error.value.code == "CALLBACK_MISMATCH"
    assert svc.callback(payload)["status"] == "duplicate"
    with db.transaction() as conn:
        assert conn.execute("SELECT status FROM payment_attempts").fetchone()["status"] == "SUCCEEDED"
        assert conn.execute("SELECT count(*) AS n FROM bookings").fetchone()["n"] == 1
        assert conn.execute("SELECT count(*) AS n FROM payment_callbacks").fetchone()["n"] == 1


def test_callback_failure_before_commit_rolls_back_and_replay_books_once(system):
    svc, db, show = system
    _, payload = make_payment(svc, show)

    class FailBeforeCommit:
        @contextmanager
        def transaction(self):
            with db.transaction() as conn:
                yield conn
                raise RuntimeError("injected before commit")

    svc.store.db = FailBeforeCommit()
    with pytest.raises(RuntimeError, match="injected before commit"):
        svc.callback(payload)
    svc.store.db = db
    assert_no_effects(db)
    assert svc.callback(payload)["status"] == "book"
    assert svc.callback(payload)["status"] == "duplicate"
    with db.transaction() as conn:
        assert conn.execute("SELECT count(*) AS n FROM bookings").fetchone()["n"] == 1
        assert conn.execute("SELECT count(*) AS n FROM outbox_events WHERE event_type='OrderPaid'").fetchone()["n"] == 1

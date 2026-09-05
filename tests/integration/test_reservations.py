from concurrent.futures import ThreadPoolExecutor
from threading import Barrier
from uuid import uuid4

import pytest
from psycopg.errors import LockNotAvailable, UniqueViolation

from ticketing.domain import Failure
from ticketing.workers import consume_event

pytestmark = pytest.mark.integration


def payment(svc, hold, actor="one"):
    attempt = svc.initiate_payment(actor, hold["order_id"], str(uuid4()), "SUCCEEDED", 0, 3)
    return {
        "callback_id": str(uuid4()),
        "payment_id": attempt["payment_id"],
        "order_id": hold["order_id"],
        "amount": hold["total"],
        "currency": hold["currency"],
        "outcome": "SUCCEEDED",
    }


def expire(db, hold):
    with db.transaction() as conn:
        conn.execute(
            "UPDATE holds SET expires_at=clock_timestamp()-interval '1 second' WHERE id=%s",
            (hold["hold_id"],),
        )
        conn.execute(
            "UPDATE event_seats SET reserved_until=clock_timestamp()-interval '1 second' WHERE hold_id=%s",
            (hold["hold_id"],),
        )


def test_same_seat_concurrent_without_redis(system):
    svc, db, event_id = system
    barrier = Barrier(24)

    def reserve(index):
        barrier.wait()
        try:
            return svc.reserve(str(index), event_id, ["A"], str(uuid4()))
        except (Failure, LockNotAvailable):
            return None

    with ThreadPoolExecutor(max_workers=24) as executor:
        results = list(executor.map(reserve, range(24)))
    assert sum(r is not None for r in results) == 1
    with db.transaction() as conn:
        assert conn.execute("SELECT count(*) AS n FROM orders").fetchone()["n"] == 1


def test_multiseat_atomic_and_idempotent(system):
    svc, db, event_id = system
    first = svc.reserve("one", event_id, ["B", "A"], "same")
    assert svc.reserve("one", event_id, ["A", "B"], "same") == first
    with pytest.raises(Failure, match="IDEMPOTENCY_MISMATCH"):
        svc.reserve("one", event_id, ["C"], "same")
    with pytest.raises(Failure):
        svc.reserve("two", event_id, ["B", "C"], "new")
    assert svc.reserve("three", event_id, ["C"], "third")
    assert svc.checkout("one", first["hold_id"], "checkout") == svc.checkout(
        "one", first["hold_id"], "checkout"
    )
    with db.transaction() as conn:
        assert conn.execute("SELECT count(*) AS n FROM orders").fetchone()["n"] == 2


def test_duplicate_payment_and_duplicate_fulfillment(system):
    svc, db, event_id = system
    hold = svc.reserve("one", event_id, ["A", "B"], "hold")
    callback = payment(svc, hold)
    assert svc.callback(callback)["status"] == "book"
    assert svc.callback(callback)["status"] == "duplicate"
    callback["callback_id"] = str(uuid4())
    assert svc.callback(callback)["status"] == "duplicate"
    envelope = {
        "event_id": str(uuid4()),
        "schema_version": 1,
        "event_type": "OrderPaid",
        "payload": {"order_id": hold["order_id"]},
    }
    consume_event(db, None, envelope)
    consume_event(db, None, envelope)
    envelope["event_id"] = str(uuid4())
    consume_event(db, None, envelope)
    with db.transaction() as conn:
        assert conn.execute("SELECT count(*) AS n FROM bookings").fetchone()["n"] == 2
        assert conn.execute("SELECT count(*) AS n FROM tickets").fetchone()["n"] == 2
    assert svc.get_order("one", hold["order_id"])["status"] == "FULFILLED"


def test_late_payment_cannot_take_new_hold(system):
    svc, db, event_id = system
    old = svc.reserve("one", event_id, ["A"], "old")
    callback = payment(svc, old)
    expire(db, old)
    new = svc.reserve("two", event_id, ["A"], "new")
    assert svc.callback(callback)["status"] == "refund"
    callback["callback_id"] = str(uuid4())
    assert svc.callback(callback)["status"] == "duplicate"
    with db.transaction() as conn:
        seat = conn.execute("SELECT * FROM event_seats WHERE seat_id='A'").fetchone()
        assert str(seat["hold_id"]) == new["hold_id"]
        assert conn.execute("SELECT count(*) AS n FROM bookings").fetchone()["n"] == 0
        assert conn.execute("SELECT count(*) AS n FROM refund_requests").fetchone()["n"] == 1


def test_expiry_housekeeping_preserves_reclaimed_seat(system):
    svc, db, event_id = system
    old = svc.reserve("one", event_id, ["A"], "old")
    expire(db, old)
    new = svc.reserve("two", event_id, ["A"], "new")
    assert svc.expire_one()
    with db.transaction() as conn:
        assert (
            str(conn.execute("SELECT hold_id FROM event_seats WHERE seat_id='A'").fetchone()["hold_id"])
            == new["hold_id"]
        )


def test_failure_then_success_refunds_and_mismatch_rolls_back(system):
    svc, db, event_id = system
    hold = svc.reserve("one", event_id, ["A"], "hold")
    callback = payment(svc, hold)
    callback["amount"] += 1
    with pytest.raises(Failure, match="PAYMENT_MISMATCH"):
        svc.callback(callback)
    callback["amount"] -= 1
    callback["outcome"] = "FAILED"
    assert svc.callback(callback)["status"] == "failed"
    callback.update(callback_id=str(uuid4()), outcome="SUCCEEDED")
    assert svc.callback(callback)["status"] == "refund"
    with db.transaction() as conn:
        assert conn.execute("SELECT count(*) AS n FROM bookings").fetchone()["n"] == 0


def test_database_unique_booking_backstop(system):
    svc, db, event_id = system
    hold = svc.reserve("one", event_id, ["A"], "hold")
    svc.callback(payment(svc, hold))
    with pytest.raises(UniqueViolation), db.transaction() as conn:
        conn.execute("INSERT INTO bookings VALUES (%s,%s,'A',%s)", (uuid4(), event_id, hold["order_id"]))


def test_concurrent_checkout_single_order(system):
    svc, _db, event_id = system
    hold = svc.reserve("one", event_id, ["A"], "hold")
    with ThreadPoolExecutor(max_workers=8) as executor:
        results = list(executor.map(lambda _: svc.checkout("one", hold["hold_id"], "same"), range(8)))
    assert all(r == results[0] for r in results)


def test_payment_races_expired_seat_reclamation(system):
    svc, db, event_id = system
    old = svc.reserve("one", event_id, ["A"], "old")
    callback = payment(svc, old)
    expire(db, old)
    gate = Barrier(2)

    def confirm():
        gate.wait()
        try:
            return svc.callback(callback)
        except LockNotAvailable:
            return None

    def reclaim():
        gate.wait()
        try:
            return svc.reserve("two", event_id, ["A"], "new")
        except LockNotAvailable:
            return None

    with ThreadPoolExecutor(max_workers=2) as executor:
        confirming, reclaiming = executor.submit(confirm), executor.submit(reclaim)
        result, new = confirming.result(), reclaiming.result()
    if result is None:
        result = svc.callback(callback)
    if new is None:
        new = svc.reserve("two", event_id, ["A"], "new")
    assert result["status"] == "refund"
    with db.transaction() as conn:
        assert (
            str(conn.execute("SELECT hold_id FROM event_seats WHERE seat_id='A'").fetchone()["hold_id"])
            == new["hold_id"]
        )
        assert conn.execute("SELECT count(*) AS n FROM bookings").fetchone()["n"] == 0
        assert conn.execute("SELECT count(*) AS n FROM refund_requests").fetchone()["n"] == 1


def test_simultaneous_duplicate_callbacks(system):
    svc, db, event_id = system
    hold = svc.reserve("one", event_id, ["A"], "hold")
    callback = payment(svc, hold)
    gate = Barrier(8)

    def confirm(_):
        gate.wait()
        try:
            return svc.callback({**callback, "callback_id": str(uuid4())})
        except LockNotAvailable:
            return None

    with ThreadPoolExecutor(max_workers=8) as executor:
        results = list(executor.map(confirm, range(8)))
    assert sum(result is not None and result["status"] == "book" for result in results) == 1
    with db.transaction() as conn:
        assert conn.execute("SELECT count(*) AS n FROM bookings").fetchone()["n"] == 1

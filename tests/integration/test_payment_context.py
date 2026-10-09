from concurrent.futures import ThreadPoolExecutor
from threading import Barrier
from uuid import uuid4

import pytest
from psycopg.errors import LockNotAvailable

from ticketing.domain import Failure
from ticketing.infrastructure import reservations as persistence
from ticketing.infrastructure.postgres import MeasuredCursor

pytestmark = pytest.mark.integration


def initiate(svc, order, key="payment"):
    return svc.initiate_payment("one", order["order_id"], key, "SUCCEEDED", 2, 3)


def expired(db, order):
    with db.transaction() as conn:
        conn.execute(
            "UPDATE holds SET expires_at=clock_timestamp()-interval '1 second' WHERE id=%s",
            (order["hold_id"],),
        )


def test_new_attempt_uses_fresh_post_lock_context_and_preserves_due_time(system, monkeypatch):
    svc, db, event_id = system
    order = svc.reserve("one", event_id, ["A"], "hold")
    statements = []
    original = MeasuredCursor.execute

    def capture(cursor, query, *args, **kwargs):
        statements.append(query)
        return original(cursor, query, *args, **kwargs)

    with monkeypatch.context() as patch:
        patch.setattr(MeasuredCursor, "execute", capture)
        result = initiate(svc, order)
    business_reads = [q for q in statements if q.startswith("SELECT") and "set_config" not in q]
    assert len(business_reads) == 2
    assert "FOR UPDATE NOWAIT" in business_reads[0]
    assert "LEFT JOIN payment_attempts" in business_reads[1]
    with db.transaction() as conn:
        row = conn.execute("SELECT * FROM payment_attempts WHERE order_id=%s", (order["order_id"],)).fetchone()
        assert str(row["id"]) == result["payment_id"]
        assert row["status"] == "PENDING"
        assert row["outcome"] == "SUCCEEDED"
        assert row["target_deliveries"] == 3
        response = conn.execute(
            "SELECT response FROM idempotency_records WHERE operation='payment'"
        ).fetchone()["response"]
        assert response == result
        assert conn.execute(
            "SELECT due_at > clock_timestamp()-interval '1 second' AS future FROM payment_attempts"
        ).fetchone()["future"]


def test_existing_payment_replays_after_expiry_and_rejects_key_mismatch(system):
    svc, db, event_id = system
    order = svc.reserve("one", event_id, ["A"], "hold")
    result = initiate(svc, order)
    expired(db, order)
    assert initiate(svc, order) == result
    assert initiate(svc, order, "new-key") == result
    with pytest.raises(Failure, match="IDEMPOTENCY_MISMATCH"):
        svc.initiate_payment("one", order["order_id"], "payment", "FAILED", 2, 3)
    with db.transaction() as conn:
        assert conn.execute("SELECT count(*) AS n FROM payment_attempts").fetchone()["n"] == 1


@pytest.mark.parametrize("case", ["expired", "released", "other_actor", "absent"])
def test_invalid_payment_has_no_attempt_or_idempotency_residue(system, case):
    svc, db, event_id = system
    order = svc.reserve("one", event_id, ["A"], "hold")
    actor, order_id, error = "one", order["order_id"], "ORDER_NOT_PAYABLE"
    if case == "expired":
        expired(db, order)
    elif case == "released":
        svc.release("one", order["hold_id"])
    elif case == "other_actor":
        actor, error = "two", "ORDER_NOT_FOUND"
    else:
        order_id, error = str(uuid4()), "ORDER_NOT_FOUND"
    with pytest.raises(Failure, match=error):
        svc.initiate_payment(actor, order_id, "rejected", "SUCCEEDED", 2, 3)
    with db.transaction() as conn:
        assert conn.execute("SELECT count(*) AS n FROM payment_attempts").fetchone()["n"] == 0
        assert conn.execute(
            "SELECT count(*) AS n FROM idempotency_records WHERE operation='payment'"
        ).fetchone()["n"] == 0


def test_response_failure_rolls_back_attempt_and_can_replay_safely(system, monkeypatch):
    svc, db, event_id = system
    order = svc.reserve("one", event_id, ["A"], "hold")

    def fail(*_):
        raise RuntimeError("response persistence failed")

    with monkeypatch.context() as patch:
        patch.setattr(persistence, "remember", fail)
        with pytest.raises(RuntimeError, match="response persistence failed"):
            initiate(svc, order)
    with db.transaction() as conn:
        assert conn.execute("SELECT count(*) AS n FROM payment_attempts").fetchone()["n"] == 0
        assert conn.execute(
            "SELECT count(*) AS n FROM idempotency_records WHERE operation='payment'"
        ).fetchone()["n"] == 0
    assert initiate(svc, order) == initiate(svc, order)


def test_concurrent_keys_resolve_to_one_payment_and_keep_nowait(system):
    svc, db, event_id = system
    order = svc.reserve("one", event_id, ["A"], "hold")
    with db.transaction() as conn:
        conn.execute("SELECT id FROM orders WHERE id=%s FOR UPDATE", (order["order_id"],))
        with pytest.raises(LockNotAvailable):
            initiate(svc, order, "locked")
    gate = Barrier(12)

    def attempt(index):
        gate.wait(timeout=5)
        try:
            return initiate(svc, order, f"concurrent-{index}")
        except LockNotAvailable:
            return None

    with ThreadPoolExecutor(max_workers=12) as executor:
        results = list(executor.map(attempt, range(12)))
    accepted = [r for r in results if r is not None]
    assert accepted
    assert len({r["payment_id"] for r in accepted}) == 1
    for index in range(12):
        assert initiate(svc, order, f"concurrent-{index}") == accepted[0]
    with db.transaction() as conn:
        assert conn.execute("SELECT count(*) AS n FROM payment_attempts").fetchone()["n"] == 1
        assert conn.execute(
            "SELECT count(*) AS n FROM idempotency_records WHERE operation='payment' AND response IS NULL"
        ).fetchone()["n"] == 0

import json
from contextlib import contextmanager
from dataclasses import replace
from types import SimpleNamespace

import pytest

from ticketing import workers
from ticketing.config import Settings
from ticketing.infrastructure.payment_simulator_latency import confirmation_delay

pytestmark = pytest.mark.integration


def test_scheduled_confirmation_replay_duplicate_callbacks_and_ticket_durability(system, monkeypatch):
    svc, db, event = system
    hold = svc.reserve("actor", event, ["A"], "hold")
    delay = confirmation_delay("bank-like", "actor", hold["order_id"], "payment")
    first = svc.initiate_payment("actor", hold["order_id"], "payment", "SUCCEEDED", delay, 3)
    assert svc.initiate_payment(
        "actor", hold["order_id"], "payment", "SUCCEEDED",
        confirmation_delay("bank-like", "actor", hold["order_id"], "payment"), 3,
    ) == first
    with db.transaction() as conn:
        # Persisted schedule lies in the approved window, allowing minor test execution time.
        remaining = conn.execute(
            "SELECT EXTRACT(EPOCH FROM due_at-clock_timestamp()) AS remaining FROM payment_attempts"
        ).fetchone()["remaining"]
        assert .5 <= remaining <= 3
    settings = replace(Settings(), simulator_latency_profile="bank-like")
    assert not workers.simulate_one(db, settings, SimpleNamespace(
        post=lambda *_: pytest.fail("Callback sent before confirmation is due")))
    with db.transaction() as conn:
        conn.execute("UPDATE payment_attempts SET due_at=clock_timestamp()-interval '1 second'")

    active, waits = [], []

    @contextmanager
    def transaction():
        with db.transaction() as conn:
            active.append(True)
            try:
                yield conn
            finally:
                active.pop()

    def wait(seconds):
        assert not active
        assert .02 <= seconds <= .1
        waits.append(seconds)

    def deliver(raw, _headers):
        assert not active
        svc.callback(json.loads(raw))

    monkeypatch.setattr(workers.time, "sleep", wait)
    observed_db = SimpleNamespace(transaction=transaction)
    for _ in range(3):
        assert workers.simulate_one(observed_db, settings, SimpleNamespace(post=deliver))
    assert not workers.simulate_one(observed_db, settings, SimpleNamespace(post=deliver))
    assert len(waits) == 3
    with db.transaction() as conn:
        paid = conn.execute(
            "SELECT id,event_type,schema_version,payload FROM outbox_events WHERE event_type='OrderPaid'"
        ).fetchall()
    assert len(paid) == 1
    envelope = {**paid[0], "event_id": str(paid[0]["id"])}
    workers.consume_event(db, None, envelope)
    workers.consume_event(db, None, envelope)
    with db.transaction() as conn:
        conn.execute("UPDATE holds SET expires_at=clock_timestamp()-interval '1 second'")
    svc.expire_batch(8)
    with db.transaction() as conn:
        assert conn.execute("SELECT count(*) AS n FROM bookings").fetchone()["n"] == 1
        assert conn.execute("SELECT count(*) AS n FROM tickets").fetchone()["n"] == 1
        assert conn.execute("SELECT status FROM orders").fetchone()["status"] == "FULFILLED"
        payment = conn.execute("SELECT deliveries,target_deliveries FROM payment_attempts").fetchone()
        assert payment["deliveries"] == payment["target_deliveries"] == 3
        assert conn.execute("SELECT count(*) AS n FROM payment_attempts").fetchone()["n"] == 1

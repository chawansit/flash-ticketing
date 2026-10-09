from unittest.mock import Mock
from uuid import uuid4

import pytest

from ticketing import workers

pytestmark = pytest.mark.integration


def test_publish_crash_reuses_event_id_after_lease(system):
    svc, db, event_id = system
    svc.reserve("one", event_id, ["A"], "one")
    uncertain = Mock()
    uncertain.send.return_value.get.side_effect = TimeoutError("ack lost after broker accepted event")
    with pytest.raises(TimeoutError):
        workers.publish_one(db, uncertain)
    first = uncertain.send.call_args.kwargs["value"]
    with db.transaction() as conn:
        row = conn.execute("SELECT * FROM outbox_events").fetchone()
        assert row["published_at"] is None
        conn.execute("UPDATE outbox_events SET lease_until=clock_timestamp()-interval '1 second'")
    recovered = Mock()
    assert workers.publish_one(db, recovered)
    assert recovered.send.call_args.kwargs["value"]["event_id"] == first["event_id"]
    with db.transaction() as conn:
        assert conn.execute("SELECT published_at FROM outbox_events").fetchone()["published_at"] is not None


def test_consumer_failure_rolls_back_inbox_and_ticket(system, monkeypatch):
    svc, db, event_id = system
    hold = svc.reserve("one", event_id, ["A"], "one")
    attempt = svc.initiate_payment("one", hold["order_id"], "payment", "SUCCEEDED", 0, 3)
    svc.callback(
        {
            "callback_id": str(uuid4()),
            "payment_id": attempt["payment_id"],
            "order_id": hold["order_id"],
            "amount": hold["total"],
            "currency": "THB",
            "outcome": "SUCCEEDED",
        }
    )
    envelope = {
        "event_id": str(uuid4()),
        "schema_version": 1,
        "event_type": "OrderPaid",
        "payload": {"order_id": hold["order_id"]},
    }
    with monkeypatch.context() as patch:
        patch.setattr(workers, "event", Mock(side_effect=RuntimeError("crash before commit")))
        with pytest.raises(RuntimeError):
            workers.consume_event(db, None, envelope)
    with db.transaction() as conn:
        assert conn.execute("SELECT count(*) AS n FROM tickets").fetchone()["n"] == 0
        assert conn.execute("SELECT count(*) AS n FROM consumer_inbox").fetchone()["n"] == 0
    workers.consume_event(db, None, envelope)
    assert svc.get_order("one", hold["order_id"])["status"] == "FULFILLED"

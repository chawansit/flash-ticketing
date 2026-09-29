"""Order reads retain actor isolation and ticket response shape."""

from contextlib import nullcontext
from uuid import uuid4

import pytest

from ticketing.domain import Failure
from ticketing.infrastructure.reservations import PostgresReservations


class FakeConnection:
    def __init__(self, row):
        self.row = row
        self.calls = []

    def execute(self, query, params=None):
        self.calls.append((query, params))
        return self

    def fetchone(self):
        return self.row


class FakeDatabase:
    def __init__(self, row):
        self.conn = FakeConnection(row)
        self.acquisitions = 0

    def connection(self):
        self.acquisitions += 1
        return nullcontext(self.conn)

    def transaction(self):
        raise AssertionError("Status read must not enter the write transaction path")


def test_order_read_returns_actor_owned_order_and_tickets_from_one_snapshot():
    order_id = uuid4()
    ticket_id = str(uuid4())
    db = FakeDatabase({"id": order_id, "actor": "buyer-a", "status": "FULFILLED",
                       "tickets": [{"id": ticket_id, "seat_id": "S1"}]})

    result = PostgresReservations(db, cache=None).get_order("buyer-a", order_id)

    assert result["tickets"] == [{"id": ticket_id, "seat_id": "S1"}]
    assert db.acquisitions == 1
    assert len(db.conn.calls) == 2  # timeout plus one actor-scoped statement
    assert db.conn.calls[1][1] == (order_id, "buyer-a")


def test_order_read_pending_has_no_ticket_and_unknown_actor_is_not_found():
    order_id = uuid4()
    pending = FakeDatabase({"id": order_id, "status": "PENDING", "tickets": []})
    assert PostgresReservations(pending, cache=None).get_order("buyer-a", order_id)["tickets"] == []

    hidden = FakeDatabase(None)
    with pytest.raises(Failure) as exc:
        PostgresReservations(hidden, cache=None).get_order("buyer-b", order_id)
    assert exc.value.code == "ORDER_NOT_FOUND"
    assert hidden.conn.calls[1][1] == (order_id, "buyer-b")

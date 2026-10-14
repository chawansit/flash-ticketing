"""The writer must decide sale eligibility from context read after seat locks."""

from datetime import UTC, datetime, timedelta
from uuid import uuid4

import psycopg
import pytest

from ticketing.domain import Failure
from ticketing.infrastructure.postgres import MeasuredCursor
from ticketing.infrastructure.reservations import PostgresReservations, digest

pytestmark = pytest.mark.integration


@pytest.mark.parametrize("pipeline", [False, True])
def test_sale_closed_after_seat_lock_rolls_back_all_writer_effects(system, monkeypatch, pipeline):
    _service, db, event_id = system
    expires = (datetime.now(UTC) + timedelta(seconds=120)).isoformat()
    hold, order = str(uuid4()), str(uuid4())
    payload = {
        "event_id": str(event_id),
        "seat_ids": ["A"],
        "actor": "writer-owner",
        "idempotency_key": "writer-key",
        "command_id": str(uuid4()),
        "hold_id": hold,
        "order_id": order,
        "expires_at": expires,
        "request_hash": digest({"event_id": str(event_id), "seats": ["A"]}),
    }
    response = {"hold_id": hold, "order_id": order, "expires_at": expires, "seats": ["A"]}
    original = MeasuredCursor.execute
    changed = []

    def execute(cursor, query, params=None, **kwargs):
        result = original(cursor, query, params, **kwargs)
        if isinstance(query, str) and "FROM event_seats" in query and "FOR UPDATE NOWAIT" in query:
            # A separate committed update between locking and the context read.
            with psycopg.connect(db.pool.conninfo) as other:
                other.execute(
                    "UPDATE events SET sale_ends=clock_timestamp()-interval '1 second' WHERE id=%s",
                    (event_id,),
                )
            changed.append(True)
        return result

    monkeypatch.setattr(MeasuredCursor, "execute", execute)
    store = PostgresReservations(db, None, persist_write_pipeline=pipeline)
    with pytest.raises(Failure, match="SALE_CLOSED"):
        store.persist_reservation_command(payload, response)
    assert changed == [True]
    with db.transaction() as conn:
        for table in ("holds", "orders", "idempotency_records", "reservation_commands", "outbox_events"):
            assert conn.execute(f"SELECT count(*) AS n FROM {table}").fetchone()["n"] == 0
        assert (
            conn.execute(
                "SELECT hold_id FROM event_seats WHERE event_id=%s AND seat_id='A'", (event_id,)
            ).fetchone()["hold_id"]
            is None
        )

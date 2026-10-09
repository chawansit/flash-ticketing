"""Exercise ADR0247 result equivalence on isolated, migrated PostgreSQL data."""
import sys
from datetime import UTC, datetime, timedelta
from pathlib import Path
from uuid import uuid4

import pytest
from psycopg.rows import tuple_row

sys.path.insert(0, str(Path(__file__).resolve().parents[2] / "scripts"))
import paid_cohort_query as candidate
from observe_paid_pipeline import sample as baseline

pytestmark = pytest.mark.integration


def record(conn, event_id, *, status="FULFILLED", payment_status="SUCCEEDED",
           deliveries=1, target=1, seats=1, issued=True, payment=True):
    hold_id, order_id = uuid4(), uuid4()
    conn.execute("INSERT INTO holds VALUES (%s,%s,%s,%s,'CONSUMED')",
                 (hold_id, "cohort-test", event_id, datetime.now(UTC) - timedelta(seconds=10)))
    conn.execute("INSERT INTO orders VALUES (%s,'cohort-test',%s,%s,%s,'THB',%s,clock_timestamp())",
                 (order_id, hold_id, event_id, seats * 100, status))
    if payment:
        conn.execute("""INSERT INTO payment_attempts
        (id,order_id,status,outcome,due_at,deliveries,target_deliveries)
        VALUES (%s,%s,%s,%s,clock_timestamp(),%s,%s)""",
        (uuid4(), order_id, payment_status, "FAILED" if payment_status == "FAILED" else "SUCCEEDED",
         deliveries, target))
    for i in range(seats):
        seat, booking_id = order_id.hex + str(i), uuid4()
        conn.execute("INSERT INTO event_seats(event_id,seat_id,price) VALUES (%s,%s,100)", (event_id, seat))
        conn.execute("INSERT INTO bookings VALUES (%s,%s,%s,%s)", (booking_id, event_id, seat, order_id))
        if issued:
            conn.execute("INSERT INTO tickets(id,booking_id) VALUES (%s,%s)", (uuid4(), booking_id))
    return order_id


def snapshot(db, shows):
    # Both variants see one repeatable-read snapshot, including unrelated globals.
    with db.connection() as conn, conn.transaction():
        conn.execute("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ, READ ONLY")
        conn.execute("SET LOCAL statement_timeout='5s'")
        factory = conn.row_factory
        try:
            # Match the real observer connection, rather than the API's dict rows.
            conn.row_factory = tuple_row
            original = baseline(conn, shows)
            modified = candidate.sample(conn, shows)
        finally:
            conn.row_factory = factory
    assert modified == original
    return modified


def test_empty_cohort_keeps_global_outbox_counter(system):
    _, db, event_id = system
    with db.transaction() as conn:
        record(conn, event_id)
        conn.execute("INSERT INTO outbox_events(id,aggregate_id,event_type,payload) VALUES (%s,%s,'Test','{}')",
                     (uuid4(), uuid4()))
    counts = snapshot(db, [])
    assert {k:counts[k] for k in candidate.SAMPLE_COLUMNS[:7]} == dict.fromkeys(candidate.SAMPLE_COLUMNS[:7], 0)
    assert counts["unpublished_outbox"] == 1


def test_mixed_states_multi_seat_and_partial_callbacks(system):
    _, db, event_id = system
    unrelated = uuid4()
    with db.transaction() as conn:
        conn.execute("INSERT INTO events VALUES (%s,'Unrelated','THB',clock_timestamp()-interval '1 day',clock_timestamp()+interval '1 day')", (unrelated,))
        record(conn, event_id, seats=3, deliveries=2, target=3)
        record(conn, event_id, status="PENDING", payment_status="PENDING", deliveries=0, target=2, issued=False)
        record(conn, event_id, status="PAID", issued=False)
        record(conn, event_id, status="FAILED", payment_status="FAILED", deliveries=2, target=2, issued=False)
        record(conn, event_id, status="EXPIRED", payment=False, issued=False)
        record(conn, unrelated, seats=4, deliveries=3, target=3)
        conn.execute("INSERT INTO outbox_events(id,aggregate_id,event_type,payload) VALUES (%s,%s,'Test','{}')",
                     (uuid4(), unrelated))
    counts = snapshot(db, [str(event_id)])
    assert {k:counts[k] for k in candidate.SAMPLE_COLUMNS[:8]} == {
        "pending_orders":1, "paid_unfulfilled":1, "fulfilled_orders":1,
        "pending_callback_deliveries":2, "pending_payment_attempts":1, "delivered_callbacks":5,
        "issued_tickets":3, "unpublished_outbox":1,
    }
    # ANY must not duplicate counts for repeated cohort IDs, and missing IDs are harmless.
    assert snapshot(db, [str(event_id), str(event_id), str(uuid4())]) == counts
    combined = snapshot(db, [str(event_id), str(unrelated)])
    assert combined["fulfilled_orders"] == 2
    assert combined["issued_tickets"] == 7
    assert combined["delivered_callbacks"] == 8


def test_cohort_counts_absent_payment_and_missing_ticket_exactly(system):
    _, db, event_id = system
    with db.transaction() as conn:
        record(conn, event_id, payment=False, seats=2, issued=False)
    counts = snapshot(db, [str(event_id)])
    assert counts["fulfilled_orders"] == 1
    assert counts["issued_tickets"] == 0
    assert counts["delivered_callbacks"] == 0
    assert counts["pending_payment_attempts"] == 0

"""Real PostgreSQL parity for the read-only paid observer cohort query."""

from uuid import uuid4

import pytest
from psycopg.rows import tuple_row

from scripts.observe_paid_pipeline import PAID_COHORT_QUERY, sample

pytestmark = pytest.mark.integration


def insert_order(conn, event, status):
    hold, order = uuid4(), uuid4()
    conn.execute("INSERT INTO holds VALUES(%s,'observer-test',%s,clock_timestamp()+interval '1 day','ACTIVE')",
                 (hold, event))
    conn.execute("INSERT INTO orders(id,actor,hold_id,event_id,total,currency,status) "
                 "VALUES(%s,'observer-test',%s,%s,100,'THB',%s)", (order, hold, event, status))
    return order


@pytest.mark.parametrize("selection", ["empty", "single", "duplicate"])
def test_cohort_counts_preserve_filters_and_global_fields(system, selection):
    _, db, event = system
    with db.transaction() as conn:
        other = uuid4()
        conn.execute("INSERT INTO events VALUES(%s,'Other','THB',clock_timestamp(),"
                     "clock_timestamp()+interval '1 day')", (other,))
        conn.execute("INSERT INTO event_seats(event_id,seat_id,price) VALUES(%s,'A',100)", (other,))
        orders = {status: insert_order(conn, event, status) for status in
                  ["PENDING", "PAID", "FULFILLED", "FAILED", "EXPIRED", "REFUND_PENDING", "REFUNDED"]}
        outside = insert_order(conn, other, "PENDING")
        for order, status, deliveries, target in [
            (orders["PENDING"], "PENDING", 0, 1),
            (orders["PAID"], "SUCCEEDED", 1, 3),
            (orders["FULFILLED"], "SUCCEEDED", 3, 3),
            (orders["FAILED"], "FAILED", 1, 1),
            (orders["REFUND_PENDING"], "SUCCEEDED", 2, 2),
            (outside, "PENDING", 0, 1),
        ]:
            conn.execute("INSERT INTO payment_attempts(id,order_id,status,outcome,due_at,deliveries,"
                         "target_deliveries) VALUES(%s,%s,%s,'SUCCEEDED',clock_timestamp(),%s,%s)",
                         (uuid4(), order, status, deliveries, target))
        # An anomalous booking/order event linkage must retain the original booking-event count.
        for booking_event, seat, order in [(event, "A", orders["FULFILLED"]),
                                           (event, "B", outside), (other, "A", outside)]:
            booking = uuid4()
            conn.execute("INSERT INTO bookings VALUES(%s,%s,%s,%s)", (booking, booking_event, seat, order))
            conn.execute("INSERT INTO tickets(id,booking_id) VALUES(%s,%s)", (uuid4(), booking))
        for published in [False, False, True]:
            conn.execute("INSERT INTO outbox_events(id,aggregate_id,event_type,schema_version,payload,"
                         "published_at) VALUES(%s,%s,'observer-test',1,'{}',"
                         "CASE WHEN %s THEN clock_timestamp() ELSE NULL END)", (uuid4(), uuid4(), published))
        selected = [] if selection == "empty" else [event] * (2 if selection == "duplicate" else 1)
        original = conn.row_factory
        conn.row_factory = tuple_row
        try:
            result = sample(conn, selected)
        finally:
            conn.row_factory = original
    expected = dict(zip(["pending_orders", "paid_unfulfilled", "fulfilled_orders",
                        "pending_callback_deliveries", "pending_payment_attempts",
                        "delivered_callbacks", "issued_tickets"], [1, 1, 1, 2, 1, 7, 2], strict=True))
    if selection == "empty":
        expected = {key: 0 for key in expected}
    assert {key: result[key] for key in expected} == expected
    assert result["unpublished_outbox"] == 2  # Global, even for an empty cohort.
    assert result["db_lock_waiters"] >= 0
    assert len(result) == 9


def test_plan_scans_orders_once_with_reused_cohort(system):
    _, db, event = system
    with db.transaction() as conn:
        original = conn.row_factory
        conn.row_factory = tuple_row
        try:
            document = conn.execute("EXPLAIN (FORMAT JSON) " + PAID_COHORT_QUERY, ([event], [event])).fetchone()[0]
        finally:
            conn.row_factory = original

    def relation_scans(node):
        return int(node.get("Relation Name") == "orders") + sum(
            relation_scans(child) for child in node.get("Plans", []))

    assert relation_scans(document[0]["Plan"]) == 1

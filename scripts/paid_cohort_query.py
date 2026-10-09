"""ADR0247 isolated observer SQL; not activated in the frozen paid runner."""

PAID_COHORT_QUERY = """WITH cohort AS MATERIALIZED (
  SELECT id,status FROM orders WHERE event_id=ANY(%s::uuid[])
), order_counts AS (
  SELECT count(*) FILTER (WHERE status='PENDING') AS pending_orders,
         count(*) FILTER (WHERE status='PAID') AS paid_unfulfilled,
         count(*) FILTER (WHERE status='FULFILLED') AS fulfilled_orders
  FROM cohort
), payment_counts AS (
  SELECT count(*) FILTER (WHERE deliveries<target_deliveries) AS pending_callbacks,
         count(*) FILTER (WHERE status='PENDING') AS pending_payments,
         coalesce(sum(deliveries),0) AS delivered_callbacks
  FROM payment_attempts WHERE order_id=ANY(ARRAY(SELECT id FROM cohort))
)
SELECT o.pending_orders,o.paid_unfulfilled,o.fulfilled_orders,
       p.pending_callbacks,p.pending_payments,p.delivered_callbacks,
       (SELECT count(*) FROM tickets
         WHERE booking_id=ANY(ARRAY(SELECT id FROM bookings WHERE event_id=ANY(%s::uuid[])))),
       (SELECT count(*) FROM outbox_events WHERE published_at IS NULL),
       (SELECT count(*) FROM pg_stat_activity
         WHERE datname=current_database() AND wait_event_type='Lock')
FROM order_counts o CROSS JOIN payment_counts p"""

SAMPLE_COLUMNS = (
    "pending_orders", "paid_unfulfilled", "fulfilled_orders",
    "pending_callback_deliveries", "pending_payment_attempts", "delivered_callbacks",
    "issued_tickets", "unpublished_outbox", "db_lock_waiters",
)


def sample(conn, show_ids):
    """Read the same exact cohort counters in one database snapshot."""
    row = conn.execute(PAID_COHORT_QUERY, (show_ids, show_ids)).fetchone()
    return dict(zip(SAMPLE_COLUMNS, row, strict=True))

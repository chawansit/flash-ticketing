"""ADR0247/ADR0303 opt-in observer SQL; inactive in the frozen paid runner."""

import hashlib

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


# Exact effective SQL measured against the ADR0301 cohort in ADR0302.
RUNTIME_BASELINE_SHA256 = "304b87b37cd635ea178cb69adf19291c33e1218e6652651af9f8ed08e930b5c2"


def install_runtime_candidate(module):
    """Replace only the measured indexed query, once, without changing defaults."""
    current = getattr(module, "PAID_COHORT_QUERY", None)
    if not isinstance(current, str) or hashlib.sha256(current.encode()).hexdigest() != RUNTIME_BASELINE_SHA256:
        raise ValueError("Exact measured runtime cohort query required")
    module.PAID_COHORT_QUERY = PAID_COHORT_QUERY
    return {"decision": "ADR0303", "baseline_query_sha256": RUNTIME_BASELINE_SHA256,
            "candidate_query_sha256": hashlib.sha256(PAID_COHORT_QUERY.encode()).hexdigest(),
            "sample_columns": list(SAMPLE_COLUMNS)}


def runtime_contract(arm):
    from pathlib import Path

    if arm not in {"control", "candidate"}:
        raise ValueError("Explicit cohort query arm required")
    candidate = hashlib.sha256(PAID_COHORT_QUERY.encode()).hexdigest()
    return {"decision": "ADR0304", "arm": arm,
            "baseline_query_sha256": RUNTIME_BASELINE_SHA256,
            "candidate_query_sha256": candidate,
            "active_query_sha256": RUNTIME_BASELINE_SHA256 if arm == "control" else candidate,
            "helper_sha256": hashlib.sha256(Path(__file__).read_bytes().replace(b"\r\n", b"\n")).hexdigest(),
            "sample_columns": list(SAMPLE_COLUMNS), "interval_seconds": 1,
            "additional_database_connections": 0, "hourly": False}


def install_runtime_comparison(module, contract):
    # Validate the transferred contract before touching the query or sampler.
    expected = runtime_contract(contract.get("arm"))
    current = getattr(module, "PAID_COHORT_QUERY", None)
    if (getattr(module, "RUNTIME_COHORT_CONTRACT", None) is not None
            or contract != expected or not isinstance(current, str)
            or hashlib.sha256(current.encode()).hexdigest() != RUNTIME_BASELINE_SHA256):
        raise ValueError("Exact source-bound runtime cohort comparison required")
    original = module.sample
    if contract["arm"] == "candidate":
        install_runtime_candidate(module)

    def observed(conn, shows):
        return {**original(conn, shows), "runtime_cohort_contract": expected}

    module.sample = observed
    module.RUNTIME_COHORT_CONTRACT = expected
    return expected

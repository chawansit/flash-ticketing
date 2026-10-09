"""Read-only PostgreSQL audit for an isolated checkout smoke fixture."""

import argparse
import json
import os
from pathlib import Path

import psycopg


def audit(conn, show_ids, expected, expected_paid=None, callback_duplicates=3):
    expected_paid = expected if expected_paid is None else expected_paid
    with conn.transaction():
        conn.execute("SET TRANSACTION READ ONLY")
        conn.execute("SET LOCAL statement_timeout = '20s'")
        values = conn.execute(
            """SELECT
              (SELECT count(*) FROM orders WHERE event_id=ANY(%s::uuid[])),
              (SELECT count(*) FROM orders WHERE event_id=ANY(%s::uuid[]) AND status='FULFILLED'),
              (SELECT count(*) FROM orders WHERE event_id=ANY(%s::uuid[]) AND status='EXPIRED'),
              (SELECT count(*) FROM orders WHERE event_id=ANY(%s::uuid[]) AND status='PENDING'),
              (SELECT count(*) FROM payment_attempts p JOIN orders o ON o.id=p.order_id
                WHERE o.event_id=ANY(%s::uuid[])),
              (SELECT count(*) FROM payment_attempts p JOIN orders o ON o.id=p.order_id
                WHERE o.event_id=ANY(%s::uuid[]) AND p.status='PENDING'),
              (SELECT count(*) FROM payment_attempts p JOIN orders o ON o.id=p.order_id
                WHERE o.event_id=ANY(%s::uuid[]) AND p.status='SUCCEEDED'),
              (SELECT count(*) FROM bookings WHERE event_id=ANY(%s::uuid[])),
              (SELECT count(*) FROM tickets t JOIN bookings b ON b.id=t.booking_id
                WHERE b.event_id=ANY(%s::uuid[])),
              (SELECT count(*) FROM payment_callbacks c JOIN payment_attempts p ON p.id=c.payment_id
                JOIN orders o ON o.id=p.order_id WHERE o.event_id=ANY(%s::uuid[])),
              (SELECT count(*) FROM payment_attempts p JOIN orders o ON o.id=p.order_id
                WHERE o.event_id=ANY(%s::uuid[]) AND p.deliveries<p.target_deliveries),
              (SELECT coalesce(sum(p.deliveries),0) FROM payment_attempts p
                JOIN orders o ON o.id=p.order_id WHERE o.event_id=ANY(%s::uuid[])),
              (SELECT coalesce(sum(p.target_deliveries),0) FROM payment_attempts p
                JOIN orders o ON o.id=p.order_id WHERE o.event_id=ANY(%s::uuid[])),
              (SELECT count(*) FROM (
                SELECT b.event_id,b.seat_id FROM bookings b WHERE b.event_id=ANY(%s::uuid[])
                GROUP BY b.event_id,b.seat_id HAVING count(*)>1) duplicate_seats),
              (SELECT count(*) FROM (
                SELECT b.order_id FROM bookings b WHERE b.event_id=ANY(%s::uuid[])
                GROUP BY b.order_id HAVING count(*)>1) multi_booking_orders),
              (SELECT count(*) FROM outbox_events WHERE published_at IS NULL),
              (SELECT count(*) FROM dead_letters)""",
            (show_ids,) * 15,
        ).fetchone()
    names = (
        "orders",
        "fulfilled_orders",
        "expired_orders",
        "pending_orders",
        "payment_attempts",
        "pending_payment_attempts",
        "succeeded_payments",
        "bookings",
        "tickets",
        "payment_callbacks",
        "incomplete_callback_deliveries",
        "callback_delivery_attempts",
        "callback_delivery_target",
        "duplicate_booked_seats",
        "multi_booking_orders",
        "unpublished_outbox",
        "dead_letters",
    )
    result = dict(zip(names, values, strict=True))
    result["expected"] = expected
    result["expected_paid"] = expected_paid
    result["expected_callback_deliveries_per_payment"] = callback_duplicates
    result["pass"] = (
        result["orders"] == expected
        and result["expired_orders"] == expected - expected_paid
        and result["pending_orders"] == 0
        and result["payment_attempts"] == expected_paid
        and result["pending_payment_attempts"] == 0
        and all(
            result[key] == expected_paid
            for key in ("fulfilled_orders", "succeeded_payments", "bookings", "tickets")
        )
        and result["payment_callbacks"] >= expected_paid
        and result["callback_delivery_attempts"]
        == result["callback_delivery_target"]
        == expected_paid * callback_duplicates
        and all(
            result[key] == 0
            for key in (
                "incomplete_callback_deliveries",
                "duplicate_booked_seats",
                "multi_booking_orders",
                "unpublished_outbox",
                "dead_letters",
            )
        )
    )
    return result


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--manifest", required=True, type=Path)
    parser.add_argument("--expected", required=True, type=int)
    parser.add_argument("--expected-paid", type=int)
    parser.add_argument("--callback-duplicates", type=int, default=3)
    parser.add_argument("--output", required=True, type=Path)
    args = parser.parse_args()
    if (
        not 1 <= args.expected <= 300000
        or args.expected_paid is not None
        and not 0 <= args.expected_paid <= args.expected
        or not 1 <= args.callback_duplicates <= 10
        or args.output.exists()
    ):
        parser.error("Expected 1-300000 journeys and a fresh output path")
    manifest = json.loads(args.manifest.read_text(encoding="utf-8"))
    if manifest.get("environment") != "development" or not manifest.get("show_ids"):
        parser.error("An isolated development fixture is required")
    url = os.getenv("TEST_DATABASE_URL")
    if not url:
        raise RuntimeError("TEST_DATABASE_URL is required")
    with psycopg.connect(url, autocommit=True) as conn:
        result = audit(
            conn, manifest["show_ids"], args.expected, args.expected_paid, args.callback_duplicates
        )
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(json.dumps(result, indent=2) + "\n", encoding="utf-8")
    print(json.dumps(result), flush=True)
    if not result["pass"]:
        raise SystemExit(1)


if __name__ == "__main__":
    main()

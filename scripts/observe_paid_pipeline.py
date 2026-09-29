"""Sample paid-ticket pipeline backlog and worker throughput during an isolated stage."""

import argparse
import json
import os
import re
import socket
import time
from datetime import UTC, datetime
from pathlib import Path
from urllib.request import urlopen

import psycopg

METRICS = {
    "simulator": ("simulate_one", "http://simulator:9101/metrics"),
    "consumer": ("consume_event", "http://consumer:9101/metrics"),
    "publisher": ("publish_batch", "http://publisher:9101/metrics"),
}


API_METRIC_NAMES = (
    "ticketing_http_requests_total",
    "ticketing_db_unavailable_total",
    "ticketing_db_pool_acquire_seconds_count",
    "ticketing_db_pool_acquiring",
    "ticketing_db_pool_in_use",
    "ticketing_db_pool_state",
)


def api_replicas(host="api", port=8000):
    return sorted({item[4][0] for item in socket.getaddrinfo(host, port, type=socket.SOCK_STREAM)})


def api_metrics(address):
    result = {}
    with urlopen(f"http://{address}:8000/metrics", timeout=2) as response:
        for line in response.read().decode("utf-8").splitlines():
            if not line or line.startswith("#") or " " not in line:
                continue
            metric, raw_value = line.rsplit(" ", 1)
            name = metric.split("{", 1)[0]
            if name not in API_METRIC_NAMES:
                continue
            labels = dict(re.findall(r'(\w+)="([^"]*)"', metric))
            value = float(raw_value)
            if name == "ticketing_http_requests_total" and labels.get("status") == "503":
                route = labels.get("route", "")
                if route.startswith(("/v1/orders", "/v1/payments")):
                    result[f"http_503:{route}:{labels.get('method', '')}"] = value
            elif name == "ticketing_db_unavailable_total":
                result[f"db_503:{labels.get('cause', '')}"] = value
            elif name == "ticketing_db_pool_acquire_seconds_count":
                result[f"pool_acquire:{labels.get('outcome', '')}"] = value
            elif name == "ticketing_db_pool_state":
                result[f"pool_state:{labels.get('state', '')}"] = value
            elif name == "ticketing_db_pool_acquiring":
                result["pool_acquiring"] = value
            elif name == "ticketing_db_pool_in_use":
                result["pool_in_use"] = value
    return result


def worker_counters(role, operation, url):
    result = {}
    with urlopen(url, timeout=2) as response:
        for line in response.read().decode("utf-8").splitlines():
            if not line or line.startswith("#") or " " not in line:
                continue
            name, value = line.rsplit(" ", 1)
            if name == f'ticketing_worker_operations_total{{operation="{operation}",outcome="ok"}}':
                result[f"{role}_calls"] = float(value)
            elif name == f'ticketing_worker_busy_seconds_total{{operation="{operation}"}}':
                result[f"{role}_busy_seconds"] = float(value)
            elif name == f'ticketing_worker_active{{operation="{operation}"}}':
                result[f"{role}_active"] = float(value)
    return result


def sample(conn, show_ids):
    row = conn.execute(
        """SELECT
          (SELECT count(*) FROM orders WHERE event_id=ANY(%s::uuid[]) AND status='PENDING'),
          (SELECT count(*) FROM orders WHERE event_id=ANY(%s::uuid[]) AND status='PAID'),
          (SELECT count(*) FROM orders WHERE event_id=ANY(%s::uuid[]) AND status='FULFILLED'),
          (SELECT count(*) FROM payment_attempts p JOIN orders o ON o.id=p.order_id
            WHERE o.event_id=ANY(%s::uuid[]) AND p.deliveries<p.target_deliveries),
          (SELECT count(*) FROM payment_attempts p JOIN orders o ON o.id=p.order_id
            WHERE o.event_id=ANY(%s::uuid[]) AND p.status='PENDING'),
          (SELECT coalesce(sum(p.deliveries),0) FROM payment_attempts p
            JOIN orders o ON o.id=p.order_id WHERE o.event_id=ANY(%s::uuid[])),
          (SELECT count(*) FROM tickets t JOIN bookings b ON b.id=t.booking_id
            WHERE b.event_id=ANY(%s::uuid[])),
          (SELECT count(*) FROM outbox_events WHERE published_at IS NULL),
          (SELECT count(*) FROM pg_stat_activity
            WHERE datname=current_database() AND wait_event_type='Lock')""",
        (show_ids,) * 7,
    ).fetchone()
    names = (
        "pending_orders",
        "paid_unfulfilled",
        "fulfilled_orders",
        "pending_callback_deliveries",
        "pending_payment_attempts",
        "delivered_callbacks",
        "issued_tickets",
        "unpublished_outbox",
        "db_lock_waiters",
    )
    return dict(zip(names, row, strict=True))


def main():
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--manifest", required=True, type=Path)
    p.add_argument("--output", required=True, type=Path)
    p.add_argument("--seconds", required=True, type=int)
    p.add_argument("--interval", type=float, default=1)
    a = p.parse_args()
    if not 1 <= a.seconds <= 400 or not 0.25 <= a.interval <= 10 or a.output.exists():
        p.error("Bounded duration, interval and fresh output required")
    manifest = json.loads(a.manifest.read_text(encoding="utf-8"))
    if manifest.get("environment") != "development" or not manifest.get("show_ids"):
        p.error("Isolated development fixture required")
    url = os.getenv("TEST_DATABASE_URL")
    if not url:
        raise RuntimeError("TEST_DATABASE_URL required")
    end = time.monotonic() + a.seconds
    with psycopg.connect(url, autocommit=True) as conn:
        conn.execute("SET statement_timeout = '3s'")
        with a.output.open("w", encoding="utf-8") as out:
            while time.monotonic() < end:
                started = time.monotonic()
                result = {"utc": datetime.now(UTC).isoformat()}
                try:
                    result.update(sample(conn, manifest["show_ids"]))
                except psycopg.Error as exc:
                    result["database_error"] = type(exc).__name__
                    conn.rollback()
                for role, (operation, address) in METRICS.items():
                    try:
                        result.update(worker_counters(role, operation, address))
                    except (OSError, ValueError) as exc:
                        result[f"{role}_metrics_error"] = type(exc).__name__
                try:
                    result["api_replicas"] = {address: api_metrics(address) for address in api_replicas()}
                except (OSError, ValueError) as exc:
                    result["api_metrics_error"] = type(exc).__name__
                out.write(json.dumps(result) + "\n")
                out.flush()
                time.sleep(max(0, a.interval - (time.monotonic() - started)))


if __name__ == "__main__":
    main()

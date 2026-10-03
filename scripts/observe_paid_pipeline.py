"""Sample paid-ticket pipeline backlog and worker throughput during an isolated stage."""

import argparse
import json
import os
import re
import socket
import time
from datetime import UTC, datetime
from pathlib import Path
from urllib.parse import urlsplit
from urllib.request import urlopen

import psycopg

METRICS = {
    "simulator": ("simulate_one", "http://simulator:9101/metrics"),
    "consumer": ("consume_event", "http://consumer:9101/metrics"),
    "publisher": ("publish_batch", "http://publisher:9101/metrics"),
}


API_METRIC_NAMES = (
    "ticketing_order_status_cache_total",
    "ticketing_http_requests_total",
    "ticketing_db_unavailable_total",
    "ticketing_db_pool_acquire_seconds_count",
    "ticketing_db_pool_acquiring",
    "ticketing_db_pool_in_use",
    "ticketing_db_pool_state",
    "ticketing_db_query_seconds_sum",
    "ticketing_db_query_seconds_count",
    "ticketing_db_commit_seconds_sum",
    "ticketing_db_commit_seconds_count",
    "ticketing_db_pool_acquire_seconds_sum",
    "ticketing_db_connection_hold_seconds_sum",
    "ticketing_db_connection_hold_seconds_count",
    "ticketing_event_loop_lag_seconds_sum",
    "ticketing_event_loop_lag_seconds_count",
    "ticketing_event_loop_lag_current_seconds",
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
            elif name == "ticketing_order_status_cache_total":
                result[f"order_cache:{labels.get('outcome', '')}"] = value
            elif name == "ticketing_db_unavailable_total":
                result[f"db_503:{labels.get('cause', '')}"] = value
            elif name == "ticketing_db_pool_acquire_seconds_count":
                outcome = labels.get("outcome", "")
                result[f"pool_acquire:{outcome}"] = value
                result[f"duration:db_pool_acquire:{outcome}:count"] = value
            elif name == "ticketing_db_pool_state":
                result[f"pool_state:{labels.get('state', '')}"] = value
            elif name == "ticketing_db_pool_acquiring":
                result["pool_acquiring"] = value
            elif name == "ticketing_db_pool_in_use":
                result["pool_in_use"] = value
            elif name == "ticketing_event_loop_lag_current_seconds":
                result["event_loop_lag_current"] = value
            elif name.endswith(("_sum", "_count")):
                duration = name.removeprefix("ticketing_").removesuffix("_seconds_sum").removesuffix("_seconds_count")
                suffix = "sum" if name.endswith("_sum") else "count"
                dimension = labels.get("command", labels.get("outcome", "all"))
                result[f"duration:{duration}:{dimension}:{suffix}"] = value
    return result


def consumer_phase_metric(name):
    match = re.fullmatch(r"ticketing_consumer_phase_seconds_(sum|count|bucket)\{(.*)\}", name)
    if not match:
        return None
    labels = dict(re.findall(r'(\w+)="([^"\\]*)"', match[2]))
    phase, partition, outcome = (labels.get(key) for key in ("phase", "partition", "outcome"))
    if phase not in {"poll", "partition", "commit", "rewind", "event_OrderPaid",
                     "event_SeatsChanged", "event_TicketsIssued", "event_RefundRequested", "event_other"}:
        return None
    if partition not in {"all", "other", *(str(i) for i in range(32))} or outcome not in {"ok", "error"}:
        return None
    suffix = match[1]
    if suffix == "bucket":
        bound = labels.get("le", "")
        try:
            if float(bound) <= 0:
                return None
        except ValueError:
            return None
        suffix += ":" + bound
    return f"{phase}:{partition}:{outcome}:{suffix}"


def worker_counters(role, operation, url):
    parsed = urlsplit(url)
    addresses = api_replicas(parsed.hostname, parsed.port or 80)
    result = {f"{role}_replicas": len(addresses)}
    if role == "consumer":
        result["consumer_batch_failures"] = {}
    for address in addresses:
        with urlopen(f"http://{address}:{parsed.port or 80}{parsed.path}", timeout=2) as response:
            for line in response.read().decode("utf-8").splitlines():
                if not line or line.startswith("#") or " " not in line:
                    continue
                name, value = line.rsplit(" ", 1)
                if name == f'ticketing_worker_operations_total{{operation="{operation}",outcome="ok"}}':
                    key = f"{role}_calls"
                elif name == f'ticketing_worker_busy_seconds_total{{operation="{operation}"}}':
                    key = f"{role}_busy_seconds"
                elif name == f'ticketing_worker_active{{operation="{operation}"}}':
                    key = f"{role}_active"
                else:
                    if role == "consumer":
                        match = re.fullmatch(r'ticketing_consumer_batch_failures_total\{sqlstate="(40P01|40001|55P03|57014|08006|53300|other)"\}', name)
                        if match:
                            failures = result.setdefault("consumer_batch_failures", {})
                            failures[match[1]] = failures.get(match[1], 0) + float(value)
                    phase_key = consumer_phase_metric(name) if role == "consumer" else None
                    if phase_key:
                        phases = result.setdefault("consumer_phases", {})
                        phases[phase_key] = phases.get(phase_key, 0) + float(value)
                    continue
                result[key] = result.get(key, 0) + float(value)
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

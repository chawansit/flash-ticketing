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
from psycopg.conninfo import conninfo_to_dict, make_conninfo
from psycopg.rows import dict_row

METRICS = {
    "simulator": ("simulate_one", "http://simulator:9101/metrics"),
    "consumer": ("consume_event", "http://consumer:9101/metrics"),
    "publisher": ("publish_batch", "http://publisher:9101/metrics"),
    "writer": ("reservation_write", "http://reservation-writer:9101/metrics"),
    "maintenance": ("refresh_batch", "http://maintenance:9101/metrics"),
    "reconciler": ("reconcile_batch", "http://reconciler:9101/metrics"),
}


API_METRIC_NAMES = (
    "process_cpu_seconds_total",
    "process_resident_memory_bytes",
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
    with urlopen(f"http://{address}:8000/metrics", timeout=2) as response:
        return parse_api_metrics(response.read().decode("utf-8"))


def parse_api_metrics(payload):
    result = {}
    for line in payload.splitlines():
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
            if route.startswith(("/v1/orders", "/v1/payments", "/v1/webhooks/payments")):
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
        elif name in {"process_cpu_seconds_total", "process_resident_memory_bytes"}:
            result[name] = value
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


def simulator_phase_metric(name):
    match = re.fullmatch(r"ticketing_simulator_phase_seconds_(sum|count|bucket)\{(.*)\}", name)
    if match:
        labels = dict(re.findall(r'(\w+)="([^"\\]*)"', match[2]))
        phase, outcome = labels.get("phase"), labels.get("outcome")
        if phase not in {"claim", "delivery", "ack", "other"} or outcome not in {"ok", "error"}:
            return None
        suffix = match[1]
        bound = labels.get("le")
    else:
        match = re.fullmatch(r"ticketing_simulator_due_to_claim_seconds_(sum|count|bucket)(?:\{(.*)\})?", name)
        if not match:
            return None
        phase, outcome, suffix = "due_to_claim", "ok", match[1]
        labels = dict(re.findall(r'(\w+)="([^"\\]*)"', match[2] or ""))
        bound = labels.get("le")
    if suffix == "bucket":
        try:
            if not float(bound) > 0:
                return None
        except (ValueError, TypeError):
            return None
        suffix += ":" + bound
    return f"{phase}:{outcome}:{suffix}"


def writer_phase_metric(name):
    match = re.fullmatch(r"ticketing_reservation_persistence_phase_seconds_(sum|count|bucket)\{(.*)\}", name)
    if match:
        labels = dict(re.findall(r'(\w+)="([^"\\]*)"', match[2]))
        phase, outcome, suffix = labels.get("phase"), labels.get("outcome"), match[1]
        if phase not in {"postgres_batch", "redis_mark_durable", "redis_compensate", "redis_acknowledge"}:
            return None
        if outcome not in {"ok", "error"} or set(labels) - {"phase", "outcome", "le"}:
            return None
    else:
        match = re.fullmatch(r"ticketing_reservation_command_age_seconds_(sum|count|bucket)(?:\{(.*)\})?", name)
        if not match:
            return None
        phase, outcome, suffix = "command_age", "ok", match[1]
        labels = dict(re.findall(r'(\w+)="([^"\\]*)"', match[2] or ""))
        if set(labels) - {"le"}:
            return None
    if suffix == "bucket":
        bound = labels.get("le")
        try:
            if not float(bound) > 0:
                return None
        except (ValueError, TypeError):
            return None
        suffix += ":" + bound
    return f"{phase}:{outcome}:{suffix}"


def host_cpu_ticks():
    values = Path("/proc/stat").read_text().splitlines()[0].split()
    if values[0] != "cpu" or len(values) < 9:
        raise ValueError("Missing aggregate host CPU ticks")
    return [int(value) for value in values[1:9]]


PG_POOL_FIELDS = ("cl_active", "cl_waiting", "sv_active", "sv_idle", "sv_used", "sv_login")
PG_STATS_FIELDS = ("total_xact_count", "total_query_count", "total_xact_time",
                   "total_query_time", "total_wait_time", "total_server_assignment_count")


def pgbouncer_view(conn, target_database):
    pools = [row for row in conn.execute("SHOW POOLS").fetchall()
             if row["database"] == target_database]
    stats = [row for row in conn.execute("SHOW STATS").fetchall()
             if row["database"] == target_database]
    if not pools or not stats:
        raise ValueError("Application PgBouncer pool/statistics missing")
    return {
        "pools": {key: sum(float(row.get(key) or 0) for row in pools)
                  for key in PG_POOL_FIELDS},
        "maxwait_seconds": max(float(row.get("maxwait") or 0)
                              + float(row.get("maxwait_us") or 0) / 1000000 for row in pools),
        "stats": {key: sum(float(row.get(key) or 0) for row in stats)
                  for key in PG_STATS_FIELDS},
    }


def worker_counters(role, operation, url):
    parsed = urlsplit(url)
    addresses = api_replicas(parsed.hostname, parsed.port or 80)
    result = {f"{role}_replicas": len(addresses), f"{role}_db_replicas": {}}
    if role == "consumer":
        result["consumer_batch_failures"] = {}
    for address in addresses:
        with urlopen(f"http://{address}:{parsed.port or 80}{parsed.path}", timeout=2) as response:
            payload = response.read().decode("utf-8")
            result[f"{role}_db_replicas"][address] = parse_api_metrics(payload)
            for line in payload.splitlines():
                if not line or line.startswith("#") or " " not in line:
                    continue
                name, value = line.rsplit(" ", 1)
                if name == f'ticketing_worker_operations_total{{operation="{operation}",outcome="ok"}}':
                    key = f"{role}_calls"
                elif name == f'ticketing_worker_busy_seconds_total{{operation="{operation}"}}':
                    key = f"{role}_busy_seconds"
                elif name == f'ticketing_worker_active{{operation="{operation}"}}':
                    key = f"{role}_active"
                elif role == "simulator" and name == "ticketing_simulator_batch_barrier_seconds_total":
                    key = "simulator_batch_barrier_seconds"
                else:
                    if role == "writer":
                        phase_key = writer_phase_metric(name)
                        if phase_key:
                            phases = result.setdefault("writer_phases", {})
                            phases[phase_key] = phases.get(phase_key, 0) + float(value)
                    if role == "simulator":
                        phase_key = simulator_phase_metric(name)
                        if phase_key:
                            phases = result.setdefault("simulator_phases", {})
                            phases[phase_key] = phases.get(phase_key, 0) + float(value)
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



MAX_PAID_STAGE_SECONDS = 300
COMPLETION_GRACE_SECONDS = 120
OBSERVER_LAUNCH_ALLOWANCE_SECONDS = 60
MAX_PAID_OBSERVER_SECONDS = MAX_PAID_STAGE_SECONDS + COMPLETION_GRACE_SECONDS + OBSERVER_LAUNCH_ALLOWANCE_SECONDS


def paid_observer_seconds(stage_seconds):
    if not 1 <= stage_seconds <= MAX_PAID_STAGE_SECONDS:
        raise ValueError("Paid stage duration must be between 1 and 300 seconds")
    return stage_seconds + COMPLETION_GRACE_SECONDS + OBSERVER_LAUNCH_ALLOWANCE_SECONDS


def pipeline_startup_view(row, expected_consumers):
    apis = row.get("api_replicas", {})
    errors = sorted(key for key in row if key.endswith("_error"))
    view = {"utc": row.get("utc"), "api_count": len(apis) if isinstance(apis, dict) else 0,
            "consumer_count": row.get("consumer_replicas"), "sample_errors": errors}
    view["pass"] = (isinstance(view["utc"], str) and "issued_tickets" in row and not errors
                    and view["api_count"] == 4 and view["consumer_count"] == expected_consumers
                    and row.get("simulator_replicas") == 1 and row.get("publisher_replicas") == 1)
    if row.get("extended_diagnostics"):
        view["diagnostics_ready"] = (
            row.get("writer_replicas") == 3 and row.get("maintenance_replicas") == 1
            and row.get("reconciler_replicas") == 1
            and len(row.get("host_cpu_ticks", [])) == 8 and bool(row.get("pgbouncer"))
            and all(
                len(row.get(f"{role}_db_replicas", {})) == count
                and all("process_cpu_seconds_total" in metric
                        for metric in row[f"{role}_db_replicas"].values())
                for role, count in {"writer": 3, "maintenance": 1, "reconciler": 1,
                                    "simulator": 1, "publisher": 1,
                                    "consumer": expected_consumers}.items()
            )
        )
        view["pass"] = view["pass"] and view["diagnostics_ready"]
    return view


def kafka_startup_view(row, expected_consumers):
    view = {"utc": row.get("utc"), "members": row.get("members"),
            "sample_error": "error_type" in row}
    view["pass"] = (isinstance(view["utc"], str) and not view["sample_error"]
                    and view["members"] == expected_consumers and "total_lag" in row)
    return view


def main():
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--manifest", required=True, type=Path)
    p.add_argument("--output", required=True, type=Path)
    p.add_argument("--seconds", required=True, type=int)
    p.add_argument("--interval", type=float, default=1)
    a = p.parse_args()
    if not 1 <= a.seconds <= MAX_PAID_OBSERVER_SECONDS or not 0.25 <= a.interval <= 10 or a.output.exists():
        p.error("Bounded duration, interval and fresh output required")
    manifest = json.loads(a.manifest.read_text(encoding="utf-8"))
    if manifest.get("environment") != "development" or not manifest.get("show_ids"):
        p.error("Isolated development fixture required")
    url = os.getenv("TEST_DATABASE_URL")
    if not url:
        raise RuntimeError("TEST_DATABASE_URL required")
    end = time.monotonic() + a.seconds
    options = conninfo_to_dict(os.environ["DATABASE_URL"])
    target_database = options.get("dbname", "ticketing")
    options.update(dbname="pgbouncer", connect_timeout="2")
    with (
        psycopg.connect(url, autocommit=True) as conn,
        psycopg.connect(make_conninfo(**options), autocommit=True,
                       prepare_threshold=None, row_factory=dict_row) as admin,
    ):
        conn.execute("SET statement_timeout = '3s'")
        with a.output.open("w", encoding="utf-8") as out:
            while time.monotonic() < end:
                started = time.monotonic()
                result = {"utc": datetime.now(UTC).isoformat(), "extended_diagnostics": True}
                try:
                    result["host_cpu_ticks"] = host_cpu_ticks()
                    result["pgbouncer"] = pgbouncer_view(admin, target_database)
                except (OSError, ValueError, psycopg.Error) as exc:
                    result["resource_metrics_error"] = type(exc).__name__
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

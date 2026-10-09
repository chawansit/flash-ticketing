"""Capture sub-second PgBouncer pool and wait statistics without logging credentials."""

from __future__ import annotations

import argparse
import json
import math
import os
import re
import time
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import psycopg
from psycopg.conninfo import conninfo_to_dict, make_conninfo
from psycopg.rows import dict_row

POOL_FIELDS = (
    "cl_active", "cl_waiting", "cl_active_cancel_req", "cl_waiting_cancel_req",
    "sv_active", "sv_active_cancel", "sv_being_canceled", "sv_idle",
    "sv_used", "sv_tested", "sv_login",
)
STATS_COUNTERS = (
    "total_xact_count", "total_query_count", "total_received", "total_sent",
    "total_xact_time", "total_query_time", "total_wait_time",
    "total_server_assignment_count",
)
STATS_GAUGES = (
    "avg_xact_count", "avg_query_count", "avg_recv", "avg_sent",
    "avg_xact_time", "avg_query_time", "avg_wait_time",
    "avg_server_assignment_count",
)


def percentile(values: list[float], fraction: float) -> float | None:
    if not values:
        return None
    return sorted(values)[math.ceil(len(values) * fraction) - 1]


def numeric(value: Any) -> float:
    try:
        return float(value)
    except (TypeError, ValueError):
        return 0.0


def admin_connection(source: str) -> tuple[str, str]:
    options = conninfo_to_dict(source)
    target_database = options.get("dbname") or "ticketing"
    options["dbname"] = "pgbouncer"
    options["connect_timeout"] = "2"
    return make_conninfo(**options), target_database


def summarize_pools(rows: list[dict[str, Any]], target_database: str) -> dict[str, float]:
    selected = [row for row in rows if row.get("database") == target_database]
    result = {field: sum(numeric(row.get(field)) for row in selected) for field in POOL_FIELDS}
    result["maxwait_seconds"] = max(
        (
            numeric(row.get("maxwait")) + numeric(row.get("maxwait_us")) / 1_000_000
            for row in selected
        ),
        default=0.0,
    )
    result["pools"] = float(len(selected))
    return result


def summarize_stats(rows: list[dict[str, Any]], target_database: str) -> dict[str, float]:
    selected = [row for row in rows if row.get("database") == target_database]
    result = {field: sum(numeric(row.get(field)) for row in selected) for field in STATS_COUNTERS}
    result.update({
        field: max((numeric(row.get(field)) for row in selected), default=0.0)
        for field in STATS_GAUGES
    })
    return result


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--seconds", type=float, required=True)
    parser.add_argument("--interval", type=float, default=0.2)
    parser.add_argument("--database-url-env", default="DATABASE_URL")
    args = parser.parse_args()
    if not 1 <= args.seconds <= 7200:
        parser.error("Use 1..7200 seconds")
    if not 0.1 <= args.interval <= 10:
        parser.error("Use a 0.1..10 second interval")
    if args.output.exists():
        parser.error("Use a fresh output file")
    source = os.environ.get(args.database_url_env)
    if not source:
        parser.error(f"{args.database_url_env} is required")

    admin_dsn, target_database = admin_connection(source)
    args.output.parent.mkdir(parents=True, exist_ok=True)
    start = time.monotonic()
    due = start
    end = start + args.seconds
    samples = 0
    errors = 0
    wake_lags: list[float] = []
    first_stats: dict[str, float] | None = None
    last_stats: dict[str, float] | None = None
    pool_peaks: dict[str, float] = {}

    with (
        psycopg.connect(
            admin_dsn, autocommit=True, row_factory=dict_row, prepare_threshold=None
        ) as conn,
        args.output.open("x", encoding="utf-8") as output,
    ):
        output.write(json.dumps({
            "type": "metadata",
            "utc": datetime.now(UTC).isoformat(),
            "seconds": args.seconds,
            "interval_seconds": args.interval,
            "target_database": target_database,
        }, separators=(",", ":")) + "\n")
        output.flush()
        while time.monotonic() < end:
            now = time.monotonic()
            wake_lag_ms = max(0.0, (now - due) * 1000)
            wake_lags.append(wake_lag_ms)
            row: dict[str, Any] = {
                "type": "sample",
                "utc": datetime.now(UTC).isoformat(),
                "elapsed_seconds": round(now - start, 6),
                "observer_wake_lag_ms": round(wake_lag_ms, 3),
            }
            try:
                with conn.cursor() as cursor:
                    pools = summarize_pools(
                        cursor.execute("SHOW POOLS").fetchall(), target_database
                    )
                    stats = summarize_stats(
                        cursor.execute("SHOW STATS").fetchall(), target_database
                    )
                row["pools"] = pools
                row["stats"] = stats
                first_stats = first_stats or stats
                last_stats = stats
                for key, value in pools.items():
                    pool_peaks[key] = max(pool_peaks.get(key, 0.0), value)
            except Exception as exc:  # noqa: BLE001
                row["error"] = re.sub(
                    r"[^A-Za-z0-9_.-]", "_", type(exc).__name__
                )
                errors += 1
            output.write(json.dumps(row, separators=(",", ":")) + "\n")
            output.flush()
            samples += 1
            due += args.interval
            time.sleep(max(0.0, due - time.monotonic()))

        counter_deltas = {
            key: max(0.0, last_stats.get(key, 0.0) - first_stats.get(key, 0.0))
            for key in STATS_COUNTERS
        } if first_stats and last_stats else {}
        summary = {
            "type": "summary",
            "utc": datetime.now(UTC).isoformat(),
            "samples": samples,
            "errors": errors,
            "observer_wake_lag_p95_ms": percentile(wake_lags, 0.95),
            "observer_wake_lag_p99_ms": percentile(wake_lags, 0.99),
            "observer_wake_lag_max_ms": max(wake_lags, default=None),
            "pool_peaks": pool_peaks,
            "counter_deltas": counter_deltas,
            "last_stats": last_stats,
        }
        output.write(json.dumps(summary, separators=(",", ":")) + "\n")
        output.flush()
    print(json.dumps(summary))
    return 0 if errors == 0 else 1


if __name__ == "__main__":
    raise SystemExit(main())

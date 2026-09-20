#!/usr/bin/env python3
"""Capture bounded, read-only, subsecond RDS wait and WAL samples."""

import argparse
import json
import os
import time
from collections import Counter
from datetime import UTC, datetime
from pathlib import Path

import psycopg
from psycopg.conninfo import make_conninfo

ACTIVITY_SQL = """
SELECT coalesce(state, 'unknown'), coalesce(wait_event_type, 'CPU'),
       coalesce(wait_event, 'none'), count(*)
FROM pg_stat_activity
WHERE datname = current_database() AND pid <> pg_backend_pid()
GROUP BY 1, 2, 3
"""
WAL_SQL = """
SELECT wal_write, wal_sync, wal_write_time, wal_sync_time
FROM pg_stat_wal
"""


def activity_counts(rows) -> dict:
    states, waits, events = Counter(), Counter(), Counter()
    for state, wait_type, wait_event, count in rows:
        states[str(state)] += int(count)
        waits[str(wait_type)] += int(count)
        if wait_type not in {"CPU", "Client"}:
            events[str(wait_event)] += int(count)
    return {
        "states": dict(states),
        "wait_types": dict(waits),
        "wait_events": dict(events),
    }


def sample_connection(conn) -> dict:
    started = time.perf_counter()
    activity = conn.execute(ACTIVITY_SQL).fetchall()
    wal = conn.execute(WAL_SQL).fetchone()
    return {
        "activity": activity_counts(activity),
        "wal": {
            "writes": wal[0],
            "syncs": wal[1],
            "write_ms": wal[2],
            "sync_ms": wal[3],
        },
        "query_ms": round((time.perf_counter() - started) * 1000, 3),
    }


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--seconds", type=float, required=True)
    parser.add_argument("--interval", type=float, default=0.2)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    if not 1 <= args.seconds <= 900 or not 0.1 <= args.interval <= 2:
        parser.error("Use 1..900 seconds and a 0.1..2 second interval")
    if args.output.exists():
        parser.error("Use a fresh output file")
    dsn = os.getenv("RDS_DATABASE_URL")
    if not dsn:
        parser.error("Set RDS_DATABASE_URL to the direct RDS endpoint")
    args.output.parent.mkdir(parents=True, exist_ok=True)
    try:
        conn = psycopg.connect(
            make_conninfo(dsn, application_name="flash-ticketing-rds-wait-observer",
                          connect_timeout=5),
            autocommit=True,
        )
    except psycopg.Error as exc:
        print(json.dumps({"error_type": type(exc).__name__}))
        return 1

    errors = 0
    samples = 0
    start = time.monotonic()
    due = start
    with conn, args.output.open("x", encoding="utf-8") as output:
        wal_timing_enabled = (
            conn.execute("SELECT current_setting('track_wal_io_timing')").fetchone()[0] == "on"
        )
        output.write(json.dumps({
            "type": "metadata", "utc": datetime.now(UTC).isoformat(),
            "interval_seconds": args.interval, "seconds": args.seconds,
            "target": "direct_rds_redacted",
            "wal_timing_enabled": wal_timing_enabled,
        }) + "\n")
        while time.monotonic() - start < args.seconds:
            row = {
                "type": "sample",
                "utc": datetime.now(UTC).isoformat(),
                "elapsed_seconds": round(time.monotonic() - start, 6),
                "observer_wake_lag_ms": round(max(0, time.monotonic() - due) * 1000, 3),
            }
            try:
                row.update(sample_connection(conn))
            except psycopg.Error as exc:
                errors += 1
                row["error_type"] = type(exc).__name__
                try:
                    conn.rollback()
                except psycopg.Error:
                    pass
            output.write(json.dumps(row, separators=(",", ":"), default=str) + "\n")
            output.flush()
            samples += 1
            due += args.interval
            time.sleep(max(0, due - time.monotonic()))
        output.write(json.dumps({
            "type": "summary", "samples": samples, "query_errors": errors,
            "utc": datetime.now(UTC).isoformat(),
        }) + "\n")
    print(json.dumps({"samples": samples, "query_errors": errors}))
    return 0 if samples and not errors else 1


if __name__ == "__main__":
    raise SystemExit(main())

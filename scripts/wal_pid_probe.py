#!/usr/bin/env python3
"""Bounded direct-RDS write probe with same-backend wait attribution."""

import argparse
import json
import math
import os
import queue
import threading
import time
from concurrent.futures import ThreadPoolExecutor
from datetime import UTC, datetime
from pathlib import Path
from uuid import uuid4

import psycopg
from psycopg import sql
from psycopg.conninfo import make_conninfo

PAYLOAD = os.urandom(8192)


def utc():
    return datetime.now(UTC).isoformat()


def percentile(values, fraction):
    data = sorted(values)
    return round(data[max(0, math.ceil(len(data) * fraction) - 1)], 3) if data else None


def insert_once(pool, statement, item_id, due):
    started = time.perf_counter()
    row = {"schedule_lag_ms": max(0, (started - due) * 1000), "success": False}
    conn = None
    try:
        conn = pool.get(timeout=5)
        row["connection_wait_ms"] = (time.perf_counter() - started) * 1000
        row["backend_pid"] = conn.info.backend_pid
        sql_started = time.perf_counter()
        conn.execute(statement, (item_id, PAYLOAD))
        row["sql_ms"] = (time.perf_counter() - sql_started) * 1000
        commit_started = time.perf_counter()
        row["commit_started_utc"] = utc()
        conn.commit()
        row["commit_ms"] = (time.perf_counter() - commit_started) * 1000
        row["commit_ended_utc"] = utc()
        row["success"] = True
    except (psycopg.Error, queue.Empty) as exc:
        row["error_type"] = type(exc).__name__
        if conn is not None:
            try:
                conn.rollback()
            except psycopg.Error:
                pass
    finally:
        if conn is not None:
            pool.put(conn)
    return row


def observe(dsn, pids, interval, path, stop, status):
    samples = errors = 0
    max_query_ms = max_lag_ms = 0.0
    try:
        with (
            psycopg.connect(
                make_conninfo(dsn, application_name="flash-wal-pid-observer"), autocommit=True
            ) as conn,
            path.open("x", encoding="utf-8") as output,
        ):
            conn.execute("SET statement_timeout = 1000")
            due = time.perf_counter()
            while not stop.is_set():
                max_lag_ms = max(max_lag_ms, max(0, time.perf_counter() - due) * 1000)
                started = time.perf_counter()
                try:
                    states = conn.execute(
                        "SELECT pid, state, wait_event_type, wait_event "
                        "FROM pg_stat_activity WHERE pid = ANY(%s::integer[])",
                        (pids,),
                    ).fetchall()
                    max_query_ms = max(max_query_ms, (time.perf_counter() - started) * 1000)
                    output.write(
                        json.dumps(
                            {
                                "utc": utc(),
                                "states": [
                                    {"pid": pid, "state": state, "wait_type": kind, "wait_event": event}
                                    for pid, state, kind, event in states
                                ],
                            },
                            separators=(",", ":"),
                        )
                        + "\n"
                    )
                    samples += 1
                except psycopg.Error:
                    errors += 1
                    conn.rollback()
                due += interval
                stop.wait(max(0, due - time.perf_counter()))
    except (psycopg.Error, OSError) as exc:
        status["error_type"] = type(exc).__name__
    status.update(
        {
            "samples": samples,
            "query_errors": errors,
            "max_query_ms": round(max_query_ms, 3),
            "max_wake_lag_ms": round(max_lag_ms, 3),
        }
    )


def correlate(rows, samples):
    by_pid = {}
    for sample in samples:
        stamp = datetime.fromisoformat(sample["utc"])
        for state in sample["states"]:
            by_pid.setdefault(state["pid"], []).append((stamp, state["wait_event"]))
    counts = {}
    slow = [row for row in rows if row.get("success") and row["commit_ms"] > 100]
    for row in slow:
        start = datetime.fromisoformat(row["commit_started_utc"])
        end = datetime.fromisoformat(row["commit_ended_utc"])
        events = {
            event for stamp, event in by_pid.get(row["backend_pid"], []) if start <= stamp <= end and event
        }
        label = ",".join(sorted(events)) if events else "not_sampled_or_not_waiting"
        counts[label] = counts.get(label, 0) + 1
    return {"slow_commits": len(slow), "same_pid_wait_events": counts}


def run(args, dsn, result):
    table = f"capacity_wal_pid_{uuid4().hex[:12]}"
    identifier = sql.Identifier("public", table)
    create = sql.SQL("CREATE TABLE {} (id BIGINT PRIMARY KEY, payload BYTEA NOT NULL)").format(identifier)
    insert = sql.SQL("INSERT INTO {} (id, payload) VALUES (%s, %s)").format(identifier)
    drop = sql.SQL("DROP TABLE IF EXISTS {}").format(identifier)
    result["recovery_table"] = table
    args.output.write_text(
        json.dumps({"status": "running", "recovery_table": table}) + "\n", encoding="utf-8"
    )
    pool = queue.Queue()
    created = False
    try:
        with psycopg.connect(dsn, autocommit=True) as conn:
            conn.execute(create)
        created = True
        for _ in range(args.workers):
            pool.put(
                psycopg.connect(make_conninfo(dsn, application_name="flash-wal-pid-probe", connect_timeout=5))
            )
        pids = [conn.info.backend_pid for conn in list(pool.queue)]
        stop = threading.Event()
        observer = {}
        thread = threading.Thread(
            target=observe,
            args=(dsn, pids, args.interval_ms / 1000, args.wait_output, stop, observer),
            daemon=True,
        )
        thread.start()
        started = time.perf_counter()
        try:
            with ThreadPoolExecutor(max_workers=args.workers) as executor:
                futures = []
                for item_id in range(args.rate * args.seconds):
                    due = started + item_id / args.rate
                    time.sleep(max(0, due - time.perf_counter()))
                    futures.append(executor.submit(insert_once, pool, insert, item_id, due))
                rows = [future.result() for future in futures]
        finally:
            stop.set()
            thread.join(timeout=10)
            if thread.is_alive():
                observer["error_type"] = "ObserverDidNotStop"
        with args.raw_output.open("x", encoding="utf-8") as output:
            for row in rows:
                output.write(json.dumps(row, separators=(",", ":")) + "\n")
        samples = [json.loads(line) for line in args.wait_output.read_text(encoding="utf-8").splitlines()]
        commits = [row["commit_ms"] for row in rows if row.get("success")]
        result.update(
            {
                "completed": len(commits),
                "errors": len(rows) - len(commits),
                "commit_p95_ms": percentile(commits, 0.95),
                "commit_p99_ms": percentile(commits, 0.99),
                "commit_max_ms": round(max(commits), 3) if commits else None,
                "max_schedule_lag_ms": round(max(row["schedule_lag_ms"] for row in rows), 3),
                "max_connection_wait_ms": round(max(row.get("connection_wait_ms", 0) for row in rows), 3),
                "observer": observer,
                "correlation": correlate(rows, samples),
            }
        )
        expected = args.seconds * 1000 / args.interval_ms
        if (
            result["errors"]
            or result["completed"] != args.rate * args.seconds
            or result["max_schedule_lag_ms"] >= 100
            or observer.get("error_type")
            or observer.get("query_errors")
            or observer.get("samples", 0) < expected * 0.8
        ):
            result["error_type"] = "StrictGateFailed"
    except (psycopg.Error, OSError, RuntimeError, ValueError) as exc:
        result["error_type"] = type(exc).__name__
    finally:
        while not pool.empty():
            pool.get_nowait().close()
        if created:
            try:
                with psycopg.connect(dsn, autocommit=True) as conn:
                    conn.execute(drop)
                result["cleanup_pass"] = True
                result.pop("recovery_table", None)
            except psycopg.Error as exc:
                result["cleanup_error_type"] = type(exc).__name__
        else:
            result["cleanup_pass"] = True
            result.pop("recovery_table", None)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--rate", type=int, default=40)
    parser.add_argument("--seconds", type=int, default=120)
    parser.add_argument("--workers", type=int, default=16)
    parser.add_argument("--interval-ms", type=int, default=20)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--raw-output", type=Path, required=True)
    parser.add_argument("--wait-output", type=Path, required=True)
    parser.add_argument("--preflight", action="store_true")
    args = parser.parse_args()
    paths = (args.output, args.raw_output, args.wait_output)
    if not 1 <= args.rate <= 50 or not 30 <= args.seconds <= 180:
        parser.error("Use 1..50 transactions/s and 30..180 seconds")
    if not 1 <= args.workers <= 16 or not 20 <= args.interval_ms <= 100:
        parser.error("Use 1..16 workers and 20..100 ms sampling")
    if len(set(paths)) != 3 or any(path.exists() for path in paths):
        parser.error("Use three distinct fresh output paths")
    original_dsn = os.getenv("TEST_DATABASE_URL")
    if not original_dsn:
        parser.error("TEST_DATABASE_URL is required")
    os.environ.pop("PGSSLROOTCERT", None)
    dsn = make_conninfo(original_dsn, sslmode="require")
    with psycopg.connect(dsn, autocommit=True) as conn:
        settings = conn.execute(
            "SELECT current_setting('synchronous_commit'), "
            "current_setting('fsync'), current_setting('transaction_read_only')"
        ).fetchone()
    passed = settings == ("on", "on", "off")
    if args.preflight:
        print(json.dumps({"durability_preflight_pass": passed, "direct_tls": True}))
        return 0 if passed else 1
    if not passed:
        raise RuntimeError("Durability preflight failed")
    for path in paths:
        path.parent.mkdir(parents=True, exist_ok=True)
    result = {
        "started_utc": utc(),
        "rate": args.rate,
        "seconds": args.seconds,
        "workers": args.workers,
        "interval_ms": args.interval_ms,
        "cleanup_pass": False,
    }
    run(args, dsn, result)
    result["ended_utc"] = utc()
    args.output.write_text(json.dumps(result, indent=2) + "\n", encoding="utf-8")
    print(
        json.dumps(
            {
                "completed": result.get("completed"),
                "slow_commits": result.get("correlation", {}).get("slow_commits"),
                "cleanup_pass": result["cleanup_pass"],
                "error_type": result.get("error_type"),
            }
        )
    )
    return 0 if result["cleanup_pass"] and not result.get("error_type") else 1


if __name__ == "__main__":
    raise SystemExit(main())

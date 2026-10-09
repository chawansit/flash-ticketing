#!/usr/bin/env python3
"""Bounded, isolated direct-RDS versus PgBouncer commit probe."""

import argparse
import json
import math
import os
import queue
import time
from concurrent.futures import ThreadPoolExecutor
from datetime import UTC, datetime
from pathlib import Path
from uuid import uuid4

import psycopg
from psycopg import sql
from psycopg.conninfo import make_conninfo

BLOCK_ORDER = ("direct", "pooled", "pooled", "direct")
PAYLOAD = os.urandom(8192)  # Incompressible write volume near the observed hold path.


def utc_now() -> str:
    return datetime.now(UTC).isoformat()


def percentile(values: list[float], fraction: float) -> float | None:
    if not values:
        return None
    ordered = sorted(values)
    return round(ordered[max(0, math.ceil(fraction * len(ordered)) - 1)], 3)


def summarize_block(path: str, rate: int, seconds: int, rows: list[dict],
                    start_utc: str, end_utc: str) -> dict:
    ok = [row for row in rows if row.get("success")]
    commits = [row["commit_ms"] for row in ok]
    sql_times = [row["sql_ms"] for row in ok]
    return {
        "path": path,
        "target_rate": rate,
        "seconds": seconds,
        "started_utc": start_utc,
        "ended_utc": end_utc,
        "scheduled": len(rows),
        "completed": len(ok),
        "errors": len(rows) - len(ok),
        "error_types": sorted({row["error_type"] for row in rows if not row.get("success")}),
        "commit_p50_ms": percentile(commits, 0.50),
        "commit_p95_ms": percentile(commits, 0.95),
        "commit_p99_ms": percentile(commits, 0.99),
        "commit_max_ms": round(max(commits), 3) if commits else None,
        "commit_over_100_ms": sum(value > 100 for value in commits),
        "sql_p95_ms": percentile(sql_times, 0.95),
        "max_schedule_lag_ms": round(max((row["schedule_lag_ms"] for row in rows),
                                          default=0), 3),
        "max_connection_wait_ms": round(max((row["connection_wait_ms"] for row in rows),
                                            default=0), 3),
        "matched_rate": max((row["schedule_lag_ms"] for row in rows), default=0) < 100,
    }


def insert_once(connections: queue.Queue, statement, item_id: int,
                scheduled_at: float) -> dict:
    started = time.perf_counter()
    row = {
        "completed_utc": None,
        "schedule_lag_ms": max(0.0, (started - scheduled_at) * 1000),
        "success": False,
    }
    conn = None
    try:
        conn = connections.get(timeout=5)
        row["connection_wait_ms"] = (time.perf_counter() - started) * 1000
        sql_start = time.perf_counter()
        conn.execute(statement, (item_id, PAYLOAD))
        row["sql_ms"] = (time.perf_counter() - sql_start) * 1000
        commit_start = time.perf_counter()
        conn.commit()
        row["commit_ms"] = (time.perf_counter() - commit_start) * 1000
        row["success"] = True
    except (psycopg.Error, queue.Empty) as exc:  # Never print a DSN.
        row["error_type"] = type(exc).__name__
        if conn is not None:
            try:
                conn.rollback()
            except psycopg.Error:
                pass
    finally:
        if conn is not None:
            connections.put(conn)
    row["completed_utc"] = utc_now()
    return row


def check_path(conn) -> tuple:
    return conn.execute(
        "SELECT current_user, current_database(), "
        "current_setting('synchronous_commit'), current_setting('fsync'), "
        "current_setting('transaction_read_only')"
    ).fetchone()


def run_block(dsn: str, path: str, table: str, block: int, rate: int,
              seconds: int, workers: int, raw_output) -> dict:
    connections = queue.Queue()
    try:
        for _ in range(workers):
            connections.put(psycopg.connect(make_conninfo(
                dsn, application_name=f"flash-wal-probe-{path}", connect_timeout=5
            )))
        statement = sql.SQL("INSERT INTO {} (id, payload) VALUES (%s, %s)").format(
            sql.Identifier("public", table)
        )
        start_utc = utc_now()
        start = time.perf_counter()
        futures = []
        with ThreadPoolExecutor(max_workers=workers) as executor:
            for number in range(rate * seconds):
                due = start + number / rate
                time.sleep(max(0, due - time.perf_counter()))
                item_id = block * rate * seconds + number
                futures.append(executor.submit(insert_once, connections, statement,
                                               item_id, due))
            rows = [future.result() for future in futures]
        end_utc = utc_now()
        for row in rows:
            raw_output.write(json.dumps({"path": path, "block": block, **row},
                                        separators=(",", ":")) + "\n")
        raw_output.flush()
        return summarize_block(path, rate, seconds, rows, start_utc, end_utc)
    finally:
        while not connections.empty():
            connections.get_nowait().close()


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--rate", type=int, default=40)
    parser.add_argument("--block-seconds", type=int, default=90)
    parser.add_argument("--workers", type=int, default=16)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--raw-output", type=Path, required=True)
    parser.add_argument("--preflight", action="store_true")
    args = parser.parse_args()
    if not 1 <= args.rate <= 50 or not 30 <= args.block_seconds <= 180:
        parser.error("Use 1..50 RPS and 30..180 seconds per block")
    if not 1 <= args.workers <= 16 or args.output.exists() or args.raw_output.exists():
        parser.error("Use 1..16 workers and fresh output paths")
    pooled_dsn = os.getenv("DATABASE_URL")
    direct_dsn = os.getenv("TEST_DATABASE_URL")
    if not pooled_dsn or not direct_dsn:
        parser.error("Both DATABASE_URL and TEST_DATABASE_URL are required")
    # Only this disposable diagnostic direct connection bypasses the untrusted
    # RDS CA file already documented in the Huawei runbook.
    os.environ.pop("PGSSLROOTCERT", None)
    direct_dsn = make_conninfo(direct_dsn, sslmode="require")
    if args.preflight:
        with psycopg.connect(direct_dsn, autocommit=True) as direct, \
             psycopg.connect(pooled_dsn, autocommit=True) as pooled:
            direct_info, pooled_info = check_path(direct), check_path(pooled)
        matched = direct_info == pooled_info and direct_info[2:] == ("on", "on", "off")
        print(json.dumps({"matched_authority_and_durability": matched,
                          "direct_tls": True, "pooled_path_reachable": True}))
        return 0 if matched else 1
    table = f"capacity_wal_probe_{uuid4().hex[:12]}"
    statement = sql.SQL("CREATE TABLE {} (id BIGINT PRIMARY KEY, payload BYTEA NOT NULL)").format(
        sql.Identifier("public", table)
    )
    drop_statement = sql.SQL("DROP TABLE IF EXISTS {}").format(sql.Identifier("public", table))
    result = {"started_utc": utc_now(), "block_order": list(BLOCK_ORDER),
              "rate": args.rate, "block_seconds": args.block_seconds,
              "workers": args.workers, "blocks": [], "cleanup_pass": False}
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.raw_output.parent.mkdir(parents=True, exist_ok=True)
    created = False
    args.output.write_text(json.dumps({"status": "running", "cleanup_table": table,
                                       "started_utc": result["started_utc"]}) + "\n",
                           encoding="utf-8")
    try:
        with psycopg.connect(direct_dsn, autocommit=True) as direct, \
             psycopg.connect(pooled_dsn, autocommit=True) as pooled:
            direct_info, pooled_info = check_path(direct), check_path(pooled)
            if direct_info != pooled_info or direct_info[2:] != ("on", "on", "off"):
                raise RuntimeError("Database authority or durability settings differ")
            direct.execute(statement)
            created = True
        with args.raw_output.open("x", encoding="utf-8") as raw:
            for block, path in enumerate(BLOCK_ORDER):
                dsn = direct_dsn if path == "direct" else pooled_dsn
                summary = run_block(dsn, path, table, block, args.rate,
                                    args.block_seconds, args.workers, raw)
                result["blocks"].append(summary)
                print(json.dumps(summary), flush=True)
                if (summary["errors"] or not summary["matched_rate"]
                        or summary["completed"] != args.rate * args.block_seconds):
                    raise RuntimeError("Probe block failed strict completion gate")
    except (psycopg.Error, RuntimeError, OSError, ValueError) as exc:
        result["error_type"] = type(exc).__name__
    finally:
        if created:
            try:
                with psycopg.connect(direct_dsn, autocommit=True) as cleanup:
                    cleanup.execute(drop_statement)
                result["cleanup_pass"] = True
            except psycopg.Error as exc:
                result["cleanup_error_type"] = type(exc).__name__
                result["cleanup_table"] = table
        else:
            result["cleanup_pass"] = True
        result["ended_utc"] = utc_now()
        args.output.write_text(json.dumps(result, indent=2) + "\n", encoding="utf-8")
    print(json.dumps({"complete": len(result["blocks"]) == len(BLOCK_ORDER),
                      "cleanup_pass": result["cleanup_pass"],
                      "error_type": result.get("error_type")}))
    return 0 if (len(result["blocks"]) == len(BLOCK_ORDER)
                 and result["cleanup_pass"] and not result.get("error_type")) else 1


if __name__ == "__main__":
    raise SystemExit(main())
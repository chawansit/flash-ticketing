"""Capture a credential-free PostgreSQL baseline and representative query plans."""

import argparse
import json
import os
from datetime import UTC, datetime
from pathlib import Path
from time import perf_counter
from uuid import UUID

import psycopg

ZERO_UUID = UUID("00000000-0000-0000-0000-000000000000")


def explain(conn, name, query, params):
    started = perf_counter()
    payload = conn.execute(
        "EXPLAIN (ANALYZE, BUFFERS, WAL, SETTINGS, FORMAT JSON) " + query, params
    ).fetchone()[0]
    return {
        "name": name,
        "client_ms": (perf_counter() - started) * 1000,
        "plan": payload[0],
    }


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--statement-timeout-ms", type=int, default=5000)
    args = parser.parse_args()
    if args.output.exists():
        parser.error("Use a fresh output path")
    if args.statement_timeout_ms < 100:
        parser.error("Statement timeout must be at least 100 ms")
    dsn = os.getenv("PROFILE_DATABASE_URL")
    if not dsn:
        parser.error("Set PROFILE_DATABASE_URL; credentials are never written to evidence")

    result = {"utc": datetime.now(UTC).isoformat(), "target": "redacted", "failures": []}
    try:
        with psycopg.connect(dsn, autocommit=True) as conn:
            server = conn.execute(
                """SELECT current_setting('server_version'),current_setting('TimeZone'),
                current_setting('server_encoding'),current_setting('max_connections')::integer,
                pg_database_size(current_database())"""
            ).fetchone()
            result["server"] = {
                "version": server[0],
                "timezone": server[1],
                "encoding": server[2],
                "max_connections": server[3],
                "database_bytes": server[4],
            }
            columns = [row[0] for row in conn.execute(
                """SELECT column_name FROM information_schema.columns
                WHERE table_schema='pg_catalog' AND table_name='pg_stat_database'
                ORDER BY ordinal_position"""
            ).fetchall()]
            row = conn.execute(
                "SELECT * FROM pg_stat_database WHERE datname=current_database()"
            ).fetchone()
            result["database_stats"] = dict(zip(columns, row))
            result["wal_stats"] = conn.execute("SELECT row_to_json(s) FROM pg_stat_wal s").fetchone()[0]
            result["bgwriter_stats"] = conn.execute(
                "SELECT row_to_json(s) FROM pg_stat_bgwriter s"
            ).fetchone()[0]
            if conn.execute("SELECT to_regclass('pg_catalog.pg_stat_checkpointer')").fetchone()[0]:
                result["checkpointer_stats"] = conn.execute(
                    "SELECT row_to_json(s) FROM pg_stat_checkpointer s"
                ).fetchone()[0]
            result["connection_states"] = [
                {"state": row[0], "wait_event_type": row[1], "count": row[2]}
                for row in conn.execute(
                    """SELECT coalesce(state,'unknown'),coalesce(wait_event_type,'none'),count(*)
                    FROM pg_stat_activity WHERE datname=current_database()
                    GROUP BY state,wait_event_type ORDER BY 1,2"""
                ).fetchall()
            ]
            result["locks"] = [
                {"mode": row[0], "granted": row[1], "count": row[2]}
                for row in conn.execute(
                    """SELECT mode,granted,count(*) FROM pg_locks
                    WHERE database=(SELECT oid FROM pg_database WHERE datname=current_database())
                    GROUP BY mode,granted ORDER BY mode,granted"""
                ).fetchall()
            ]
            result["table_stats"] = [
                dict(zip(("table", "seq_scan", "idx_scan", "inserted", "updated", "deleted", "live", "dead"), row))
                for row in conn.execute(
                    """SELECT relname,seq_scan,idx_scan,n_tup_ins,n_tup_upd,n_tup_del,n_live_tup,n_dead_tup
                    FROM pg_stat_user_tables ORDER BY relname"""
                ).fetchall()
            ]
            result["index_stats"] = [
                dict(zip(("table", "index", "scans", "tuples_read", "tuples_fetched"), row))
                for row in conn.execute(
                    """SELECT relname,indexrelname,idx_scan,idx_tup_read,idx_tup_fetch
                    FROM pg_stat_user_indexes ORDER BY relname,indexrelname"""
                ).fetchall()
            ]

            fixture = conn.execute(
                """SELECT e.id,s.seat_id FROM events e JOIN LATERAL
                (SELECT seat_id FROM event_seats WHERE event_id=e.id ORDER BY seat_id LIMIT 1) s ON true
                ORDER BY e.id LIMIT 1"""
            ).fetchone()
            if not fixture:
                result["failures"].append("fixture_required")
            else:
                event_id, seat_id = fixture
                owner = conn.execute(
                    "SELECT hold_id FROM event_seats WHERE hold_id IS NOT NULL LIMIT 1"
                ).fetchone()
                hold_id = owner[0] if owner else ZERO_UUID
                conn.execute("BEGIN")
                conn.execute(f"SET LOCAL statement_timeout = '{args.statement_timeout_ms}ms'")
                try:
                    result["plans"] = [
                        explain(conn, "event_lookup", "SELECT * FROM events WHERE id=%s", (event_id,)),
                        explain(
                            conn,
                            "seat_lock",
                            """SELECT * FROM event_seats WHERE event_id=%s
                            AND seat_id=ANY(%s) ORDER BY seat_id FOR UPDATE NOWAIT""",
                            (event_id, [seat_id]),
                        ),
                        explain(
                            conn,
                            "active_hold_owner",
                            """SELECT seat_id FROM event_seats WHERE hold_id=%s
                            ORDER BY seat_id FOR UPDATE NOWAIT""",
                            (hold_id,),
                        ),
                        explain(
                            conn,
                            "expiry_candidate",
                            """SELECT o.id FROM orders o JOIN holds h ON h.id=o.hold_id
                            WHERE h.status='ACTIVE' AND h.expires_at<=clock_timestamp()
                            ORDER BY h.expires_at LIMIT 1 FOR UPDATE OF o SKIP LOCKED""",
                            (),
                        ),
                        explain(
                            conn,
                            "outbox_pending",
                            """SELECT id FROM outbox_events WHERE published_at IS NULL
                            AND (lease_until IS NULL OR lease_until<clock_timestamp())
                            ORDER BY occurred_at LIMIT 32 FOR UPDATE SKIP LOCKED""",
                            (),
                        ),
                        explain(
                            conn,
                            "reconciliation_due",
                            """SELECT event_id FROM event_reconciliation
                            WHERE next_due_at<=clock_timestamp()
                            ORDER BY next_due_at,event_id LIMIT 8 FOR UPDATE SKIP LOCKED""",
                            (),
                        ),
                    ]
                finally:
                    conn.execute("ROLLBACK")
    except (OSError, ValueError, KeyError, psycopg.Error) as exc:
        result["failures"].append("profile_exception")
        result["error_type"] = type(exc).__name__

    result["pass"] = not result["failures"]
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(json.dumps(result, indent=2, default=str) + "\n", encoding="utf-8")
    print(json.dumps({"pass": result["pass"], "failures": result["failures"]}))
    return 0 if result["pass"] else 1


if __name__ == "__main__":
    raise SystemExit(main())

"""Read-only readiness and schema checks for a managed PostgreSQL target."""

import argparse
import hashlib
import json
import os
import socket
from datetime import UTC, datetime
from pathlib import Path
from statistics import median
from time import perf_counter

import psycopg
from psycopg.conninfo import conninfo_to_dict, make_conninfo

REQUIRED_TABLES = {
    "bookings",
    "consumer_inbox",
    "dead_letters",
    "event_reconciliation",
    "event_seats",
    "events",
    "holds",
    "idempotency_records",
    "order_items",
    "orders",
    "outbox_events",
    "payment_attempts",
    "payment_callbacks",
    "refund_requests",
    "schema_migrations",
    "seat_refresh_requests",
    "tickets",
}
REQUIRED_INDEXES = {
    "event_reconciliation_due",
    "event_seats_active_hold",
    "events_pkey",
    "events_sale_window",
    "event_seats_pkey",
    "holds_expiry",
    "idempotency_records_pkey",
    "outbox_pending",
    "seat_refresh_pending",
}


def percentile(values: list[float], fraction: float) -> float | None:
    if not values:
        return None
    ordered = sorted(values)
    return ordered[min(len(ordered) - 1, int((len(ordered) - 1) * fraction + 0.999999))]


def migration_checksums(directory: Path) -> dict[str, str]:
    return {
        path.name: hashlib.sha256(path.read_text(encoding="utf-8").encode()).hexdigest()
        for path in sorted(directory.glob("*.sql"))
    }


def validate_server(
    snapshot: dict, minimum_version: int, minimum_connections: int, require_tls: bool = True
) -> list[str]:
    failures = []
    if snapshot["server_version_num"] < minimum_version * 10000:
        failures.append("server_version")
    if snapshot["in_recovery"]:
        failures.append("primary_required")
    if snapshot["transaction_read_only"] != "off":
        failures.append("read_write_required")
    if snapshot["encoding"] != "UTF8":
        failures.append("utf8_required")
    if snapshot["max_connections"] < minimum_connections:
        failures.append("connection_budget")
    if require_tls and not snapshot["ssl"]:
        failures.append("tls_required")
    return failures


def validate_schema(actual_migrations, expected_migrations, tables, indexes) -> list[str]:
    failures = []
    if actual_migrations != expected_migrations:
        failures.append("migration_checksums")
    if not REQUIRED_TABLES.issubset(tables):
        failures.append("required_tables")
    if not REQUIRED_INDEXES.issubset(indexes):
        failures.append("required_indexes")
    return failures


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--attempts", type=int, default=5)
    parser.add_argument("--transaction-attempts", type=int, default=10)
    parser.add_argument("--minimum-version", type=int, default=15)
    parser.add_argument("--minimum-connections", type=int, default=40)
    parser.add_argument("--require-schema", action="store_true")
    parser.add_argument("--allow-plaintext", action="store_true", help=argparse.SUPPRESS)
    parser.add_argument("--migrations", type=Path, default=Path("migrations"))
    args = parser.parse_args()
    if args.output.exists():
        parser.error("Use a fresh output path")
    if args.attempts < 1 or args.transaction_attempts < 1:
        parser.error("Attempt counts must be positive")

    dsn = os.getenv("RDS_DATABASE_URL")
    if not dsn:
        parser.error("Set RDS_DATABASE_URL; credentials are read only from the environment")
    values = conninfo_to_dict(dsn)
    host = values.get("host")
    port = int(values.get("port", 5432))
    if not host:
        parser.error("RDS_DATABASE_URL must include a host")

    started = datetime.now(UTC)
    result = {
        "utc": started.isoformat(),
        "target": "redacted",
        "attempts": args.attempts,
        "transaction_attempts": args.transaction_attempts,
        "require_schema": args.require_schema,
        "failures": [],
    }
    try:
        dns_started = perf_counter()
        addresses = socket.getaddrinfo(host, port, type=socket.SOCK_STREAM)
        result["dns_ms"] = (perf_counter() - dns_started) * 1000
        result["resolved_address_count"] = len({row[4][0] for row in addresses})

        tcp_ms = []
        for _ in range(args.attempts):
            attempt = perf_counter()
            with socket.create_connection((host, port), timeout=5):
                tcp_ms.append((perf_counter() - attempt) * 1000)
        result["tcp_ms"] = {
            "minimum": min(tcp_ms),
            "median": median(tcp_ms),
            "p95": percentile(tcp_ms, 0.95),
            "maximum": max(tcp_ms),
        }

        connect_ms = []
        snapshot = None
        safe_dsn = make_conninfo(dsn, application_name="flash-ticketing-rds-preflight")
        for _ in range(args.attempts):
            attempt = perf_counter()
            with psycopg.connect(safe_dsn, autocommit=True) as conn:
                connect_ms.append((perf_counter() - attempt) * 1000)
                if snapshot is None:
                    row = conn.execute(
                        """SELECT current_setting('server_version_num')::integer,
                        current_setting('server_version'),current_setting('TimeZone'),
                        current_setting('server_encoding'),current_setting('max_connections')::integer,
                        pg_is_in_recovery(),current_setting('transaction_read_only')"""
                    ).fetchone()
                    ssl = conn.execute(
                        "SELECT ssl,version,cipher FROM pg_stat_ssl WHERE pid=pg_backend_pid()"
                    ).fetchone()
                    snapshot = {
                        "server_version_num": row[0],
                        "server_version": row[1],
                        "timezone": row[2],
                        "encoding": row[3],
                        "max_connections": row[4],
                        "in_recovery": row[5],
                        "transaction_read_only": row[6],
                        "ssl": bool(ssl and ssl[0]),
                        "tls_version": ssl[1] if ssl else None,
                        "tls_cipher": ssl[2] if ssl else None,
                    }
        result["connect_ms"] = {
            "minimum": min(connect_ms),
            "median": median(connect_ms),
            "p95": percentile(connect_ms, 0.95),
            "maximum": max(connect_ms),
        }
        result["server"] = snapshot
        result["failures"].extend(
            validate_server(snapshot, args.minimum_version, args.minimum_connections, not args.allow_plaintext)
        )

        transaction_ms = []
        with psycopg.connect(safe_dsn, autocommit=True) as conn:
            for _ in range(args.transaction_attempts):
                attempt = perf_counter()
                conn.execute("BEGIN READ ONLY")
                conn.execute("SELECT 1").fetchone()
                conn.execute("COMMIT")
                transaction_ms.append((perf_counter() - attempt) * 1000)
            result["transaction_ms"] = {
                "minimum": min(transaction_ms),
                "median": median(transaction_ms),
                "p95": percentile(transaction_ms, 0.95),
                "maximum": max(transaction_ms),
            }

            if args.require_schema:
                expected = migration_checksums(args.migrations)
                actual = dict(conn.execute("SELECT name,checksum FROM schema_migrations ORDER BY name").fetchall())
                tables = {
                    row[0]
                    for row in conn.execute(
                        "SELECT tablename FROM pg_tables WHERE schemaname=current_schema()"
                    ).fetchall()
                }
                indexes = {
                    row[0]
                    for row in conn.execute(
                        "SELECT indexname FROM pg_indexes WHERE schemaname=current_schema()"
                    ).fetchall()
                }
                result["schema"] = {
                    "migration_count": len(actual),
                    "expected_migration_count": len(expected),
                    "required_tables_found": len(REQUIRED_TABLES & tables),
                    "required_tables_expected": len(REQUIRED_TABLES),
                    "required_indexes_found": len(REQUIRED_INDEXES & indexes),
                    "required_indexes_expected": len(REQUIRED_INDEXES),
                }
                result["failures"].extend(validate_schema(actual, expected, tables, indexes))
    except (OSError, ValueError, KeyError, psycopg.Error) as exc:
        result["failures"].append("preflight_exception")
        result["error_type"] = type(exc).__name__

    result["pass"] = not result["failures"]
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(json.dumps(result, indent=2) + "\n", encoding="utf-8")
    print(json.dumps(result))
    return 0 if result["pass"] else 1


if __name__ == "__main__":
    raise SystemExit(main())

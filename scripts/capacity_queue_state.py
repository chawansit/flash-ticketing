#!/usr/bin/env python3
"""Read-only queue and fixture state used by capacity-stage preflight."""

import argparse
import json
import os
from pathlib import Path

import psycopg


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--manifest", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    if args.output.exists():
        parser.error("Use a fresh output path")
    shows = json.loads(args.manifest.read_text(encoding="utf-8"))["show_ids"]
    database_url = os.getenv("TEST_DATABASE_URL") or os.getenv("STAGE_DATABASE_URL")
    if not database_url:
        raise RuntimeError("TEST_DATABASE_URL or STAGE_DATABASE_URL is required")
    with psycopg.connect(database_url) as conn:
        conn.execute("SET TRANSACTION READ ONLY")
        conn.execute("SET LOCAL statement_timeout = '20s'")
        row = conn.execute(
            """SELECT
            (SELECT count(*) FROM outbox_events WHERE published_at IS NULL),
            (SELECT count(*) FROM seat_refresh_requests WHERE generation>completed_generation),
            (SELECT count(*) FROM dead_letters),
            (SELECT count(*) FROM holds WHERE event_id=ANY(%s::uuid[]) AND status='ACTIVE'),
            (SELECT count(*) FROM holds WHERE event_id=ANY(%s::uuid[]) AND status='ACTIVE'
              AND expires_at<clock_timestamp()),
            (SELECT count(*) FROM orders WHERE event_id=ANY(%s::uuid[]) AND status='PENDING')""",
            (shows, shows, shows),
        ).fetchone()
    names = [
        "unpublished_outbox",
        "pending_refresh",
        "dead_letters",
        "active_fixture_holds",
        "overdue_fixture_holds",
        "pending_fixture_orders",
    ]
    result = dict(zip(names, row, strict=True))
    result["pass"] = all(value == 0 for value in row)
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(json.dumps(result, indent=2) + "\n", encoding="utf-8")
    print(json.dumps(result), flush=True)
    if not result["pass"]:
        raise SystemExit(1)


if __name__ == "__main__":
    main()

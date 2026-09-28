#!/usr/bin/env python3
"""Read-only queue and fixture state used by capacity-stage preflight."""

import argparse
import json
import os
from pathlib import Path

import psycopg
from redis import Redis
from redis.exceptions import ResponseError


def reservation_queue_state(redis_client, batch_size=256) -> tuple[int, int]:
    entries = pending = 0
    cursor = 0
    while True:
        cursor, streams = redis_client.scan(
            cursor=cursor, match="reservation-stream:*", count=1000
        )
        for start in range(0, len(streams), batch_size):
            batch = streams[start:start + batch_size]
            pipeline = redis_client.pipeline(transaction=False)
            for stream in batch:
                pipeline.xlen(stream)
                pipeline.xpending(stream, "reservation-writers")
            results = pipeline.execute(raise_on_error=False)
            for index in range(0, len(results), 2):
                length, group = results[index:index + 2]
                if isinstance(length, ResponseError):
                    raise length
                entries += length
                if isinstance(group, ResponseError):
                    if "NOGROUP" not in str(group):
                        raise group
                else:
                    pending += group["pending"]
        if cursor == 0:
            return entries, pending


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
    redis_url = os.getenv("TEST_REDIS_URL") or os.getenv("REDIS_URL")
    if not redis_url:
        raise RuntimeError("TEST_REDIS_URL or REDIS_URL is required")
    stream_entries, stream_pending = reservation_queue_state(
        Redis.from_url(redis_url, decode_responses=True)
    )
    result["reservation_stream_entries"] = stream_entries
    result["reservation_stream_pending"] = stream_pending
    result["pass"] = all(value == 0 for value in result.values())
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(json.dumps(result, indent=2) + "\n", encoding="utf-8")
    print(json.dumps(result), flush=True)
    if not result["pass"]:
        raise SystemExit(1)


if __name__ == "__main__":
    main()

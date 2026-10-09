#!/usr/bin/env python3
"""Create a fresh, isolated development fixture for distributed capacity tests."""

import argparse
import json
import os
from datetime import UTC, datetime, timedelta
from pathlib import Path
from uuid import uuid4

import psycopg
from paid_fixture_layout import LAYOUTS, fixture_layout

from ticketing.config import Settings
from ticketing.infrastructure.cache import RedisSeats
from ticketing.infrastructure.postgres import Postgres
from ticketing.workers import full_snapshot_batch


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--shows", type=int, default=800)
    parser.add_argument("--fixture-layout", choices=LAYOUTS, default="distributed")
    parser.add_argument("--seats", type=int, default=300)
    parser.add_argument("--sale-hours", type=int, default=6)
    return parser.parse_args()


def validate(args: argparse.Namespace, settings: Settings) -> None:
    if settings.environment != "development":
        raise RuntimeError("Capacity fixtures are development-only")
    if args.output.exists():
        raise ValueError("Use a fresh output path")
    if not 1 <= args.shows <= 2000:
        raise ValueError("Shows must be between 1 and 2000")
    fixture_layout(getattr(args, "fixture_layout", "distributed"), args.shows, args.seats)
    if not 1 <= args.sale_hours <= 24:
        raise ValueError("Sale hours must be between 1 and 24")


def main() -> None:
    args = parse_args()
    settings = Settings()
    validate(args, settings)
    database_url = os.getenv("TEST_DATABASE_URL") or os.getenv("STAGE_DATABASE_URL")
    redis_url = os.getenv("TEST_REDIS_URL") or os.getenv("REDIS_URL")
    if not database_url or not redis_url:
        raise RuntimeError("TEST_DATABASE_URL/STAGE_DATABASE_URL and TEST_REDIS_URL/REDIS_URL are required")

    fixture_id = str(uuid4())
    event_ids = [uuid4() for _ in range(args.shows)]
    sale_starts = datetime.now(UTC) - timedelta(minutes=5)
    sale_ends = datetime.now(UTC) + timedelta(hours=args.sale_hours)
    with psycopg.connect(database_url) as conn:
        conn.execute("SET LOCAL statement_timeout = 0")
        with conn.cursor().copy(
            "COPY events(id,title,currency,sale_starts,sale_ends) FROM STDIN"
        ) as copy:
            for index, event_id in enumerate(event_ids):
                copy.write_row(
                    (event_id, f"Capacity fixture {fixture_id} show {index}", "THB", sale_starts, sale_ends)
                )
        with conn.cursor().copy(
            "COPY event_seats(event_id,seat_id,price) FROM STDIN"
        ) as copy:
            for event_id in event_ids:
                for seat in range(args.seats):
                    copy.write_row((event_id, f"S{seat}", 100))

    db = Postgres(database_url)
    cache = RedisSeats(redis_url)
    try:
        for offset in range(0, len(event_ids), 16):
            batch = event_ids[offset : offset + 16]
            completed, errors = full_snapshot_batch(db, cache, batch)
            if errors or completed != set(batch):
                raise RuntimeError("Failed to pre-warm the complete capacity fixture") from (
                    errors[0] if errors else None
                )
    finally:
        db.close()
        cache.redis.close()

    result = {
        "schema_version": 1,
        "environment": "development",
        "fixture_id": fixture_id,
        "created_at": datetime.now(UTC).isoformat(),
        "sale_ends": sale_ends.isoformat(),
        "shows": args.shows,
        "seats_per_show": args.seats,
        "fixture_layout": args.fixture_layout,
        "show_ids": [str(event_id) for event_id in event_ids],
    }
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(json.dumps(result, indent=2) + "\n", encoding="utf-8")
    print(json.dumps({key: result[key] for key in result if key != "show_ids"}), flush=True)


if __name__ == "__main__":
    main()

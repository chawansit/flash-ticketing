#!/usr/bin/env python3
"""Pre-warm a private capacity manifest with bounded PostgreSQL and Redis batches."""

import argparse
import json
import os
import time
from datetime import UTC, datetime
from pathlib import Path

from ticketing.config import Settings
from ticketing.infrastructure.cache import RedisSeats
from ticketing.infrastructure.postgres import Postgres
from ticketing.workers import full_snapshot_batch


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--manifest", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--batch-size", type=int, default=16)
    args = parser.parse_args()
    if args.output.exists() or not 1 <= args.batch_size <= 64:
        parser.error("Use a fresh output and a batch size between 1 and 64")

    settings = Settings()
    manifest = json.loads(args.manifest.read_text(encoding="utf-8"))
    if settings.environment != "development" or manifest.get("environment") != "development":
        raise RuntimeError("Capacity pre-warm is development-only")
    event_ids = manifest.get("show_ids")
    if not isinstance(event_ids, list) or not event_ids:
        raise ValueError("Manifest must contain show_ids")
    database_url = os.getenv("STAGE_DATABASE_URL") or os.getenv("TEST_DATABASE_URL")
    redis_url = os.getenv("TEST_REDIS_URL") or os.getenv("REDIS_URL")
    if not database_url or not redis_url:
        raise RuntimeError("Database and Redis URLs are required")

    started = time.perf_counter()
    db, cache = Postgres(database_url), RedisSeats(redis_url)
    completed = 0
    try:
        for offset in range(0, len(event_ids), args.batch_size):
            batch = event_ids[offset : offset + args.batch_size]
            projected, errors = full_snapshot_batch(db, cache, batch)
            if errors or len(projected) != len(batch):
                raise RuntimeError("Capacity pre-warm batch failed") from (
                    errors[0] if errors else None
                )
            completed += len(projected)
    finally:
        db.close()
        cache.redis.close()

    result = {
        "utc": datetime.now(UTC).isoformat(),
        "shows": len(event_ids),
        "completed": completed,
        "batch_size": args.batch_size,
        "elapsed_seconds": round(time.perf_counter() - started, 6),
        "pass": completed == len(event_ids),
    }
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(json.dumps(result, indent=2) + "\n", encoding="utf-8")
    print(json.dumps(result), flush=True)


if __name__ == "__main__":
    main()

"""Isolated-schema adapter benchmark; does not estimate end-to-end capacity."""

import json
import math
import os
from pathlib import Path
from time import perf_counter
from uuid import uuid4

import psycopg
from psycopg import sql
from psycopg.conninfo import make_conninfo
from redis import Redis
from redis.exceptions import RedisError

from ticketing.config import Settings
from ticketing.infrastructure.cache import RedisSeats
from ticketing.infrastructure.postgres import Postgres
from ticketing.workers import changed_snapshot, snapshot


def main():
    if Settings().environment != "development":
        raise RuntimeError("Development benchmark only")
    dsn = os.environ["TEST_DATABASE_URL"]
    cache = RedisSeats(os.environ["TEST_REDIS_URL"])
    schema = "projection_probe_" + uuid4().hex
    with psycopg.connect(dsn) as conn:
        conn.execute(sql.SQL("CREATE SCHEMA {}").format(sql.Identifier(schema)))
    db = Postgres(make_conninfo(dsn, options=f"-c search_path={schema}"), maximum=2)
    events, results = [], []
    try:
        with db.transaction() as conn:
            for migration in sorted(Path("migrations").glob("*.sql")):
                conn.execute(migration.read_text(encoding="utf-8-sig"))
        for count in (3000, 10000, 50000):
            event = uuid4()
            events.append(event)
            with db.transaction() as conn:
                conn.execute(
                    "INSERT INTO events VALUES (%s,'Projection benchmark','THB',clock_timestamp(),clock_timestamp()+interval '1 day')",
                    (event,),
                )
                conn.execute(
                    "INSERT INTO event_seats(event_id,seat_id,price) SELECT %s,'S'||i,100 FROM generate_series(0,%s) i",
                    (event, count - 1),
                )
            try:
                snapshot(db, cache, event)
            except RedisError as exc:
                results.append({"inventory": count, "initial_snapshot_error": type(exc).__name__})
                continue
            durations = {"full": [], "patch": []}
            for repeat in range(20):
                for mode in ["full", "patch"] if repeat % 2 else ["patch", "full"]:
                    with db.transaction() as conn:
                        conn.execute(
                            "UPDATE event_seats SET version=version+1 WHERE event_id=%s AND seat_id='S0'",
                            (event,),
                        )
                    start = perf_counter()
                    try:
                        if mode == "full":
                            snapshot(db, cache, event)
                        else:
                            changed_snapshot(db, cache, event, ["S0"])
                    except RedisError as exc:
                        results.append({"inventory": count, "mode": mode, "error": type(exc).__name__})
                        continue
                    durations[mode].append((perf_counter() - start) * 1000)
            assert cache.read(event)["version"] == 40
            results.append(
                {
                    "inventory": count,
                    "iterations_per_mode": 20,
                    "milliseconds": durations,
                    "p95_ms": {
                        mode: sorted(values)[max(0, math.ceil(len(values) * 0.95) - 1)] if values else None
                        for mode, values in durations.items()
                    },
                }
            )
        print(
            json.dumps(
                {
                    "results": results,
                    "note": "Direct local PostgreSQL and Redis, isolated schema, alternating modes, one changed seat, 20 observations per mode; not API capacity.",
                },
                indent=2,
            )
        )
    finally:
        try:
            # Cleanup has its own generous timeout; measurements retain the app's 100ms timeout.
            with Redis.from_url(os.environ["TEST_REDIS_URL"], socket_timeout=5) as cleaner:
                for event in events:
                    cleaner.delete(cache.key(event))
        finally:
            cache.redis.close()
            db.close()
            with psycopg.connect(dsn) as conn:
                conn.execute(sql.SQL("DROP SCHEMA {} CASCADE").format(sql.Identifier(schema)))


if __name__ == "__main__":
    main()

"""Sample an isolated cloud benchmark's backend; no load or data mutations."""

import argparse
import json
import os
import time
from datetime import UTC, datetime
from pathlib import Path

import psycopg

from ticketing.infrastructure.cache import RedisSeats

p = argparse.ArgumentParser(description=__doc__)
p.add_argument("--fixtures", type=Path, required=True)
p.add_argument("--output", type=Path, required=True)
p.add_argument("--seconds", type=int, default=600)
a = p.parse_args()
shows = json.loads(a.fixtures.read_text())["show_ids"]
cache = RedisSeats(os.environ["REDIS_URL"])
samples = []
end = time.monotonic() + a.seconds
with psycopg.connect(os.environ["TEST_DATABASE_URL"], autocommit=True) as conn:
    while time.monotonic() < end:
        try:
            with cache.redis.pipeline(transaction=False) as pipe:
                for show in shows:
                    pipe.ttl(cache.key(show))
                ttls = pipe.execute()
            overdue = conn.execute(
                "SELECT count(*),coalesce(max(extract(epoch FROM clock_timestamp()-expires_at)),0) FROM holds WHERE event_id=ANY(%s::uuid[]) AND status='ACTIVE' AND expires_at<clock_timestamp()",
                (shows,),
            ).fetchone()
            age = conn.execute(
                "SELECT coalesce(max(extract(epoch FROM clock_timestamp()-last_reconciled_at)),0) FROM event_reconciliation WHERE event_id=ANY(%s::uuid[])",
                (shows,),
            ).fetchone()[0]
            activity = conn.execute(
                "SELECT count(*),count(*) FILTER (WHERE wait_event_type='Lock'),count(*) FILTER (WHERE state='active') FROM pg_stat_activity WHERE datname=current_database()"
            ).fetchone()
            sample = {
                "utc": datetime.now(UTC).isoformat(),
                "missing_maps": ttls.count(-2),
                "maps_without_ttl": ttls.count(-1),
                "minimum_ttl": min(ttls),
                "overdue_active_holds": overdue[0],
                "oldest_overdue_seconds": float(overdue[1]),
                "reconciliation_age_seconds": float(age),
                "database_connections": activity[0],
                "lock_waiters": activity[1],
                "active_connections": activity[2],
                "redis_used_bytes": cache.redis.info("memory")["used_memory"],
            }
        except Exception as exc:  # noqa: BLE001 - retain observer failures as evidence
            sample = {"utc": datetime.now(UTC).isoformat(), "error": type(exc).__name__}
        samples.append(sample)
        a.output.write_text(json.dumps(samples, indent=2) + "\n")
        time.sleep(2)

"""Sample an isolated cloud benchmark's backend; no load or data mutations."""

import argparse
import json
import os
import time
from datetime import UTC, datetime
from pathlib import Path
from typing import Any
from urllib.request import Request, urlopen

import psycopg

from ticketing.infrastructure.cache import RedisSeats


def _to_float(value: Any) -> float:
    try:
        return float(value)
    except (TypeError, ValueError):
        return 0.0


def _to_int(value: Any) -> int:
    try:
        return int(value)
    except (TypeError, ValueError):
        return 0


def _query_optional(cursor, sql: str):
    try:
        row = cursor.execute(sql).fetchone()
    except psycopg.Error:
        return None
    return row


def _query_metrics() -> dict[str, float]:
    sample: dict[str, float] = {}
    names = {
        "ticketing_db_commit_seconds_count": True,
        "ticketing_db_commit_seconds_sum": True,
        "ticketing_db_rollback_seconds_count": True,
        "ticketing_db_rollback_seconds_sum": True,
        "ticketing_db_pool_return_seconds_count": True,
        "ticketing_db_pool_return_seconds_sum": True,
        "ticketing_db_pool_acquire_seconds_count": True,
        "ticketing_db_pool_acquire_seconds_sum": True,
        "ticketing_db_transaction_seconds_count": True,
        "ticketing_db_transaction_seconds_sum": True,
        "ticketing_db_query_seconds_count": True,
        "ticketing_db_query_seconds_sum": True,
        "ticketing_db_transaction_body_seconds_count": True,
        "ticketing_db_transaction_body_seconds_sum": True,
        "ticketing_worker_busy_seconds_total": True,
        "ticketing_worker_active": True,
        "ticketing_http_connection_close_total": True,
        "ticketing_http_connection_age_seconds_count": True,
        "ticketing_http_connection_age_seconds_sum": True,
        "ticketing_db_pool_state": True,
    }
    request = Request(API_METRICS)
    request.add_header("Accept", "text/plain; version=0.0.4")
    with urlopen(request, timeout=2) as response:
        for line in response.read().decode("utf-8").splitlines():
            line = line.strip()
            if not line or line.startswith("#"):
                continue
            if " " not in line:
                continue
            metric, value = line.rsplit(" ", 1)
            metric_name = metric.split("{", 1)[0]
            if metric_name not in names:
                continue
            try:
                sample[metric] = float(value)
            except ValueError:
                continue
    return sample


a = argparse.ArgumentParser(description=__doc__)
a.add_argument("--fixtures", type=Path, required=True)
a.add_argument("--output", type=Path, required=True)
a.add_argument("--seconds", type=int, default=600)
args = a.parse_args()


API_METRICS = os.environ.get("API_METRICS_URL", "http://127.0.0.1:8000/metrics")

OVERDUE_SQL = """
SELECT
    count(*),
    coalesce(max(extract(epoch FROM clock_timestamp() - expires_at)), 0)
FROM holds
WHERE event_id=ANY(%s::uuid[])
  AND status='ACTIVE'
  AND expires_at < clock_timestamp()
""".strip()

AGE_SQL = """
SELECT coalesce(max(extract(epoch FROM clock_timestamp()-last_reconciled_at)),0)
FROM event_reconciliation
WHERE event_id=ANY(%s::uuid[])
""".strip()

ACTIVITY_SQL = """
SELECT
    count(*),
    count(*) FILTER (WHERE wait_event_type='Lock'),
    count(*) FILTER (WHERE state='active')
FROM pg_stat_activity
WHERE datname=current_database()
""".strip()

WAL_SQL = """
SELECT wal_records, wal_fpi, wal_bytes, wal_sync_time, wal_write_time
FROM pg_stat_wal
""".strip()

BGWRITER_SQL = """
SELECT
    checkpoints_timed,
    checkpoints_req,
    checkpoint_sync_time,
    checkpoint_write_time,
    buffers_checkpoint,
    buffers_clean,
    buffers_backend_fsync
FROM pg_stat_bgwriter
""".strip()

WAL_IO_SQL = """
SELECT coalesce(sum(read_time), 0), coalesce(sum(write_time), 0),
       coalesce(sum(fsync_time), 0), coalesce(sum(sync_time), 0)
FROM pg_stat_io
WHERE backend_type = 'wal writer'
""".strip()


shows = json.loads(args.fixtures.read_text())['show_ids']
cache = RedisSeats(os.environ['REDIS_URL'])
samples: list[dict] = []
end = time.monotonic() + args.seconds

with psycopg.connect(os.environ['TEST_DATABASE_URL'], autocommit=True) as conn:
    while time.monotonic() < end:
        sample: dict[str, Any] = {"utc": datetime.now(UTC).isoformat()}
        try:
            with conn.cursor() as cursor:
                overdue = cursor.execute(OVERDUE_SQL, (shows,)).fetchone()
                age = cursor.execute(AGE_SQL, (shows,)).fetchone()
                activity = cursor.execute(ACTIVITY_SQL).fetchone()
                wal = cursor.execute(WAL_SQL).fetchone()
                bgw = cursor.execute(BGWRITER_SQL).fetchone()
                wal_io = _query_optional(cursor, WAL_IO_SQL)

            sample["overdue_active_holds"] = _to_int(overdue[0]) if overdue else 0
            sample["oldest_overdue_seconds"] = _to_float(overdue[1]) if overdue else 0.0
            sample["reconciliation_age_seconds"] = _to_float(age[0]) if age else 0.0
            sample["database_connections"] = _to_int(activity[0]) if activity else 0
            sample["lock_waiters"] = _to_int(activity[1]) if activity else 0
            sample["active_connections"] = _to_int(activity[2]) if activity else 0
            sample["wal_records"] = _to_int(wal[0]) if wal else 0
            sample["wal_fpi"] = _to_int(wal[1]) if wal else 0
            sample["wal_bytes"] = _to_int(wal[2]) if wal else 0
            sample["wal_sync_ms"] = _to_float(wal[3]) if wal else 0.0
            sample["wal_write_ms"] = _to_float(wal[4]) if wal else 0.0
            sample["checkpoints_timed"] = _to_int(bgw[0]) if bgw else 0
            sample["checkpoints_requested"] = _to_int(bgw[1]) if bgw else 0
            sample["checkpoint_sync_ms"] = _to_float(bgw[2]) if bgw else 0.0
            sample["checkpoint_write_ms"] = _to_float(bgw[3]) if bgw else 0.0
            sample["buffers_checkpoint"] = _to_int(bgw[4]) if bgw else 0
            sample["buffers_clean"] = _to_int(bgw[5]) if bgw else 0
            sample["buffers_backend_fsync"] = _to_int(bgw[6]) if bgw else 0
            if wal_io:
                sample["wal_writer_io_ms"] = {
                    "read_ms": _to_float(wal_io[0]),
                    "write_ms": _to_float(wal_io[1]),
                    "fsync_ms": _to_float(wal_io[2]),
                    "sync_ms": _to_float(wal_io[3]),
                }
            else:
                sample["wal_writer_io_ms"] = {"unavailable": True}

            with cache.redis.pipeline(transaction=False) as pipe:
                for show in shows:
                    pipe.ttl(cache.key(show))
                ttls = pipe.execute()
            sample["missing_maps"] = ttls.count(-2)
            sample["maps_without_ttl"] = ttls.count(-1)
            sample["minimum_ttl"] = min(ttls)
            sample["redis_used_bytes"] = cache.redis.info("memory").get("used_memory")
            sample["redis_connected_clients"] = cache.redis.info("clients").get("connected_clients")

            sample["api_metrics"] = _query_metrics()
        except Exception as exc:  # noqa: BLE001
            sample = {"utc": datetime.now(UTC).isoformat(), "error": type(exc).__name__}

        samples.append(sample)
        args.output.write_text(json.dumps(samples, indent=2) + "\n")
        time.sleep(2)

cache.redis.close()

"""Measure bounded reconciliation scheduling against real PostgreSQL and Redis.

Reports the legacy full-inventory sweep cost, achieved scheduler throughput, the
resulting overdue backlog and the show count that fits the configured interval and
the 30-second Redis seat-map TTL. Local measurements only: not production capacity.

  python scripts/benchmark_reconciliation.py --shows 200 --seats 300 --seconds 30
"""

import argparse
import json
import os
import statistics
import time
from concurrent.futures import ThreadPoolExecutor
from dataclasses import replace
from pathlib import Path
from uuid import uuid4

import psycopg
from psycopg import sql
from psycopg.conninfo import make_conninfo

from ticketing import workers
from ticketing.config import Settings
from ticketing.infrastructure.cache import RedisSeats
from ticketing.infrastructure.postgres import Postgres
from ticketing.observability import RECONCILE_EVENTS

TTL_SECONDS = 30  # Set by the Redis seat-map writer and extended only by a full snapshot.


def seed(db, shows, seats):
    event_ids = [uuid4() for _ in range(shows)]
    with db.transaction() as conn:
        conn.execute("SET LOCAL statement_timeout = 0")
        for event_id in event_ids:
            conn.execute(
                """INSERT INTO events VALUES (%s,'Benchmark show','THB',
                clock_timestamp()-interval '1 hour',clock_timestamp()+interval '6 hours')""",
                (event_id,),
            )
        with conn.cursor().copy("COPY event_seats(event_id,seat_id,price) FROM STDIN") as copy:
            for event_id in event_ids:
                for row in range(1, seats + 1):
                    copy.write_row((event_id, f"R{row // 30}-{row:03d}", 100))
    return event_ids


def legacy_sweep(db, cache):
    """The behaviour being replaced: rebuild every retained event's whole seat map."""
    started = time.monotonic()
    with db.transaction() as conn:
        conn.execute("SET LOCAL statement_timeout = 0")
        events = conn.execute("SELECT id FROM events").fetchall()
    durations, failures = [], 0
    for item in events:
        each = time.monotonic()
        try:
            workers.snapshot(db, cache, item["id"])
        except Exception as exc:  # noqa: BLE001 - a timeout here is the reported result
            failures += 1
            print(f"  legacy snapshot failed: {type(exc).__name__}")
        durations.append(time.monotonic() - each)
    return {
        "events": len(events),
        "wall_seconds": round(time.monotonic() - started, 3),
        "per_event_p50_ms": round(statistics.median(durations) * 1000, 2) if durations else None,
        "per_event_p95_ms": round(sorted(durations)[int(len(durations) * 0.95)] * 1000, 2)
        if durations
        else None,
        "failures": failures,
    }


def observe(db):
    with db.transaction() as conn:
        conn.execute("SET LOCAL statement_timeout = 0")
        return conn.execute(
            """SELECT count(*) AS tracked,
            count(*) FILTER (WHERE next_due_at<=clock_timestamp()) AS due,
            count(*) FILTER (WHERE last_reconciled_at IS NULL) AS never_reconciled,
            coalesce(max(consecutive_failures),0) AS worst_failures,
            coalesce(EXTRACT(EPOCH FROM clock_timestamp()-min(next_due_at)),0) AS overdue,
            coalesce(EXTRACT(EPOCH FROM clock_timestamp()-min(last_reconciled_at)),0) AS stalest
            FROM event_reconciliation"""
        ).fetchone()


def scheduler_run(db, cache, config, seconds, workers_count, label):
    """Drive reconcile_pass exactly as the maintenance loop does and sample the backlog.

    With `config.reconcile_interval_seconds` at its minimum every event becomes due again
    immediately, so throughput measures the scheduler ceiling. At the production interval
    the same run is demand-limited and instead shows whether deadlines are being met.
    """
    workers.maintain_schedule(db, config)
    stop = time.monotonic() + seconds
    counts, pass_times, peak = [0] * workers_count, [], {"overdue": 0.0, "due": 0, "stale": 0.0}

    def loop(index):
        done = 0
        while time.monotonic() < stop:
            started = time.monotonic()
            done += workers.reconcile_pass(
                db, cache, config, time.monotonic() + config.reconcile_budget_ms / 1000
            )
            pass_times.append(time.monotonic() - started)
        counts[index] = done

    def sampler(_):
        while time.monotonic() < stop:
            workers.maintain_schedule(db, config)
            row = observe(db)
            peak["overdue"] = max(peak["overdue"], float(row["overdue"]))
            peak["due"] = max(peak["due"], row["due"])
            peak["stale"] = max(peak["stale"], float(row["stalest"]))
            time.sleep(0.5)

    acknowledged_before = RECONCILE_EVENTS.labels("ok")._value.get()
    started = time.monotonic()
    with ThreadPoolExecutor(max_workers=workers_count + 1) as pool:
        jobs = [pool.submit(loop, index) for index in range(workers_count)]
        jobs.append(pool.submit(sampler, None))
        for job in jobs:
            job.result()
    elapsed = time.monotonic() - started
    final = observe(db)
    completed = RECONCILE_EVENTS.labels("ok")._value.get() - acknowledged_before
    rate = completed / elapsed if elapsed else 0
    # With N tracked events and interval I, no worker can exceed N/I reconciliations per
    # second: each event goes back to sleep once finished. A rate near that ceiling means
    # the run was demand-limited, so the number is NOT a capacity measurement.
    ceiling = final["tracked"] / config.reconcile_interval_seconds if final["tracked"] else 0
    return {
        "mode": label,
        "workers": workers_count,
        "interval_seconds": config.reconcile_interval_seconds,
        "wall_seconds": round(elapsed, 3),
        "reconciliations": completed,
        "events_per_second": round(rate, 2),
        "demand_ceiling_events_per_second": round(ceiling, 2),
        "demand_limited": rate >= 0.9 * ceiling if ceiling else True,
        "attempted_per_worker": counts,
        "pass_duration_p95_ms": round(sorted(pass_times)[int(len(pass_times) * 0.95)] * 1000, 2)
        if pass_times
        else None,
        "tracked_events": final["tracked"],
        "due_at_end": final["due"],
        "never_reconciled": final["never_reconciled"],
        "peak_backlog": peak["due"],
        "peak_overdue_seconds": round(peak["overdue"], 2),
        "peak_seconds_since_last_reconciliation": round(peak["stale"], 2),
        "worst_consecutive_failures": final["worst_failures"],
    }


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--shows", type=int, default=200)
    parser.add_argument("--seats", type=int, default=300)
    parser.add_argument("--seconds", type=int, default=30)
    parser.add_argument("--workers", type=int, default=1)
    parser.add_argument("--skip-legacy", action="store_true")
    parser.add_argument("--out", default=None)
    args = parser.parse_args()

    url = os.environ["TEST_DATABASE_URL"]
    schema = "bench_" + uuid4().hex[:8]
    with psycopg.connect(url, autocommit=True) as conn:
        conn.execute(sql.SQL("CREATE SCHEMA {}").format(sql.Identifier(schema)))
    db = Postgres(make_conninfo(url, options=f"-c search_path={schema}"), maximum=16)
    db.pool.wait(timeout=10)
    cache = RedisSeats(os.environ["TEST_REDIS_URL"])
    config = replace(Settings(), reconcile_batch_size=8, reconcile_budget_ms=500)
    event_ids = []
    try:
        with db.transaction() as conn:
            conn.execute("SET LOCAL statement_timeout = 0")
            for migration in sorted(Path("migrations").glob("*.sql")):
                conn.execute(migration.read_text(encoding="utf-8-sig"))
        started = time.monotonic()
        event_ids = seed(db, args.shows, args.seats)
        report = {
            "shows": args.shows,
            "seats_per_show": args.seats,
            "seat_rows": args.shows * args.seats,
            "seed_seconds": round(time.monotonic() - started, 2),
            "interval_seconds": config.reconcile_interval_seconds,
            "batch_size": config.reconcile_batch_size,
            "budget_ms": config.reconcile_budget_ms,
            "ttl_seconds": TTL_SECONDS,
        }
        if not args.skip_legacy:
            report["legacy_full_sweep"] = legacy_sweep(db, cache)
        saturated = scheduler_run(
            db,
            cache,
            replace(config, reconcile_interval_seconds=1),
            args.seconds,
            args.workers,
            "saturated",
        )
        with db.transaction() as conn:
            conn.execute("UPDATE event_reconciliation SET next_due_at=clock_timestamp()")
        steady = scheduler_run(db, cache, config, args.seconds, args.workers, "steady_state")
        rate = saturated["events_per_second"]
        report["saturated_capacity"] = saturated
        report["steady_state"] = steady
        report["derived"] = {
            "note": "Derived from measured saturated throughput on this machine only.",
            "valid": not saturated["demand_limited"],
            "shows_reconcilable_within_interval": None
            if saturated["demand_limited"]
            else int(rate * config.reconcile_interval_seconds),
            "throughput_times_ttl_not_a_capacity_guarantee": None
            if saturated["demand_limited"]
            else int(rate * TTL_SECONDS),
            "sampled_schedule_age_within_ttl_not_redis_probe": steady[
                "peak_seconds_since_last_reconciliation"
            ]
            < TTL_SECONDS
            and steady["never_reconciled"] == 0,
        }
        if saturated["demand_limited"]:
            report["derived"]["warning"] = (
                "Saturating phase never queued: measured rate is at the demand ceiling, "
                "so it is a floor on capacity, not capacity. Re-run with more shows."
            )
        print(json.dumps(report, indent=2))
        if args.out:
            Path(args.out).write_text(json.dumps(report, indent=2) + "\n")
    finally:
        for event_id in event_ids:
            cache.redis.delete(cache.key(event_id))
        cache.redis.close()
        db.close()
        with psycopg.connect(url, autocommit=True) as conn:
            conn.execute(sql.SQL("DROP SCHEMA {} CASCADE").format(sql.Identifier(schema)))


if __name__ == "__main__":
    main()

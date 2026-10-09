"""Bounded development-only constant-arrival capacity probe; not a 100k-RPS generator."""

import argparse
import asyncio
import json
import math
import os
import time
import urllib.parse
import urllib.request
from collections import Counter, defaultdict
from datetime import UTC, datetime, timedelta
from pathlib import Path
from uuid import uuid4

import httpx
import jwt
import psycopg
from redis import Redis
from redis.exceptions import RedisError

from ticketing.config import Settings


def percentile(values, fraction):
    return sorted(values)[max(0, math.ceil(len(values) * fraction) - 1)] if values else None


async def run(args):
    settings = Settings()
    if settings.environment != "development":
        raise RuntimeError("Development fixtures and simulated payments only")
    if args.profile == "hot" and args.seconds + 10 >= settings.hold_seconds:
        raise ValueError("Hot run plus request timeout must finish before the hold TTL")
    async with httpx.AsyncClient(base_url=args.url, timeout=5) as probe:
        (await probe.get("/health/ready")).raise_for_status()
    event = str(uuid4())
    total = args.rate * args.seconds
    dsn = os.environ["TEST_DATABASE_URL"]
    if not 1 <= args.seats <= 100000:
        raise ValueError("Inventory must be between 1 and 100000 seats")
    inventory = 1 if args.profile == "hot" else max(total, args.seats)
    # Fresh event: no existing customer inventory is modified.
    with psycopg.connect(dsn) as conn:
        conn.execute(
            "INSERT INTO events VALUES (%s,'Capacity probe','THB',"
            "clock_timestamp()-interval '1 day',clock_timestamp()+interval '1 day')",
            (event,),
        )
        with conn.cursor() as cur:
            cur.executemany(
                "INSERT INTO event_seats(event_id,seat_id,price) VALUES (%s,%s,100)",
                [(event, f"S{i}") for i in range(inventory)],
            )
    cache = Redis.from_url(os.getenv("TEST_REDIS_URL", settings.redis_url), decode_responses=True)
    samples = []
    monitor_stop = asyncio.Event()

    def observe():
        with psycopg.connect(dsn) as conn:
            row = conn.execute(
                "SELECT count(*), count(*) FILTER (WHERE status='FULFILLED') FROM orders WHERE event_id=%s",
                (event,),
            ).fetchone()
            remaining = conn.execute(
                "SELECT coalesce(sum(greatest(p.target_deliveries-p.deliveries,0)),0) "
                "FROM payment_attempts p JOIN orders o ON o.id=p.order_id WHERE o.event_id=%s",
                (event,),
            ).fetchone()[0]
            outbox = conn.execute(
                "SELECT count(*), coalesce(extract(epoch FROM clock_timestamp()-min(occurred_at)),0) "
                "FROM outbox_events WHERE published_at IS NULL"
            ).fetchone()
            unconsumed = conn.execute(
                "SELECT count(*) FROM outbox_events e JOIN orders o ON o.id=e.aggregate_id "
                "WHERE o.event_id=%s AND NOT EXISTS (SELECT 1 FROM consumer_inbox i "
                "WHERE i.event_id=e.id AND i.consumer='fulfillment')",
                (event,),
            ).fetchone()[0]
            sql_version = int(
                conn.execute(
                    "SELECT coalesce(sum(version),0) FROM event_seats WHERE event_id=%s", (event,)
                ).fetchone()[0]
            )
            refresh_supported = conn.execute("SELECT to_regclass('seat_refresh_requests')").fetchone()[0]
            refresh = (
                conn.execute(
                    "SELECT generation-completed_generation, "
                    "extract(epoch FROM clock_timestamp()-requested_at) "
                    "FROM seat_refresh_requests WHERE event_id=%s AND generation>completed_generation",
                    (event,),
                ).fetchone()
                if refresh_supported
                else None
            )
            activity = conn.execute("""SELECT count(*) FILTER (WHERE backend_type='client backend'),
                count(*) FILTER (WHERE state='active'),
                count(*) FILTER (WHERE wait_event_type='Lock'),
                coalesce(max(extract(epoch FROM clock_timestamp()-query_start))
                    FILTER (WHERE wait_event_type='Lock'),0),
                count(*) FILTER (WHERE cardinality(pg_blocking_pids(pid))>0)
                FROM pg_stat_activity WHERE datname=current_database() AND pid<>pg_backend_pid()""").fetchone()
            try:
                raw_version = cache.hget(f"seatmap:v2:{{{event}}}", "version")
                raw = cache.get(f"seatmap:{event}") if raw_version is None else None
                cached_version = (
                    int(raw_version)
                    if raw_version is not None
                    else json.loads(raw)["version"]
                    if raw
                    else None
                )
            except RedisError:
                cached_version = None
            return {
                "utc": datetime.now(UTC).isoformat(),
                "db_activity": dict(
                    zip(
                        [
                            "connections",
                            "active",
                            "lock_waiters",
                            "oldest_waiting_query_seconds",
                            "blocked_sessions",
                        ],
                        [float(x) for x in activity],
                    )
                ),
                "orders": row[0],
                "sql_inventory_version": sql_version,
                "cache_version": cached_version,
                "cache_version_lag": max(0, sql_version - cached_version)
                if cached_version is not None
                else None,
                "refresh_queue_supported": bool(refresh_supported),
                "refresh_generations_pending": refresh[0] if refresh else 0,
                "refresh_oldest_seconds": float(refresh[1]) if refresh else 0,
                "fulfilled": row[1],
                "remaining_callbacks": remaining,
                "run_events_not_consumed": unconsumed,
                "global_unpublished_outbox": outbox[0],
                "global_oldest_outbox_seconds": float(outbox[1]),
            }

    def metrics():
        query = urllib.parse.urlencode(
            {
                "query": '{__name__=~"ticketing_db_.*|ticketing_worker_.*|ticketing_cache_rows_total|process_cpu_seconds_total"}'
            }
        )
        try:
            with urllib.request.urlopen(args.prometheus + "/api/v1/query?" + query, timeout=3) as response:
                return json.load(response)["data"]["result"]
        except (OSError, ValueError, KeyError) as exc:
            return {"error": type(exc).__name__}

    metrics_start = await asyncio.to_thread(metrics)

    async def monitor():
        while not monitor_stop.is_set():
            try:
                sample = await asyncio.to_thread(observe)
                sample["metrics"] = await asyncio.to_thread(metrics)
                samples.append(sample)
            except psycopg.Error as exc:
                samples.append({"utc": datetime.now(UTC).isoformat(), "error": type(exc).__name__})
            try:
                await asyncio.wait_for(monitor_stop.wait(), timeout=2)
            except TimeoutError:
                pass

    monitor_task = asyncio.create_task(monitor())
    statuses = defaultdict(Counter)
    latency = defaultdict(list)
    lag = []
    pending = set()
    all_tasks = []
    dropped = 0
    dispatched = 0
    accepted_payments = 0
    tokens = [
        jwt.encode(
            {
                "sub": f"{event}-{i}",
                "aud": "ticketing",
                "iss": "ticketing",
                "exp": datetime.now(UTC) + timedelta(hours=1),
            },
            settings.jwt_secret,
            algorithm="HS256",
        )
        for i in range(total)
    ]
    async with httpx.AsyncClient(
        base_url=args.url,
        timeout=10,
        limits=httpx.Limits(max_connections=args.inflight, max_keepalive_connections=args.inflight),
    ) as client:

        async def post(operation, path, payload, index):
            start = time.perf_counter()
            try:
                response = await client.post(
                    path,
                    json=payload,
                    headers={
                        "Authorization": "Bearer " + tokens[index],
                        "Idempotency-Key": f"{event}-{index}-{operation}",
                    },
                )
                statuses[operation][str(response.status_code)] += 1
                latency[f"{operation}:{response.status_code}"].append((time.perf_counter() - start) * 1000)
                return response
            except httpx.HTTPError:
                statuses[operation]["transport_error"] += 1
                latency[f"{operation}:transport_error"].append((time.perf_counter() - start) * 1000)
                return None

        async def journey(index):
            nonlocal accepted_payments
            held = await post(
                "hold",
                "/v1/holds",
                {"event_id": event, "seat_ids": [f"S{0 if args.profile == 'hot' else index}"]},
                index,
            )
            if args.profile == "checkout" and held is not None and held.status_code == 201:
                paid = await post(
                    "payment",
                    f"/v1/orders/{held.json()['order_id']}/payments",
                    {"duplicates": 3, "delay_seconds": 0},
                    index,
                )
                if paid is not None and paid.status_code == 202:
                    accepted_payments += 1

        start = time.perf_counter()
        for index in range(total):
            due = start + index / args.rate
            await asyncio.sleep(max(0, due - time.perf_counter()))
            late = time.perf_counter() - due
            lag.append(late * 1000)
            # No unbounded queue and no catch-up burst when the generator falls behind.
            if late > max(0.05, 1 / args.rate) or len(pending) >= args.inflight:
                dropped += 1
                continue
            task = asyncio.create_task(journey(index))
            pending.add(task)
            all_tasks.append(task)
            task.add_done_callback(pending.discard)
            dispatched += 1
        if all_tasks:
            await asyncio.gather(*all_tasks)
        elapsed = time.perf_counter() - start

    http_phase_end = await asyncio.to_thread(observe)
    drain_start = time.monotonic()
    while True:
        with psycopg.connect(dsn) as conn:
            states = dict(
                conn.execute(
                    "SELECT status,count(*) FROM orders WHERE event_id=%s GROUP BY status", (event,)
                ).fetchall()
            )
            holds = conn.execute("SELECT count(*) FROM holds WHERE event_id=%s", (event,)).fetchone()[0]
            duplicates = conn.execute(
                "SELECT count(*) FROM (SELECT seat_id FROM bookings WHERE event_id=%s "
                "GROUP BY seat_id HAVING count(*)>1) AS d",
                (event,),
            ).fetchone()[0]
            tickets = conn.execute(
                "SELECT count(*) FROM tickets t JOIN bookings b ON b.id=t.booking_id WHERE b.event_id=%s",
                (event,),
            ).fetchone()[0]
            remaining_deliveries = conn.execute(
                "SELECT coalesce(sum(greatest(p.target_deliveries-p.deliveries,0)),0) "
                "FROM payment_attempts p JOIN orders o ON o.id=p.order_id WHERE o.event_id=%s",
                (event,),
            ).fetchone()[0]
        delivery_done = args.profile != "checkout" or (
            states.get("FULFILLED", 0) == accepted_payments and remaining_deliveries == 0
        )
        projection = await asyncio.to_thread(observe) if args.wait_projection else None
        projection_done = not args.wait_projection or (
            projection["run_events_not_consumed"] == 0
            and projection["refresh_generations_pending"] == 0
            and projection["cache_version_lag"] == 0
        )
        if delivery_done and projection_done:
            break
        if time.monotonic() - drain_start >= args.drain:
            break
        await asyncio.sleep(0.5)
    monitor_stop.set()
    await monitor_task
    samples.append(await asyncio.to_thread(observe))
    with psycopg.connect(dsn) as conn:
        fulfillment = conn.execute(
            "SELECT percentile_cont(0.95) WITHIN GROUP (ORDER BY "
            "extract(epoch FROM t.issued_at-o.created_at)) "
            "FROM tickets t JOIN bookings b ON b.id=t.booking_id "
            "JOIN orders o ON o.id=b.order_id WHERE o.event_id=%s",
            (event,),
        ).fetchone()[0]
    wins = statuses["hold"]["201"]
    unexpected = sum(
        count
        for op, counts in statuses.items()
        for status, count in counts.items()
        if status not in ({"201", "409", "429", "503"} if op == "hold" else {"202", "409", "429", "503"})
    )
    correct = (
        duplicates == 0
        and holds == wins
        and wins > 0
        and unexpected == 0
        and (args.profile != "hot" or wins == 1)
        and (
            args.profile != "checkout"
            or states.get("FULFILLED", 0) == accepted_payments == tickets
            and accepted_payments > 0
            and remaining_deliveries == 0
        )
    )
    result = {
        "utc": datetime.now(UTC).isoformat(),
        "inventory_seats": inventory,
        "metrics_start": metrics_start,
        "metrics_end": await asyncio.to_thread(metrics),
        "profile": args.profile,
        "backlog_samples": samples,
        "http_phase_end": http_phase_end,
        "projection_drain_requested": args.wait_projection,
        "projection_drained": projection_done if args.wait_projection else None,
        "order_creation_to_ticket_p95_seconds": float(fulfillment) if fulfillment is not None else None,
        "event_id": event,
        "target_journeys_per_second": args.rate,
        "duration_seconds": args.seconds,
        "inflight_limit": args.inflight,
        "scheduled": total,
        "dispatched": dispatched,
        "generator_dropped": dropped,
        "scheduling_lag_p95_ms": percentile(lag, 0.95),
        "dispatch_and_http_completion_seconds": elapsed,
        "completed_http_requests_per_second": sum(sum(v.values()) for v in statuses.values()) / elapsed,
        "statuses": dict(statuses),
        "latency_ms_by_operation_status": {
            key: {"count": len(values), "p95": percentile(values, 0.95), "p99": percentile(values, 0.99)}
            for key, values in latency.items()
        },
        "durable_holds": holds,
        "accepted_payments": accepted_payments,
        "order_states": states,
        "tickets": tickets,
        "duplicate_booked_seats": duplicates,
        "remaining_simulated_callback_deliveries": remaining_deliveries,
        "drain_seconds": time.monotonic() - drain_start,
        "correctness_pass": correct,
        "note": "Local baseline only; callback traffic is not included in generator HTTP RPS.",
    }
    Path(args.output).parent.mkdir(parents=True, exist_ok=True)
    Path(args.output).write_text(json.dumps(result, indent=2), encoding="utf-8")
    print(json.dumps(result, indent=2))
    cache.close()
    if not correct or not projection_done:
        raise SystemExit(1)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--seats", type=int, default=3000)
    parser.add_argument("--prometheus", default="http://127.0.0.1:9090")
    parser.add_argument("--wait-projection", action="store_true")
    parser.add_argument("--profile", choices=["hot", "spread", "checkout"], required=True)
    parser.add_argument("--rate", type=int, default=10)
    parser.add_argument("--seconds", type=int, default=5)
    parser.add_argument("--inflight", type=int, default=50)
    parser.add_argument("--drain", type=int, default=45)
    parser.add_argument("--url", default="http://127.0.0.1:8000")
    parser.add_argument("--output", required=True)
    args = parser.parse_args()
    if min(args.rate, args.seconds, args.inflight, args.drain) < 1 or args.rate * args.seconds > 100000:
        parser.error("Use positive limits and at most 100000 scheduled journeys per local run")
    asyncio.run(run(args))

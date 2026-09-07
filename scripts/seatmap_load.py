"""Development read-heavy probe: 300 seats/show, fixed inventory, legacy vs conditional reads."""

import argparse
import asyncio
import json
import math
import os
import random
from collections import Counter, defaultdict
from datetime import UTC, datetime, timedelta
from pathlib import Path
from time import perf_counter
from uuid import uuid4

import httpx
import jwt
import psycopg

from ticketing.config import Settings
from ticketing.infrastructure.cache import RedisSeats
from ticketing.infrastructure.postgres import Postgres
from ticketing.workers import snapshot


def p95(values):
    return sorted(values)[math.ceil(len(values) * 0.95) - 1] if values else None


async def run(args):
    settings = Settings()
    if settings.environment != "development":
        raise RuntimeError("Development fixtures only")
    dsn = os.environ["TEST_DATABASE_URL"]
    cache = RedisSeats(os.environ["TEST_REDIS_URL"])
    db = Postgres(dsn)
    shows = [str(uuid4()) for _ in range(args.shows)]
    with psycopg.connect(dsn) as conn:
        for event in shows:
            conn.execute(
                "INSERT INTO events VALUES (%s,'Read-heavy probe','THB',clock_timestamp()-interval '1 day',clock_timestamp()+interval '1 day')",
                (event,),
            )
            conn.execute(
                "INSERT INTO event_seats(event_id,seat_id,price) SELECT %s,'S'||i,100 FROM generate_series(0,299) i",
                (event,),
            )
    next_seat = Counter()
    results = []
    try:
        for rate in args.rates:
            modes = args.modes or (
                ["legacy", "conditional"] if args.rates.index(rate) % 2 == 0 else ["conditional", "legacy"]
            )
            for mode in modes:
                # Warm outside the measured phase, using the same fixed set of shows.
                for event in shows:
                    await asyncio.to_thread(snapshot, db, cache, event)
                async with httpx.AsyncClient(
                    base_url=args.url,
                    timeout=10,
                    limits=httpx.Limits(
                        max_connections=args.inflight, max_keepalive_connections=args.inflight
                    ),
                ) as client:
                    tags = {}
                    for event in shows:
                        response = await client.get(f"/v1/events/{event}/availability")
                        response.raise_for_status()
                        tags[event] = response.headers["etag"]
                    validators = {i: tags[shows[i % args.shows]] for i in range(args.viewers)}
                    randomizer = random.Random(42)
                    work = []
                    for index in range(rate * args.seconds):
                        viewer = randomizer.randrange(args.viewers)
                        event = shows[viewer % args.shows]
                        write = index % 20 == 0
                        seat, token = None, None
                        if write:
                            seat = f"S{next_seat[event]}"
                            next_seat[event] += 1
                            if next_seat[event] > 300:
                                raise ValueError("Probe exhausted unique inventory; use more shows")
                            token = jwt.encode(
                                {
                                    "sub": str(uuid4()),
                                    "aud": "ticketing",
                                    "iss": "ticketing",
                                    "exp": datetime.now(UTC) + timedelta(hours=1),
                                },
                                settings.jwt_secret,
                                algorithm="HS256",
                            )
                        work.append((viewer, event, write, seat, token))
                    statuses, times, server_times = defaultdict(Counter), defaultdict(list), defaultdict(list)
                    bytes_received, drops, read_bytes = 0, 0, 0
                    pending, tasks = set(), []

                    async def request(
                        item,
                        mode=mode,
                        validators=validators,
                        statuses=statuses,
                        times=times,
                        server_times=server_times,
                    ):
                        nonlocal bytes_received, read_bytes
                        viewer, event, write, seat, token = item
                        operation = "hold" if write else "read"
                        start = perf_counter()
                        try:
                            if write:
                                response = await client.post(
                                    "/v1/holds",
                                    json={"event_id": event, "seat_ids": [seat]},
                                    headers={
                                        "Authorization": "Bearer " + token,
                                        "Idempotency-Key": str(uuid4()),
                                    },
                                )
                            else:
                                path = "seats" if mode == "legacy" else "availability"
                                headers = (
                                    {"If-None-Match": validators[viewer]} if mode == "conditional" else {}
                                )
                                response = await client.get(f"/v1/events/{event}/{path}", headers=headers)
                                if response.status_code == 200 and mode == "conditional":
                                    validators[viewer] = response.headers["etag"]
                                read_bytes += len(response.content)
                            status = str(response.status_code)
                            bytes_received += len(response.content)
                            if "Server-Timing" in response.headers:
                                server_times[operation + ":" + status].append(
                                    float(response.headers["Server-Timing"].split("dur=")[1])
                                )
                        except httpx.HTTPError:
                            status = "transport_error"
                        statuses[operation][status] += 1
                        times[operation + ":" + status].append((perf_counter() - start) * 1000)

                    samples = []
                    stop_observer = asyncio.Event()

                    def observe():
                        with psycopg.connect(dsn) as conn:
                            row = conn.execute(
                                """SELECT
                                (SELECT count(*) FROM holds WHERE event_id=ANY(%s::uuid[]) AND status='EXPIRED'),
                                (SELECT count(*) FROM holds WHERE event_id=ANY(%s::uuid[]) AND status='ACTIVE' AND expires_at<clock_timestamp()),
                                (SELECT coalesce(max(extract(epoch FROM clock_timestamp()-expires_at)),0) FROM holds WHERE event_id=ANY(%s::uuid[]) AND status='ACTIVE' AND expires_at<clock_timestamp()),
                                (SELECT coalesce(max(extract(epoch FROM clock_timestamp()-last_reconciled_at)),0) FROM event_reconciliation WHERE event_id=ANY(%s::uuid[])),
                                (SELECT count(*) FROM event_reconciliation WHERE event_id=ANY(%s::uuid[]) AND last_reconciled_at IS NULL),
                                (SELECT count(*) FROM seat_refresh_requests WHERE event_id=ANY(%s::uuid[]) AND generation>completed_generation)
                            """,
                                (shows, shows, shows, shows, shows, shows),
                            ).fetchone()
                        with cache.redis.pipeline(transaction=False) as pipe:
                            for event in shows:
                                pipe.ttl(cache.key(event))
                            ttls = pipe.execute()
                        return {
                            "utc": datetime.now(UTC).isoformat(),
                            **dict(
                                zip(
                                    [
                                        "expired_holds",
                                        "overdue_active_holds",
                                        "oldest_overdue_hold_seconds",
                                        "oldest_reconciliation_seconds",
                                        "never_reconciled",
                                        "dirty_events",
                                    ],
                                    [float(v) for v in row],
                                )
                            ),
                            "missing_maps": sum(ttl == -2 for ttl in ttls),
                            "maps_without_ttl": sum(ttl == -1 for ttl in ttls),
                            "minimum_map_ttl_seconds": min(ttls),
                        }

                    async def monitor(stop_observer=stop_observer, samples=samples, observe=observe):
                        while not stop_observer.is_set():
                            try:
                                samples.append(await asyncio.to_thread(observe))
                            except Exception as exc:  # noqa: BLE001 - retain observer failure as evidence
                                samples.append(
                                    {"utc": datetime.now(UTC).isoformat(), "error": type(exc).__name__}
                                )
                            try:
                                await asyncio.wait_for(stop_observer.wait(), timeout=2)
                            except TimeoutError:
                                pass

                    observer = asyncio.create_task(monitor()) if args.observe else None
                    start = perf_counter()
                    for index, item in enumerate(work):
                        due = start + index / rate
                        await asyncio.sleep(max(0, due - perf_counter()))
                        if perf_counter() - due > max(0.05, 1 / rate) or len(pending) >= args.inflight:
                            drops += 1
                            continue
                        task = asyncio.create_task(request(item))
                        pending.add(task)
                        tasks.append(task)
                        task.add_done_callback(pending.discard)
                    await asyncio.gather(*tasks)
                    elapsed = perf_counter() - start
                    if observer:
                        stop_observer.set()
                        await observer
                        samples.append(await asyncio.to_thread(observe))
                result = {
                    "observations": samples,
                    "mode": mode,
                    "offered_rps": rate,
                    "seconds": args.seconds,
                    "shows": args.shows,
                    "seats_per_show": 300,
                    "viewers": args.viewers,
                    "write_percent": 5,
                    "generator_drops": drops,
                    "statuses": dict(statuses),
                    "elapsed_seconds": elapsed,
                    "response_body_bytes": bytes_received,
                    "read_body_bytes": read_bytes,
                    "read_p95_ms": p95(
                        [
                            value
                            for key, values in times.items()
                            if key.startswith("read:")
                            for value in values
                        ]
                    ),
                    "latency_ms": {key: {"count": len(v), "p95": p95(v)} for key, v in times.items()},
                    "server_p95_ms": {key: p95(v) for key, v in server_times.items()},
                }
                result["local_read_gate_pass"] = (
                    drops == 0
                    and result["read_p95_ms"] is not None
                    and result["read_p95_ms"] <= 150
                    and not any(status not in {"200", "304"} for status in statuses["read"])
                    and not any(status != "201" for status in statuses["hold"])
                )
                results.append(result)
                Path(args.output).parent.mkdir(parents=True, exist_ok=True)
                Path(args.output).write_text(
                    json.dumps(
                        {
                            "utc": datetime.now(UTC).isoformat(),
                            "show_ids": shows,
                            "results": results,
                            "note": "Fixed retained 300-seat shows; 95% reads / 5% holds; prewarmed viewer validators; layout/bootstrap excluded; same-machine generator; body bytes exclude headers.",
                        },
                        indent=2,
                    )
                )
                print(json.dumps(result), flush=True)
    finally:
        db.close()
        cache.redis.close()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--modes", nargs="+", choices=["legacy", "conditional"])
    parser.add_argument("--observe", action="store_true")
    parser.add_argument("--rates", type=int, nargs="+", default=[50, 100, 200])
    parser.add_argument("--seconds", type=int, default=30)
    parser.add_argument("--shows", type=int, default=100)
    parser.add_argument("--viewers", type=int, default=1000)
    parser.add_argument("--inflight", type=int, default=64)
    parser.add_argument("--url", default="http://127.0.0.1:8000")
    parser.add_argument("--output", required=True)
    args = parser.parse_args()
    if (
        min(*args.rates, args.seconds, args.shows, args.viewers, args.inflight) < 1
        or args.shows > 2000
        or sum(args.rates) * args.seconds * 2 > 100000
    ):
        parser.error("Use positive limits, at most 2000 shows and 100000 scheduled requests")
    asyncio.run(run(args))

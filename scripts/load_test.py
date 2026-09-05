"""Run inside Compose: python scripts/load_test.py --attempts 1000 --concurrency 200."""

import argparse
import asyncio
import json
import os
import time
from collections import Counter
from contextlib import AsyncExitStack
from datetime import UTC, datetime, timedelta
from uuid import uuid4

import httpx
import jwt

from ticketing.config import Settings


async def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--attempts", type=int, default=1000)
    parser.add_argument("--concurrency", type=int, default=200)
    parser.add_argument("--client-pools", type=int, default=20)
    parser.add_argument("--event", default="00000000-0000-0000-0000-000000000001")
    parser.add_argument("--seat", default="A001")
    args = parser.parse_args()
    if min(args.attempts, args.concurrency, args.client_pools) < 1:
        parser.error("attempts, concurrency and client-pools must be positive")
    gate = asyncio.Semaphore(args.concurrency)
    async with AsyncExitStack() as stack:
        clients = [
            await stack.enter_async_context(
                httpx.AsyncClient(
                    base_url=os.getenv("API_URL", "http://localhost:8000"),
                    limits=httpx.Limits(
                        max_connections=args.concurrency, max_keepalive_connections=args.concurrency
                    ),
                    timeout=10,
                )
            )
            for _ in range(min(args.client_pools, args.concurrency))
        ]

        async def attempt(index):
            client = clients[index % len(clients)]
            token = jwt.encode(
                {
                    "sub": str(uuid4()),
                    "aud": "ticketing",
                    "iss": "ticketing",
                    "exp": datetime.now(UTC) + timedelta(minutes=10),
                },
                Settings().jwt_secret,
                algorithm="HS256",
            )
            async with gate:
                start = time.perf_counter()
                try:
                    response = await client.post(
                        "/v1/holds",
                        json={"event_id": args.event, "seat_ids": [args.seat]},
                        headers={"Authorization": "Bearer " + token, "Idempotency-Key": str(uuid4())},
                    )
                    timing = response.headers.get("Server-Timing", "").partition("dur=")[2]
                    return (
                        response.status_code,
                        (time.perf_counter() - start) * 1000,
                        float(timing) if timing else None,
                    )
                except httpx.HTTPError:
                    return 0, (time.perf_counter() - start) * 1000, None

        results = await asyncio.gather(*(attempt(i) for i in range(args.attempts)))
    counts = Counter(r[0] for r in results)
    times = sorted(r[1] for r in results)
    failed = sorted(r[1] for r in results if r[0] == 409)
    server = sorted(r[2] for r in results if r[2] is not None)
    server_conflicts = sorted(r[2] for r in results if r[0] == 409 and r[2] is not None)
    print(
        json.dumps(
            {
                "attempts": args.attempts,
                "concurrency": args.concurrency,
                "client_pools": len(clients),
                "statuses": dict(counts),
                "server_p95_ms": server[min(len(server) - 1, int(len(server) * 0.95))] if server else None,
                "server_conflict_p95_ms": server_conflicts[
                    min(len(server_conflicts) - 1, int(len(server_conflicts) * 0.95))
                ]
                if server_conflicts
                else None,
                "p95_ms": times[min(len(times) - 1, int(len(times) * 0.95))],
                "conflict_p95_ms": failed[min(len(failed) - 1, int(len(failed) * 0.95))] if failed else None,
            },
            indent=2,
        )
    )
    assert counts[201] == 1, "Expected one winner; use an available seat and finish within its hold TTL"
    assert set(counts) <= {201, 409, 429, 503}, "Unexpected HTTP errors"


if __name__ == "__main__":
    asyncio.run(main())

"""Offline CPU attribution: real ASGI/auth/cache/pool code, simulated I/O only."""

import argparse
import asyncio
import hashlib
import inspect
import json
import statistics
import time
from datetime import UTC, datetime
from pathlib import Path
from uuid import UUID

import httpx
import jwt

from ticketing.api import app, service, settings
from ticketing.application.reservations import Reservations
from ticketing.infrastructure.order_status_cache import RedisOrderStatusCache, scalar
from ticketing.infrastructure.postgres import AcquisitionLimitedPool, Postgres, SharedAcquisitionBudget
from ticketing.infrastructure.reservations import PostgresReservations

ORDER = UUID("00000000-0000-0000-0000-000000000001")
ROW = dict(
    id=ORDER,
    actor="cpu-probe",
    hold_id=ORDER,
    event_id=ORDER,
    total=100,
    currency="THB",
    status="PENDING",
    created_at=datetime.now(UTC),
    tickets=[],
)


class Connection:
    def __enter__(self):
        return self

    def __exit__(self, *args):
        pass

    def execute(self, *args):
        return self

    def commit(self):
        pass

    def rollback(self):
        pass

    def fetchall(self):
        return [{**ROW, "_ticket_id": None, "_ticket_seat_id": None}]


class Pool:
    timeout = 0.5

    def getconn(self, **kwargs):
        return Connection()

    def putconn(self, conn):
        pass

    def get_stats(self):
        return dict(pool_size=2, pool_available=2, requests_waiting=0, pool_max=2)


class Redis:
    def __init__(self, hit):
        self.hit = hit
        self.calls = 0
        self.raw = json.dumps(
            dict(schema_version=1, snapshot_start_ms=1000, fresh_until_ms=2000, order=ROW), default=scalar
        )

    def eval(self, script, count, key, *args):
        if len(args) != 1:
            return 1
        self.calls += 1
        hit = self.hit if self.hit is not None else self.calls % 100 < 56
        return self.raw if hit else "", 1000, 1000 if hit else -2, 0


def make_service(hit):
    native = {role: Pool() for role in ("general", "payment")}
    budget = SharedAcquisitionBudget(20, dict(general=18, payment=18), native)
    pools = {role: AcquisitionLimitedPool(pool, budget, role) for role, pool in native.items()}
    db = Postgres.__new__(Postgres)
    db.pool = pools["general"]
    db._metric_pool_roles = pools
    db._metric_waiter_limits = dict(general=18, payment=18)
    db._shared_acquisition_budget = budget
    return Reservations(PostgresReservations(db, None, 120), RedisOrderStatusCache(Redis(hit), 1000))


async def run(hit, count=3000):
    svc = make_service(hit)

    async def provide():
        return svc

    app.dependency_overrides[service] = provide
    token = jwt.encode(
        dict(sub="cpu-probe", exp=int(time.time()) + 600, aud="ticketing", iss="ticketing"),
        settings.jwt_secret,
        algorithm="HS256",
    )
    headers = {"Authorization": "Bearer " + token}
    async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url="http://offline") as client:
        for _ in range(30):
            assert (await client.get(f"/v1/orders/{ORDER}", headers=headers)).status_code == 200
        started = time.process_time()
        wall = time.perf_counter()
        for start in range(0, count, 50):
            responses = await asyncio.gather(
                *(client.get(f"/v1/orders/{ORDER}", headers=headers) for _ in range(min(50, count - start)))
            )
            assert all(r.status_code == 200 for r in responses)
        return dict(
            requests=count,
            process_cpu_seconds=time.process_time() - started,
            wall_seconds=time.perf_counter() - wall,
        )


async def main():
    from ticketing.api import actor

    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--requests", type=int, default=4000)
    parser.add_argument("--pairs", type=int, default=3)
    args = parser.parse_args()
    if not 100 <= args.requests <= 10000 or not 1 <= args.pairs <= 5:
        raise ValueError("Bounded offline requests/pairs required")
    source = inspect.getsource(actor)
    if not source.startswith("async def actor("):
        raise ValueError("ADR0336 candidate API required")
    # Recreate the exact old synchronous body; no new verifier or bypass.
    namespace = dict(actor.__globals__)
    exec(compile(source.replace("async def actor(", "def actor(", 1), "<control-auth>", "exec"), namespace)
    control = namespace["actor"]
    rows = []
    previous = dict(app.dependency_overrides)
    try:
        for pair in range(args.pairs):
            # Alternate pair ordering to reduce warm-up/order bias.
            for mode in ("control", "candidate") if pair % 2 == 0 else ("candidate", "control"):
                # Equalize FastAPI's dependency-override reconstruction cost.
                # Comparing an overridden control with a native candidate is confounded.
                app.dependency_overrides[actor] = control if mode == "control" else actor
                result = await run(None, args.requests)
                rows.append(dict(pair=pair, mode=mode, **result))
    finally:
        app.dependency_overrides.clear()
        app.dependency_overrides.update(previous)
    means = {
        mode: statistics.mean(r["process_cpu_seconds"] for r in rows if r["mode"] == mode)
        for mode in ("control", "candidate")
    }
    out = dict(
        scope="offline in-memory ASGI, simulated Redis/PostgreSQL I/O; not cloud capacity",
        cache_hit_ratio=0.56,
        concurrency=50,
        both_arms_use_same_dependency_override_path=True,
        rows=rows,
        mean_process_cpu_seconds=means,
        cpu_reduction_percent=100 * (1 - means["candidate"] / means["control"]),
        api_source_sha256=hashlib.sha256(
            Path(inspect.getfile(actor)).read_bytes().replace(b"\r\n", b"\n")
        ).hexdigest(),
    )
    print(json.dumps(out, indent=2))


if __name__ == "__main__":
    asyncio.run(main())

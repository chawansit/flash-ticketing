import os
from concurrent.futures import ThreadPoolExecutor
from datetime import UTC, datetime, timedelta
from threading import Barrier
from uuid import uuid4

import httpx
import jwt
import psycopg
import pytest

from ticketing.config import Settings

pytestmark = pytest.mark.e2e


def test_100_http_contenders_have_exactly_one_durable_hold():
    url, dsn = os.getenv("E2E_API_URL"), os.getenv("TEST_DATABASE_URL")
    if not url or not dsn:
        pytest.skip("Requires real API and its PostgreSQL database")
    event_id = str(uuid4())
    with psycopg.connect(dsn) as conn:
        conn.execute(
            "INSERT INTO events VALUES (%s,'100-contender test','THB',"
            "clock_timestamp()-interval '1 day',clock_timestamp()+interval '1 day')",
            (event_id,),
        )
        conn.execute("INSERT INTO event_seats(event_id,seat_id,price) VALUES (%s,'A',100)", (event_id,))
    tokens = [
        jwt.encode(
            {
                "sub": f"{event_id}-{i}",
                "aud": "ticketing",
                "iss": "ticketing",
                "exp": datetime.now(UTC) + timedelta(minutes=5),
            },
            Settings().jwt_secret,
            algorithm="HS256",
        )
        for i in range(100)
    ]
    start = Barrier(100)
    with httpx.Client(
        base_url=url, timeout=20, limits=httpx.Limits(max_connections=100, max_keepalive_connections=100)
    ) as client:

        def reserve(index):
            start.wait(timeout=20)
            return client.post(
                "/v1/holds",
                json={"event_id": event_id, "seat_ids": ["A"]},
                headers={"Authorization": "Bearer " + tokens[index], "Idempotency-Key": str(uuid4())},
            )

        with ThreadPoolExecutor(max_workers=100) as pool:
            results = list(pool.map(reserve, range(100)))
    winners = [r for r in results if r.status_code == 201]
    assert len(winners) == 1, [r.status_code for r in results]
    assert all(r.status_code in {201, 409, 429, 503} for r in results)
    with psycopg.connect(dsn) as conn:
        assert conn.execute("SELECT count(*) FROM holds WHERE event_id=%s", (event_id,)).fetchone()[0] == 1
        assert conn.execute("SELECT count(*) FROM orders WHERE event_id=%s", (event_id,)).fetchone()[0] == 1
        held = conn.execute("SELECT hold_id FROM event_seats WHERE event_id=%s", (event_id,)).fetchone()[0]
        assert str(held) == winners[0].json()["hold_id"]

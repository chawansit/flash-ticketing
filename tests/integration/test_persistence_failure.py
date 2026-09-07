import os
import time

import pytest
from psycopg.errors import DivisionByZero
from redis.exceptions import RedisError

from ticketing.infrastructure import reservations
from ticketing.infrastructure.cache import RELEASE, RedisSeats
from ticketing.infrastructure.reservations import PostgresReservations
from ticketing.workers import snapshot

pytestmark = pytest.mark.integration


@pytest.mark.parametrize("release_fails", [False, True])
def test_redis_success_sql_failure_rolls_back_and_recovers(system, monkeypatch, release_fails):
    url = os.getenv("TEST_REDIS_URL")
    if not url:
        pytest.skip("TEST_REDIS_URL not configured")
    _, db, event_id = system
    cache = RedisSeats(url)
    shield = f"shield:{{{event_id}}}:A"
    service = PostgresReservations(db, cache)
    original_event = reservations.event
    original_eval = cache.redis.eval

    def fail_after_writes(conn, *args):
        assert cache.redis.exists(shield)
        assert conn.execute("SELECT count(*) AS n FROM holds").fetchone()["n"] == 1
        conn.execute("SELECT 1 / 0")

    def maybe_fail_release(script, *args):
        if release_fails and script == RELEASE:
            raise RedisError("injected release outage")
        return original_eval(script, *args)

    try:
        snapshot(db, cache, event_id)
        before = cache.read(str(event_id))
        monkeypatch.setattr(reservations, "event", fail_after_writes)
        monkeypatch.setattr(cache.redis, "eval", maybe_fail_release)
        with pytest.raises(DivisionByZero):
            service.reserve("actor", event_id, ["A"], "retry-key")
        with db.transaction() as conn:
            for table in ("holds", "orders", "order_items", "idempotency_records", "outbox_events"):
                assert conn.execute(f"SELECT count(*) AS n FROM {table}").fetchone()["n"] == 0
            seat = conn.execute(
                "SELECT * FROM event_seats WHERE event_id=%s AND seat_id='A'", (event_id,)
            ).fetchone()
            assert seat["hold_id"] is None
            assert seat["reserved_until"] is None
            assert seat["version"] == 0
        assert cache.read(str(event_id)) == before
        if release_fails:
            assert 0 < cache.redis.pttl(shield) <= 2000
            deadline = time.monotonic() + 3
            while cache.redis.exists(shield) and time.monotonic() < deadline:
                time.sleep(0.05)
        assert not cache.redis.exists(shield)
        monkeypatch.setattr(reservations, "event", original_event)
        monkeypatch.setattr(cache.redis, "eval", original_eval)
        result = service.reserve("actor", event_id, ["A"], "retry-key")
        assert service.reserve("actor", event_id, ["A"], "retry-key") == result
        with db.transaction() as conn:
            assert conn.execute("SELECT count(*) AS n FROM holds").fetchone()["n"] == 1
        snapshot(db, cache, event_id)
        assert cache.read(str(event_id))["version"] > before["version"]
    finally:
        cache.redis.delete(shield, cache.key(event_id))
        cache.redis.close()


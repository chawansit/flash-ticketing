import json
import os
from concurrent.futures import ThreadPoolExecutor
from uuid import uuid4

import pytest
from psycopg.errors import LockNotAvailable
from psycopg_pool import PoolTimeout

from ticketing import workers
from ticketing.domain import Failure
from ticketing.infrastructure.cache import RedisSeats
from ticketing.infrastructure.postgres import Postgres
from ticketing.observability import DB_ERRORS, DB_POOL_SECONDS, DB_QUERY_SECONDS

pytestmark = pytest.mark.integration


@pytest.fixture
def cache():
    url = os.getenv("TEST_REDIS_URL")
    if not url:
        pytest.skip("TEST_REDIS_URL not configured")
    target = RedisSeats(url)
    yield target
    target.redis.close()


def state(name, version, status="HELD"):
    return {
        "seat_id": name,
        "source_version": version,
        "version": version,
        "status": status,
        "price": 100,
        "reserved_until": None,
    }


def test_patch_replay_and_racing_old_full_snapshot(cache):
    event = str(uuid4())
    initial = {"seats": [state("A", 0), state("B", 0)]}
    try:
        assert not cache.patch(event, [state("A", 1)])
        cache.put(event, 0, initial)
        assert cache.patch(event, [state("A", 2, "SOLD")])
        first = cache.read(event)
        assert first["version"] == 2
        assert cache.patch(event, [state("B", 1)])
        assert cache.patch(event, [state("A", 1)])
        cache.put(event, 1, {"seats": [state("A", 1), state("B", 0)]})
        result = cache.read(event)
        assert result["version"] == 3
        assert result["seats"][0]["status"] == "SOLD"
        assert [r["seat_id"] for r in result["seats"] if r["version"] > first["version"]] == ["B"]
        cache.patch(event, [state("B", 1)])
        assert cache.read(event) == result
        cache.redis.pexpire(cache.key(event), 1000)
        cache.patch(event, [state("B", 2)])
        assert cache.redis.pttl(cache.key(event)) <= 1000
        cache.redis.pexpire(cache.key(event), 0)
        assert not cache.patch(event, [state("A", 3)])
        assert not cache.redis.exists(cache.key(event))
    finally:
        cache.redis.delete(cache.key(event))


def envelope(event, seats):
    return {
        "event_id": str(uuid4()),
        "schema_version": 1,
        "event_type": "SeatsChanged",
        "payload": {"event_id": str(event), "seats": seats},
    }


def test_incremental_dirty_overlap_and_missing_cache_recovery(system, cache, monkeypatch):
    svc, db, event = system
    try:
        workers.snapshot(db, cache, event)
        svc.reserve("a", event, ["A"], "a")
        message = envelope(event, ["A"])
        workers.consume_event(db, cache, message)
        workers.consume_event(db, cache, message)
        original = cache.patch

        def overlap(event_id, seats):
            assert [r["seat_id"] for r in seats] == ["A"]
            result = original(event_id, seats)
            svc.reserve("b", event, ["B"], "b")
            workers.consume_event(db, cache, envelope(event, ["B"]))
            return result

        with monkeypatch.context() as patch:
            patch.setattr(cache, "patch", overlap)
            workers.refresh_one(db, cache)
        with db.transaction() as conn:
            row = conn.execute("SELECT * FROM seat_refresh_requests").fetchone()
            assert (row["generation"], row["completed_generation"]) == (2, 1)
            assert set(row["seat_ids"]) == {"A", "B"}
            conn.execute("UPDATE seat_refresh_requests SET next_attempt_at=clock_timestamp()")
        # Loss between acknowledgement and the next generation must rebuild all seats.
        cache.redis.delete(cache.key(event))
        workers.refresh_one(db, cache)
        result = cache.read(event)
        assert len(result["seats"]) == 3
        assert result["version"] == 2
        with db.transaction() as conn:
            row = conn.execute("SELECT * FROM seat_refresh_requests").fetchone()
            assert row["seat_ids"] is None
            assert row["generation"] == row["completed_generation"]
    finally:
        cache.redis.delete(cache.key(event))


def test_incremental_write_then_crash_is_replayable(system, cache, monkeypatch):
    svc, db, event = system
    try:
        workers.snapshot(db, cache, event)
        svc.reserve("a", event, ["A"], "a")
        workers.consume_event(db, cache, envelope(event, ["A"]))
        original = cache.patch

        def fail(event_id, seats):
            original(event_id, seats)
            raise RuntimeError("lost acknowledgement")

        with monkeypatch.context() as patch:
            patch.setattr(cache, "patch", fail)
            with pytest.raises(RuntimeError):
                workers.refresh_one(db, cache)
        first = cache.read(event)
        with db.transaction() as conn:
            row = conn.execute("SELECT * FROM seat_refresh_requests").fetchone()
            assert row["completed_generation"] == 0
            conn.execute("UPDATE seat_refresh_requests SET lease_until=clock_timestamp()-interval '1 second'")
        workers.refresh_one(db, cache)
        assert cache.read(event) == first
    finally:
        cache.redis.delete(cache.key(event))


def test_query_lock_error_and_pool_wait_metrics(system):
    _, db, event = system
    before = DB_QUERY_SECONDS.labels("SELECT")._sum.get()
    with db.transaction() as conn:
        conn.execute("SELECT pg_sleep(0.02)")
    assert DB_QUERY_SECONDS.labels("SELECT")._sum.get() - before >= 0.02
    errors = DB_ERRORS.labels("55P03")._value.get()
    with db.transaction() as owner:
        owner.execute("SELECT * FROM event_seats WHERE event_id=%s FOR UPDATE", (event,))
        with pytest.raises(LockNotAvailable), db.transaction() as contender:
            contender.execute("SELECT * FROM event_seats WHERE event_id=%s FOR UPDATE NOWAIT", (event,))
    assert DB_ERRORS.labels("55P03")._value.get() == errors + 1
    pool = Postgres(os.environ["TEST_DATABASE_URL"], maximum=1)
    pool.pool.wait(5)
    before = DB_POOL_SECONDS.labels("error")._sum.get()
    try:
        with pool.transaction(), pytest.raises(PoolTimeout), pool.transaction():
            pass
        assert DB_POOL_SECONDS.labels("error")._sum.get() - before >= 0.1
        with pool.transaction() as conn:
            assert conn.execute("SELECT 1 AS n").fetchone()["n"] == 1
    finally:
        pool.close()


def test_concurrent_patches_preserve_maximum_per_seat_and_reconcile(system, cache):
    svc, db, event = system
    try:
        workers.snapshot(db, cache, event)
        updates = [("A", i) for i in range(1, 31)] + [("B", i) for i in range(1, 31)]
        with ThreadPoolExecutor(max_workers=8) as pool:
            list(pool.map(lambda item: cache.patch(event, [state(*item)]), reversed(updates)))
        result = cache.read(event)
        assert result["version"] == 60
        assert {r["seat_id"]: r["source_version"] for r in result["seats"]} == {"A": 30, "B": 30, "C": 0}
        # Separate fresh map demonstrates periodic reconciliation of a missed notification.
        cache.redis.delete(cache.key(event))
        workers.snapshot(db, cache, event)
        svc.reserve("c", event, ["C"], "c")
        workers.snapshot(db, cache, event)
        assert cache.read(event)["seats"][2]["status"] == "HELD"
    finally:
        cache.redis.delete(cache.key(event))


def test_partial_lua_write_requires_full_repair(cache):
    event = str(uuid4())
    try:
        cache.put(event, 0, {"seats": [state("A", 0), state("B", 0)]})
        # Model a Redis runtime error after a seat write but before metadata commit.
        cache.redis.hset(
            cache.key(event), mapping={"updating": 1, "seat:A": json.dumps(state("A", 2, "SOLD"))}
        )
        with pytest.raises(Failure, match="SEATMAP_WARMING"):
            cache.read(event)
        assert not cache.patch(event, [state("B", 1)])
        cache.put(event, 1, {"seats": [state("A", 1), state("B", 0)]})
        result = cache.read(event)
        assert result["version"] == 2
        assert result["seats"][0]["status"] == "SOLD"
    finally:
        cache.redis.delete(cache.key(event))

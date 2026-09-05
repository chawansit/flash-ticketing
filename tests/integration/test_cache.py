import os
from uuid import uuid4

import pytest

from ticketing.domain import Failure
from ticketing.infrastructure.cache import RedisSeats
from ticketing.workers import snapshot

pytestmark = pytest.mark.integration


def test_shield_and_monotonic_cache(system):
    url = os.getenv("TEST_REDIS_URL")
    if not url:
        pytest.skip("TEST_REDIS_URL not configured")
    svc, db, event_id = system
    cache = RedisSeats(url)
    try:
        with cache.shield(str(event_id), ["A"]):
            with pytest.raises(Failure, match="SEAT_BUSY"), cache.shield(str(event_id), ["A", "B"]):
                pass
            # The failed all-or-nothing shield must not acquire B.
            with cache.shield(str(event_id), ["B"]):
                pass
        snapshot(db, cache, event_id)
        initial = cache.read(str(event_id))
        svc.reserve("one", event_id, ["A"], str(uuid4()))
        snapshot(db, cache, event_id)
        newer = cache.read(str(event_id))
        assert newer["version"] > initial["version"]
        changed = [s for s in newer["seats"] if s["version"] > initial["version"]]
        assert [s["seat_id"] for s in changed] == ["A"]
        cache.put(str(event_id), initial["version"], initial)
        assert cache.read(str(event_id)) == newer
    finally:
        cache.redis.delete(f"seatmap:{event_id}")
        cache.redis.close()

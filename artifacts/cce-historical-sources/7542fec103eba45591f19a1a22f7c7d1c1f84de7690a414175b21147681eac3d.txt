import json

from ticketing.infrastructure.cache import RedisSeats


def test_retained_body_limits_and_eviction(monkeypatch):
    cache = RedisSeats("redis://localhost", browse_entries=2, browse_bytes=16)

    def browse(event, kind, validators):
        tag = '"' + event + '"'
        if tag in validators.split(","):
            return 304, tag, None
        return 200, tag, event

    monkeypatch.setattr(cache, "browse", browse)
    try:
        for event in ["a", "b", "a", "c"]:
            assert json.loads(cache.browse_encoded(event, "availability")[2]) == event
        assert list(cache._bodies) == [("a", "availability"), ("c", "availability")]
        cache.browse_encoded("1234567890", "availability")
        assert cache._body_bytes <= 16
        assert len(cache._bodies) <= 2
        cache.browse_encoded("x" * 30, "availability")
        assert ("x" * 30, "availability") not in cache._bodies
        assert cache._body_bytes == sum(len(v[1]) for v in cache._bodies.values())
    finally:
        cache.redis.close()


def test_eviction_during_validation_keeps_request_reference(monkeypatch):
    cache = RedisSeats("redis://localhost", browse_entries=1)
    count = 0

    def browse(event, kind, validators):
        nonlocal count
        count += 1
        if count == 1:
            return 200, '"v1"', {"value": 1}
        # Simulate eviction by another request while Redis I/O is in flight.
        with cache._body_lock:
            cache._bodies.clear()
            cache._body_bytes = 0
        return 304, '"v1"', None

    monkeypatch.setattr(cache, "browse", browse)
    try:
        first = cache.browse_encoded("a", "availability")
        assert cache.browse_encoded("a", "availability") == first
    finally:
        cache.redis.close()

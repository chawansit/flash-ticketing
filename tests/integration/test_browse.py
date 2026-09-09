import os
from concurrent.futures import ThreadPoolExecutor

import pytest
from fastapi.testclient import TestClient

from ticketing.api import app
from ticketing.domain import Failure
from ticketing.infrastructure.cache import RedisSeats
from ticketing.workers import changed_snapshot, snapshot

pytestmark = pytest.mark.integration


@pytest.fixture
def browse_system(system, monkeypatch):
    url = os.getenv("TEST_REDIS_URL")
    if not url:
        pytest.skip("TEST_REDIS_URL not configured")
    svc, db, event = system
    cache = RedisSeats(url)
    snapshot(db, cache, event)
    monkeypatch.setattr(app.state, "cache", cache, raising=False)
    try:
        yield svc, db, event, cache, TestClient(app)
    finally:
        cache.redis.delete(cache.key(event))
        cache.redis.close()


def test_http_conditional_layout_and_availability(browse_system):
    svc, db, event, cache, client = browse_system
    base = f"/v1/events/{event}"
    layout = client.get(base + "/layout")
    assert layout.status_code == 200
    assert "max-age=3600" in layout.headers["cache-control"]
    assert set(layout.json()["seats"][0]) == {"seat_id", "price"}
    initial = client.get(base + "/availability")
    assert initial.status_code == 200
    assert initial.headers["cache-control"] == "private, no-cache"
    assert set(initial.json()["seats"][0]) == {"seat_id", "status", "reserved_until"}
    for validator in [
        initial.headers["etag"],
        initial.headers["etag"].removeprefix("W/"),
        '"unrelated", ' + initial.headers["etag"],
        "*",
    ]:
        unchanged = client.get(base + "/availability", headers={"If-None-Match": validator})
        assert unchanged.status_code == 304
        assert unchanged.content == b""
        assert unchanged.headers["etag"] == initial.headers["etag"]
    held = svc.reserve("a", event, ["A"], "a")
    changed_snapshot(db, cache, event, ["A"])
    newer = client.get(base + "/availability", headers={"If-None-Match": initial.headers["etag"]})
    assert newer.status_code == 200
    assert newer.json()["seats"][0]["status"] == "HELD"
    assert newer.json()["seats"][0]["reserved_until"] is not None
    assert newer.headers["etag"] != initial.headers["etag"]
    assert client.get(base + "/layout", headers={"If-None-Match": layout.headers["etag"]}).status_code == 304
    svc.release("a", held["hold_id"])
    changed_snapshot(db, cache, event, ["A"])
    released = client.get(base + "/availability", headers={"If-None-Match": newer.headers["etag"]})
    assert released.status_code == 200
    assert released.json()["seats"][0]["status"] == "AVAILABLE"
    schema = client.get("/openapi.json").json()
    assert "304" in schema["paths"]["/v1/events/{event_id}/availability"]["get"]["responses"]


def test_cache_loss_and_interrupted_write_never_validate_old_body(browse_system):
    _, db, event, cache, client = browse_system
    path = f"/v1/events/{event}/availability"
    old = client.get(path).headers["etag"]
    cache.redis.hset(cache.key(event), "updating", 1)
    assert client.get(path, headers={"If-None-Match": old}).status_code == 503
    cache.redis.delete(cache.key(event))
    assert client.get(path, headers={"If-None-Match": "*"}).status_code == 503
    snapshot(db, cache, event)
    rebuilt = client.get(path, headers={"If-None-Match": old})
    assert rebuilt.status_code == 200
    assert rebuilt.headers["etag"] != old


def test_304_does_not_decode_seats_and_redis_outage_is_503(browse_system, monkeypatch):
    _, _, event, cache, _ = browse_system
    _, tag, _ = cache.browse(event, "availability")
    cache.redis.hset(cache.key(event), "seat:A", "invalid seat JSON")
    assert cache.browse(event, "availability", tag) == (304, tag, None)
    from redis.exceptions import ConnectionError

    def offline(*_):
        raise ConnectionError("offline")

    monkeypatch.setattr(cache.redis, "eval", offline)
    with pytest.raises(Failure, match="SEATMAP_UNAVAILABLE"):
        cache.browse(event, "availability", tag)


def test_atomic_validator_body_during_updates(browse_system):
    _, _, event, cache, _ = browse_system

    def writer():
        for version in range(1, 31):
            cache.patch(
                event,
                [
                    {
                        "seat_id": "A",
                        "source_version": version,
                        "price": 100,
                        "status": "HELD",
                        "reserved_until": None,
                    }
                ],
            )

    with ThreadPoolExecutor(max_workers=1) as pool:
        future = pool.submit(writer)
        for _ in range(30):
            status, tag, body = cache.browse(event, "availability")
            assert status == 200
            assert tag.endswith(":" + str(body["version"]) + '"')
        future.result()


def test_encoded_reuse_still_checks_redis_and_incarnation(browse_system, monkeypatch):
    _, db, event, cache, client = browse_system
    path = f"/v1/events/{event}/availability"
    first = client.get(path)
    # Reusing the retained version must skip decoding even for a new viewer.
    cache.redis.hset(cache.key(event), "seat:A", "invalid seat JSON")
    second = client.get(path)
    assert second.status_code == 200
    assert second.content == first.content
    assert second.headers["etag"] == first.headers["etag"]
    assert second.headers["content-type"] == "application/json"
    cache.redis.delete(cache.key(event))
    assert client.get(path).status_code == 503
    snapshot(db, cache, event)
    rebuilt = client.get(path)
    assert rebuilt.status_code == 200
    assert rebuilt.headers["etag"] != first.headers["etag"]
    from redis.exceptions import ConnectionError

    def offline(*_):
        raise ConnectionError("offline")

    monkeypatch.setattr(cache.redis, "eval", offline)
    assert client.get(path).status_code == 503


def test_encoded_validator_body_remains_consistent_during_updates(browse_system):
    import json

    _, _, event, cache, _ = browse_system

    def writer():
        for version in range(1, 61):
            cache.patch(event, [{"seat_id": "A", "source_version": version,
                                 "price": 100, "status": "HELD", "reserved_until": None}])

    def reader():
        for _ in range(60):
            status, tag, encoded = cache.browse_encoded(event, "availability")
            body = json.loads(encoded)
            assert status == 200
            assert tag.endswith(":" + str(body["version"]) + '"')

    with ThreadPoolExecutor(max_workers=5) as pool:
        futures = [pool.submit(writer)] + [pool.submit(reader) for _ in range(4)]
        for future in futures:
            future.result()

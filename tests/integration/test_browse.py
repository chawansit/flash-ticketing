import json
import os
import time
from concurrent.futures import ThreadPoolExecutor

import pytest
from fastapi.testclient import TestClient

from ticketing.api import app
from ticketing.domain import Failure
from ticketing.infrastructure.cache import RedisSeats
from ticketing.observability import SEAT_DELTA_RESETS
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
        cache.redis.delete(cache.key(event), cache.delta_key(event))
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

def test_successful_browse_refreshes_configured_ttl_in_one_read(browse_system):
    _, _, event, existing_cache, _ = browse_system
    existing_cache.redis.delete(existing_cache.key(event))
    cache = RedisSeats(os.environ["TEST_REDIS_URL"], seatmap_ttl_seconds=3)
    try:
        cache.put(
            event,
            0,
            {
                "seats": [
                    {
                        "seat_id": "A",
                        "source_version": 0,
                        "price": 100,
                        "status": "AVAILABLE",
                        "reserved_until": None,
                    }
                ]
            },
        )
        time.sleep(1.1)
        assert cache.redis.ttl(cache.key(event)) <= 2

        status, tag, _ = cache.browse(event, "availability")
        assert status == 200
        assert cache.redis.ttl(cache.key(event)) >= 2

        time.sleep(1.1)
        assert cache.browse(event, "availability", tag) == (304, tag, None)
        assert cache.redis.ttl(cache.key(event)) >= 2
    finally:
        cache.redis.delete(cache.key(event))
        cache.redis.close()

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

def test_versioned_deltas_avoid_full_map_reads_and_collapse_repeated_seats(browse_system):
    svc, db, event, cache, client = browse_system
    base = f"/v1/events/{event}"
    initial = client.get(base + "/availability").json()
    unchanged = client.get(base + f"/seat-deltas?since={initial['version']}")
    assert unchanged.status_code == 200
    assert unchanged.json() == {
        "event_id": str(event),
        "from_version": initial["version"],
        "version": initial["version"],
        "reset_required": False,
        "seats": [],
    }

    held = svc.reserve("a", event, ["A"], "delta-hold")
    changed_snapshot(db, cache, event, ["A"])
    held_delta = client.get(base + f"/seat-deltas?since={initial['version']}").json()
    assert held_delta["reset_required"] is False
    assert held_delta["version"] > initial["version"]
    assert held_delta["seats"][0]["seat_id"] == "A"
    assert held_delta["seats"][0]["status"] == "HELD"
    assert held_delta["seats"][0]["reserved_until"] is not None

    svc.release("a", held["hold_id"])
    changed_snapshot(db, cache, event, ["A"])
    released = client.get(base + f"/seat-deltas?since={initial['version']}").json()
    assert released["reset_required"] is False
    assert released["version"] > held_delta["version"]
    assert released["seats"] == [
        {"seat_id": "A", "status": "AVAILABLE", "reserved_until": None}
    ]

def test_delta_history_gap_returns_full_reset_snapshot(browse_system):
    svc, db, event, cache, client = browse_system
    base = f"/v1/events/{event}"
    initial = client.get(base + "/availability").json()
    svc.reserve("a", event, ["A"], "delta-gap")
    changed_snapshot(db, cache, event, ["A"])
    current = cache.read(event)["version"]
    assert current > initial["version"]

    cache.redis.delete(cache.delta_key(event))
    resets_before = SEAT_DELTA_RESETS.labels("missing_history")._value.get()
    response = client.get(base + f"/seat-deltas?since={initial['version']}")
    assert SEAT_DELTA_RESETS.labels("missing_history")._value.get() == resets_before + 1
    assert response.status_code == 200
    body = response.json()
    assert body["reset_required"] is True
    assert body["from_version"] == initial["version"]
    assert body["version"] == current
    assert len(body["seats"]) == 3
    assert {seat["seat_id"] for seat in body["seats"]} == {"A", "B", "C"}

def test_delta_history_is_bounded_and_trim_gap_fails_safe(browse_system):
    _, _, event, cache, _ = browse_system
    initial = cache.read(event)["version"]
    source = max(seat["source_version"] for seat in cache.read(event)["seats"])
    for offset in range(1, 514):
        assert cache.patch(
            event,
            [{
                "seat_id": "A",
                "source_version": source + offset,
                "price": 100,
                "status": "HELD" if offset % 2 else "AVAILABLE",
                "reserved_until": None,
            }],
        )
    assert cache.redis.zcard(cache.delta_key(event)) == 512
    reset = cache.deltas(event, initial)
    assert reset["reset_required"] is True
    assert len(reset["seats"]) == 3

def test_delta_history_distinguishes_missing_and_overlapping_ranges(browse_system):
    _, _, event, cache, _ = browse_system
    initial = cache.read(event)["version"]
    current = initial + 2
    cache.redis.hset(cache.key(event), "version", current)

    cache.redis.delete(cache.delta_key(event))
    cache.redis.zadd(cache.delta_key(event), {
        json.dumps({"from_version": initial + 1, "version": current, "seats": []}): current,
    })
    missing_before = SEAT_DELTA_RESETS.labels("history_missing")._value.get()
    assert cache.deltas(event, initial)["reset_required"] is True
    assert SEAT_DELTA_RESETS.labels("history_missing")._value.get() == missing_before + 1

    cache.redis.delete(cache.delta_key(event))
    cache.redis.zadd(cache.delta_key(event), {
        json.dumps({"from_version": initial, "version": current, "seats": []}): current,
    })
    overlap_before = SEAT_DELTA_RESETS.labels("history_overlap")._value.get()
    assert cache.deltas(event, initial + 1)["reset_required"] is True
    assert SEAT_DELTA_RESETS.labels("history_overlap")._value.get() == overlap_before + 1


def test_delta_higher_prior_incarnation_version_returns_reset(browse_system):
    _, _, event, cache, client = browse_system
    version = cache.read(event)["version"]
    path = f"/v1/events/{event}/seat-deltas"

    invalid = client.get(path, params={"since": -1})
    assert invalid.status_code == 422
    assert invalid.json()["code"] == "INVALID_VERSION"

    response = client.get(path, params={"since": version + 1})
    assert response.status_code == 200
    assert response.json()["reset_required"] is True
    assert response.json()["from_version"] == version + 1
    assert response.json()["version"] == version

    schema = client.get("/openapi.json").json()
    response_schema = schema["paths"]["/v1/events/{event_id}/seat-deltas"]["get"]["responses"]["200"]
    name = response_schema["content"]["application/json"]["schema"]["$ref"].rsplit("/", 1)[-1]
    assert "reset_required" in schema["components"]["schemas"][name]["properties"]


def test_recreated_delta_history_inherits_remaining_map_ttl(browse_system):
    _, _, event, cache, _ = browse_system
    source = max(seat["source_version"] for seat in cache.read(event)["seats"])
    cache.redis.delete(cache.delta_key(event))
    assert cache.patch(
        event,
        [{
            "seat_id": "A",
            "source_version": source + 1,
            "price": 100,
            "status": "HELD",
            "reserved_until": None,
        }],
    )
    map_ttl = cache.redis.ttl(cache.key(event))
    delta_ttl = cache.redis.ttl(cache.delta_key(event))
    assert 0 < delta_ttl <= map_ttl

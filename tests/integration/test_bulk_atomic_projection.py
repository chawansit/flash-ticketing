"""Multi-group atomic Redis projection, replay and interrupted publication."""
import json
import os
from concurrent.futures import ThreadPoolExecutor
from threading import Barrier
from uuid import uuid4

import pytest
from redis.exceptions import ResponseError

from ticketing.domain import Failure
from ticketing.infrastructure.cache import PUT, RedisSeats

pytestmark = pytest.mark.integration


def seat(index, version=0, status="AVAILABLE"):
    return {"seat_id": f"S{index}", "source_version": version, "version": version,
            "price": 100, "status": status, "reserved_until": None}


@pytest.fixture
def projection():
    url = os.getenv("TEST_REDIS_URL")
    if not url:
        pytest.skip("TEST_REDIS_URL not configured")
    cache = RedisSeats(url)
    event = str(uuid4())
    try:
        yield cache, event
    finally:
        cache.redis.delete(cache.key(event), cache.delta_key(event))
        cache.redis.close()


@pytest.mark.parametrize("count", [0, 1, 255, 256, 257, 513, 18000])
def test_full_projection_across_group_boundaries(projection, count):
    cache, event = projection
    data = {"seats": [seat(index) for index in range(count)]}
    assert cache.put(event, 0, data) == 0  # Original100ms adapter deadline.
    initial = cache.read(event)
    assert len(initial["seats"]) == count
    assert {row["seat_id"] for row in initial["seats"]} == {f"S{i}" for i in range(count)}
    assert cache.redis.zcard(cache.delta_key(event)) == 0
    assert cache.put(event, 0, data) == 0
    assert cache.read(event) == initial


def test_concurrent_stale_full_and_newer_patch_across_groups(projection):
    cache, event = projection
    cache.put(event, 0, {"seats": [seat(i) for i in range(1025)]})
    before = cache.read(event)
    updates = [seat(i, 2, "SOLD") for i in range(513)]
    stale = {"seats": [seat(i, 1, "HELD") if i < 513 else seat(i) for i in range(1025)]}
    ready = Barrier(2)

    def full():
        ready.wait(timeout=5)
        return cache.put(event, 513, stale)

    def patch():
        ready.wait(timeout=5)
        return cache.patch(event, updates)

    with ThreadPoolExecutor(max_workers=2) as pool:
        futures = [pool.submit(full), pool.submit(patch)]
        for future in futures:
            future.result(timeout=5)
    result = cache.read(event)
    assert result["version"] == 1026
    assert all(row["source_version"] == 2 and row["status"] == "SOLD"
               for row in result["seats"] if int(row["seat_id"][1:]) < 513)
    assert result["incarnation"] == before["incarnation"]
    history = cache.deltas(event, 0, before["incarnation"])
    assert not history["reset_required"]
    assert len(history["seats"]) == 513
    assert cache.patch(event, updates)
    cache.put(event, 513, stale)
    assert cache.read(event) == result


def test_group_interruption_is_unreadable_and_full_reconciliation_repairs(projection):
    cache, event = projection
    data = {"seats": [seat(i, 1) for i in range(513)]}
    marker = "redis.call('HSET',KEYS[1],unpack(fields))"
    assert PUT.count(marker) == 1
    broken = PUT.replace(marker, marker+"\n  if first == 1 then return redis.error_reply('local group interruption') end")
    with pytest.raises(ResponseError, match="local group interruption"):
        cache.redis.eval(broken, 2, *cache._full_arguments(event, data))
    assert cache.redis.hexists(cache.key(event), "updating")
    assert cache.redis.hexists(cache.key(event), "seat:S255")
    assert not cache.redis.hexists(cache.key(event), "seat:S256")
    with pytest.raises(Failure, match="SEATMAP_WARMING"):
        cache.read(event)
    assert not cache.patch(event, [seat(0, 2, "SOLD")])
    assert cache.put(event, 513, data) == 513
    repaired = cache.read(event)
    assert len(repaired["seats"]) == 513
    assert all(row["source_version"] == 1 for row in repaired["seats"])
    assert not cache.redis.hexists(cache.key(event), "updating")


def test_lost_group_write_ack_replay_preserves_history_ttl_and_incarnation(projection, monkeypatch):
    cache, event = projection
    initial = {"seats": [seat(i) for i in range(513)]}
    cache.put(event, 0, initial)
    before = cache.read(event)
    cache.redis.pexpire(cache.key(event), 1500)
    updates = [seat(i, 1, "HELD") for i in range(513)]
    original = cache.redis.eval

    def lost_ack(*args, **kwargs):
        original(*args, **kwargs)
        raise ConnectionError("local acknowledgement lost after atomic publication")

    with monkeypatch.context() as context:
        context.setattr(cache.redis, "eval", lost_ack)
        with pytest.raises(ConnectionError, match="acknowledgement lost"):
            cache.patch(event, updates)
    assert 0 < cache.redis.pttl(cache.key(event)) <= 1500
    assert cache.patch(event, updates)
    assert 0 < cache.redis.pttl(cache.key(event)) <= 1500
    assert cache.redis.zcard(cache.delta_key(event)) == 1
    history = json.loads(cache.redis.zrange(cache.delta_key(event), 0, -1)[0])
    assert history["from_version"] == 0 and history["version"] == 513
    assert len(history["seats"]) == 513
    result = cache.read(event)
    assert result["incarnation"] == before["incarnation"]
    assert result["version"] == 513
    cache.redis.delete(cache.key(event))
    assert not cache.patch(event, updates)
    cache.put(event, 0, initial)
    rebuilt = cache.read(event)
    assert rebuilt["incarnation"] != result["incarnation"]
    assert rebuilt["version"] == 0
    assert cache.redis.zcard(cache.delta_key(event)) == 0


def test_preencoded_strings_preserve_json_escaping_and_delta_states(projection):
    cache, event = projection
    unusual = seat(0)
    unusual["seat_id"] = 'Q"\\\nÃƒÂ©Ã¢â‚¬ÂºÃ‚Âª'
    unusual["label"] = 'quoted"\\\n},\"version\":999'
    cache.put(event, 0, {"seats": [unusual]})
    initial = cache.read(event)
    assert initial["seats"] == [unusual]
    updated = {**unusual, "source_version": 2, "status": "SOLD", "version": 9876}
    assert cache.patch(event, [updated])
    current = cache.read(event)
    expected = {**updated, "version": 2}
    assert current["version"] == 2
    assert current["seats"] == [expected]
    delta = cache.deltas(event, 0, initial["incarnation"])
    assert delta["seats"] == [{"seat_id": unusual["seat_id"], "status": "SOLD",
                               "reserved_until": None}]
    raw_delta = json.loads(cache.redis.zrange(cache.delta_key(event), 0, -1)[0])
    assert raw_delta["seats"] == [expected]
    cache.put(event, 0, {"seats": [unusual]})
    assert cache.read(event) == current


def test_preencoded_patch_preserves_configured_history_bound(projection):
    cache, event = projection
    cache.delta_history_entries = 2
    cache.put(event, 0, {"seats": [seat(0)]})
    initial = cache.read(event)
    for version in range(1, 4):
        assert cache.patch(event, [seat(0, version, "HELD")])
    entries = [json.loads(raw) for raw in cache.redis.zrange(cache.delta_key(event), 0, -1)]
    assert [entry["version"] for entry in entries] == [2, 3]
    assert cache.read(event)["version"] == 3
    assert cache.deltas(event, 0, initial["incarnation"])["reset_required"]


@pytest.mark.parametrize("invalid_payload", ["", "{}", "{}\n{}\n{}", "not-json\n{}", "{}\n{", "{}\n{}\n"])
@pytest.mark.parametrize("existing", [False, True])
def test_malformed_frame_rejected_before_map_or_history_mutation(projection, invalid_payload, existing):
    cache, event = projection
    if existing:
        cache.put(event, 0, {"seats": [seat(0), seat(1)]})
        cache.patch(event, [seat(0, 1, "HELD")])
    else:
        # Invalid full rebuild must not delete stale history before rejecting input.
        cache.redis.zadd(cache.delta_key(event), {"retained history": 1})
    before_map = cache.redis.hgetall(cache.key(event))
    before_history = cache.redis.zrange(cache.delta_key(event), 0, -1, withscores=True)
    args = list(cache._full_arguments(event, {"seats": [seat(0), seat(1)]}))
    assert len(args) == 14  # Two keys, fixed ten-field header, payload and prediction.
    args[-2] = invalid_payload
    with pytest.raises(ResponseError, match="invalid seat payload"):
        cache.redis.eval(PUT, 2, *args)
    assert cache.redis.hgetall(cache.key(event)) == before_map
    assert cache.redis.zrange(cache.delta_key(event), 0, -1, withscores=True) == before_history


def test_existing_hash_without_version_keeps_newer_retained_source(projection):
    cache, event = projection
    cache.put(event, 0, {"seats": [seat(0), seat(1)]})
    cache.patch(event, [seat(0, 2, "SOLD")])
    before = cache.read(event)
    cache.redis.hdel(cache.key(event), "version")
    assert cache.put(event, 0, {"seats": [seat(0), seat(1)]}) == 2
    repaired = cache.read(event)
    assert repaired["incarnation"] == before["incarnation"]
    assert repaired["seats"][0]["status"] == "SOLD"
    assert repaired["seats"][0]["source_version"] == 2


def test_prediction_never_overrides_unrelated_newer_seat_versions(projection):
    cache, event = projection
    cache.put(event, 0, {"seats": [seat(0), seat(1)]})
    cache.patch(event, [seat(0, 4, "SOLD")])
    assert cache.patch(event, [seat(1, 1, "HELD")])  # Input prediction1, actual aggregate5.
    current = cache.read(event)
    assert current["version"] == 5
    assert current["seats"][1]["version"] == 5
    cache.put(event, 1, {"seats": [seat(0), seat(1, 1, "HELD")]})
    assert cache.read(event) == current

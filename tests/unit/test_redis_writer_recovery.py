import pytest
from redis.exceptions import RedisError

from ticketing.infrastructure.redis_reservations import RedisReservationIntake


class FakePool:
    def __init__(self):
        self.disconnects = 0

    def disconnect(self):
        self.disconnects += 1


class FakePipeline:
    def __init__(self, redis):
        self.redis = redis
        self.streams = []

    def xlen(self, stream):
        self.streams.append(stream)
        return self

    def execute(self):
        return [self.redis.lengths.get(stream, 1) for stream in self.streams]


class FakeRedis:
    def __init__(self, registry=(), lengths=None):
        self.registry = set(registry)
        self.lengths = lengths or {}
        self.connection_pool = FakePool()
        self.smembers_calls = 0
        self.scan_calls = 0
        self.srem_calls = []
        self.group_calls = []
        self.read_batches = []

    def smembers(self, _key):
        self.smembers_calls += 1
        return set(self.registry)

    def scan(self, cursor, **_kwargs):
        self.scan_calls += 1
        return 0, []

    def pipeline(self, **_kwargs):
        return FakePipeline(self)

    def srem(self, key, *streams):
        self.srem_calls.append((key, streams))
        self.registry.difference_update(streams)

    def sadd(self, _key, *streams):
        self.registry.update(streams)

    def xgroup_create(self, stream, _group, **_kwargs):
        self.group_calls.append(stream)

    def xautoclaim(self, *_args, **_kwargs):
        return "0-0", [], []

    def xreadgroup(self, _group, _consumer, streams, **_kwargs):
        self.read_batches.append(tuple(streams))
        return []


class IncrementalScanRedis(FakeRedis):
    def scan(self, cursor, **_kwargs):
        self.scan_calls += 1
        next_cursor = cursor + 1
        return next_cursor, [f"reservation-stream:scan-{next_cursor}"]


class FakeCache:
    def __init__(self, redis):
        self.redis = redis


def test_stream_polling_rotates_over_bounded_batches_and_caches_groups():
    streams = [f"reservation-stream:{{event-{index}}}" for index in range(5)]
    redis = FakeRedis(streams)
    intake = RedisReservationIntake(
        FakeCache(redis),
        stream_batch_size=2,
        stream_refresh_seconds=60,
    )

    for _ in range(3):
        assert list(intake.messages("writer")) == []

    ordered = tuple(sorted(streams))
    assert redis.read_batches == [
        (ordered[0],),
        (ordered[1],),
        (ordered[2],),
        (ordered[3],),
        (ordered[4],),
        (ordered[0],),
    ]
    assert redis.smembers_calls == 1
    assert redis.scan_calls == 1
    assert sorted(redis.group_calls) == sorted(streams)
    assert len(redis.group_calls) == len(streams)


def test_discovery_scan_is_bounded_and_connection_reset_rebuilds_state():
    redis = IncrementalScanRedis()
    intake = RedisReservationIntake(
        FakeCache(redis),
        stream_batch_size=1,
        stream_refresh_seconds=60,
        stream_scan_steps=2,
    )

    assert list(intake.messages("writer")) == []
    assert redis.scan_calls == 2
    assert len(intake.streams()) == 2

    intake.reset_connection_state()

    assert redis.connection_pool.disconnects == 1
    assert intake._streams == []
    assert intake._known_groups == set()
    assert list(intake.messages("writer")) == []
    assert redis.scan_calls == 4

class PerStreamCountRedis(FakeRedis):
    def xreadgroup(self, _group, _consumer, streams, **kwargs):
        self.read_batches.append(tuple(streams))
        stream = next(iter(streams))
        count = kwargs["count"]
        entries = [(f"{index}-0", {"payload": "{}"}) for index in range(min(2, count))]
        return [(stream, entries)]


def test_messages_applies_count_as_a_hard_total_across_streams():
    streams = [f"reservation-stream:{{event-{index}}}" for index in range(3)]
    redis = PerStreamCountRedis(streams)
    intake = RedisReservationIntake(
        FakeCache(redis),
        stream_batch_size=3,
        stream_refresh_seconds=60,
    )

    messages = list(intake.messages("writer", count=4))

    assert len(messages) == 4
    ordered = tuple(sorted(streams))
    assert redis.read_batches == [(ordered[0],), (ordered[1],)]


def test_discovery_rotates_beyond_a_full_window_without_starvation():
    streams = [f"reservation-stream:{{event-{index}}}" for index in range(7)]
    redis = FakeRedis(streams)
    intake = RedisReservationIntake(
        FakeCache(redis),
        stream_batch_size=2,
        stream_refresh_seconds=0,
    )
    ordered = tuple(sorted(streams))

    intake._refresh_streams(limit=5)
    assert intake.streams(limit=5) == list(ordered[:5])
    for _ in range(3):
        intake._next_stream_batch()

    intake._refresh_streams(limit=5)

    assert intake.streams(limit=5) == [ordered[5], ordered[6], ordered[0], ordered[1], ordered[2]]


def test_discovery_skips_empty_window_and_reaches_later_nonempty_streams():
    streams = [f"reservation-stream:{{event-{index}}}" for index in range(7)]
    ordered = tuple(sorted(streams))
    lengths = {stream: 0 for stream in streams}
    lengths[ordered[5]] = 1
    lengths[ordered[6]] = 2
    redis = FakeRedis(streams, lengths=lengths)
    intake = RedisReservationIntake(
        FakeCache(redis),
        stream_batch_size=2,
        stream_refresh_seconds=0,
    )

    intake._refresh_streams(limit=5)
    assert intake._streams == []
    assert intake._stream_cycle_complete is True

    intake._refresh_streams(limit=5)

    assert set(intake.streams(limit=5)) == {ordered[5], ordered[6]}
    assert redis.srem_calls == [("reservation-stream-registry", tuple(ordered[:5]))]
    assert redis.registry == {ordered[5], ordered[6]}


class CompactionFailureRedis(FakeRedis):
    def srem(self, _key, *_streams):
        raise RedisError("registry unavailable")


def test_registry_compaction_failure_keeps_nonempty_stream_eligible():
    streams = ["reservation-stream:{empty}", "reservation-stream:{active}"]
    lengths = {streams[0]: 0, streams[1]: 1}
    redis = CompactionFailureRedis(streams, lengths=lengths)
    intake = RedisReservationIntake(
        FakeCache(redis),
        stream_batch_size=2,
        stream_refresh_seconds=60,
    )

    intake._refresh_streams(limit=2)

    assert intake.streams(limit=2) == [streams[1]]


def test_empty_discovery_throttles_registry_refresh_and_scan_repair():
    redis = FakeRedis()
    intake = RedisReservationIntake(
        FakeCache(redis),
        stream_batch_size=2,
        stream_refresh_seconds=10,
        stream_scan_refresh_seconds=60,
    )

    assert list(intake.messages("writer")) == []
    assert list(intake.messages("writer")) == []
    assert redis.smembers_calls == 1
    assert redis.scan_calls == 1

    intake._last_stream_refresh -= 11
    assert list(intake.messages("writer")) == []
    assert redis.smembers_calls == 2
    assert redis.scan_calls == 1

    intake._last_stream_refresh -= 11
    intake._last_scan_refresh -= 61
    assert list(intake.messages("writer")) == []
    assert redis.smembers_calls == 3
    assert redis.scan_calls == 2

    intake.reset_connection_state()
    assert list(intake.messages("writer")) == []
    assert redis.scan_calls == 3


@pytest.mark.parametrize("failure", [None, "remove", "recheck", "restore"])
def test_prune_keeps_concurrent_or_uncertain_work_eligible(monkeypatch, failure):
    stream = "reservation-stream:{concurrent}"
    redis = FakeRedis([stream], lengths={stream: 0})
    intake = RedisReservationIntake(FakeCache(redis), stream_refresh_seconds=60)
    original_remove, original_pipeline = redis.srem, redis.pipeline
    calls = []

    def concurrent_remove(key, *streams):
        # New successful enqueue arrives after the first XLEN snapshot.
        redis.lengths[stream] = 1
        redis.sadd(key, stream)
        original_remove(key, *streams)
        if failure == "remove":
            raise RedisError("ambiguous removal")

    def pipeline(**kwargs):
        result = original_pipeline(**kwargs)
        calls.append(result)
        if len(calls) == 2 and failure == "recheck":
            def unavailable():
                raise RedisError("length recheck unavailable")
            result.execute = unavailable
        return result

    monkeypatch.setattr(redis, "srem", concurrent_remove)
    monkeypatch.setattr(redis, "pipeline", pipeline)
    if failure == "restore":
        def unavailable_sadd(_key, *streams):
            # Simulate successful enqueue's SADD, followed by failed writer repair.
            if not calls or len(calls) == 1:
                redis.registry.update(streams)
            else:
                raise RedisError("registry restore unavailable")
        monkeypatch.setattr(redis, "sadd", unavailable_sadd)

    intake._refresh_streams(limit=1)
    assert intake.streams(limit=1) == [stream]
    assert len(calls) == 2
    assert calls[1].streams == [stream]
    if failure != "restore":
        assert stream in redis.registry
    assert list(intake.messages("writer")) == []
    assert redis.read_batches == [(stream,)]



def test_enqueue_after_zero_recheck_remains_registered_for_next_refresh(monkeypatch):
    stream = "reservation-stream:{late}"
    redis = FakeRedis([stream], lengths={stream: 0})
    intake = RedisReservationIntake(FakeCache(redis), stream_refresh_seconds=60)
    original_pipeline = redis.pipeline
    calls = []

    def pipeline(**kwargs):
        result = original_pipeline(**kwargs)
        calls.append(result)
        if len(calls) == 2:
            original_execute = result.execute

            def enqueue_after_read():
                lengths = original_execute()
                redis.lengths[stream] = 1
                redis.sadd("reservation-stream-registry", stream)
                return lengths

            result.execute = enqueue_after_read
        return result

    monkeypatch.setattr(redis, "pipeline", pipeline)
    intake._refresh_streams(limit=1)
    assert intake._streams == []
    assert stream in redis.registry
    intake._last_stream_refresh = 0
    assert intake.streams(limit=1) == [stream]

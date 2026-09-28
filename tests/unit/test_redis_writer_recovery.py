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
        stream_refresh_seconds=60,
    )

    intake._refresh_streams(limit=5)
    assert intake._streams == []
    assert intake._stream_cycle_complete is True

    intake._refresh_streams(limit=5)

    assert intake.streams(limit=5) == [ordered[5], ordered[6]]
    assert intake._stream_window_cursor == 3

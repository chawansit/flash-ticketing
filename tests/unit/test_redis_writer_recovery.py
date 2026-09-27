from ticketing.infrastructure.redis_reservations import RedisReservationIntake


class FakePool:
    def __init__(self):
        self.disconnects = 0

    def disconnect(self):
        self.disconnects += 1


class FakeRedis:
    def __init__(self, registry=()):
        self.registry = set(registry)
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
        ordered[0:2],
        ordered[2:4],
        (ordered[4], ordered[0]),
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

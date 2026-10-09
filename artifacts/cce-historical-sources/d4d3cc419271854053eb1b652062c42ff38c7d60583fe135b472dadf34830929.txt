import sys
from pathlib import Path

from redis.exceptions import ResponseError

sys.path.insert(0, str(Path(__file__).parents[2] / "scripts"))
from capacity_queue_state import reservation_queue_state


class FakePipeline:
    def __init__(self, redis):
        self.redis = redis
        self.commands = []

    def xlen(self, stream):
        self.commands.append(("xlen", stream))
        return self

    def xpending(self, stream, group):
        self.commands.append(("xpending", stream, group))
        return self

    def execute(self, *, raise_on_error):
        assert raise_on_error is False
        self.redis.execute_calls += 1
        results = []
        for command in self.commands:
            if command[0] == "xlen":
                results.append(self.redis.lengths[command[1]])
            elif command[1] == "stream-without-group":
                results.append(ResponseError("NOGROUP no such key or consumer group"))
            else:
                results.append({"pending": self.redis.pending[command[1]]})
        return results


class FakeRedis:
    def __init__(self):
        self.lengths = {"stream-a": 3, "stream-b": 2, "stream-without-group": 0}
        self.pending = {"stream-a": 1, "stream-b": 2}
        self.execute_calls = 0
        self.scan_calls = 0

    def scan(self, *, cursor, match, count):
        assert match == "reservation-stream:*"
        assert count == 1000
        self.scan_calls += 1
        if cursor == 0:
            return 7, ["stream-a", "stream-b"]
        return 0, ["stream-without-group"]

    def pipeline(self, *, transaction):
        assert transaction is False
        return FakePipeline(self)


def test_reservation_queue_state_counts_entries_and_pending_in_bounded_pipelines():
    redis = FakeRedis()
    assert reservation_queue_state(redis, batch_size=2) == (5, 3)
    assert redis.scan_calls == 2
    assert redis.execute_calls == 2

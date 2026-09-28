import sys
from pathlib import Path

from redis.exceptions import ResponseError

sys.path.insert(0, str(Path(__file__).parents[2] / "scripts"))
from capacity_queue_state import reservation_queue_state


class FakeRedis:
    def scan_iter(self, **_kwargs):
        return iter(("stream-a", "stream-b", "stream-without-group"))

    def xlen(self, stream):
        return {"stream-a": 3, "stream-b": 2, "stream-without-group": 0}[stream]

    def xpending(self, stream, _group):
        if stream == "stream-without-group":
            raise ResponseError("NOGROUP no such key or consumer group")
        return {"pending": {"stream-a": 1, "stream-b": 2}[stream]}


def test_reservation_queue_state_counts_entries_and_pending_across_streams():
    assert reservation_queue_state(FakeRedis()) == (5, 3)

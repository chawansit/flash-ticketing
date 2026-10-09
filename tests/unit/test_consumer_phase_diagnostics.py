from types import SimpleNamespace

import pytest
from kafka import TopicPartition

from ticketing import observability, workers


class Consumer:
    def __init__(self, fail_commit=False):
        self.calls = []
        self.fail_commit = fail_commit
        self.batches = {TopicPartition("ticketing.events", p): [SimpleNamespace(offset=10+p)]
                        for p in (0, 1)}

    def poll(self, **kwargs):
        self.calls.append(("poll", kwargs))
        return self.batches

    def commit(self):
        self.calls.append(("commit",))
        if self.fail_commit:
            raise RuntimeError("broker commit failed")

    def seek(self, tp, offset):
        self.calls.append(("seek", tp.partition, offset))


def test_commit_follows_all_partition_handlers(monkeypatch):
    c = Consumer()
    monkeypatch.setattr(workers, "consume_kafka_messages",
                        lambda db, cache, msgs: c.calls.append(("handled", msgs[0].offset)))
    assert workers.consume_iteration(c, None, None, 100)
    assert c.calls == [("poll", {"timeout_ms": 500, "max_records": 100}),
                       ("handled", 10), ("handled", 11), ("commit",)]


def test_handler_failure_rewinds_every_start_and_does_not_commit(monkeypatch):
    c = Consumer()

    def handle(db, cache, messages):
        if messages[0].offset == 11:
            raise RuntimeError("durable handler failed")

    monkeypatch.setattr(workers, "consume_kafka_messages", handle)
    with pytest.raises(RuntimeError, match="durable handler failed"):
        workers.consume_iteration(c, None, None, 100)
    assert c.calls[-2:] == [("seek", 0, 10), ("seek", 1, 11)]
    assert ("commit",) not in c.calls


def test_commit_failure_propagates_after_handlers(monkeypatch):
    c = Consumer(fail_commit=True)
    monkeypatch.setattr(workers, "consume_kafka_messages", lambda *_: None)
    with pytest.raises(RuntimeError, match="broker commit failed"):
        workers.consume_iteration(c, None, None, 100)
    assert c.calls[-1] == ("commit",)
    assert not any(row[0] == "seek" for row in c.calls)


def test_empty_poll_does_not_commit(monkeypatch):
    c = Consumer()
    c.batches = {}
    monkeypatch.setattr(workers, "consume_kafka_messages", lambda *_: pytest.fail("unexpected handler"))
    assert not workers.consume_iteration(c, None, None, 100)
    assert len(c.calls) == 1


def test_error_timings_preserve_exception_and_bound_labels(monkeypatch):
    ticks = iter((10, 10.25))
    monkeypatch.setattr(observability, "monotonic", lambda: next(ticks))
    sample = observability.CONSUMER_PHASE_SECONDS.labels("event_other", "other", "error")
    before = sample._sum.get()
    with (pytest.raises(RuntimeError, match="unchanged"),
          observability.consumer_phase("private-event-id", "private-partition")):
        raise RuntimeError("unchanged")
    assert sample._sum.get() - before == .25
    labels = [row.labels for metric in observability.CONSUMER_PHASE_SECONDS.collect()
              for row in metric.samples]
    assert all(row['phase'] != 'private-event-id' and row['partition'] != 'private-partition'
               for row in labels)

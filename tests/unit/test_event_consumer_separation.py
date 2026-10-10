"""ADR0265: separate offset progress without changing durable business handlers."""
import json
from dataclasses import replace
from types import SimpleNamespace
from unittest.mock import Mock
from uuid import uuid4

import pytest
from kafka import TopicPartition

from ticketing import workers
from ticketing.config import Settings


def envelope(kind):
    return {"schema_version": 1, "event_id": str(uuid4()), "event_type": kind, "payload": {}}


@pytest.mark.parametrize("separated", [False, True])
def test_group_identity_and_offset_configuration(monkeypatch, separated):
    settings = replace(Settings(), event_consumer_separation=separated)
    constructor = Mock()
    monkeypatch.setattr(workers, "KafkaConsumer", constructor)
    workers.create_event_consumer(settings, "consumer")
    args, kwargs = constructor.call_args
    assert args == (workers.EVENT_TOPIC,)
    assert kwargs["group_id"] == workers.FULFILLMENT_GROUP
    assert kwargs["enable_auto_commit"] is False
    assert kwargs["auto_offset_reset"] == "earliest"
    assert kwargs["max_poll_records"] == settings.consumer_batch_size
    assert workers.event_consumer_identity(settings, "consumer")[1] == (
        "fulfillment" if separated else "mixed"
    )
    if separated:
        workers.create_event_consumer(settings, "projection-consumer")
        assert constructor.call_args.kwargs["group_id"] == workers.PROJECTION_GROUP
        assert workers.PROJECTION_GROUP != workers.FULFILLMENT_GROUP
    else:
        with pytest.raises(RuntimeError, match="EVENT_CONSUMER_SEPARATION"):
            workers.create_event_consumer(settings, "projection-consumer")
        assert constructor.call_count == 1


@pytest.mark.parametrize("lane,kinds", [
    ("mixed", ["SeatsChanged", "OrderPaid", "TicketsIssued", "RefundRequested"]),
    ("fulfillment", ["OrderPaid", "TicketsIssued", "RefundRequested"]),
    ("projection", ["SeatsChanged"]),
])
def test_disjoint_lanes_keep_business_order(monkeypatch, lane, kinds):
    handler = Mock()
    projector = object()
    monkeypatch.setattr(workers, "consume_events", handler)
    batch = [envelope(k) for k in ["SeatsChanged", "OrderPaid", "TicketsIssued", "RefundRequested"]]
    workers.consume_lane_events(None, None, batch, projector, lane=lane)
    assert [e["event_type"] for e in handler.call_args.args[2]] == kinds
    assert handler.call_args.kwargs["order_status_projector"] is projector


@pytest.mark.parametrize("lane,kind", [("fulfillment", "SeatsChanged"), ("projection", "OrderPaid")])
def test_unselected_records_do_no_database_or_cache_work(monkeypatch, lane, kind):
    handler = Mock(side_effect=AssertionError("foreign-lane side effect"))
    monkeypatch.setattr(workers, "consume_events", handler)
    workers.consume_lane_events(object(), object(), [envelope(kind)], lane=lane)
    handler.assert_not_called()


@pytest.mark.parametrize("lane", ["fulfillment", "projection"])
@pytest.mark.parametrize("bad", [None, {}, {"schema_version": 2, "event_type": "SeatsChanged"},
                                 {"schema_version": 1, "event_type": "unknown"}])
def test_unsupported_envelopes_cannot_be_silently_skipped(lane, bad):
    with pytest.raises(ValueError):
        workers.consume_lane_events(None, None, [bad], lane=lane)


@pytest.mark.parametrize("lane", ["fulfillment", "projection"])
def test_poison_fallback_keeps_lane_routing(monkeypatch, lane):
    calls, dead = [], []
    monkeypatch.setattr(workers.time, "sleep", lambda _: None)
    def process(db, cache, batch):
        calls.extend(e["event_type"] for e in batch)
        if any(e["payload"].get("poison") for e in batch):
            raise ValueError("selected poison")
    monkeypatch.setattr(workers, "consume_events", process)
    monkeypatch.setattr(workers, "dead_letter_message", lambda db, message: dead.append(message))
    selected = "OrderPaid" if lane == "fulfillment" else "SeatsChanged"
    foreign = "SeatsChanged" if lane == "fulfillment" else "OrderPaid"
    poison = envelope(selected)
    poison["payload"]["poison"] = True
    messages = [SimpleNamespace(value=json.dumps(e).encode()) for e in [poison, envelope(foreign)]]
    workers.consume_kafka_messages(None, None, messages, lane=lane)
    assert calls == [selected] * 6  # Five bounded attempts and one selected fallback.
    assert dead == [messages[0]]


class Consumer:
    def __init__(self, batch):
        self.batch = {TopicPartition(workers.EVENT_TOPIC, 0): batch}
        self.committed = False
        self.rewinds = []
    def poll(self, **kwargs):
        return self.batch
    def commit(self):
        self.committed = True
    def seek(self, tp, offset):
        self.rewinds.append((tp, offset))


def test_fulfillment_commits_independently_of_projection_failure(monkeypatch):
    batch = [SimpleNamespace(value=json.dumps(envelope("SeatsChanged")).encode(), offset=10),
             SimpleNamespace(value=json.dumps(envelope("OrderPaid")).encode(), offset=11)]
    fulfillment, projection = Consumer(batch), Consumer(batch)
    def handler(db, cache, messages, *, lane):
        if lane == "projection":
            raise RuntimeError("refresh cannot finish durable handling")
    monkeypatch.setattr(workers, "consume_kafka_messages", handler)
    assert workers.consume_iteration(fulfillment, None, None, 100, lane="fulfillment")
    with pytest.raises(RuntimeError, match="refresh cannot finish"):
        workers.consume_iteration(projection, None, None, 100, lane="projection")
    assert fulfillment.committed and not projection.committed
    assert projection.rewinds == [(TopicPartition(workers.EVENT_TOPIC, 0), 10)]
    assert not fulfillment.rewinds


def test_invalid_lane_fails_before_poll_or_retry(monkeypatch):
    poller = Mock()
    sleeper = Mock()
    monkeypatch.setattr(workers.time, "sleep", sleeper)
    with pytest.raises(ValueError, match="lane"):
        workers.consume_iteration(poller, None, None, 100, lane="typo")
    poller.poll.assert_not_called()
    with pytest.raises(ValueError, match="lane"):
        workers.consume_kafka_messages(None, None, [], lane="typo")
    sleeper.assert_not_called()


def test_default_remains_mixed():
    assert Settings().event_consumer_separation is False

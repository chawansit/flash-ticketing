"""Bound only the projection lane without changing established consumers."""
from dataclasses import replace
from unittest.mock import Mock

from ticketing import workers
from ticketing.config import Settings


def test_projection_frame_and_fetch_budgets_are_explicit(monkeypatch):
    constructor = Mock()
    monkeypatch.setattr(workers, "KafkaConsumer", constructor)
    settings = replace(Settings(), event_consumer_separation=True)
    workers.create_event_consumer(settings, "projection-consumer")
    values = constructor.call_args.kwargs
    assert values["receive_message_max_bytes"] == 8 * 1024 * 1024
    assert values["fetch_max_bytes"] == 1024 * 1024
    assert values["max_poll_records"] == settings.consumer_batch_size
    for separated in (False, True):
        workers.create_event_consumer(replace(settings, event_consumer_separation=separated), "consumer")
        assert "receive_message_max_bytes" not in constructor.call_args.kwargs
        assert "fetch_max_bytes" not in constructor.call_args.kwargs

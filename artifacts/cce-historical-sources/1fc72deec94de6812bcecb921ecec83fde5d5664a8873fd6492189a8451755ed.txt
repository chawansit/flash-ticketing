from contextlib import contextmanager
from dataclasses import replace
from datetime import UTC, datetime
from unittest.mock import Mock, patch
from uuid import uuid4

import pytest
from redis.exceptions import TimeoutError

from ticketing.config import Settings
from ticketing.infrastructure.order_status_cache import RedisOrderStatusCache
from ticketing.infrastructure.order_status_projector import CommittedOrderStatusProjector
from ticketing.workers import consume_event


def row():
    return {"id": uuid4(), "actor": "actual-owner", "hold_id": uuid4(), "event_id": uuid4(),
            "total": 100, "currency": "THB", "status": "FULFILLED", "created_at": datetime.now(UTC),
            "_ticket_id": uuid4(), "_ticket_seat_id": "A"}


def test_projector_releases_connection_and_derives_owner_from_committed_row():
    order, connection, cache = row(), Mock(), Mock()
    connection.execute.return_value.fetchall.return_value = [order]
    held = []

    @contextmanager
    def transaction():
        held.append(True)
        try:
            yield connection
        finally:
            held.pop()

    cache.snapshot_start.return_value = 1000
    cache.publish.side_effect = lambda *_: pytest.fail("held DB connection") if held else None
    database = Mock(transaction=transaction)
    CommittedOrderStatusProjector(database, cache).refresh(str(order["id"]))
    actor, identifier, snapshot, stamp = cache.publish.call_args.args
    assert actor == "actual-owner" and identifier == order["id"] and stamp == 1000
    assert snapshot["tickets"] == [{"id": order["_ticket_id"], "seat_id": "A"}]
    assert "_ticket_id" not in snapshot
    assert connection.execute.call_args.args[1] == (order["id"],)


@pytest.mark.parametrize("phase", ["clock", "read", "publication"])
def test_advisory_errors_do_not_escape_after_commit(phase):
    order, connection, cache = row(), Mock(), Mock()
    connection.execute.return_value.fetchall.return_value = [order]
    database = Mock()
    database.transaction.return_value.__enter__ = Mock(return_value=connection)
    database.transaction.return_value.__exit__ = Mock(return_value=False)
    if phase == "clock":
        cache.snapshot_start.side_effect = TimeoutError("clock unavailable")
    elif phase == "read":
        connection.execute.side_effect = RuntimeError("snapshot unavailable")
    else:
        cache.publish.side_effect = TimeoutError("publication unavailable")
    CommittedOrderStatusProjector(database, cache).refresh(order["id"])
    if phase == "clock":
        database.transaction.assert_not_called()
    if phase != "publication":
        cache.publish.assert_not_called()


def test_missing_or_invalid_order_does_not_publish():
    connection, cache, database = Mock(), Mock(), Mock()
    connection.execute.return_value.fetchall.return_value = []
    database.transaction.return_value.__enter__ = Mock(return_value=connection)
    database.transaction.return_value.__exit__ = Mock(return_value=False)
    projector = CommittedOrderStatusProjector(database, cache)
    projector.refresh("bad-id")
    database.transaction.assert_not_called()
    projector.refresh(uuid4())
    cache.publish.assert_not_called()


@pytest.mark.parametrize("event_type", ["OrderPaid", "TicketsIssued", "RefundRequested"])
def test_publication_runs_only_after_successful_financial_boundary_including_replay(event_type):
    database, cache, projector, identifier = Mock(), Mock(), Mock(), uuid4()
    envelope = {"schema_version": 1, "event_type": event_type, "payload": {"order_id": str(identifier)}}
    committed = []
    projector.refresh.side_effect = lambda *_: committed.append("refresh")
    with patch("ticketing.workers._consume_event_transaction", side_effect=lambda *_: committed.append("commit")):
        consume_event(database, cache, envelope, projector)
        consume_event(database, cache, envelope, projector)
    assert committed == ["commit", "refresh", "commit", "refresh"]


def test_financial_commit_failure_propagates_without_advisory_publication():
    projector = Mock()
    envelope = {"event_type": "OrderPaid", "payload": {"order_id": str(uuid4())}}
    with (
        patch("ticketing.workers._consume_event_transaction", side_effect=RuntimeError("commit failure")),
        pytest.raises(RuntimeError, match="commit failure"),
    ):
        consume_event(Mock(), Mock(), envelope, projector)
    projector.refresh.assert_not_called()


def test_default_and_seat_events_have_no_projector_work():
    projector = Mock()
    envelope = {"event_type": "SeatsChanged", "payload": {}}
    with patch("ticketing.workers._consume_event_transaction"):
        consume_event(Mock(), Mock(), envelope, projector)
        consume_event(Mock(), Mock(), {"event_type": "OrderPaid", "payload": {}})
    projector.refresh.assert_not_called()


def test_event_refresh_requires_enabled_bounded_cache():
    assert not Settings().order_status_event_refresh
    with pytest.raises(RuntimeError, match="ORDER_STATUS_EVENT_REFRESH"):
        replace(Settings(), order_status_event_refresh=True, order_status_cache_ms=0).validate()
    replace(Settings(), order_status_event_refresh=True, order_status_cache_ms=1000).validate()


def test_publish_rejects_noninteger_stamp_and_wrong_actor_before_redis():
    cache = RedisOrderStatusCache(Mock(), 1000)
    order = row()
    order.pop("_ticket_id")
    order.pop("_ticket_seat_id")
    order.update(status="PENDING", tickets=[])
    cache.publish(order["actor"], order["id"], order, True)
    cache.publish("other", order["id"], order, 1000)
    cache.redis.eval.assert_not_called()

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
    projector.refresh.side_effect = lambda *_, **__: committed.append("refresh")
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


def dedup_fixture():
    import json

    from ticketing.infrastructure.order_status_cache import scalar
    order, connection, cache, database = row(), Mock(), Mock(), Mock()
    connection.execute.return_value.fetchall.return_value = [order]
    database.transaction.return_value.__enter__ = Mock(return_value=connection)
    database.transaction.return_value.__exit__ = Mock(return_value=False)
    cache.max_age_ms, cache.snapshot_start.return_value = 1000, 1000
    cache.publish.return_value = True
    snapshot = {k: v for k, v in order.items() if k not in {"_ticket_id", "_ticket_seat_id"}}
    snapshot["tickets"] = [{"id": order["_ticket_id"], "seat_id": "A"}]
    snapshot = json.loads(json.dumps(snapshot, default=scalar))
    cache.lookup.return_value = (snapshot, None)
    return order, connection, cache, database


@pytest.mark.parametrize("enabled,reads", [(False, 2), (True, 1)])
def test_fresh_successful_orderpaid_ticketsissued_pair_deduplicates_only_when_enabled(enabled, reads):
    order, _, cache, database = dedup_fixture()
    projector = CommittedOrderStatusProjector(database, cache, deduplicate=enabled)
    with patch("ticketing.infrastructure.order_status_projector.time.monotonic", return_value=10):
        projector.refresh(order["id"], event_type="OrderPaid")
        projector.refresh(order["id"], event_type="TicketsIssued")
    assert database.transaction.call_count == reads
    assert cache.publish.call_count == reads
    if enabled:
        cache.lookup.assert_called_once_with("actual-owner", order["id"])
    else:
        cache.lookup.assert_not_called()


@pytest.mark.parametrize("publication", [False, None, 1])
def test_only_explicit_successful_publication_establishes_hint(publication):
    order, _, cache, database = dedup_fixture()
    cache.publish.return_value = publication
    projector = CommittedOrderStatusProjector(database, cache, deduplicate=True)
    projector.refresh(order["id"], event_type="OrderPaid")
    projector.refresh(order["id"], event_type="TicketsIssued")
    assert database.transaction.call_count == 2 and not projector._recent
    cache.lookup.assert_not_called()


@pytest.mark.parametrize("mutation", ["missing", "owner", "identifier", "status", "tickets", "total", "error"])
def test_missing_changed_or_failed_cache_lookup_reads_current_database(mutation):
    order, _, cache, database = dedup_fixture()
    projector = CommittedOrderStatusProjector(database, cache, deduplicate=True)
    projector.refresh(order["id"], event_type="OrderPaid")
    snapshot = cache.lookup.return_value[0]
    if mutation == "missing":
        cache.lookup.return_value = (None, None)
    elif mutation == "owner":
        snapshot["actor"] = "other"
    elif mutation == "identifier":
        snapshot["id"] = str(uuid4())
    elif mutation == "status":
        snapshot.update(status="REFUND_PENDING", tickets=[])
    elif mutation == "tickets":
        snapshot["tickets"][0]["id"] = str(uuid4())
    elif mutation == "total":
        snapshot["total"] += 1
    else:
        cache.lookup.side_effect = TimeoutError("lookup unavailable")
    projector.refresh(order["id"], event_type="TicketsIssued")
    assert database.transaction.call_count == 2 and not projector._recent


@pytest.mark.parametrize("late_lookup", [False, True])
def test_expired_hint_or_lookup_finishing_after_deadline_cannot_skip(late_lookup):
    order, _, cache, database = dedup_fixture()
    projector = CommittedOrderStatusProjector(database, cache, deduplicate=True)
    clock = [10.0]
    with patch("ticketing.infrastructure.order_status_projector.time.monotonic", side_effect=lambda: clock[0]):
        projector.refresh(order["id"], event_type="OrderPaid")
        if late_lookup:
            snapshot = cache.lookup.return_value
            def delayed(*_):
                clock[0] = 11.0
                return snapshot
            cache.lookup.side_effect = delayed
        else:
            clock[0] = 11.0
        projector.refresh(order["id"], event_type="TicketsIssued")
    assert database.transaction.call_count == 2 and not projector._recent


def test_skips_do_not_extend_hint_or_publish_ttl_and_restart_reads_again():
    order, _, cache, database = dedup_fixture()
    projector = CommittedOrderStatusProjector(database, cache, deduplicate=True)
    with patch("ticketing.infrastructure.order_status_projector.time.monotonic", return_value=10):
        projector.refresh(order["id"], event_type="OrderPaid")
        original = dict(projector._recent)
        for _ in range(3):
            projector.refresh(order["id"], event_type="TicketsIssued")
        assert dict(projector._recent) == original
        assert database.transaction.call_count == 1 and cache.publish.call_count == 1
        CommittedOrderStatusProjector(database, cache, deduplicate=True).refresh(order["id"], event_type="TicketsIssued")
    assert database.transaction.call_count == 2


def test_refund_invalidates_hint_and_orderpaid_replay_always_reads():
    order, _, cache, database = dedup_fixture()
    projector = CommittedOrderStatusProjector(database, cache, deduplicate=True)
    projector.refresh(order["id"], event_type="OrderPaid")
    projector.refresh(order["id"], event_type="OrderPaid")
    assert database.transaction.call_count == 2
    projector.refresh(order["id"], event_type="RefundRequested")
    assert not projector._recent
    projector.refresh(order["id"], event_type="TicketsIssued")
    assert database.transaction.call_count == 4


def test_failed_republication_clears_prior_hint_and_replay_can_repair():
    order, _, cache, database = dedup_fixture()
    projector = CommittedOrderStatusProjector(database, cache, deduplicate=True)
    projector.refresh(order["id"], event_type="OrderPaid")
    cache.publish.side_effect = TimeoutError("publication unavailable")
    projector.refresh(order["id"], event_type="OrderPaid")
    assert not projector._recent
    cache.publish.side_effect = None
    projector.refresh(order["id"], event_type="TicketsIssued")
    assert database.transaction.call_count == 3


def test_hint_count_and_expiry_cleanup_are_bounded():
    order, connection, cache, database = dedup_fixture()
    projector = CommittedOrderStatusProjector(database, cache, deduplicate=True)
    identifiers = [uuid4() for _ in range(3)]
    with (patch("ticketing.infrastructure.order_status_projector.MAX_HINTS", 2),
          patch("ticketing.infrastructure.order_status_projector.time.monotonic", return_value=10)):
        for identifier in identifiers:
            connection.execute.return_value.fetchall.return_value = [{**order, "id": identifier}]
            projector.refresh(identifier, event_type="OrderPaid")
        assert list(projector._recent) == identifiers[1:]
    with patch("ticketing.infrastructure.order_status_projector.time.monotonic", return_value=12):
        identifier = uuid4()
        connection.execute.return_value.fetchall.return_value = [{**order, "id": identifier}]
        projector.refresh(identifier, event_type="OrderPaid")
        assert list(projector._recent) == [identifier]


def test_dedup_setting_requires_event_refresh_and_defaults_off():
    assert not Settings().order_status_event_refresh_dedup
    with pytest.raises(RuntimeError, match="ORDER_STATUS_EVENT_REFRESH_DEDUP"):
        replace(Settings(), order_status_event_refresh_dedup=True).validate()
    replace(Settings(), order_status_event_refresh=True, order_status_event_refresh_dedup=True,
            order_status_cache_ms=1000).validate()
    with pytest.raises(TypeError, match="boolean"):
        CommittedOrderStatusProjector(Mock(), Mock(), deduplicate=1)


@pytest.mark.parametrize("result,success", [(1, True), (0, False), (-1, False)])
def test_publish_returns_success_only_when_redis_accepts_snapshot(result, success):
    import json

    from ticketing.infrastructure.order_status_cache import scalar
    order, _, _, _ = dedup_fixture()
    snapshot = {k: v for k, v in order.items() if k not in {"_ticket_id", "_ticket_seat_id"}}
    snapshot["tickets"] = [{"id": order["_ticket_id"], "seat_id": "A"}]
    snapshot = json.loads(json.dumps(snapshot, default=scalar))
    redis = Mock()
    redis.eval.return_value = result
    assert RedisOrderStatusCache(redis, 1000).publish("actual-owner", order["id"], snapshot, 1000) is success

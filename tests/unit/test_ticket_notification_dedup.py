"""ADR0322: notification hints do not provide or extend customer cache freshness."""
from dataclasses import replace
from datetime import UTC, datetime
from unittest.mock import Mock, patch
from uuid import uuid4

import pytest

from ticketing.config import Settings
from ticketing.infrastructure.order_status_projector import CommittedOrderStatusProjector


def scenario(**options):
    order = {"id": str(uuid4()), "actor": "owner", "hold_id": str(uuid4()),
             "event_id": str(uuid4()), "total": 100, "currency": "THB", "status": "FULFILLED",
             "created_at": datetime.now(UTC).isoformat(),
             "tickets": [{"id": str(uuid4()), "seat_id": "A"}]}
    db, cache = Mock(), Mock(max_age_ms=1000)
    cache.publish.return_value = True
    projector = CommittedOrderStatusProjector(db, cache, deduplicate=True,
                                            capture_committed=True, **options)
    return order, db, cache, projector


def test_delayed_notification_skips_only_refresh_without_republishing_or_cache_lookup():
    order, db, cache, projector = scenario(notification_dedup=True)
    clock = [10.0]
    with patch("ticketing.infrastructure.order_status_projector.time.monotonic", side_effect=lambda: clock[0]):
        projector.publish_committed(order, 1000, 10)
        deadline = dict(projector._published)
        clock[0] = 16  # Customer snapshot and old fresh-reuse hint have expired.
        for _ in range(3):
            projector.refresh(order["id"], event_type="TicketsIssued")
        assert dict(projector._published) == deadline
    db.transaction.assert_not_called()
    cache.lookup.assert_not_called()
    assert cache.publish.call_count == 1


@pytest.mark.parametrize("publication", [False, None, 1])
def test_unsuccessful_publication_does_not_establish_notification_hint(publication):
    order, db, cache, projector = scenario(notification_dedup=True)
    cache.publish.return_value = publication
    projector.publish_committed(order, 1000, 10)
    assert not projector._published
    with patch.object(projector, "_can_reuse", return_value=False):
        projector.refresh(order["id"], event_type="TicketsIssued")
    db.transaction.assert_called_once()


def test_deadline_and_restart_restore_database_fallback():
    order, db, cache, projector = scenario(notification_dedup=True)
    with patch("ticketing.infrastructure.order_status_projector.time.monotonic", return_value=10):
        projector.publish_committed(order, 1000, 10)
    with patch("ticketing.infrastructure.order_status_projector.time.monotonic", return_value=40):
        projector.refresh(order["id"], event_type="TicketsIssued")
    assert not projector._published
    db.transaction.assert_called_once()
    restarted = CommittedOrderStatusProjector(db, cache, deduplicate=True,
                                             capture_committed=True, notification_dedup=True)
    restarted.refresh(order["id"], event_type="TicketsIssued")
    assert db.transaction.call_count == 2


@pytest.mark.parametrize("event_type", ["RefundRequested", "OrderPaid"])
def test_state_changing_event_invalidates_notification_hint(event_type):
    order, db, _, projector = scenario(notification_dedup=True)
    with patch("ticketing.infrastructure.order_status_projector.time.monotonic", return_value=10):
        projector.publish_committed(order, 1000, 10)
        projector.refresh(order["id"], event_type=event_type)
        assert not projector._published
        projector.refresh(order["id"], event_type="TicketsIssued")
    assert db.transaction.call_count == 2


def test_new_capture_clears_previous_publication_before_replay_or_failure():
    order, _, cache, projector = scenario(notification_dedup=True)
    cache.snapshot_start.return_value = 1000
    projector.publish_committed(order, 1000, 10)
    assert projector._published
    projector.begin_capture(order["id"])
    assert not projector._published


def test_notification_table_is_bounded_and_eviction_restores_fallback():
    order, db, _, projector = scenario(notification_dedup=True)
    with (patch("ticketing.infrastructure.order_status_projector.MAX_HINTS", 2),
          patch("ticketing.infrastructure.order_status_projector.time.monotonic", return_value=10)):
        projector.publish_committed(order, 1000, 10)
        for _ in range(2):
            projector.publish_committed({**order, "id": str(uuid4())}, 1000, 10)
        assert len(projector._published) == 2
        assert all(str(identifier) != order["id"] for identifier in projector._published)
        projector.refresh(order["id"], event_type="TicketsIssued")
    db.transaction.assert_called_once()


def test_flag_defaults_off_and_requires_committed_capture_and_dedup():
    assert Settings().order_status_notification_dedup is False
    for missing in ("order_status_committed_snapshot", "order_status_event_refresh_dedup"):
        settings = replace(Settings(), order_status_cache_ms=1000, order_status_event_refresh=True,
                           order_status_event_refresh_dedup=True, order_status_committed_snapshot=True,
                           order_status_notification_dedup=True)
        with pytest.raises(RuntimeError, match="NOTIFICATION_DEDUP"):
            replace(settings, **{missing: False}).validate()
    with pytest.raises(TypeError):
        CommittedOrderStatusProjector(Mock(), Mock(), notification_dedup=1)
    with pytest.raises(ValueError):
        CommittedOrderStatusProjector(Mock(), Mock(), notification_dedup=True)


def test_failed_financial_commit_never_establishes_notification_suppression():
    from test_committed_fulfillment_status import setup

    from ticketing.workers import consume_event
    db, cache, _, envelope = setup(commit_error=True)
    projector = CommittedOrderStatusProjector(db, cache, deduplicate=True,
                                             capture_committed=True, notification_dedup=True)
    with pytest.raises(RuntimeError, match="commit failed"):
        consume_event(db, None, envelope, projector)
    assert not projector._published
    cache.publish.assert_not_called()

"""ADR0322: real PostgreSQL/Redis expiry, authorization and durable replay."""
import time
from unittest.mock import patch
from uuid import uuid4

import pytest
from test_order_status_event_refresh import drain_outbox, event_cache, prepare  # noqa: F401

from ticketing.domain import Failure
from ticketing.infrastructure.order_status_projector import CommittedOrderStatusProjector
from ticketing.workers import consume_event

pytestmark = pytest.mark.integration


@pytest.mark.parametrize("damage", ["expired", "deleted", "corrupt"])
def test_delayed_notification_keeps_expired_cache_and_authorized_durable_tickets(system, event_cache, damage):  # noqa: F811
    svc, db, hold, cache, _, _, envelope = prepare(system, event_cache, age=100)
    projector = CommittedOrderStatusProjector(db, cache, deduplicate=True,
                                             capture_committed=True, notification_dedup=True)
    issued = {"event_id": str(uuid4()), "schema_version": 1, "event_type": "TicketsIssued",
              "payload": {"order_id": hold["order_id"], "actor": "other"}}
    with (patch.object(db, "transaction", wraps=db.transaction) as transactions,
          patch.object(cache, "publish", wraps=cache.publish) as publish):
        consume_event(db, None, envelope, projector)
        assert transactions.call_count == 1 and publish.call_count == 1
        assert projector._published
        key = cache.key("owner", hold["order_id"])
        if damage == "expired":
            time.sleep(max(0, cache.redis.pttl(key))/1000+.02)
        elif damage == "deleted":
            cache.redis.delete(key)
        else:
            cache.redis.set(key, "invalid", px=1000)
        consume_event(db, None, issued, projector)
        consume_event(db, None, issued, projector)
        assert transactions.call_count == 3 and publish.call_count == 1
        assert cache.lookup("owner", hold["order_id"])[0] is None
    durable = svc.get_order("owner", hold["order_id"])
    assert durable["status"] == "FULFILLED" and len(durable["tickets"]) == 1
    with pytest.raises(Failure) as denied:
        svc.get_order("other", hold["order_id"])
    assert denied.value.status == 404
    restarted = CommittedOrderStatusProjector(db, cache, deduplicate=True,
                                             capture_committed=True, notification_dedup=True)
    consume_event(db, None, issued, restarted)
    assert cache.lookup("owner", hold["order_id"])[0]["tickets"] == [{"id": str(t["id"]), "seat_id": t["seat_id"]} for t in durable["tickets"]]
    with db.transaction() as conn:
        assert conn.execute("SELECT count(*) AS n FROM tickets").fetchone()["n"] == 1
        assert conn.execute("SELECT count(*) AS n FROM bookings").fetchone()["n"] == 1
    drain_outbox(db)

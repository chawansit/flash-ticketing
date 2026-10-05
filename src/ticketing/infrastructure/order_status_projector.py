"""Best-effort advisory snapshots; invoked only after the financial transaction commits."""

import hashlib
import json
import logging
import time
from collections import OrderedDict
from uuid import UUID

from ticketing.infrastructure.order_status_cache import scalar
from ticketing.observability import ORDER_STATUS_CACHE

log = logging.getLogger("ticketing.order_status_projector")
MAX_HINTS = 1024
ORDER_EVENTS = frozenset({"OrderPaid", "TicketsIssued", "RefundRequested"})


class CommittedOrderStatusProjector:
    def __init__(self, database, cache, *, deduplicate=False):
        if type(deduplicate) is not bool:
            raise TypeError("Explicit deduplication boolean required")
        self.database, self.cache, self.deduplicate = database, cache, deduplicate
        # One existing consumer thread owns this bounded, non-authoritative hint table.
        self._recent = OrderedDict()

    @staticmethod
    def _fingerprint(snapshot):
        raw = json.dumps(snapshot, sort_keys=True, separators=(",", ":"), default=scalar)
        return hashlib.sha256(raw.encode()).digest()

    def _can_reuse(self, identifier):
        hint = self._recent.get(identifier)
        if hint is None:
            return False
        actor, fingerprint, deadline = hint
        if time.monotonic() >= deadline:
            self._recent.pop(identifier)
            return False
        try:
            snapshot, _ = self.cache.lookup(actor, identifier)
            reusable = (snapshot is not None and self._fingerprint(snapshot) == fingerprint
                        and time.monotonic() < deadline)
        except Exception:  # noqa: BLE001 - advisory lookup failure must fall back to a committed read
            ORDER_STATUS_CACHE.labels("event_dedup_lookup_error").inc()
            reusable = False
        if not reusable:
            self._recent.pop(identifier)
        return reusable

    def _remember(self, identifier, order, started):
        deadline = started + self.cache.max_age_ms / 1000
        now = time.monotonic()
        while self._recent and next(iter(self._recent.values()))[2] <= now:
            self._recent.popitem(last=False)
        if now < deadline:
            self._recent[identifier] = (order["actor"], self._fingerprint(order), deadline)
            while len(self._recent) > MAX_HINTS:
                self._recent.popitem(last=False)

    def refresh(self, order_id, *, event_type=None):
        try:
            identifier = UUID(str(order_id))
            if self.deduplicate:
                if event_type in {"OrderPaid", "RefundRequested"}:
                    self._recent.pop(identifier, None)
                if event_type == "TicketsIssued" and self._can_reuse(identifier):
                    ORDER_STATUS_CACHE.labels("event_read_deduplicated").inc()
                    return
            started = time.monotonic()
            stamp = self.cache.snapshot_start()
            with self.database.transaction() as conn:
                rows = conn.execute(
                    """SELECT o.*, t.id AS _ticket_id, b.seat_id AS _ticket_seat_id
                    FROM orders o
                    LEFT JOIN (bookings b JOIN tickets t ON t.booking_id=b.id) ON b.order_id=o.id
                    WHERE o.id=%s ORDER BY b.seat_id""",
                    (identifier,),
                ).fetchall()
                if not rows:
                    ORDER_STATUS_CACHE.labels("event_order_missing").inc()
                    return
                order = {key: value for key, value in rows[0].items()
                         if key not in {"_ticket_id", "_ticket_seat_id"}}
                order["tickets"] = [{"id": row["_ticket_id"], "seat_id": row["_ticket_seat_id"]}
                                    for row in rows if row["_ticket_id"] is not None]
            # Never retain a database connection while publishing to Redis.
            published = self.cache.publish(order["actor"], identifier, order, stamp)
            if (self.deduplicate and event_type == "OrderPaid" and published is True
                    and order["status"] == "FULFILLED" and order["tickets"]):
                self._remember(identifier, order, started)
        except Exception:
            # This boundary is advisory only. Financial transaction failures propagate
            # before this method is called and are never caught here.
            ORDER_STATUS_CACHE.labels("event_refresh_error").inc()
            log.warning("order_status_refresh_failed", exc_info=True)

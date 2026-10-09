"""Best-effort advisory snapshots; invoked only after the financial transaction commits."""

import logging
from uuid import UUID

from ticketing.observability import ORDER_STATUS_CACHE

log = logging.getLogger("ticketing.order_status_projector")
ORDER_EVENTS = frozenset({"OrderPaid", "TicketsIssued", "RefundRequested"})


class CommittedOrderStatusProjector:
    def __init__(self, database, cache):
        self.database, self.cache = database, cache

    def refresh(self, order_id):
        try:
            identifier = UUID(str(order_id))
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
            self.cache.publish(order["actor"], identifier, order, stamp)
        except Exception:
            # This boundary is advisory only. Financial transaction failures propagate
            # before this method is called and are never caught here.
            ORDER_STATUS_CACHE.labels("event_refresh_error").inc()
            log.warning("order_status_refresh_failed", exc_info=True)

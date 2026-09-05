from ticketing.application.ports import ReservationStore
from ticketing.domain import Failure


class Reservations:
    """Use cases depend on an atomic persistence port, never a database driver."""

    def __init__(self, store: ReservationStore):
        self.store = store

    def reserve(self, actor, event_id, seat_ids, key):
        if not 1 <= len(seat_ids) <= 8 or len(set(seat_ids)) != len(seat_ids):
            raise Failure("INVALID_SEATS", 422)
        if any(not seat or len(seat) > 64 for seat in seat_ids):
            raise Failure("INVALID_SEATS", 422)
        return self.store.reserve(actor, event_id, sorted(seat_ids), key)

    def checkout(self, actor, hold_id, key):
        return self.store.checkout(actor, hold_id, key)

    def get_order(self, actor, order_id):
        return self.store.get_order(actor, order_id)

    def get_hold(self, actor, hold_id):
        return self.store.get_hold(actor, hold_id)

    def release(self, actor, hold_id):
        return self.store.release(actor, hold_id)

    def expire_one(self):
        return self.store.expire_one()

    def initiate_payment(self, actor, order_id, key, outcome, delay_seconds, duplicates):
        if (
            outcome not in {"SUCCEEDED", "FAILED"}
            or not 0 <= delay_seconds <= 600
            or not 1 <= duplicates <= 10
        ):
            raise Failure("INVALID_PAYMENT", 422)
        return self.store.initiate_payment(actor, order_id, key, outcome, delay_seconds, duplicates)

    def callback(self, payload):
        return self.store.callback(payload)

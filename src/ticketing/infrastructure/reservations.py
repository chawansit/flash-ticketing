import hashlib
import json
from datetime import timedelta
from uuid import uuid4

from psycopg.types.json import Jsonb

from ticketing.application.ports import Database, SeatCache
from ticketing.domain import Failure, payment_decision
from ticketing.observability import OUTCOMES, REQUEST_ID, TimedHoldResource, hold_phase


def digest(value):
    return hashlib.sha256(json.dumps(value, sort_keys=True, default=str).encode()).hexdigest()


def event(conn, aggregate, kind, payload):
    conn.execute(
        "INSERT INTO outbox_events(id,aggregate_id,event_type,payload) VALUES (%s,%s,%s,%s)",
        (uuid4(), aggregate, kind, Jsonb({**payload, "correlation_id": REQUEST_ID.get()})),
    )


def idem(conn, actor, operation, key, request):
    hashed = digest(request)
    row = conn.execute(
        """INSERT INTO idempotency_records(actor,operation,key,request_hash)
        VALUES (%s,%s,%s,%s) ON CONFLICT DO NOTHING RETURNING key""",
        (actor, operation, key, hashed),
    ).fetchone()
    if row:
        return None
    previous = conn.execute(
        """SELECT * FROM idempotency_records
        WHERE actor=%s AND operation=%s AND key=%s""",
        (actor, operation, key),
    ).fetchone()
    if previous["request_hash"] != hashed:
        OUTCOMES.labels("idempotency", "mismatch").inc()
        raise Failure("IDEMPOTENCY_MISMATCH")
    OUTCOMES.labels("idempotency", "replay").inc()
    return previous["response"]


def remember(conn, actor, operation, key, response):
    conn.execute(
        """UPDATE idempotency_records SET response=%s
        WHERE actor=%s AND operation=%s AND key=%s""",
        (Jsonb(response), actor, operation, key),
    )
    return response


class PostgresReservations:
    """Sole seat-write owner. Lock order: order -> payment -> hold -> sorted seats.

    New reservations lock only seats; they never lock previous owners' orders/holds.
    No gateway, Redis, or Kafka calls occur while database locks are held.
    """

    def __init__(self, db: Database, cache: SeatCache, hold_seconds=120):
        self.db, self.cache, self.hold_seconds = db, cache, hold_seconds

    def reserve(self, actor, event_id, seat_ids, key):
        seats = sorted(set(seat_ids))
        if len(seats) != len(seat_ids) or not 1 <= len(seats) <= 8:
            raise Failure("INVALID_SEATS", 422)
        request = {"event_id": str(event_id), "seats": seats}
        # Admission precedes every DB access. A concurrent replay can return SEAT_BUSY;
        # replaying the same key after the original request completes returns its result.
        with (
            TimedHoldResource("redis", self.cache.shield(str(event_id), seats)),
            TimedHoldResource("db", self.db.transaction()) as conn,
            hold_phase("database_body"),
        ):
            replay = idem(conn, actor, "hold", key, request)
            if replay is not None:
                return replay
            sale = conn.execute("SELECT * FROM events WHERE id=%s", (event_id,)).fetchone()
            if not sale:
                raise Failure("EVENT_NOT_FOUND", 404)
            rows = conn.execute(
                """SELECT * FROM event_seats WHERE event_id=%s
                    AND seat_id=ANY(%s) ORDER BY seat_id FOR UPDATE NOWAIT""",
                (event_id, seats),
            ).fetchall()
            now = conn.execute("SELECT clock_timestamp() AS now").fetchone()["now"]
            if not sale["sale_starts"] <= now < sale["sale_ends"]:
                raise Failure("SALE_CLOSED")
            if len(rows) != len(seats):
                raise Failure("SEAT_NOT_FOUND", 404)
            if any(r["booked_order_id"] or (r["reserved_until"] and r["reserved_until"] > now) for r in rows):
                raise Failure("SEAT_UNAVAILABLE")
            hold, order, expires = uuid4(), uuid4(), now + timedelta(seconds=self.hold_seconds)
            conn.execute("INSERT INTO holds VALUES (%s,%s,%s,%s,'ACTIVE')", (hold, actor, event_id, expires))
            total = sum(r["price"] for r in rows)
            conn.execute(
                """INSERT INTO orders(id,actor,hold_id,event_id,total,currency,status)
                    VALUES (%s,%s,%s,%s,%s,%s,'PENDING')""",
                (order, actor, hold, event_id, total, sale["currency"]),
            )
            conn.execute(
                """UPDATE event_seats SET hold_id=%s,reserved_until=%s,version=version+1
                    WHERE event_id=%s AND seat_id=ANY(%s)""",
                (hold, expires, event_id, seats),
            )
            for row in rows:
                conn.execute(
                    "INSERT INTO order_items VALUES (%s,%s,%s,%s)",
                    (order, event_id, row["seat_id"], row["price"]),
                )
            event(conn, order, "SeatsChanged", {"event_id": str(event_id), "seats": seats})
            return remember(
                conn,
                actor,
                "hold",
                key,
                {
                    "hold_id": str(hold),
                    "order_id": str(order),
                    "expires_at": expires.isoformat(),
                    "total": total,
                    "currency": sale["currency"],
                    "seats": seats,
                },
            )

    def checkout(self, actor, hold_id, key):
        with self.db.transaction() as conn:
            replay = idem(conn, actor, "order", key, {"hold_id": str(hold_id)})
            if replay is not None:
                return replay
            row = conn.execute(
                "SELECT * FROM orders WHERE hold_id=%s AND actor=%s FOR UPDATE NOWAIT", (hold_id, actor)
            ).fetchone()
            if not row:
                raise Failure("HOLD_NOT_FOUND", 404)
            hold = conn.execute("SELECT * FROM holds WHERE id=%s", (hold_id,)).fetchone()
            now = conn.execute("SELECT clock_timestamp() AS now").fetchone()["now"]
            if row["status"] == "PENDING" and hold["expires_at"] <= now:
                raise Failure("HOLD_EXPIRED")
            return remember(
                conn,
                actor,
                "order",
                key,
                {
                    "order_id": str(row["id"]),
                    "status": row["status"],
                    "total": row["total"],
                    "currency": row["currency"],
                },
            )

    def get_order(self, actor, order_id):
        with self.db.transaction() as conn:
            row = conn.execute("SELECT * FROM orders WHERE id=%s AND actor=%s", (order_id, actor)).fetchone()
            if not row:
                raise Failure("ORDER_NOT_FOUND", 404)
            row["tickets"] = conn.execute(
                """SELECT t.id,b.seat_id FROM tickets t JOIN bookings b
                ON b.id=t.booking_id WHERE b.order_id=%s ORDER BY b.seat_id""",
                (order_id,),
            ).fetchall()
            return row

    def get_hold(self, actor, hold_id):
        with self.db.transaction() as conn:
            row = conn.execute("SELECT * FROM holds WHERE id=%s AND actor=%s", (hold_id, actor)).fetchone()
            if not row:
                raise Failure("HOLD_NOT_FOUND", 404)
            now = conn.execute("SELECT clock_timestamp() AS now").fetchone()["now"]
            if row["status"] == "ACTIVE" and row["expires_at"] <= now:
                row["status"] = "EXPIRED"
            return row

    def release(self, actor, hold_id):
        with self.db.transaction() as conn:
            order = conn.execute(
                "SELECT * FROM orders WHERE hold_id=%s AND actor=%s FOR UPDATE NOWAIT", (hold_id, actor)
            ).fetchone()
            if not order:
                raise Failure("HOLD_NOT_FOUND", 404)
            if order["status"] in {"PAID", "FULFILLED"}:
                raise Failure("ALREADY_BOOKED")
            self._release(conn, order, "RELEASED")
        return {"status": "RELEASED"}

    def _release(self, conn, order, state):
        conn.execute("SELECT id FROM holds WHERE id=%s FOR UPDATE NOWAIT", (order["hold_id"],))
        conn.execute(
            """SELECT seat_id FROM event_seats WHERE hold_id=%s
            ORDER BY seat_id FOR UPDATE NOWAIT""",
            (order["hold_id"],),
        ).fetchall()
        released = conn.execute(
            """UPDATE event_seats SET hold_id=NULL,reserved_until=NULL,version=version+1
            WHERE hold_id=%s AND booked_order_id IS NULL RETURNING seat_id""",
            (order["hold_id"],),
        ).fetchall()
        conn.execute("UPDATE holds SET status=%s WHERE id=%s AND status='ACTIVE'", (state, order["hold_id"]))
        conn.execute("UPDATE orders SET status='EXPIRED' WHERE id=%s AND status='PENDING'", (order["id"],))
        event(
            conn,
            order["id"],
            "SeatsChanged",
            {"event_id": str(order["event_id"]), "seats": [r["seat_id"] for r in released]},
        )

    def expire_one(self):
        with self.db.transaction() as conn:
            order = conn.execute("""SELECT o.* FROM orders o JOIN holds h ON h.id=o.hold_id
                WHERE h.status='ACTIVE' AND h.expires_at <= clock_timestamp()
                ORDER BY h.expires_at LIMIT 1 FOR UPDATE OF o SKIP LOCKED""").fetchone()
            if not order:
                return False
            self._release(conn, order, "EXPIRED")
            return True

    def initiate_payment(self, actor, order_id, key, outcome, delay_seconds, duplicates):
        request = {
            "order_id": str(order_id),
            "outcome": outcome,
            "delay_seconds": delay_seconds,
            "duplicates": duplicates,
        }
        with self.db.transaction() as conn:
            replay = idem(conn, actor, "payment", key, request)
            if replay is not None:
                return replay
            order = conn.execute(
                "SELECT * FROM orders WHERE id=%s AND actor=%s FOR UPDATE NOWAIT", (order_id, actor)
            ).fetchone()
            if not order:
                raise Failure("ORDER_NOT_FOUND", 404)
            existing = conn.execute(
                "SELECT id FROM payment_attempts WHERE order_id=%s", (order_id,)
            ).fetchone()
            if existing:
                result = {"payment_id": str(existing["id"]), "order_id": str(order_id)}
            else:
                hold = conn.execute("SELECT * FROM holds WHERE id=%s", (order["hold_id"],)).fetchone()
                now = conn.execute("SELECT clock_timestamp() AS now").fetchone()["now"]
                if order["status"] != "PENDING" or hold["expires_at"] <= now:
                    raise Failure("ORDER_NOT_PAYABLE")
                payment_id = uuid4()
                conn.execute(
                    """INSERT INTO payment_attempts
                    (id,order_id,status,outcome,due_at,target_deliveries)
                    VALUES (%s,%s,'PENDING',%s,%s,%s)""",
                    (payment_id, order_id, outcome, now + timedelta(seconds=delay_seconds), duplicates),
                )
                result = {"payment_id": str(payment_id), "order_id": str(order_id)}
            return remember(conn, actor, "payment", key, result)

    def callback(self, payload):
        with self.db.transaction() as conn:
            payment = conn.execute(
                "SELECT * FROM payment_attempts WHERE id=%s", (payload["payment_id"],)
            ).fetchone()
            if not payment:
                raise Failure("PAYMENT_NOT_FOUND", 404)
            order = conn.execute(
                "SELECT * FROM orders WHERE id=%s FOR UPDATE NOWAIT", (payment["order_id"],)
            ).fetchone()
            payment = conn.execute(
                "SELECT * FROM payment_attempts WHERE id=%s FOR UPDATE NOWAIT", (payload["payment_id"],)
            ).fetchone()
            if (
                str(order["id"]) != payload["order_id"]
                or order["total"] != payload["amount"]
                or order["currency"] != payload["currency"]
            ):
                raise Failure("PAYMENT_MISMATCH", 422)
            previous = conn.execute(
                "SELECT * FROM payment_callbacks WHERE id=%s", (payload["callback_id"],)
            ).fetchone()
            if previous:
                if previous["payload_hash"] != digest(payload):
                    raise Failure("CALLBACK_MISMATCH")
                return {"status": "duplicate"}
            conn.execute(
                "INSERT INTO payment_callbacks(id,payment_id,payload_hash) VALUES (%s,%s,%s)",
                (payload["callback_id"], payload["payment_id"], digest(payload)),
            )
            if payment["status"] == "SUCCEEDED":
                return {"status": "duplicate"}
            hold = conn.execute(
                "SELECT * FROM holds WHERE id=%s FOR UPDATE NOWAIT", (order["hold_id"],)
            ).fetchone()
            seats = conn.execute(
                """SELECT s.* FROM event_seats s JOIN order_items i
                ON i.event_id=s.event_id AND i.seat_id=s.seat_id
                WHERE i.order_id=%s ORDER BY s.seat_id FOR UPDATE OF s NOWAIT""",
                (order["id"],),
            ).fetchall()
            now = conn.execute("SELECT clock_timestamp() AS now").fetchone()["now"]
            valid = (
                bool(seats)
                and hold["status"] == "ACTIVE"
                and hold["expires_at"] > now
                and all(
                    r["hold_id"] == hold["id"] and r["booked_order_id"] is None and r["reserved_until"] > now
                    for r in seats
                )
            )
            decision = payment_decision(order["status"], valid, payload["outcome"])
            conn.execute(
                "UPDATE payment_attempts SET status=%s WHERE id=%s", (payload["outcome"], payment["id"])
            )
            if decision == "BOOK":
                for seat in seats:
                    conn.execute(
                        "INSERT INTO bookings VALUES (%s,%s,%s,%s)",
                        (uuid4(), order["event_id"], seat["seat_id"], order["id"]),
                    )
                conn.execute(
                    """UPDATE event_seats SET booked_order_id=%s,hold_id=NULL,
                    reserved_until=NULL,version=version+1 WHERE hold_id=%s""",
                    (order["id"], hold["id"]),
                )
                conn.execute("UPDATE holds SET status='CONSUMED' WHERE id=%s", (hold["id"],))
                conn.execute("UPDATE orders SET status='PAID' WHERE id=%s", (order["id"],))
                event(conn, order["id"], "OrderPaid", {"order_id": str(order["id"])})
            elif decision in {"REFUND", "FAILED"}:
                self._release(conn, order, "EXPIRED" if decision == "REFUND" else "RELEASED")
                conn.execute(
                    "UPDATE orders SET status=%s WHERE id=%s",
                    ("REFUND_PENDING" if decision == "REFUND" else "FAILED", order["id"]),
                )
                if decision == "REFUND":
                    conn.execute(
                        "INSERT INTO refund_requests VALUES (%s,%s,'PENDING') ON CONFLICT DO NOTHING",
                        (payment["id"], order["id"]),
                    )
                    event(
                        conn,
                        order["id"],
                        "RefundRequested",
                        {"payment_id": str(payment["id"]), "order_id": str(order["id"])},
                    )
            event(
                conn,
                order["id"],
                "SeatsChanged",
                {"event_id": str(order["event_id"]), "seats": [r["seat_id"] for r in seats]},
            )
            return {"status": decision.lower()}

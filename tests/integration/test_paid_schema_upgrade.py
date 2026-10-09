"""Validate additive paid schema against both fresh and populated main databases."""
import json
import os
from pathlib import Path
from uuid import uuid4

import psycopg
import pytest
from psycopg import sql
from psycopg.rows import dict_row
from psycopg.types.json import Jsonb


@pytest.fixture(params=["fresh", "upgrade"])
def upgraded(request):
    url = os.getenv("TEST_DATABASE_URL")
    if not url:
        pytest.skip("TEST_DATABASE_URL not configured")
    schema = "schema_upgrade_" + uuid4().hex
    with psycopg.connect(url, autocommit=True, row_factory=dict_row) as conn:
        conn.execute(sql.SQL("CREATE SCHEMA {}").format(sql.Identifier(schema)))
        conn.execute(sql.SQL("SET search_path TO {}").format(sql.Identifier(schema)))
        try:
            migrations = sorted(Path("migrations").glob("*.sql"))
            before, after = migrations[:6], migrations[6:]
            assert [p.name[:3] for p in after] == ["007", "008", "009", "010"]
            for path in before:
                conn.execute(path.read_text(encoding="utf-8-sig"))
            if request.param == "fresh":
                for path in after:
                    conn.execute(path.read_text(encoding="utf-8-sig"))
            event, hold, order, payment = (uuid4() for _ in range(4))
            conn.execute("INSERT INTO events VALUES (%s,'Upgrade','THB',now(),now()+interval '1 day')", (event,))
            conn.execute("INSERT INTO holds VALUES (%s,'owner',%s,now()+interval '1 hour','CONSUMED')", (hold, event))
            conn.execute("INSERT INTO orders(id,actor,hold_id,event_id,total,currency,status) "
                         "VALUES (%s,'owner',%s,%s,200,'THB','PAID')", (order, hold, event))
            for seat in ("A", "B"):
                conn.execute("INSERT INTO event_seats(event_id,seat_id,price,booked_order_id) "
                             "VALUES (%s,%s,100,%s)", (event, seat, order))
                booking = uuid4()
                conn.execute("INSERT INTO bookings VALUES (%s,%s,%s,%s)", (booking, event, seat, order))
                conn.execute("INSERT INTO tickets(id,booking_id) VALUES (%s,%s)", (uuid4(), booking))
            conn.execute("INSERT INTO payment_attempts(id,order_id,status,outcome,due_at,deliveries,target_deliveries) "
                         "VALUES (%s,%s,'SUCCEEDED','SUCCEEDED',now(),1,3)", (payment, order))
            tables = ("events", "holds", "orders", "event_seats", "bookings", "tickets", "payment_attempts")
            def snapshot():
                return {table: conn.execute(sql.SQL("SELECT * FROM {} ORDER BY 1,2").format(
                    sql.Identifier(table))).fetchall() for table in tables}
            retained = snapshot()
            with conn.transaction():
                for path in after:
                    conn.execute(path.read_text(encoding="utf-8-sig"))
            assert snapshot() == retained
            # Repeated migration does not alter paid data or create duplicate objects.
            with conn.transaction():
                for path in after:
                    conn.execute(path.read_text(encoding="utf-8-sig"))
            assert snapshot() == retained
            yield conn, event, order, payment
        finally:
            conn.execute("SET search_path TO public")
            conn.execute(sql.SQL("DROP SCHEMA {} CASCADE").format(sql.Identifier(schema)))


def test_indexes_preserve_multiseat_and_duplicate_callback_eligibility(upgraded):
    conn, event, order, payment = upgraded
    indexes = {row["relname"]: row for row in conn.execute("""
        SELECT c.relname, i.indisvalid, i.indisready, i.indisunique,
               am.amname, pg_get_expr(i.indpred, i.indrelid) AS predicate,
               pg_get_indexdef(i.indexrelid,1,true) AS first_key
        FROM pg_index i JOIN pg_class c ON c.oid=i.indexrelid
        JOIN pg_namespace n ON n.oid=c.relnamespace JOIN pg_am am ON am.oid=c.relam
        WHERE n.nspname=current_schema()
    """).fetchall()}
    for name, key in (("payment_attempts_dispatch_due", "due_at"),
                      ("bookings_order_id", "order_id"), ("orders_event_id", "event_id")):
        index = indexes[name]
        assert index["indisvalid"] and index["indisready"] and not index["indisunique"]
        assert index["first_key"] == key and index["amname"] == "btree"
    assert indexes["payment_attempts_dispatch_due"]["predicate"] == "(deliveries < target_deliveries)"
    assert indexes["bookings_order_id"]["predicate"] is None
    assert indexes["orders_event_id"]["predicate"] is None
    assert len(conn.execute("SELECT id FROM bookings WHERE order_id=%s", (order,)).fetchall()) == 2
    assert conn.execute("SELECT id FROM payment_attempts WHERE deliveries<target_deliveries").fetchone()["id"] == payment
    # Force access-path eligibility only; this is not a loaded performance benchmark.
    with conn.transaction():
        conn.execute("SET LOCAL enable_seqscan=off")
        for query, args, index in (
            ("SELECT id FROM bookings WHERE order_id=%s", (order,), "bookings_order_id"),
            ("SELECT id FROM orders WHERE event_id=%s", (event,), "orders_event_id"),
            ("SELECT id FROM payment_attempts WHERE deliveries<target_deliveries ORDER BY due_at", (),
             "payment_attempts_dispatch_due"),
        ):
            plan = conn.execute("EXPLAIN (FORMAT JSON) " + query, args).fetchone()["QUERY PLAN"]
            assert index in json.dumps(plan)
    conn.execute("UPDATE payment_attempts SET deliveries=target_deliveries WHERE id=%s", (payment,))
    assert conn.execute("SELECT count(*) AS n FROM payment_attempts WHERE deliveries<target_deliveries").fetchone()["n"] == 0
    with pytest.raises(psycopg.errors.UniqueViolation), conn.transaction():
        conn.execute("INSERT INTO bookings VALUES (%s,%s,'A',%s)", (uuid4(), event, order))
    booking = conn.execute("SELECT id FROM bookings WHERE order_id=%s LIMIT 1", (order,)).fetchone()["id"]
    with pytest.raises(psycopg.errors.UniqueViolation), conn.transaction():
        conn.execute("INSERT INTO tickets(id,booking_id) VALUES (%s,%s)", (uuid4(), booking))


def receipt(conn, provider, callback, **values):
    columns = ["provider", "callback_id", "payload_hash", "payload", *values]
    conn.execute(sql.SQL("INSERT INTO payment_webhook_receipts ({}) VALUES ({})").format(
        sql.SQL(",").join(map(sql.Identifier, columns)),
        sql.SQL(",").join(sql.Placeholder() for _ in columns)),
        [provider, callback, "hash", Jsonb({"order_id": str(uuid4())}), *values.values()])


def test_receipt_identity_state_and_transactional_rollback(upgraded):
    conn, _event, _order, _payment = upgraded
    for provider in ("one", "two"):
        conn.execute("INSERT INTO payment_receipt_capacity(provider) VALUES (%s)", (provider,))
    callback = uuid4()
    receipt(conn, "one", callback)
    with pytest.raises(psycopg.errors.UniqueViolation), conn.transaction():
        receipt(conn, "one", callback)
    receipt(conn, "two", callback)  # Provider namespace is part of identity.
    conn.execute("UPDATE payment_webhook_receipts SET status='PROCESSING', lease_token=%s, "
                 "lease_until=now()+interval '30 seconds' WHERE provider='one'", (uuid4(),))
    conn.execute("UPDATE payment_webhook_receipts SET status='COMPLETED',completed_at=now(), "
                 "lease_token=NULL,lease_until=NULL WHERE provider='one'")
    before = conn.execute("SELECT * FROM payment_receipt_capacity ORDER BY provider").fetchall()
    rolled_back = uuid4()
    with pytest.raises(RuntimeError, match="injected failure"), conn.transaction():
        conn.execute("UPDATE payment_receipt_capacity SET outstanding=outstanding+1 WHERE provider='one'")
        receipt(conn, "one", rolled_back)
        raise RuntimeError("injected failure")
    assert conn.execute("SELECT * FROM payment_receipt_capacity ORDER BY provider").fetchall() == before
    assert conn.execute("SELECT count(*) AS n FROM payment_webhook_receipts WHERE callback_id=%s",
                        (rolled_back,)).fetchone()["n"] == 0
    with pytest.raises(psycopg.errors.CheckViolation), conn.transaction():
        conn.execute("UPDATE payment_receipt_capacity SET outstanding=-1 WHERE provider='one'")
    with pytest.raises(psycopg.errors.ForeignKeyViolation), conn.transaction():
        receipt(conn, "unknown", uuid4())


@pytest.mark.parametrize("values", [
    {"status": "UNKNOWN"}, {"attempts": -1}, {"status": "PROCESSING"},
    {"lease_token": uuid4()}, {"lease_until": "2099-01-01"}, {"status": "COMPLETED"},
    {"completed_at": "2099-01-01"},
])
def test_invalid_receipt_states_rejected(upgraded, values):
    conn, *_ = upgraded
    conn.execute("INSERT INTO payment_receipt_capacity(provider) VALUES ('provider')")
    with pytest.raises(psycopg.errors.CheckViolation), conn.transaction():
        receipt(conn, "provider", uuid4(), **values)

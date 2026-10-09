import json
import os
from uuid import uuid4

import psycopg
import pytest
from psycopg.conninfo import make_conninfo

from scripts.apply_bookings_order_index import apply, inspect
from ticketing.workers import consume_event

pytestmark = pytest.mark.integration


def test_order_lookup_index_preserves_multiseat_duplicate_callback_and_event(system):
    svc, db, show = system
    hold = svc.reserve('buyer', show, ['A', 'B', 'C'], 'multiseat')
    payment = svc.initiate_payment('buyer', hold['order_id'], 'pay', 'SUCCEEDED', 0, 1)
    payload = {'callback_id': str(uuid4()), 'payment_id': payment['payment_id'],
               'order_id': hold['order_id'], 'amount': 300, 'currency': 'THB', 'outcome': 'SUCCEEDED'}
    assert svc.callback(payload)['status'] == 'book'
    assert svc.callback(payload)['status'] == 'duplicate'
    with db.transaction() as conn:
        row = conn.execute("SELECT * FROM outbox_events WHERE event_type='OrderPaid'").fetchone()
    envelope = dict(row, event_id=str(row['id']))
    consume_event(db, None, envelope)
    consume_event(db, None, envelope)
    order = svc.get_order('buyer', hold['order_id'])
    assert order['status'] == 'FULFILLED'
    assert [ticket['seat_id'] for ticket in order['tickets']] == ['A', 'B', 'C']
    assert len({ticket['id'] for ticket in order['tickets']}) == 3
    with db.transaction() as conn:
        assert conn.execute('SELECT count(*) AS n FROM bookings').fetchone()['n'] == 3
        assert conn.execute('SELECT count(*) AS n FROM payment_callbacks').fetchone()['n'] == 1
        assert conn.execute("SELECT count(*) AS n FROM outbox_events WHERE event_type='TicketsIssued'").fetchone()['n'] == 1
        conn.execute('SET LOCAL enable_seqscan=off')
        for query in (
            'SELECT id FROM bookings WHERE order_id=%s',
            'SELECT t.id,b.seat_id FROM tickets t JOIN bookings b ON b.id=t.booking_id WHERE b.order_id=%s ORDER BY b.seat_id',
        ):
            plan = conn.execute('EXPLAIN (FORMAT JSON) '+query, (hold['order_id'],)).fetchone()['QUERY PLAN']
            assert 'bookings_order_id' in json.dumps(plan)
        # Adding this access index must preserve the separate seat uniqueness index.
        names = {r['indexname'] for r in conn.execute(
            "SELECT indexname FROM pg_indexes WHERE schemaname=current_schema() AND tablename='bookings'")}
        assert 'bookings_event_id_seat_id_key' in names


def test_online_booking_index_build_and_verifier_reject_wrong_unique_index(system):
    _svc, db, _show = system
    with db.pool.connection() as original:
        schema = original.execute('SELECT current_schema() AS name').fetchone()['name']
    with psycopg.connect(make_conninfo(os.environ['TEST_DATABASE_URL'],
                                     options=f'-c search_path={schema}'), autocommit=True) as conn:
        assert apply(conn, verify_only=True)['pass']
        assert not apply(conn)['created']
        conn.execute('DROP INDEX bookings_order_id')
        assert not apply(conn, verify_only=True)['pass']
        assert apply(conn)['created']
        assert inspect(conn)['pass']
        conn.execute('DROP INDEX bookings_order_id')
        conn.execute('CREATE UNIQUE INDEX bookings_order_id ON bookings(order_id)')
        with pytest.raises(RuntimeError, match='unexpected'):
            apply(conn)

import json
import os

import pytest
from psycopg.conninfo import make_conninfo

from scripts.apply_payment_dispatch_index import apply, inspect
from ticketing.config import Settings
from ticketing.workers import simulate_one

pytestmark = pytest.mark.integration


def test_succeeded_payment_still_dispatches_remaining_duplicates(system, monkeypatch):
    svc, db, show = system
    hold = svc.reserve('buyer', show, ['A'], 'hold')
    svc.initiate_payment('buyer', hold['order_id'], 'payment', 'SUCCEEDED', 0, 3)
    observed = []

    class Response:
        def __enter__(self):
            return self

        def __exit__(self, *_args):
            return False

        def read(self):
            return b'{}'

    def callback(request, timeout):
        assert timeout == 5
        observed.append(svc.callback(json.loads(request.data))['status'])
        return Response()

    monkeypatch.setattr('ticketing.workers.urllib.request.urlopen', callback)
    assert simulate_one(db, Settings())
    with db.transaction() as conn:
        row = conn.execute('SELECT status,deliveries,target_deliveries FROM payment_attempts').fetchone()
        assert row == {'status': 'SUCCEEDED', 'deliveries': 1, 'target_deliveries': 3}
        conn.execute('SET LOCAL enable_seqscan=off')
        plan = conn.execute("""EXPLAIN (FORMAT JSON) SELECT id FROM payment_attempts
            WHERE deliveries<target_deliveries ORDER BY due_at LIMIT 1""").fetchone()['QUERY PLAN']
        assert 'payment_attempts_dispatch_due' in json.dumps(plan)
    assert simulate_one(db, Settings())
    assert simulate_one(db, Settings())
    assert not simulate_one(db, Settings())
    assert observed == ['book', 'duplicate', 'duplicate']
    with db.transaction() as conn:
        assert conn.execute('SELECT count(*) AS n FROM bookings').fetchone()['n'] == 1
        assert conn.execute('SELECT count(*) AS n FROM payment_callbacks').fetchone()['n'] == 1
        assert conn.execute('SELECT count(*) AS n FROM payment_attempts WHERE deliveries<target_deliveries').fetchone()['n'] == 0


def test_online_index_verifier_accepts_valid_and_refuses_wrong_definition(system):
    import psycopg

    _svc, db, _show = system
    with db.pool.connection() as original:
        schema = original.execute('SELECT current_schema() AS name').fetchone()['name']
        dsn = os.environ["TEST_DATABASE_URL"]
    with psycopg.connect(make_conninfo(dsn, options=f'-c search_path={schema}'), autocommit=True) as conn:
        assert apply(conn, verify_only=True)['pass']
        assert not apply(conn)['created']
        conn.execute('DROP INDEX payment_attempts_dispatch_due')
        assert not apply(conn, verify_only=True)['pass']
        assert apply(conn)['created']
        assert inspect(conn)['pass']
        conn.execute('DROP INDEX payment_attempts_dispatch_due')
        conn.execute('CREATE INDEX payment_attempts_dispatch_due ON payment_attempts(id)')
        with pytest.raises(RuntimeError, match='unexpected'):
            apply(conn)

"""Real PostgreSQL qualification of the simulator's single-statement claim."""
import json
from concurrent.futures import ThreadPoolExecutor, as_completed
from contextlib import contextmanager
from threading import Barrier, Event
from types import SimpleNamespace
from uuid import uuid4

import pytest

from ticketing.config import Settings
from ticketing.workers import CLAIM_PAYMENT_SQL, consume_event, simulate_one

pytestmark = pytest.mark.integration


def payment(system, duplicates=1):
    svc, db, show = system
    hold = svc.reserve('owner', show, ['A'], 'hold')
    svc.initiate_payment('owner', hold['order_id'], 'pay', 'SUCCEEDED', 0, duplicates)
    return svc, db


def fulfill(db):
    with db.transaction() as conn:
        rows = conn.execute("SELECT id,event_type,schema_version,payload FROM outbox_events WHERE event_type='OrderPaid'").fetchall()
    assert len(rows) == 1
    envelope = {**rows[0], 'event_id':str(rows[0]['id'])}
    consume_event(db, None, envelope)
    consume_event(db, None, envelope)  # Replay must not issue another ticket.


def test_delivery_uses_two_statements_and_http_is_outside_claim_transaction(system):
    svc, db = payment(system)
    statements, active = [], []
    @contextmanager
    def transaction():
        with db.transaction() as conn:
            active.append(True)
            try:
                def execute(query, params=None):
                    statements.append(query)
                    return conn.execute(query, params)
                yield SimpleNamespace(execute=execute)
            finally:
                active.pop()
    class Transport:
        def post(self, body, _headers):
            assert not active
            svc.callback(json.loads(body))
    assert simulate_one(SimpleNamespace(transaction=transaction), Settings(), Transport())
    assert len(statements) == 2 and statements[0] == CLAIM_PAYMENT_SQL
    fulfill(db)
    with db.transaction() as conn:
        assert conn.execute('SELECT count(*) AS n FROM tickets').fetchone()['n'] == 1


def test_twenty_four_concurrent_claims_dispatch_one_payment(system):
    svc, db = payment(system)
    entered, release = Event(), Event()
    barrier = Barrier(24)
    class Transport:
        def post(self, body, _headers):
            entered.set()
            assert release.wait(10)
            svc.callback(json.loads(body))
    def attempt():
        barrier.wait(timeout=8)
        return simulate_one(db, Settings(), Transport())
    with ThreadPoolExecutor(max_workers=24) as pool:
        futures = [pool.submit(attempt) for _ in range(24)]
        try:
            assert entered.wait(8)
            completed = as_completed(futures, timeout=8)
            assert [next(completed).result() for _ in range(23)] == [False]*23
        finally:
            release.set()
        assert sum(f.result(timeout=8) is True for f in futures) == 1
    fulfill(db)
    with db.transaction() as conn:
        assert conn.execute('SELECT deliveries FROM payment_attempts').fetchone()['deliveries'] == 1
        assert conn.execute('SELECT count(*) AS n FROM tickets').fetchone()['n'] == 1
        assert conn.execute('SELECT count(*) AS n FROM bookings').fetchone()['n'] == 1


def test_rolled_back_claim_is_immediately_available_to_another_owner(system):
    _, db = payment(system)
    with db.connection() as conn, conn.transaction(force_rollback=True):
        first = conn.execute(CLAIM_PAYMENT_SQL, (uuid4(),)).fetchone()
        assert first is not None and first['lease_token'] is not None
    class Transport:
        def post(self, *_):pass
    assert simulate_one(db, Settings(), Transport())


def test_stale_delivery_ack_cannot_change_replacement_lease(system):
    svc, db = payment(system)
    replacement = uuid4()
    class StaleTransport:
        def post(self, body, _headers):
            with db.transaction() as conn:
                conn.execute('UPDATE payment_attempts SET lease_token=%s', (replacement,))
            svc.callback(json.loads(body))
    assert simulate_one(db, Settings(), StaleTransport())
    with db.transaction() as conn:
        row = conn.execute('SELECT deliveries,lease_token FROM payment_attempts').fetchone()
        assert row == {'deliveries':0, 'lease_token':replacement}
        conn.execute("UPDATE payment_attempts SET lease_until=clock_timestamp()-interval '1 second'")
    class NewTransport:
        def post(self, body, _headers):assert svc.callback(json.loads(body))['status'] == 'duplicate'
    assert simulate_one(db, Settings(), NewTransport())
    assert not simulate_one(db, Settings(), NewTransport())
    fulfill(db)
    with db.transaction() as conn:
        assert conn.execute('SELECT count(*) AS n FROM tickets').fetchone()['n'] == 1
        assert conn.execute('SELECT count(*) AS n FROM bookings').fetchone()['n'] == 1
        assert conn.execute('SELECT deliveries FROM payment_attempts').fetchone()['deliveries'] == 1



def test_lost_claim_commit_response_sends_no_http_and_recovers_after_lease(system):
    svc, db = payment(system)
    requests = []
    @contextmanager
    def transaction():
        with db.transaction() as conn:
            yield conn
        raise ConnectionError('Claim commit response lost after durable commit')
    class Transport:
        def post(self, body, _headers):
            requests.append(body)
            svc.callback(json.loads(body))
    with pytest.raises(ConnectionError):
        simulate_one(SimpleNamespace(transaction=transaction), Settings(), Transport())
    assert not requests
    assert not simulate_one(db, Settings(), Transport())
    with db.transaction() as conn:
        row = conn.execute('SELECT deliveries,lease_token FROM payment_attempts').fetchone()
        assert row['deliveries'] == 0 and row['lease_token'] is not None
        conn.execute("UPDATE payment_attempts SET lease_until=clock_timestamp()-interval '1 second'")
    assert simulate_one(db, Settings(), Transport())
    assert not simulate_one(db, Settings(), Transport())
    fulfill(db)
    with db.transaction() as conn:
        assert conn.execute('SELECT count(*) AS n FROM tickets').fetchone()['n'] == 1
        assert conn.execute('SELECT count(*) AS n FROM bookings').fetchone()['n'] == 1
        assert conn.execute('SELECT count(*) AS n FROM payment_attempts WHERE deliveries<target_deliveries').fetchone()['n'] == 0

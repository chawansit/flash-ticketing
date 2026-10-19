"""Actual PostgreSQL fenced group claims and independent delivery slots."""
import json
import threading
from concurrent.futures import ThreadPoolExecutor
from contextlib import contextmanager
from dataclasses import replace
from uuid import uuid4

import psycopg
import pytest

from ticketing import workers
from ticketing.config import Settings

pytestmark=pytest.mark.integration


@pytest.mark.parametrize('limit',[0,17,-1,True,1.0,None,'2'])
def test_invalid_claim_bounds_never_touch_database(limit):
    with pytest.raises(ValueError):workers.claim_due_callbacks(None,limit)


def seed(system,count=4):
    svc,db,event=system
    identifiers=[]
    for i in range(count):
        seat='batch-'+str(i)
        with db.transaction() as conn:
            conn.execute('INSERT INTO event_seats(event_id,seat_id,price) VALUES (%s,%s,100)',(event,seat))
        order=svc.reserve('actor-'+str(i),event,[seat],str(uuid4()))
        payment=svc.initiate_payment('actor-'+str(i),order['order_id'],str(uuid4()),'SUCCEEDED',0,1)
        identifiers.append(payment['payment_id'])
    return svc,db,identifiers


class Gateway:
    def __init__(self,svc,db,*,fail=False,stale=False):
        self.svc,self.db,self.fail,self.stale=svc,db,fail,stale
        self.payloads=[]

    def post(self,raw,_headers):
        payload=json.loads(raw);self.payloads.append(payload)
        with self.db.transaction() as conn:
            row=conn.execute('SELECT * FROM payment_attempts WHERE id=%s FOR UPDATE NOWAIT',
                             (payload['payment_id'],)).fetchone()
            assert row['lease_token'] is not None and row['lease_until'] is not None
        self.svc.callback(payload)
        if self.stale:
            with self.db.transaction() as conn:
                conn.execute('UPDATE payment_attempts SET lease_token=%s WHERE id=%s',(uuid4(),payload['payment_id']))
        if self.fail:raise ConnectionError('Lost committed response')


def test_batch_eligibility_concurrency_and_lease_commit_before_http(system):
    svc,db,ids=seed(system,8)
    with db.transaction() as conn:
        conn.execute("UPDATE payment_attempts SET due_at=clock_timestamp()+interval '1 hour' WHERE id=%s",(ids[0],))
        conn.execute("UPDATE payment_attempts SET lease_until=clock_timestamp()+interval '1 hour' WHERE id=%s",(ids[1],))
        conn.execute('UPDATE payment_attempts SET deliveries=target_deliveries WHERE id=%s',(ids[2],))
    with db.transaction() as locked:
        locked.execute('SELECT id FROM payment_attempts WHERE id=%s FOR UPDATE',(ids[3],))
        with ThreadPoolExecutor(max_workers=2) as executor:
            groups=list(executor.map(lambda _:workers.claim_due_callbacks(db,2),range(2)))
        claimed=[str(row['id']) for rows,_ in groups for row in rows]
        assert len(set(claimed))==4 and set(claimed)==set(ids[4:])
        for rows,token in groups:
            for row in rows:
                assert row['lease_until']>row['claim_observed_at']
                assert 14<(row['lease_until']-row['claim_observed_at']).total_seconds()<16
                assert workers.deliver_claimed_callback(db,Settings(),row,token,Gateway(svc,db))
    with db.transaction() as conn:
        assert conn.execute('SELECT count(*) AS n FROM payment_attempts WHERE deliveries=1').fetchone()['n']==5


@pytest.mark.parametrize('uncertainty',['lost_response','stale_token'])
def test_callback_idempotency_and_lease_fencing_survive_uncertainty(system,uncertainty):
    svc,db,ids=seed(system,1)
    rows,token=workers.claim_due_callbacks(db,1)
    gateway=Gateway(svc,db,fail=uncertainty=='lost_response',stale=uncertainty=='stale_token')
    if uncertainty=='lost_response':
        with pytest.raises(ConnectionError):workers.deliver_claimed_callback(db,Settings(),rows[0],token,gateway)
    else:assert workers.deliver_claimed_callback(db,Settings(),rows[0],token,gateway)
    with db.transaction() as conn:
        row=conn.execute('SELECT deliveries FROM payment_attempts WHERE id=%s',(ids[0],)).fetchone()
        assert row['deliveries']==0
        assert conn.execute('SELECT count(*) AS n FROM payment_callbacks').fetchone()['n']==1
        conn.execute("UPDATE payment_attempts SET lease_until=clock_timestamp()-interval '1 second' WHERE id=%s",(ids[0],))
    retry,new_token=workers.claim_due_callbacks(db,1)
    assert new_token!=token and retry[0]['id']==rows[0]['id']
    next_gateway=Gateway(svc,db)
    assert workers.deliver_claimed_callback(db,Settings(),retry[0],new_token,next_gateway)
    assert gateway.payloads[0]==next_gateway.payloads[0]
    with db.transaction() as conn:
        assert conn.execute('SELECT count(*) AS n FROM payment_callbacks').fetchone()['n']==1
        assert conn.execute('SELECT deliveries FROM payment_attempts').fetchone()['deliveries']==1


def test_group_claim_failure_rolls_back_every_lease(system):
    _,db,ids=seed(system,2)
    with db.transaction() as conn:
        conn.execute("""CREATE FUNCTION reject_claim() RETURNS trigger LANGUAGE plpgsql AS $$
            BEGIN IF NEW.lease_until IS NOT NULL THEN RAISE EXCEPTION 'test claim failure'; END IF; RETURN NEW; END $$""")
        conn.execute('CREATE TRIGGER reject_claim BEFORE UPDATE ON payment_attempts FOR EACH ROW EXECUTE FUNCTION reject_claim()')
    with pytest.raises(psycopg.errors.RaiseException):workers.claim_due_callbacks(db,2)
    with db.transaction() as conn:
        assert conn.execute('SELECT count(*) AS n FROM payment_attempts WHERE lease_until IS NULL AND deliveries=0').fetchone()['n']==len(ids)


def test_exact_simulator_transaction_reduction_for_sixteen_due_callbacks(system):
    svc,db,_=seed(system,32)
    class Counted:
        def __init__(self):self.count=0;self.lock=threading.Lock()
        @contextmanager
        def transaction(self,**kwargs):
            with self.lock:self.count+=1
            with db.transaction(**kwargs) as conn:yield conn
    measured=Counted()
    with ThreadPoolExecutor(max_workers=16) as executor:
        assert all(executor.map(lambda _:workers.simulate_one(measured,Settings(),Gateway(svc,db)),range(16)))
    assert measured.count==32
    measured.count=0
    rows,token=workers.claim_due_callbacks(measured,16)
    assert len(rows)==16
    with ThreadPoolExecutor(max_workers=16) as executor:
        assert all(executor.map(lambda row:workers.deliver_claimed_callback(measured,Settings(),row,token,Gateway(svc,db)),rows))
    assert measured.count==17
    with db.transaction() as conn:
        assert conn.execute('SELECT count(*) AS n FROM payment_callbacks').fetchone()['n']==32
        assert conn.execute('SELECT count(*) AS n FROM payment_attempts WHERE deliveries=1').fetchone()['n']==32


def test_slow_delivery_does_not_block_refill_and_stop_drains(system):
    svc,db,ids=seed(system,3)
    entered,release,third,stop=threading.Event(),threading.Event(),threading.Event(),threading.Event()
    base=Gateway(svc,db)
    class Delayed:
        def post(self,raw,headers):
            identifier=json.loads(raw)['payment_id']
            if identifier==ids[0]:
                entered.set()
                if not release.wait(5):raise TimeoutError('Release test callback')
            base.post(raw,headers)
            if identifier==ids[2]:third.set();stop.set()
    settings=replace(Settings(),simulator_batch_claim=True,simulator_dispatch_mode='refill',simulator_concurrency=2)
    settings.validate()
    with ThreadPoolExecutor(max_workers=2) as delivery,ThreadPoolExecutor(max_workers=1) as loop:
        future=loop.submit(workers.simulate_refill,db,settings,delivery,lambda:not stop.is_set(),Delayed())
        try:
            assert entered.wait(5)
            assert third.wait(5),'A completed free slot must refill before the slow slot finishes'
        finally:
            stop.set();release.set()
        future.result(timeout=5)
    with db.transaction() as conn:
        assert conn.execute('SELECT count(*) AS n FROM payment_attempts WHERE deliveries=1').fetchone()['n']==3

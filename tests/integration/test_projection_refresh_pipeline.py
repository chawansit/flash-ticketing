"""ADR0363 actual PostgreSQL rollback, replay, ordering and generation semantics."""
from concurrent.futures import ThreadPoolExecutor
from uuid import uuid4

import pytest
from psycopg.errors import LockNotAvailable

from ticketing import workers

pytestmark = pytest.mark.integration


def change(show, seats):
    return {'schema_version':1,'event_id':str(uuid4()),'event_type':'SeatsChanged',
            'payload':{'event_id':str(show),'seats':seats}}


def shows(db, count):
    result = [uuid4() for _ in range(count)]
    with db.transaction() as conn:
        for show in result:
            conn.execute("INSERT INTO events VALUES (%s,'Projection','THB',clock_timestamp()-interval '1 day',clock_timestamp()+interval '1 day')",(show,))
    return sorted(result,key=str)


@pytest.mark.parametrize('size',[1,4,32])
def test_many_show_commit_and_lost_ack_replay(system,size):
    _,db,_ = system
    targets = shows(db,size)
    batch = [change(show,['A','B']) for show in reversed(targets)]
    assert workers.consume_refresh_batch(db,batch)==size
    assert workers.consume_refresh_batch(db,batch)==0
    with db.transaction() as conn:
        rows = conn.execute('SELECT generation,seat_ids FROM seat_refresh_requests').fetchall()
        assert len(rows)==size
        assert all(row['generation']==1 and set(row['seat_ids'])=={'A','B'} for row in rows)
        assert conn.execute('SELECT count(*) AS n FROM consumer_inbox').fetchone()['n']==size


@pytest.mark.parametrize('completed',[False,True])
@pytest.mark.parametrize('incoming',[None,['B']])
def test_pending_union_full_refresh_and_completed_replacement(system,completed,incoming):
    _,db,show = system
    workers.consume_refresh_batch(db,[change(show,['A'])])
    if completed:
        with db.transaction() as conn:
            conn.execute('UPDATE seat_refresh_requests SET completed_generation=generation')
    workers.consume_refresh_batch(db,[change(show,incoming)])
    with db.transaction() as conn:
        row=conn.execute('SELECT generation,seat_ids FROM seat_refresh_requests').fetchone()
        assert row['generation']==2
        assert row['seat_ids'] is None if incoming is None else set(row['seat_ids'])==({'B'} if completed else {'A','B'})


def test_late_locked_write_rolls_back_all_earlier_writes_and_inbox(system):
    _,db,_=system
    targets=shows(db,4)
    workers.consume_refresh_batch(db,[change(targets[-1],['old'])])
    batch=[change(show,['new']) for show in reversed(targets)]
    with db.transaction() as blocker:
        blocker.execute('SELECT event_id FROM seat_refresh_requests WHERE event_id=%s FOR UPDATE',(targets[-1],))
        with pytest.raises(LockNotAvailable):
            workers.consume_refresh_batch(db,batch)
        with db.transaction() as conn:
            assert conn.execute('SELECT count(*) AS n FROM seat_refresh_requests').fetchone()['n']==1
            assert conn.execute('SELECT count(*) AS n FROM consumer_inbox').fetchone()['n']==1
    assert workers.consume_refresh_batch(db,batch)==4
    assert workers.consume_refresh_batch(db,batch)==0


def test_opposite_input_order_concurrent_batches_share_sorted_lock_order(system):
    _,db,_=system
    targets=shows(db,12)
    first=[change(show,['A']) for show in targets]
    second=[change(show,['B']) for show in reversed(targets)]
    with ThreadPoolExecutor(max_workers=2) as executor:
        results=list(executor.map(lambda batch:workers.consume_refresh_batch(db,batch),(first,second)))
    assert results==[12,12]
    with db.transaction() as conn:
        rows=conn.execute('SELECT generation,seat_ids FROM seat_refresh_requests').fetchall()
        assert len(rows)==12 and all(row['generation']==2 and set(row['seat_ids'])=={'A','B'} for row in rows)

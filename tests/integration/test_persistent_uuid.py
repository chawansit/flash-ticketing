"""ADR0364 actual image UUID layout, uniqueness and mixed-version durable replay."""
from concurrent.futures import ThreadPoolExecutor
from uuid import UUID

import pytest
from test_redis_first_reservations import (
    redis_first as redis_first,  # noqa: PLC0414 - explicit pytest fixture re-export
)

from ticketing import domain, workers

pytestmark = pytest.mark.integration


def test_rfc9562_appendix_a6_vector(monkeypatch):
    monkeypatch.setenv('PERSISTENT_UUID_V7','1')
    monkeypatch.setattr(domain,'time_ns',lambda:1645557742000*1_000_000)
    random=(0xCC3<<62)|0x18C4DC0C0C07398F
    monkeypatch.setattr(domain,'randbits',lambda bits:random if bits==74 else pytest.fail('wrong entropy width'))
    value=domain.persistent_uuid()
    assert str(value)=='017f22e2-79b0-7cc3-98c4-dc0c0c07398f'
    assert value.version==7 and value.variant=='specified in RFC 4122'


@pytest.mark.parametrize('setting',['0','', 'unknown'])
def test_disabled_factory_delegates_to_original_uuid4(monkeypatch,setting):
    sentinel=object()
    monkeypatch.setenv('PERSISTENT_UUID_V7',setting)
    monkeypatch.setattr(domain,'uuid4',lambda:sentinel)
    monkeypatch.setattr(domain,'time_ns',lambda:pytest.fail('disabled factory consulted clock'))
    assert domain.persistent_uuid() is sentinel


@pytest.mark.parametrize('milliseconds',[-1,1<<48])
def test_invalid_timestamp_emits_no_identifier(monkeypatch,milliseconds):
    monkeypatch.setenv('PERSISTENT_UUID_V7','1')
    monkeypatch.setattr(domain,'time_ns',lambda:milliseconds*1_000_000)
    monkeypatch.setattr(domain,'randbits',lambda _:pytest.fail('invalid clock requested entropy'))
    with pytest.raises(RuntimeError,match='outside48-bit'):domain.persistent_uuid()


def test_concurrent_same_millisecond_identifiers_are_unique(monkeypatch):
    monkeypatch.setenv('PERSISTENT_UUID_V7','1')
    monkeypatch.setattr(domain,'time_ns',lambda:1_790_000_000_000*1_000_000)
    with ThreadPoolExecutor(max_workers=8) as executor:
        values=list(executor.map(lambda _:domain.persistent_uuid(),range(16384)))
    assert len(set(values))==16384
    assert all(v.version==7 and v.int>>80==1_790_000_000_000 for v in values)


def test_backward_clock_does_not_reuse_randomness_or_drive_correctness(monkeypatch):
    monkeypatch.setenv('PERSISTENT_UUID_V7','1')
    times=iter([2000,1000,1000]);randoms=iter([1,2,3])
    monkeypatch.setattr(domain,'time_ns',lambda:next(times)*1_000_000)
    monkeypatch.setattr(domain,'randbits',lambda _:next(randoms))
    values=[domain.persistent_uuid() for _ in range(3)]
    assert len(set(values))==3 and [v.int>>80 for v in values]==[2000,1000,1000]


@pytest.mark.parametrize('first,second',[('0','1'),('1','0')])
def test_mixed_version_reservation_payment_replay_and_ticket(system,monkeypatch,first,second):
    svc,db,show=system
    monkeypatch.setenv('PERSISTENT_UUID_V7',first)
    hold=svc.reserve('mixed',show,['A'],'hold-key')
    payment=svc.initiate_payment('mixed',hold['order_id'],'payment-key','SUCCEEDED',0,1)
    expected=7 if first=='1' else 4
    for field in ('hold_id','order_id'):assert UUID(str(hold[field])).version==expected
    assert UUID(str(payment['payment_id'])).version==expected
    monkeypatch.setenv('PERSISTENT_UUID_V7',second)
    assert svc.reserve('mixed',show,['A'],'hold-key')==hold
    assert svc.initiate_payment('mixed',hold['order_id'],'payment-key','SUCCEEDED',0,1)==payment
    callback={'callback_id':str(domain.uuid4()),'payment_id':payment['payment_id'],'order_id':hold['order_id'],
              'amount':hold['total'],'currency':hold['currency'],'outcome':'SUCCEEDED'}
    assert svc.callback(callback)['status']=='book'
    assert svc.callback(callback)['status']=='duplicate'
    workers.consume_event(db,None,{'schema_version':1,'event_id':str(domain.uuid4()),'event_type':'OrderPaid','payload':{'order_id':hold['order_id']}})
    assert svc.get_order('mixed',hold['order_id'])['status']=='FULFILLED'
    with db.transaction() as conn:
        for table in ('bookings','tickets'):
            rows=conn.execute(f'SELECT id FROM {table}').fetchall()
            assert len(rows)==1 and rows[0]['id'].version==(7 if second=='1' else 4)
        assert conn.execute('SELECT count(*) AS n FROM payment_attempts').fetchone()['n']==1


def test_redis_intake_ids_and_replay_survive_flag_rollback(redis_first,monkeypatch):
    store,intake,_cache,db,show=redis_first
    monkeypatch.setenv('PERSISTENT_UUID_V7','1')
    pending=intake.enqueue('mixed-intake',show,['A'],'key')
    for field in ('command_id','hold_id','order_id'):assert UUID(pending[field]).version==7
    assert workers.persist_reservation_batch(store,intake,'uuid-writer',4)
    durable=intake.status(show,pending['command_id'])
    monkeypatch.setenv('PERSISTENT_UUID_V7','0')
    assert intake.enqueue('mixed-intake',show,['A'],'key')==durable
    with db.transaction() as conn:
        assert conn.execute('SELECT count(*) AS n FROM reservation_commands').fetchone()['n']==1
        assert conn.execute('SELECT count(*) AS n FROM orders').fetchone()['n']==1

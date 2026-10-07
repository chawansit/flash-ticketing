"""ADR0197 concrete audit ordering, ownership and deadline regressions."""
import ast
import copy
import importlib.util
import json
import sys
from datetime import UTC, datetime, timedelta
from pathlib import Path
from uuid import uuid4

import pytest

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT / 'scripts'))
import work_envelope as policy
import worker_separation_audits as audit
import worker_separation_execution as execution
from fixture_identity_evidence import retain_fixture_identity

NOW = datetime(2026, 10, 7, 5, tzinfo=UTC)
HELPERS = {k:'a'*64 for k in audit.HELPERS}


def setup(monkeypatch, tmp_path, *, arm='candidate'):
    spec = importlib.util.spec_from_file_location('audit_execution_fixture', ROOT / 'tests/unit/test_worker_separation_execution.py')
    module = importlib.util.module_from_spec(spec);spec.loader.exec_module(module)
    engine, session = module.create(monkeypatch, arm)
    binding = engine.runtime.guard.binding
    binding.update(worker_audit_contract_sha256=policy.digest(audit.audit_contract(HELPERS)),
                   worker_audit_expectations={'expected_orders':1,'expected_paid':1,'callbacks':3,'shows':1})
    engine.scope_sha = engine.runtime.scope_binding_sha256 = policy.digest(binding)
    for host,seal in engine.configurations.seals.items():
        value = module.configuration.bundle(engine.pair,engine.saved,arm,host,engine.scope_sha)
        engine.configurations.seals[host] = module.module('test_worker_separation_configuration').seal(value,seal['owner'])
    engine.runtime.output = tmp_path
    events = []
    engine.runtime._write = lambda k,v:events.append((k,copy.deepcopy(v)))
    component = audit.AuditActions(engine, HELPERS, now=lambda:NOW)
    return component, engine, session, events


def bind(component, engine):
    stage = engine.runtime.output / engine.arm;stage.mkdir()
    fixture = {'schema_version':1,'environment':'development','fixture_layout':'distributed',
               'shows':1,'seats_per_show':300,'show_ids':[str(uuid4())],'fixture_id':str(uuid4()),
               'created_at':NOW.isoformat(),'sale_ends':(NOW+timedelta(minutes=15)).isoformat()}
    retain_fixture_identity(stage, {'arm':engine.arm,'customers_dispatched':False}, fixture, 1, now=NOW)
    path = stage/'fixture-identity.json'
    component.bind_fixture(path, 1, 1, 3)
    return path


def good_queue(members=6):
    return {**dict.fromkeys(audit.QUEUE_ZERO,0),'kafka_members':members,'pass':True}


@pytest.mark.parametrize('key',audit.QUEUE_ZERO)
def test_every_pending_queue_blocks_even_when_other_counts_are_zero(key):
    value=good_queue();value[key]=1;value['pass']=False
    assert not audit.queue_checks(value,6)
    value['pass']=True
    with pytest.raises(ValueError):audit.queue_checks(value,6)


@pytest.mark.parametrize('change',['missing','extra','boolean','negative','wrong_members'])
def test_queue_schema_and_membership_are_strict(change):
    value=good_queue()
    if change=='missing':value.pop('pending_confirmation_receipts')
    elif change=='extra':value['unexpected']=0
    elif change=='boolean':value['kafka_total_lag']=False
    elif change=='negative':value['reservation_stream_entries']=-1
    else:value['kafka_members']=1
    if change=='wrong_members':assert not audit.queue_checks(value,6)
    else:
        with pytest.raises(ValueError):audit.queue_checks(value,6)


def test_fixture_must_be_retained_before_binding_and_cannot_be_rebound(monkeypatch,tmp_path):
    component,engine,_,events=setup(monkeypatch,tmp_path)
    path=bind(component,engine)
    assert component.cohort['expected_paid']==1
    assert events[-1][0]=='audit-cohort'
    with pytest.raises(ValueError):component.bind_fixture(path,1,1,3)


@pytest.mark.parametrize('change',['scope','helper','arm','expectation','hash','path'])
def test_wrong_authority_or_fixture_is_rejected(monkeypatch,tmp_path,change):
    component,engine,_,_=setup(monkeypatch,tmp_path)
    if change=='scope':engine.runtime.guard.binding['worker_audit_contract_sha256']='f'*64
    elif change=='helper':component.helpers[audit.HELPERS[0]]='f'*64
    elif change=='expectation':engine.runtime.guard.binding['worker_audit_expectations']['expected_paid']=0
    if change in {'scope','helper','expectation'}:
        with pytest.raises(ValueError):bind(component,engine)
        return
    path=bind(component,engine)
    receipt=json.loads(path.read_text())
    if change=='arm':receipt['arm']='control'
    elif change=='hash':receipt['fixture_identity_sha256']='f'*64
    else:path=tmp_path/'fixture-identity.json'
    path.write_text(json.dumps(receipt))
    component.cohort=None
    with pytest.raises(ValueError):component.bind_fixture(path,1,1,3)


def test_missing_fixture_does_not_skip_independent_duplicate_audit(monkeypatch,tmp_path):
    component,_,_,_=setup(monkeypatch,tmp_path)
    calls=[]
    component._query=lambda body,timeout,cleanup:calls.append(body) or {'duplicate_booked_seats':0,'zero_double_booking':True}
    with pytest.raises(ValueError):component.payment_durability()
    assert not calls
    assert component.zero_double_booking()=={'zero_double_booking':True}
    assert len(calls)==1
    with pytest.raises(ValueError):component.payment_durability()


@pytest.mark.parametrize('changed',['file','cohort','orders'])
def test_late_cohort_mutation_blocks_query(monkeypatch,tmp_path,changed):
    component,engine,_,_=setup(monkeypatch,tmp_path);path=bind(component,engine)
    if changed=='file':path.write_text('{}')
    elif changed=='cohort':component.cohort['show_ids']=[str(uuid4())]
    else:component.cohort['expected_paid']=0
    component._query=lambda *a,**k:pytest.fail('No query with changed cohort')
    with pytest.raises(ValueError):component.payment_durability()


def test_journal_failure_still_executes_mandatory_read_and_blocks_success(monkeypatch,tmp_path):
    component,engine,_,_=setup(monkeypatch,tmp_path)
    calls=[]
    def write(k,v):
        if k=='audit-intent':raise OSError('disk full')
    engine.runtime._write=write
    component._query=lambda *a,**k:calls.append(True) or {'duplicate_booked_seats':0,'zero_double_booking':True}
    with pytest.raises(RuntimeError):component.zero_double_booking()
    assert calls and not engine.journal_healthy and not engine.session.cleanup_mode


def test_transport_loss_does_not_replay_financial_query(monkeypatch,tmp_path):
    component,engine,_,_=setup(monkeypatch,tmp_path);bind(component,engine)
    calls=[]
    def lost(*a,**k):calls.append(True);raise TimeoutError()
    component._query=lost
    with pytest.raises(TimeoutError):component.payment_durability()
    with pytest.raises(ValueError):component.payment_durability()
    assert calls==[True] and not engine.session.cleanup_mode


@pytest.mark.parametrize('bad',['post_ttl_complete','payments_durable','ticket_relationships_valid'])
def test_failed_financial_gate_cannot_be_certified(monkeypatch,tmp_path,bad):
    component,engine,_,_=setup(monkeypatch,tmp_path);bind(component,engine)
    checks=dict.fromkeys(['post_ttl_complete','payments_durable','ticket_relationships_valid'],True);checks[bad]=False
    component._query=lambda *a,**k:{'pass':False,'checks':checks}
    with pytest.raises(ValueError):component.payment_durability()


@pytest.mark.parametrize('arm',['control','candidate'])
def test_full_queue_adapter_uses_actual_layout_and_fresh_bound_receipt(monkeypatch,tmp_path,arm):
    component,engine,session,events=setup(monkeypatch,tmp_path,arm=arm)
    engine.configure_common();engine.start_workers()
    calls=[]
    component._query=lambda body,timeout,cleanup:calls.append((body,timeout,cleanup)) or good_queue()
    result=component.queue_drain(engine.runtime.audit_binding)
    assert result['binding_sha256']==policy.digest(engine.runtime.audit_binding)
    assert result['all_queues_zero'] and result['kafka_drained'] and calls
    assert events[-1][0]=='queue-audit-sample'
    assert not session.cleanup_mode


def test_queue_deadline_cannot_be_extended_by_pending_work(monkeypatch,tmp_path):
    component,engine,_,_=setup(monkeypatch,tmp_path)
    engine.configure_common();engine.start_workers()
    ticks=[0]
    component.clock=lambda:ticks[0]
    component.sleep=lambda n:ticks.__setitem__(0,ticks[0]+n)
    value=good_queue();value['pending_confirmation_receipts']=1;value['pass']=False
    component._query=lambda *a,**k:value
    with pytest.raises(TimeoutError):component.queue_drain(engine.runtime.audit_binding,cleanup=True)
    assert ticks[0]==120 and not engine.session.cleanup_mode


def test_scope_mismatch_blocks_before_queue_observation(monkeypatch,tmp_path):
    component,_,session,_=setup(monkeypatch,tmp_path)
    previous=len(session.calls)
    with pytest.raises(ValueError):component.queue_drain({'wrong':'binding'})
    assert len(session.calls)==previous


def test_generated_programs_compile_and_bind_private_payload(monkeypatch,tmp_path):
    component,engine,session,_=setup(monkeypatch,tmp_path);bind(component,engine)
    engine.configure_common()
    before=session.values['primary'];target=execution.row_identity(before['rows'][0])
    # Select an API explicitly, not an infrastructure role by incidental ordering.
    target=execution.row_identity(next(r for r in before['rows'] if r['Config']['Labels']['com.docker.compose.service']=='api'))
    for body in (audit.financial_body(component.cohort),audit.duplicate_body(),audit.queue_body(HELPERS)):
        ast.parse(body)
        code=audit.transport_program(engine.saved,before,target,body,180)
        ast.parse(code)
        assert "'docker','exec','-i'" in code and 'signal.alarm(' in code
        assert 'input=audit_body' in code
        assert 'body_budget=int(budget(180))-6' in code


@pytest.mark.parametrize('phase',['original','infrastructure','partial','restored'])
def test_handover_queue_membership_is_observed_not_inferred(monkeypatch,tmp_path,phase):
    component,engine,session,_=setup(monkeypatch,tmp_path)
    if phase=='original':session.values['primary']['rows']=copy.deepcopy(session.original_rows)
    else:
        engine.configure_common()
        if phase in {'partial','restored'}:
            engine.start_workers()
            if phase=='partial':
                session.values['secondary']['rows']=session.values['secondary']['rows'][:2]
            else:engine.stop_workers();engine.restore()
    members=1 if phase in {'original','restored'} else 2 if phase=='partial' else 0
    bodies=[]
    component._query=lambda body,timeout,cleanup:bodies.append(body) or good_queue(members)
    result=component.queue_drain(engine.runtime.audit_binding,cleanup=True)
    assert result['all_queues_zero']
    assert ('dormant_kafka_sample(observer' in bodies[0]) is (members==0)


def test_journal_full_does_not_block_owned_queue_observation(monkeypatch,tmp_path):
    component,engine,_,_=setup(monkeypatch,tmp_path)
    engine.configure_common();engine.start_workers()
    def full(*a):raise OSError('disk full')
    engine.runtime._write=full
    component._query=lambda *a,**k:good_queue()
    assert component.queue_drain(engine.runtime.audit_binding,cleanup=True)['all_queues_zero']
    assert not engine.journal_healthy
    with pytest.raises(OSError):component.queue_drain(engine.runtime.audit_binding)


def dormant_fixture():
    from types import SimpleNamespace

    from kafka import TopicPartition
    part=TopicPartition('ticketing.events',0)
    state=SimpleNamespace(error_code=0,state='Empty',members=[])
    admin=SimpleNamespace(describe_consumer_groups=lambda groups:[state],
                          list_consumer_group_offsets=lambda *a,**k:{part:SimpleNamespace(offset=4)})
    reader=SimpleNamespace(partitions_for_topic=lambda topic:{0},end_offsets=lambda parts:{part:4})
    return SimpleNamespace(admin=admin,end_reader=reader,topic='ticketing.events'),part,state


def test_dormant_group_requires_real_offsets():
    observer,_,_=dormant_fixture()
    assert audit.dormant_kafka_sample(observer,'ticketing-fulfillment-v1')=={'total_lag':0,'members':0}


@pytest.mark.parametrize('fault',['lag','missing_offset','missing_end','negative_offset','metadata_drift','group_drift','no_metadata'])
def test_dormant_kafka_never_turns_absent_or_changed_offsets_into_zero(fault):
    from types import SimpleNamespace
    observer,part,state=dormant_fixture()
    if fault=='lag':observer.end_reader.end_offsets=lambda parts:{part:5}
    elif fault=='missing_offset':observer.admin.list_consumer_group_offsets=lambda *a,**k:{}
    elif fault=='missing_end':observer.end_reader.end_offsets=lambda *a:{}
    elif fault=='negative_offset':observer.admin.list_consumer_group_offsets=lambda *a,**k:{part:SimpleNamespace(offset=-1)}
    elif fault=='metadata_drift':
        values=iter([{0},{0,1}]);observer.end_reader.partitions_for_topic=lambda topic:next(values)
    elif fault=='group_drift':
        states=iter([[state],[SimpleNamespace(error_code=0,state='Stable',members=[object()])]])
        observer.admin.describe_consumer_groups=lambda *a:next(states)
    else:observer.end_reader.partitions_for_topic=lambda *a:set()
    if fault=='lag':assert audit.dormant_kafka_sample(observer,'g')['total_lag']==1
    else:
        with pytest.raises(ValueError):audit.dormant_kafka_sample(observer,'g')


def test_dormant_membership_wait_is_bounded(monkeypatch):
    observer,_,state=dormant_fixture();state.state='PreparingRebalance'
    ticks=[0]
    monkeypatch.setattr(audit.time,'monotonic',lambda:ticks[0])
    monkeypatch.setattr(audit.time,'sleep',lambda n:ticks.__setitem__(0,ticks[0]+n))
    with pytest.raises(TimeoutError):audit.dormant_kafka_sample(observer,'g')
    assert ticks[0]==10



@pytest.mark.parametrize('changed',['committed','end'])
def test_dormant_offset_changes_during_sample_cannot_be_certified(changed):
    from types import SimpleNamespace
    observer,part,_=dormant_fixture()
    if changed=='committed':
        samples=iter([{part:SimpleNamespace(offset=4)},{part:SimpleNamespace(offset=5)}])
        observer.admin.list_consumer_group_offsets=lambda *a,**k:next(samples)
    else:
        samples=iter([{part:4},{part:5}]);observer.end_reader.end_offsets=lambda *a:next(samples)
    with pytest.raises(ValueError):audit.dormant_kafka_sample(observer,'g')



@pytest.mark.parametrize('arm',['control','candidate'])
def test_concrete_queue_provider_covers_start_stop_and_restoration(monkeypatch,tmp_path,arm):
    _,engine,session,_=setup(monkeypatch,tmp_path,arm=arm)
    component=audit.connect_audits(engine,HELPERS,now=lambda:NOW)
    assert engine.drain_provider is engine.runtime.drain_provider
    calls=[]
    def query(body,timeout,*,cleanup):
        members=sum(r['State']['Running'] and r['Config']['Labels']['com.docker.compose.service']=='consumer'
                    for value in session.values.values() for r in value['rows'])
        calls.append((members,cleanup))
        return good_queue(members)
    component._query=query
    engine.configure_common();engine.start_workers();engine.stop_workers();engine.restore()
    assert calls==[(0,False),(0,False),(6,True),(0,True)]
    assert engine.restore is not None and not session.values['secondary']['rows']
    with pytest.raises(ValueError):audit.connect_audits(engine,HELPERS)


def test_forward_pending_queue_blocks_placement_mutation(monkeypatch,tmp_path):
    _,engine,session,_=setup(monkeypatch,tmp_path)
    component=audit.connect_audits(engine,HELPERS,now=lambda:NOW)
    ticks=[0];component.clock=lambda:ticks[0]
    component.sleep=lambda n:ticks.__setitem__(0,ticks[0]+n)
    value=good_queue(0);value['pending_confirmation_receipts']=1;value['pass']=False
    component._query=lambda *a,**k:value
    with pytest.raises(TimeoutError):engine.configure_common()
    assert not session.mutations and engine.failed

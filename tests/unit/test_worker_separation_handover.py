"""ADR0199 restored-control continuation; actual adapters with synthetic transports."""
import copy
import importlib.util
import sys
from datetime import UTC, datetime, timedelta
from pathlib import Path

import pytest

ROOT=Path(__file__).resolve().parents[2]
sys.path.insert(0,str(ROOT/'scripts'))
import work_envelope as policy
import worker_separation_handover as handover
import worker_separation_runtime as runtime
from stage_status_refresh_images import new_stage_output


def module(name):
    spec=importlib.util.spec_from_file_location(name,ROOT/'tests/unit'/(name+'.py'))
    result=importlib.util.module_from_spec(spec);spec.loader.exec_module(result)
    return result


def control(monkeypatch):
    value=module('test_worker_separation_recovery').setup(monkeypatch,'control')
    lifecycle,stage,engine,session,_events=value
    engine.runtime.now=engine.now
    assert lifecycle.run()['pass']
    return lifecycle,stage,engine,session


def candidate(proof,engine,session,**kwargs):
    return runtime.RuntimeActions(session,engine.runtime.guard,engine.runtime.inputs,engine.saved,
                                  'candidate',new_stage_output(),drain_provider=engine.drain_provider,
                                  now=kwargs.get('now',engine.now),handover=proof)


def test_new_restored_ids_bound_to_candidate_and_exact_five_worker_removal(monkeypatch):
    lifecycle,_stage,engine,session=control(monkeypatch)
    saved=copy.deepcopy(engine.saved);binding=copy.deepcopy(engine.runtime.guard.binding)
    restored=copy.deepcopy(session.values['primary']['rows'])
    proof=handover.issue(lifecycle)
    assert proof.container_ids!=saved['original_containers']
    adapter=candidate(proof,engine,session)
    assert adapter.binding['handover_sha256']==proof.sha256
    assert adapter.audit_binding['lifecycle_binding_sha256']==policy.digest(adapter.binding)
    docker_module=module('test_worker_separation_runtime')
    docker=docker_module.Docker(restored,session.values['primary']['volumes'])
    original=session.call
    def call(host,code,timeout):
        if 'owned_workers_stopped' in code:
            assert host=='primary'
            result=docker_module.execute(code,docker)
            session.values['primary']['rows']=copy.deepcopy(docker.rows)
            return result
        return original(host,code,timeout)
    session.call=call
    for action in runtime.ORDER:
        assert adapter.perform({**adapter.binding,'action':action,'cleanup':False,
                                'timeout_seconds':runtime.LIMITS[action]})['pass']
    expected={r['Id'] for r in restored if r['Config']['Labels']['com.docker.compose.service'] in runtime.WORKERS}
    assert set(docker.removed)==expected and len(docker.removed)==5
    assert engine.saved==saved and engine.runtime.saved==saved and adapter.saved==saved
    assert engine.runtime.guard.binding==binding
    with pytest.raises(ValueError,match='single-use'):candidate(proof,engine,session)


@pytest.mark.parametrize('change',['failed','wrong_arm','missing_summary','summary','journal','busy','queues','restart_during_check'])
def test_invalid_control_cannot_issue_candidate_proof(monkeypatch,change):
    lifecycle,_stage,engine,session=control(monkeypatch)
    if change=='failed':lifecycle.record['pass']=False
    elif change=='wrong_arm':engine.arm='candidate'
    elif change=='missing_summary':lifecycle.summary_path.unlink()
    elif change=='summary':lifecycle.summary_path.write_text('{}')
    elif change=='journal':lifecycle.journal_healthy=False
    elif change=='busy':session.generator_idle=False
    elif change=='queues':engine.drain_provider=lambda _:{'binding_sha256':'f'*64,'checked_at':engine.now().isoformat(),'dispatch_stopped':True,'all_queues_zero':False,'kafka_drained':True}
    else:
        original=engine.drain_provider
        def drain(bound):
            session.values['primary']['rows'][0]['State']['StartedAt']='changed'
            return original(bound)
        engine.drain_provider=drain
    before=len(session.mutations)
    with pytest.raises((ValueError,FileNotFoundError)):handover.issue(lifecycle)
    assert len(session.mutations)==before
    with pytest.raises(ValueError,match='single-use'):handover.issue(lifecycle)


@pytest.mark.parametrize('change',['id','started','image','bind','secondary','receipt','summary','sources','scope'])
def test_identity_or_evidence_drift_blocks_candidate_stop(monkeypatch,change):
    lifecycle,_stage,engine,session=control(monkeypatch)
    proof=handover.issue(lifecycle);adapter=candidate(proof,engine,session)
    before=len(session.mutations)
    if change=='id':session.values['primary']['rows'][0]['Id']='f'*64
    elif change=='started':session.values['primary']['rows'][0]['State']['StartedAt']='changed'
    elif change=='image':session.values['primary']['rows'][0]['Image']='sha256:'+'f'*64
    elif change=='bind':session.values['primary']['bind_sha256']={'/actual/runtime-nginx.conf':'f'*64}
    elif change=='secondary':session.values['secondary']['rows']=[copy.deepcopy(session.values['primary']['rows'][0])]
    elif change=='receipt':proof.path.write_text('{}')
    elif change=='summary':lifecycle.summary_path.write_text('{}')
    elif change=='sources':monkeypatch.setattr(handover,'adapter_identity',lambda:{'changed':'f'*64})
    else:adapter.guard.key='foreign'
    with pytest.raises(ValueError):
        adapter.perform({**adapter.binding,'action':'verify_original_runtime','cleanup':False,'timeout_seconds':45})
    assert adapter.failed and len(session.mutations)==before


@pytest.mark.parametrize('change',['session','guard','snapshot','stale'])
def test_candidate_claim_rejects_unrelated_or_stale_binding(monkeypatch,change):
    lifecycle,_stage,engine,session=control(monkeypatch)
    proof=handover.issue(lifecycle)
    kwargs={}
    if change=='session':session=copy.copy(session)
    elif change=='guard':session.action_guard=copy.copy(session.action_guard)
    elif change=='snapshot':engine.saved=copy.deepcopy(engine.saved);engine.saved['original_containers'][0]='f'*64
    else:kwargs['now']=lambda:engine.now()+timedelta(seconds=31)
    with pytest.raises(ValueError):candidate(proof,engine,session,**kwargs)
    assert not proof.used


@pytest.mark.parametrize('phase',['candidate-handover','candidate-handover-claim'])
def test_handover_journal_loss_blocks_replay(monkeypatch,phase):
    lifecycle,_stage,engine,session=control(monkeypatch)
    original=engine.runtime._write
    def write(kind,value):
        if kind==phase:raise OSError('Synthetic full disk')
        return original(kind,value)
    if phase=='candidate-handover':
        engine.runtime._write=write
        with pytest.raises(OSError):handover.issue(lifecycle)
        with pytest.raises(ValueError):handover.issue(lifecycle)
    else:
        proof=handover.issue(lifecycle);engine.runtime._write=write
        with pytest.raises(OSError):candidate(proof,engine,session)
        assert proof.used and proof.runtime is None
        with pytest.raises(ValueError):candidate(proof,engine,session)


def test_arbitrary_summary_dictionary_cannot_issue_handover():
    with pytest.raises(ValueError):handover.issue({'pass':True,'status':'PASSED_RESTORED'})
    with pytest.raises(TypeError):handover.CandidateHandover(None,None,None,None,None)


def test_configuration_builder_carries_proof_without_rewriting_baseline(monkeypatch):
    from worker_separation_configuration import ConfigurationActions
    lifecycle,_stage,engine,session=control(monkeypatch)
    engine.runtime.now=lambda:datetime.now(UTC)
    proof=handover.issue(lifecycle)
    cfg=ConfigurationActions(session,engine.runtime.guard,engine.runtime.inputs,engine.saved,
                             'candidate',new_stage_output(),handover=proof)
    assert cfg.runtime.handover is proof and cfg.runtime.saved==engine.saved
    assert cfg.runtime._original()['primary']['rows']==session.values['primary']['rows']


@pytest.mark.parametrize('change',['started','image','partial'])
def test_generated_stop_rechecks_restored_targets_and_never_replays_unknown_result(monkeypatch,change):
    lifecycle,_stage,engine,session=control(monkeypatch)
    proof=handover.issue(lifecycle);adapter=candidate(proof,engine,session)
    docker_module=module('test_worker_separation_runtime')
    docker=docker_module.Docker(session.values['primary']['rows'],session.values['primary']['volumes'])
    docker.change=change
    original=session.call
    def call(host,code,timeout):
        if 'owned_workers_stopped' in code:return docker_module.execute(code,docker)
        return original(host,code,timeout)
    session.call=call
    for action in runtime.ORDER[:2]:
        adapter.perform({**adapter.binding,'action':action,'cleanup':False,'timeout_seconds':runtime.LIMITS[action]})
    request={**adapter.binding,'action':'stop_original_workers','cleanup':False,'timeout_seconds':60}
    with pytest.raises((ValueError,TimeoutError)):adapter.perform(request)
    assert adapter.failed and len(docker.removed)==(1 if change=='partial' else 0)
    with pytest.raises(ValueError,match='replay'):adapter.perform(request)
    assert len(docker.removed)==(1 if change=='partial' else 0)


def test_changed_receipt_blocks_later_forward_action_before_remote_call(monkeypatch):
    lifecycle,_stage,engine,session=control(monkeypatch)
    proof=handover.issue(lifecycle);adapter=candidate(proof,engine,session)
    adapter.perform({**adapter.binding,'action':'verify_original_runtime','cleanup':False,'timeout_seconds':45})
    proof.path.write_text('{}')
    before=len(session.calls)
    with pytest.raises(ValueError):
        adapter.perform({**adapter.binding,'action':'verify_generator_idle','cleanup':False,'timeout_seconds':30})
    assert len(session.calls)==before


@pytest.mark.parametrize('change',['replace','size','mtime','ctime'])
def test_receipt_reader_rejects_file_changes_during_read(monkeypatch,tmp_path,change):
    from types import SimpleNamespace
    path=tmp_path/'receipt.json';path.write_text('{}')
    real=handover.os.fstat
    def changed(fd):
        value=real(fd)
        names=('st_dev','st_ino','st_size','st_mtime_ns','st_ctime_ns')
        values={k:getattr(value,k) for k in names}
        key={'replace':'st_ino','size':'st_size','mtime':'st_mtime_ns','ctime':'st_ctime_ns'}[change]
        values[key]+=1
        # A change between descriptor observations must be detected as well.
        if change=='ctime':values[key]+=changed.calls
        changed.calls+=1
        return SimpleNamespace(**values)
    changed.calls=0
    monkeypatch.setattr(handover.os,'fstat',changed)
    with pytest.raises(ValueError,match='changed'):handover.evidence(path)

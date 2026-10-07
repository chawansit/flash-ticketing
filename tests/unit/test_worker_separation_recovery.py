"""ADR0198 restoration assembly uses real guarded primitives with synthetic transports."""
import copy
import importlib.util
import sys
from pathlib import Path
from types import SimpleNamespace

import pytest

ROOT=Path(__file__).resolve().parents[2]
sys.path.insert(0,str(ROOT/'scripts'))
import worker_separation_paid_stage as paid
import worker_separation_recovery as recovery
from worker_separation_artifacts import validate_seal
from worker_separation_snapshot import verify_restored


def fixture_module(name):
    spec=importlib.util.spec_from_file_location(name,ROOT/'tests/unit'/(name+'.py'))
    result=importlib.util.module_from_spec(spec);spec.loader.exec_module(result)
    return result


def setup(monkeypatch,arm='candidate'):
    fixture=fixture_module('test_worker_separation_execution')
    engine,session=fixture.create(monkeypatch,arm)
    engine.configure_common();engine.start_workers()
    original=session.call
    events=[]
    def remote(host,code,timeout):
        if 'owned_artifacts_removed' in code:
            assert session.cleanup_mode
            events.append(('artifact' if 'remove=True' in code else 'artifact_preflight',host))
            if session.fail=='artifact_'+host:raise TimeoutError('Synthetic artifact cleanup response lost')
            return {'owned_artifacts_removed' if 'remove=True' in code else 'owned_artifacts_verified':True}
        if 'owned_configuration_removed' in code:
            events.append(('configuration',host))
            if session.fail=='configuration_'+host:raise TimeoutError('Synthetic configuration response lost')
            return {'owned_configuration_removed':True}
        return original(host,code,timeout)
    session.call=remote
    stage=object.__new__(paid.PaidStage)
    stage.execution,stage.runtime,stage.session=engine,engine.runtime,session
    stage.used=False;stage.journal_healthy=True
    stage._guard=engine._guard
    stage.locations={h:'/qualification/'+h+'/owner/'+arm for h in ('primary','secondary','generator','container')}
    stage.artifact_attempts=list(stage.locations)
    stage.artifact_seals={h:validate_seal({'fresh_arm_directory':True,
        'root':{'device':1,'inode':1,'uid':0,'mode':0o700},
        'directory':{'device':1,'inode':2,'uid':0,'mode':0o700}},p) for h,p in stage.locations.items()}
    def retired():
        events.append(('retirement','primary'))
        value=engine._observe(cleanup=True)
        assert verify_restored(engine.saved,{h:o['rows'] for h,o in value.items()},value['primary']['volumes'],value['primary']['bind_sha256'])==recovery.RESTORED
        return True
    def credentials(**kwargs):
        assert kwargs=={'restored':True};assert session.cleanup_mode
        events.append(('credentials','primary'))
        return {'diagnostic_credentials_removed':True}
    stage.diagnostics=SimpleNamespace(sources={'files':{'observer.py':'synthetic'}},attempted=True,
                                    container_retired=retired,cleanup=credentials,cid='a'*64)
    class Audits:
        def __init__(self):self.used=set()
        fail=None
        def payment_durability(self):
            self.used.add('financial');events.append(('financial','primary'))
            if self.fail=='financial':raise ValueError('Synthetic financial mismatch')
            return copy.deepcopy(recovery.FINANCIAL)
        def zero_double_booking(self):
            self.used.add('duplicates');events.append(('duplicates','primary'))
            if self.fail=='duplicates':raise ValueError('Synthetic duplicate booking')
            return {'zero_double_booking':True}
        def queue_drain(self,binding,cleanup=True):
            assert cleanup
            events.append(('queues','primary'))
            if self.fail=='queues':raise ValueError('Synthetic pending queue')
            return copy.deepcopy(engine.drain_provider(binding))
    stage.audits=Audits()
    def stop():
        stage.jobs.cleanup_used=True;events.append(('stop_jobs','generator'))
        return {'pass':True,'all_jobs_stopped':True,'dispatch_stopped':True,'journal_healthy':True,'failures':[]}
    stage.jobs=SimpleNamespace(cleanup_used=False,attempts=[],stop_all=stop)
    def run():
        stage.used=True
        return {'stage_pass':True,'job_cleanup':stop(),'financial':stage.audits.payment_durability(),
                'duplicates':stage.audits.zero_double_booking(),'queues':stage.audits.queue_drain(engine.runtime.audit_binding)}
    stage.run=run
    lifecycle=recovery.RestoredPaidArm(stage)
    return lifecycle,stage,engine,session,events


@pytest.mark.parametrize('arm',['control','candidate'])
def test_complete_stage_restores_runtime_then_removes_only_known_files(monkeypatch,arm):
    lifecycle,_stage,engine,session,events=setup(monkeypatch,arm)
    result=lifecycle.run()
    assert result['pass'] and result['status']=='PASSED_RESTORED' and result['restoration_complete']
    assert result['private_artifact_cleanup_complete']
    assert [m[0] for m in session.mutations]==['common','start','stop','restore']
    assert result['events']==['stop_workers','verify_worker_absence','restore_original','verify_restoration',
                            'restored_queues','restored_idle','cleanup_files']
    assert [h for n,h in events if n=='artifact']==['primary','secondary','generator']
    assert len([n for n,h in events if n=='artifact_preflight'])==3
    assert not session.values['secondary']['rows'] and session.cleanup_mode is False
    assert engine.configurations.cleanup_used==set(engine.configurations.seals)
    with pytest.raises(ValueError,match='replayed'):lifecycle.run()


@pytest.mark.parametrize('gate',['financial','duplicates','queues'])
def test_failed_audit_keeps_evidence_but_still_attempts_independent_checks_and_restoration(monkeypatch,gate):
    lifecycle,stage,engine,session,events=setup(monkeypatch)
    stage.audits.fail=gate
    result=lifecycle.run()
    assert not result['pass'] and result['status']=='RECOVERY_REQUIRED'
    assert not result['private_artifact_cleanup_complete']
    assert {'financial','duplicates','queues'}<={n for n,h in events}
    assert 'verify_restoration' in result['events'] and 'restored_idle' in result['events']
    assert not any(n in {'artifact','credentials','configuration'} for n,h in events)
    assert not engine.configurations.cleanup_used
    assert session.cleanup_mode is False


@pytest.mark.parametrize('failure',['stop','restore'])
def test_unknown_mutation_not_replayed_and_independent_restoration_proof_is_retained(monkeypatch,failure):
    lifecycle,_stage,_engine,session,_events=setup(monkeypatch)
    session.fail=failure
    result=lifecycle.run()
    assert not result['pass'] and result['status']=='RECOVERY_REQUIRED'
    assert result['restoration_complete']  # Independent observations prove actual restoration, not the lost ack.
    assert len([m for m in session.mutations if m[0]==failure])==1
    assert not result['private_artifact_cleanup_complete'] and 'cleanup_files' not in result['events']
    assert 'restored_queues' in result['events'] and 'restored_idle' in result['events']


@pytest.mark.parametrize('phase',['worker-arm-start','worker-arm-intent','execution-intent','worker-arm-summary'])
def test_journal_loss_never_skips_cleanup_or_certifies_completion(monkeypatch,phase):
    lifecycle,_stage,engine,session,_events=setup(monkeypatch)
    original=engine.runtime._write
    def write(kind,value):
        if kind==phase:raise OSError('Synthetic full disk')
        return original(kind,value)
    engine.runtime._write=write
    result=lifecycle.run()
    assert not result['pass'] and result['status']=='RECOVERY_REQUIRED' and not result['journal_healthy']
    assert 'verify_restoration' in result['events'] and 'restored_idle' in result['events']
    assert result['restoration_complete'] and session.cleanup_mode is False
    assert len([m for m in session.mutations if m[0]=='restore'])==1


@pytest.mark.parametrize('change',['unknown_seal','replaced_scope','generator_busy','restored_bind','configuration_owner','configuration_missing','fixture_guard'])
def test_ownership_and_restoration_faults_block_artifact_deletion(monkeypatch,change):
    lifecycle,stage,engine,session,events=setup(monkeypatch)
    original=stage.run
    def run():
        result=original()
        if change=='unknown_seal':stage.artifact_seals.pop('generator')
        elif change=='replaced_scope':engine.runtime.guard.key='foreign'
        elif change=='generator_busy':session.generator_idle=False
        elif change=='configuration_owner':engine.configurations.seals['primary']['owner']='/qualification/repository/tmp/foreign'
        elif change=='configuration_missing':engine.configurations.seals.pop('secondary')
        elif change=='fixture_guard':stage._guard=lambda *a,**k:(_ for _ in ()).throw(ValueError('Synthetic retained fixture changed'))
        else:session.values['primary']['bind_sha256']={'/actual/runtime-nginx.conf':'f'*64}
        return result
    stage.run=run
    result=lifecycle.run()
    assert not result['pass'] and result['status']=='RECOVERY_REQUIRED'
    assert not result['private_artifact_cleanup_complete']
    assert not any(n in {'artifact','credentials','configuration'} for n,h in events)
    assert 'verify_restoration' in result['events'] and 'restored_idle' in result['events']
    if change=='fixture_guard':assert result['restoration_complete']


@pytest.mark.parametrize('failure',['artifact_primary','configuration_primary'])
def test_cleanup_response_loss_still_attempts_other_known_sites(monkeypatch,failure):
    lifecycle,_stage,_engine,session,events=setup(monkeypatch)
    session.fail=failure
    result=lifecycle.run()
    assert not result['pass'] and result['status']=='RECOVERY_REQUIRED'
    if failure=='artifact_primary':
        # Preflight rejection deletes nothing.
        assert not any(n in {'artifact','credentials','configuration'} for n,h in events)
    else:
        assert [h for n,h in events if n=='configuration']==['primary','secondary']
    assert session.cleanup_mode is False


def test_failed_customer_gate_with_complete_safety_restores_but_cannot_certify_capacity(monkeypatch):
    lifecycle,stage,_engine,_session,_events=setup(monkeypatch)
    original=stage.run
    def failed():
        value=original();value['stage_pass']=False;return value
    stage.run=failed
    result=lifecycle.run()
    assert result['status']=='FAILED_RESTORED' and not result['pass'] and result['restoration_complete']
    assert result['private_artifact_cleanup_complete']


def test_asynchronous_interruption_before_stage_audits_runs_all_cleanup(monkeypatch):
    lifecycle,stage,_engine,_session,events=setup(monkeypatch)
    stage.run=lambda:(_ for _ in ()).throw(KeyboardInterrupt('Synthetic user interruption'))
    result=lifecycle.run()
    assert result['status']=='FAILED_RESTORED' and not result['pass']
    assert {'stop_jobs','financial','duplicates','queues'}<={n for n,h in events}
    assert result['restoration_complete']


def test_constructor_rejects_unstarted_deployment(monkeypatch):
    _lifecycle,stage,engine,_session,_events=setup(monkeypatch)
    engine.used.remove('start_arm_workers')
    with pytest.raises(ValueError,match='started'):recovery.RestoredPaidArm(stage)


def test_explicit_artifact_set_does_not_adopt_unknown_subdirectories(monkeypatch):
    _lifecycle,stage,_engine,_session,_events=setup(monkeypatch)
    stage.jobs.attempts=[{'site':'generator','expected':{'name':'run_synchronized_paid_generator'}}]
    names=recovery.artifacts(stage,'generator')
    assert {'manifest.private.json','customer.json','job-run_synchronized_paid_generator-identity.json'}<=names
    assert not any('paid-shards-' in n or '__pycache__' in n for n in names)
    assert 'fixture.json' not in recovery.artifacts(stage,'primary')


@pytest.mark.parametrize('bad',[1,0,'true',None])
def test_financial_checks_require_actual_true_boolean(monkeypatch,bad):
    lifecycle,stage,_engine,_session,_events=setup(monkeypatch)
    original=stage.run
    def run():
        value=original();value['financial']['payments_durable']=bad;return value
    stage.run=run
    result=lifecycle.run()
    assert not result['pass'] and not result['private_artifact_cleanup_complete']
    assert result['status']=='RECOVERY_REQUIRED'



def test_restored_container_ids_cannot_be_reused_as_original_handover_identity(monkeypatch):
    lifecycle,_stage,engine,session,_events=setup(monkeypatch,'control')
    assert lifecycle.run()['pass']
    assert sorted(r['Id'] for r in session.values['primary']['rows'])!=engine.saved['original_containers']
    before=len(session.mutations)
    with pytest.raises(ValueError,match='Original containers replaced'):engine.runtime._original()
    assert len(session.mutations)==before

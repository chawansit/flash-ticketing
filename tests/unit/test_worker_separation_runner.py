"""ADR0200 connected bootstrap/recovery adapters with simulated backend outcomes."""
import copy
import hashlib
import importlib.util
import sys
from datetime import UTC, datetime, timedelta
from pathlib import Path
from types import SimpleNamespace
from urllib.parse import unquote, urlsplit
from uuid import uuid4

import pytest

ROOT=Path(__file__).resolve().parents[2]
sys.path.insert(0,str(ROOT/'scripts'))
import work_envelope as policy
import worker_separation_audits as audit
import worker_separation_paid_stage as paid
import worker_separation_readiness as readiness
import worker_separation_runner as runner
import worker_separation_runtime as runtime
import worker_separation_staging as staging
from diagnostic_runner_connection import ProtectedContext
from fixture_identity_evidence import retain_fixture_identity
from run_two_host_paid_comparison import frozen_bundle
from stage_status_refresh_images import new_stage_output
from worker_separation_observer_bundle import prepare as observers
from worker_separation_snapshot import runtime_semantic, verify_restored

CA='synthetic exact CA bytes\n'
HELPERS={k:'a'*64 for k in audit.HELPERS}


def module(name):
    spec=importlib.util.spec_from_file_location(name,ROOT/'tests/unit'/(name+'.py'))
    value=importlib.util.module_from_spec(spec);spec.loader.exec_module(value)
    return value


@pytest.fixture(scope='module')
def source_bundle():return frozen_bundle()


def setup(monkeypatch,tmp_path,source_bundle):
    fixture=module('test_worker_separation_execution')
    saved,_rows,_volumes,inputs=fixture.fixture()
    route=urlsplit(inputs['pair']['control']['primary']['services']['consumer']['environment']['DATABASE_URL'])
    fields={'DB_HOST':'10.0.0.8','DB_PORT':'5432','DB_NAME':unquote(route.path[1:]),'DB_USER':'root',
            'DB_PASSWORD':unquote(route.password),'SERVER_TLS_SSLMODE':'verify-full','SERVER_TLS_CA_FILE':'/etc/pgbouncer/ca.pem'}
    mount={'type':'bind','source':'/root/qualification/ca.pem','target':fields['SERVER_TLS_CA_FILE'],'read_only':True}
    saved['model']['services']['pgbouncer']['environment'].update(fields)
    saved['model']['services']['pgbouncer'].setdefault('volumes',[]).append(mount)
    saved['bind_sha256'][mount['source']]=hashlib.sha256(CA.encode()).hexdigest()
    for arm in ('control','candidate'):
        pg=inputs['pair'][arm]['primary']['services']['pgbouncer']
        pg['environment'].update(fields);pg.setdefault('volumes',[]).append(copy.deepcopy(mount))
    updated_route=route._replace(netloc='root:'+route.password+'@'+route.hostname+':'+str(route.port)).geturl()
    for arm in ('control','candidate'):
        for host in ('primary','secondary'):
            model=inputs['pair'][arm][host]
            if model is not None:
                for service in model['services'].values():
                    if 'DATABASE_URL' in service['environment']:service['environment']['DATABASE_URL']=updated_route
    rows=fixture.rows_for(saved['model'],saved['counts'])
    saved['semantics']={r['Config']['Labels']['com.docker.compose.service']:policy.digest(runtime_semantic(r)) for r in rows}
    saved['original_containers']=sorted(r['Id'] for r in rows)
    inputs['saved_runtime_sha256']=policy.digest(saved)
    metadata=module('test_worker_separation_staging').inputs()[1]
    inputs['staging_contract']=staging.make_contract(inputs['pair'],inputs['role_sources'],metadata)
    inputs['archive_receipt']['contract_sha256']=policy.digest(inputs['staging_contract'])
    sources={'api':next(iter(inputs['role_sources'].values())),**copy.deepcopy(inputs['role_sources'])}
    observer_sources=observers();artifact_output=new_stage_output()
    target={'host':fields['DB_HOST'],'port':5432,'dbname':fields['DB_NAME'],'user':fields['DB_USER'],
            'ca_source_path':mount['source'],'ca_sha256':saved['bind_sha256'][mount['source']]}
    dependency=readiness.dependency_context(SimpleNamespace(pair=inputs['pair'],saved=saved),CA)
    binding={**runtime.binding_for(inputs),'worker_dependency_context_sha256':policy.digest(dependency),
             'worker_inventory_sources_sha256':policy.digest(sources),
             'worker_observer_manifest_sha256':observer_sources['manifest_sha256'],
             'worker_observer_owner_name':artifact_output.name,'worker_diagnostic_target_sha256':policy.digest(target),
             'worker_paid_stage_contract_sha256':policy.digest(paid.stage_sources(source_bundle)[1]),
             'worker_audit_contract_sha256':policy.digest(audit.audit_contract(HELPERS)),
             'worker_audit_expectations':{'expected_orders':18000,'expected_paid':18000,'callbacks':3,'shows':60}}
    guard=object.__new__(policy.ActionGuard);guard.key='synthetic-worker-scope';guard.binding=binding
    session=fixture.Session(guard,saved,rows);session.original_rows=copy.deepcopy(rows)
    session.config.update(generator={'repo':'/qualification/generator'})
    session.phase=lambda _:None;session.begin_cleanup=lambda:setattr(session,'cleanup_mode',True)
    session.paused=False;session.original_stops=0;session.paid_arms=[];session.bootstrap_fail=None;session.arm_factories=[]
    def check(_):
        if session.paused:raise ValueError('Synthetic human pause')
    guard.check=check
    monkeypatch.setattr(policy,'PROFILES',{staging.PROFILE:('synthetic','ADR0200')})
    monkeypatch.setattr(runtime,'check_authority',lambda *a:{'status':'PASS'})
    state=tmp_path/'scope.json';monkeypatch.setattr(policy,'STATE',state)
    policy.write(state,{guard.key:{'binding':binding,'paid_runs_authorized':2,'paid_runs_started':0,'attempted_paid_arms':[]}})
    original=session.call
    def call(host,code,timeout):
        constants=fixture.literal_assignments(code)
        if 'manifest' in constants and 'payload' in constants:
            if session.bootstrap_fail=='install':raise TimeoutError('Synthetic unknown configuration creation')
            return module('test_worker_separation_configuration').seal({'manifest':constants['manifest']},constants['owner'])
        if 'configuration_matches' in code and constants.get('cleanup') is False:
            return {'configuration_matches':True}
        if 'owned_configuration_removed' in code:
            if session.bootstrap_fail=='configuration_cleanup':raise TimeoutError('Synthetic lost cleanup acknowledgement')
            return {'owned_configuration_removed':True}
        if 'owned_artifacts_removed' in code:
            return {'owned_artifacts_removed' if 'remove=True' in code else 'owned_artifacts_verified':True}
        if 'owned_workers_stopped' in code and 'targets' in constants:
            session.original_stops+=1
            ids={t['container_id'] for t in constants['targets']}
            if session.bootstrap_fail=='original_stop':
                ids={next(iter(ids))}
            session.values['primary']['rows']=[r for r in session.values['primary']['rows'] if r['Id'] not in ids]
            if session.bootstrap_fail=='original_stop':raise TimeoutError('Synthetic partial original stop')
            return {'owned_workers_stopped':True,'target_count':5}
        if 'body' in constants and 'probe_removed' in code:
            if session.bootstrap_fail=='readiness':raise TimeoutError('Synthetic dependency probe response lost')
            return {'checks':{k:True for k in readiness.CHECKS},'probe_removed':True,'runtime_unchanged':True}
        if 'audit_body' in constants and 'expected_helpers=' in constants['audit_body']:
            if session.bootstrap_fail=='queues':raise ValueError('Synthetic pending queue')
            count=sum(r['State']['Running'] and r['Config']['Labels']['com.docker.compose.service']=='consumer'
                      for v in session.values.values() for r in v['rows'])
            return {**dict.fromkeys(audit.QUEUE_ZERO,0),'kafka_members':count,'pass':True}
        if 'audit_body' in constants and 'duplicate_snapshot' in constants['audit_body']:
            return {'duplicate_booked_seats':0,'zero_double_booking':True}
        if 'audit_body' in constants:
            return {'pass':True,'checks':{'post_ttl_complete':True,'payments_durable':True,'ticket_relationships_valid':True}}
        return original(host,code,timeout)
    session.call=call
    def collect(transport,pair,arm,role_sources,*,retained=None):
        assert transport is session and role_sources==sources
        session.arm_factories.append(arm)
        if session.bootstrap_fail=='inventory':raise ValueError('Synthetic source inventory mismatch')
        from worker_separation_inventory import inspect_layout
        observed={h:v['rows'] for h,v in session.values.items()}
        return {'decision':'ADR0184','arm':arm,'prepared_pair_sha256':policy.digest(pair),
                'containers':inspect_layout(pair,arm,observed),'budgets':pair['budgets']}
    monkeypatch.setattr(runner,'collect_roles',collect)
    def stage_images(*args, **kwargs):
        if session.bootstrap_fail=='staging':return {'pass':False,'status':'RECOVERY_REQUIRED'}
        return {'pass':True,'status':'STAGED_VERIFIED','runtime_unchanged':True}
    monkeypatch.setattr(runner,'stage_package',stage_images)
    def paid_run(stage):
        stage.used=True;stage.inventory=stage.diagnostics.inventory
        stage.record['scheduled_offered_start_utc']=datetime.now(UTC).isoformat()
        stage.local.mkdir()
        fixture_data={'schema_version':1,'environment':'development','fixture_layout':'distributed','shows':60,
                      'seats_per_show':300,'show_ids':[str(uuid4()) for _ in range(60)],'fixture_id':str(uuid4()),
                      'created_at':datetime.now(UTC).isoformat(),'sale_ends':(datetime.now(UTC)+timedelta(minutes=15)).isoformat()}
        retain_fixture_identity(stage.local,stage.record,fixture_data,60)
        stage.audits.bind_fixture(stage.local/'fixture-identity.json',18000,18000,3)
        stage._claim();session.paid_arms.append(stage.execution.arm)
        from worker_separation_artifacts import validate_seal
        stage.artifact_attempts=list(stage.locations)
        stage.artifact_seals={h:validate_seal({'fresh_arm_directory':True,'root':{'device':1,'inode':1,'uid':0,'mode':0o700},
                                    'directory':{'device':1,'inode':2,'uid':0,'mode':0o700}},p) for h,p in stage.locations.items()}
        def retired():
            observed=stage.execution._observe(cleanup=True);primary=observed['primary']
            verify_restored(stage.execution.saved,{h:v['rows'] for h,v in observed.items()},primary['volumes'],primary['bind_sha256'])
            return True
        stage.diagnostics.container_retired=retired
        result={'stage_pass':session.bootstrap_fail!='customer','customers_dispatched':True,
                'job_cleanup':stage.jobs.stop_all(),'financial':stage.audits.payment_durability(),
                'duplicates':stage.audits.zero_double_booking(),'queues':stage.audits.queue_drain(stage.runtime.audit_binding,cleanup=True)}
        if session.bootstrap_fail=='pause_after_control' and stage.execution.arm=='control':session.paused=True
        return result
    monkeypatch.setattr(paid.PaidStage,'run',paid_run)
    components={'helpers':HELPERS,'ca_pem':CA,'inventory_sources':sources,'observer_sources':observer_sources,
                'diagnostic_context':ProtectedContext(target,'synthetic-private-secret'),'frozen_bundle':source_bundle,
                'artifact_output':artifact_output}
    comparison=runner.WorkerComparison(session,guard,inputs,saved,tmp_path/'images.tar',**components)
    return comparison,session


def test_connected_pair_uses_fresh_handover_and_preserves_accounting(monkeypatch,tmp_path,source_bundle):
    comparison,session=setup(monkeypatch,tmp_path,source_bundle)
    result=comparison.run()
    assert result['pass'] and result['status']=='PASSED_RESTORED',result
    assert session.paid_arms==['control','candidate'] and session.original_stops==2
    assert session.arm_factories==['control','candidate']
    scope=policy.read(policy.STATE)[comparison.guard.key]
    assert scope['paid_runs_started']==2 and scope['attempted_paid_arms']==['control','candidate']
    assert scope['worker_control_authorization']['handover_sha256']==comparison.candidate.runtime.handover.sha256
    assert comparison.candidate.runtime.saved==comparison.saved and comparison.control.runtime.saved==comparison.saved
    assert all(r['restoration_complete'] is True for r in result['arms'].values())
    assert not session.values['secondary']['rows'] and session.cleanup_mode is False
    with pytest.raises(ValueError,match='single-use'):comparison.run()


@pytest.mark.parametrize('failure',['customer','original_stop','install','readiness','inventory','queues','staging','pause_after_control'])
def test_failed_control_or_unknown_setup_never_constructs_or_dispatches_candidate(monkeypatch,tmp_path,source_bundle,failure):
    comparison,session=setup(monkeypatch,tmp_path,source_bundle);session.bootstrap_fail=failure
    result=comparison.run()
    assert not result['pass'] and comparison.candidate is None and 'candidate' not in result['arms']
    assert 'candidate' not in session.paid_arms and 'candidate' not in session.arm_factories
    scope=policy.read(policy.STATE)[comparison.guard.key]
    assert scope['paid_runs_started']==(1 if failure in {'customer','pause_after_control'} else 0)
    if failure in {'readiness','inventory'}:
        assert result['arms']['control']['status']=='FAILED_RESTORED'
        assert result['arms']['control']['zero_dispatch_proven'] is True
        assert result['arms']['control']['bootstrap_recovery']['bootstrap_zero_dispatch']['financial_cohort']=='not_created'
    if failure=='original_stop':
        assert session.original_stops==1 and result['arms']['control']['status']=='RECOVERY_REQUIRED'
    assert session.cleanup_mode is False


@pytest.mark.parametrize('failure',['common','start','partial_start'])
def test_unknown_bootstrap_mutations_are_not_replayed_and_restore_is_independently_observed(monkeypatch,tmp_path,source_bundle,failure):
    comparison,session=setup(monkeypatch,tmp_path,source_bundle);session.fail=failure
    result=comparison.run()
    assert not result['pass'] and comparison.candidate is None and not session.paid_arms
    assert result['arms']['control']['restoration_complete'] is True
    assert len([m for m in session.mutations if m[0]==('start' if failure=='partial_start' else failure)])==1
    assert session.cleanup_mode is False


@pytest.mark.parametrize('kind',['bootstrap-ack','worker-arm-summary','candidate-authorization-ack','worker-comparison-summary'])
def test_journal_failure_cannot_authorize_candidate_or_certify_pair(monkeypatch,tmp_path,source_bundle,kind):
    comparison,session=setup(monkeypatch,tmp_path,source_bundle)
    original=runtime.RuntimeActions._write
    def write(component,name,value):
        if name==kind and component.binding['arm']=='control':raise OSError('Synthetic full disk')
        return original(component,name,value)
    monkeypatch.setattr(runtime.RuntimeActions,'_write',write)
    result=comparison.run()
    assert not result['pass'] and result['status']=='RECOVERY_REQUIRED'
    if kind!='worker-comparison-summary':assert comparison.candidate is None and 'candidate' not in session.paid_arms
    assert session.cleanup_mode is False
    scope=policy.read(policy.STATE)[comparison.guard.key]
    assert scope['paid_runs_started']==(2 if kind=='worker-comparison-summary' else 1 if kind in {'worker-arm-summary','candidate-authorization-ack'} else 0)
    if kind=='candidate-authorization-ack':
        assert scope['worker_control_authorization']['decision']=='ADR0200'
        assert comparison.control.lifecycle.handover_attempted
    with pytest.raises(ValueError,match='single-use'):comparison.run()


@pytest.mark.parametrize('change',['api_sources','observer_owner','frozen_generator','diagnostic_target','ca'])
def test_input_drift_fails_before_image_staging_or_runtime_mutation(monkeypatch,tmp_path,source_bundle,change):
    comparison,session=setup(monkeypatch,tmp_path,source_bundle)
    if change=='api_sources':comparison.components['inventory_sources']=copy.deepcopy(comparison.components['inventory_sources']);comparison.components['inventory_sources']['api']={'foreign':'value'}
    elif change=='observer_owner':comparison.components['artifact_output']=new_stage_output()
    elif change=='frozen_generator':comparison.components['frozen_bundle']=dict(source_bundle);comparison.components['frozen_bundle']['scripts/paid_ticket_sharded_generator.py']+=b'\n# changed\n'
    elif change=='diagnostic_target':
        target=dict(comparison.components['diagnostic_context'].target);target['dbname']='other'
        comparison.components['diagnostic_context']=ProtectedContext(target,'synthetic-private-secret')
    else:comparison.components['ca_pem']='different CA'
    result=comparison.run()
    assert not result['pass'] and result['status']=='RECOVERY_REQUIRED' and 'staging' not in result
    assert not session.mutations and not session.paid_arms and session.original_stops==0


def test_configuration_cleanup_unknown_ack_retains_recovery_status(monkeypatch,tmp_path,source_bundle):
    comparison,session=setup(monkeypatch,tmp_path,source_bundle);session.bootstrap_fail='configuration_cleanup'
    result=comparison.run()
    assert not result['pass'] and comparison.candidate is None
    assert result['arms']['control']['restoration_complete'] is True
    assert result['arms']['control']['status']=='RECOVERY_REQUIRED'
    assert not result['arms']['control']['private_artifact_cleanup_complete']


def test_clean_failed_staging_runs_independent_recovery_without_dispatch(monkeypatch,tmp_path,source_bundle):
    comparison,session=setup(monkeypatch,tmp_path,source_bundle)
    cleaned={'pass':False,'status':'FAILED_CLEANED','runtime_unchanged':True,'service_deployments':0,'customer_dispatches':0,
        'hosts':{'primary':{'creation_attempted':True,'archive_removed':True,'owner':'/synthetic/owner','seal':{'owner':'/synthetic/owner'}}}}
    monkeypatch.setattr(runner,'stage_package',lambda *args,**kwargs:copy.deepcopy(cleaned))
    result=comparison.run()
    assert not result['pass'] and result['status']=='FAILED_RESTORED',result
    assert session.paid_arms==[] and session.original_stops==0 and not session.mutations
    assert list(result['arms'])==['control'] and result['arms']['control']['zero_dispatch_proven'] is True
    import worker_separation_profile as profile
    state=policy.read(policy.STATE);scope=state[comparison.guard.key];scope['worker_run']='adr0153-parents-aaaaaaaaaaaa'
    result.update(run=scope['worker_run'],binding_sha256=policy.digest(comparison.guard.binding))
    assert profile.outcome(result,scope,comparison.guard.binding)==(True,True,False)
    result['staging']['hosts']['primary']['archive_removed']=False
    assert profile.outcome(result,scope,comparison.guard.binding)==(False,False,False)


def test_failed_staging_recovery_never_mutates_changed_runtime(monkeypatch,tmp_path,source_bundle):
    comparison,session=setup(monkeypatch,tmp_path,source_bundle)
    cleaned={'pass':False,'status':'FAILED_CLEANED','runtime_unchanged':True,'service_deployments':0,'customer_dispatches':0,
        'hosts':{'primary':{'creation_attempted':True,'archive_removed':True,'owner':'/synthetic/owner','seal':{'owner':'/synthetic/owner'}}}}
    monkeypatch.setattr(runner,'stage_package',lambda *args,**kwargs:copy.deepcopy(cleaned))
    session.values['primary']['rows'][0]['Image']='sha256:'+'f'*64
    result=comparison.run()
    assert not result['pass'] and result['status']=='RECOVERY_REQUIRED',result
    assert not session.mutations and session.original_stops==0 and not session.paid_arms

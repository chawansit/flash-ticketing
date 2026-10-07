"""ADR0201 boundary/accounting tests; transports and customer outcomes are simulated."""
import copy
import hashlib
import importlib.util
import json
import sys
from pathlib import Path

import pytest

ROOT=Path(__file__).resolve().parents[2]
sys.path.insert(0,str(ROOT/'scripts'))
import run_status_refresh_comparison as legacy
import run_work_envelope as entrypoint
import work_envelope as policy
import worker_separation_profile as worker
from run_two_host_paid_comparison import frozen_bundle
from stage_status_refresh_images import new_stage_output
from worker_separation_recovery import FINANCIAL, RESTORED


@pytest.fixture(scope='module')
def bundle():return frozen_bundle()


@pytest.fixture
def prepared(tmp_path,monkeypatch,bundle):
    original=policy.envelope();registry=dict(policy.PROFILES)
    spec=importlib.util.spec_from_file_location('worker_fixture',ROOT/'tests/unit/test_worker_separation_runner.py')
    fixture=importlib.util.module_from_spec(spec);spec.loader.exec_module(fixture)
    comparison,session=fixture.setup(monkeypatch,tmp_path,bundle)
    monkeypatch.setattr(policy,'PROFILES',registry)
    config=copy.deepcopy(session.config)
    config['primary']['private_ipv4']='10.0.0.1'
    config['secondary'].update(prepared_directory='/qualification/secondary',private_ipv4='10.0.0.2')
    config['generator']['private_ipv4']='10.0.0.3'
    package={'schema':1,'decision':'ADR0201','inputs':copy.deepcopy(comparison.inputs),'saved':comparison.saved,
             'inventory_sources':comparison.components['inventory_sources'],'helpers':comparison.components['helpers']}
    owner=new_stage_output();owner.mkdir();archive=owner/'images.tar';archive.write_bytes(b'synthetic-image-archive')
    package['inputs']['archive_receipt'].update(bytes=archive.stat().st_size,sha256=hashlib.sha256(archive.read_bytes()).hexdigest())
    for name,value in (('contract.json',package['inputs']['staging_contract']),('source-expectations.json',package['inputs']['role_sources']),('archive.json',package['inputs']['archive_receipt'])):
        policy.write(owner/name,value)
    monkeypatch.setattr(worker,'validate_archive',lambda *args:None)  # Native archive validation is qualified separately.
    monkeypatch.setattr(worker,'frozen_bundle',lambda:bundle)
    result=worker.Prepared(config,package,archive,fixture.CA,dict(comparison.components['diagnostic_context'].target))
    for key,name in (('ENVELOPE','envelope.json'),('STATE','state.json'),('JOURNAL','ledger.json'),('LOCK','run.lock'),('PAUSE','pause')):
        monkeypatch.setattr(policy,key,tmp_path/name)
    monkeypatch.setattr(legacy,'LOCK',policy.LOCK)
    original['qualified_profiles']=list(policy.PROFILES)
    original['existing_resource_configuration_sha256']=policy.digest(config)
    policy.write(policy.ENVELOPE,original)
    policy.write(policy.JOURNAL,{'schema_version':1,'envelope_id':original['envelope_id'],'human_pause':False,'experiments':[]})
    policy.write(policy.STATE,{'current_run':None,'human_pause':False})
    result._synthetic_session=session
    return result


def arm(passed=True):
    return {'status':'PASSED_RESTORED' if passed else 'FAILED_RESTORED','pass':passed,'restoration_complete':True,
        'stage':{'stage_pass':passed,'customers_dispatched':True,'financial':dict(FINANCIAL),
                 'duplicates':{'zero_double_booking':True},'queues':dict.fromkeys(('dispatch_stopped','all_queues_zero','kafka_drained'),True),
                 'job_cleanup':{'pass':True,'all_jobs_stopped':True}},
        'private_artifact_cleanup_complete':True,'verify_restoration':dict(RESTORED),
        'restored_queues':dict.fromkeys(('dispatch_stopped','all_queues_zero','kafka_drained'),True),
        'restored_idle':{'generator_idle':True}}


def install_transport(monkeypatch, mode=None):
    events=[];contexts=[]
    class Session:
        def __init__(self,config,output,password,*,action_guard):
            assert policy.LOCK.exists() and password=='synthetic-ssh-secret'
            action_guard.check(45)
            assert policy.read(policy.STATE)['current_run']==output.name
            assert policy.journal(policy.envelope())['experiments'][-1]['status']=='ACTIVE'
            if mode=='connection':raise TimeoutError('synthetic-ssh-secret must not appear in report')
            self.action_guard=action_guard;self.state={};events.append('connected')
        def close(self):
            events.append('closed')
            if mode=='close':raise OSError('Synthetic transport close failure')
    class Comparison:
        def __init__(self,session,guard,inputs,saved,archive,**components):
            self.guard=guard;contexts.append(components['diagnostic_context'])
        def run(self):
            events.append('comparison')
            if mode=='unknown':raise TimeoutError('synthetic unknown action')
            state=policy.read(policy.STATE);scope=state[self.guard.key]
            failed=mode=='customer'
            scope.update(paid_runs_started=1 if failed else 2,
                         attempted_paid_arms=['control'] if failed else ['control','candidate'])
            policy.write(policy.STATE,state)
            report={'pass':not failed,'status':'FAILED_RESTORED' if failed else 'PASSED_RESTORED',
                'staging':{'pass':True,'runtime_unchanged':True,'hosts':{h:{'archive_removed':True} for h in ('primary','secondary')}},
                'arms':{'control':arm(not failed)}}
            if not failed:report['arms']['candidate']=arm()
            if mode=='financial':report['arms']['candidate']['stage']['financial']['payments_durable']=False
            if mode=='queues':report['arms']['control']['restored_queues']['all_queues_zero']=False
            if mode=='ownership':
                state=policy.read(policy.STATE);state['current_run']='foreign';policy.write(policy.STATE,state)
            if mode=='counter':
                state=policy.read(policy.STATE);state[self.guard.key]['paid_runs_started']=1;policy.write(policy.STATE,state)
            if mode=='malformed':report['arms']=None
            return report
    monkeypatch.setattr(worker,'Session',Session);monkeypatch.setattr(worker,'WorkerComparison',Comparison)
    return events,contexts


def test_single_boundary_reserves_before_connection_and_finishes_exact_pair(prepared,monkeypatch):
    events,contexts=install_transport(monkeypatch)
    receipt=worker.execute(prepared,'synthetic-ssh-secret','synthetic-diagnostic-secret')
    assert receipt['status']=='PASSED_RESTORED' and receipt['paid_runs_started']==2
    assert events==['connected','comparison','closed'] and contexts[0].password is None
    state=policy.read(policy.STATE);scope=state[receipt['ledger']]
    assert state['current_run'] is None and scope['active_run'] is None
    assert scope['qualification_runs_authorized']==0 and scope['safety_tickets_authorized']==0
    assert len(policy.journal(policy.envelope())['experiments'])==1 and receipt['actual_elapsed_seconds']>0
    assert not policy.LOCK.exists()
    text=(prepared.output/'worker-envelope-result.json').read_text()
    assert 'synthetic-ssh-secret' not in text and 'synthetic-diagnostic-secret' not in text
    with pytest.raises(ValueError,match='Finished'):policy.ActionGuard(receipt['ledger'],prepared.binding).finish([],1)


@pytest.mark.parametrize('mode',['connection','customer','unknown','financial','queues','ownership','counter','receipt','malformed','close'])
def test_failure_accounting_retains_consumption_and_blocks_uncertain_recovery(prepared,monkeypatch,mode):
    events,contexts=install_transport(monkeypatch,mode)
    if mode=='receipt':monkeypatch.setattr(worker,'_receipt',lambda *args:(_ for _ in ()).throw(OSError('Synthetic disk full')))
    receipt=worker.execute(prepared,'synthetic-ssh-secret','synthetic-diagnostic-secret')
    safe=mode in {'connection','customer'}
    assert receipt['status']==('FAILED_RESTORED' if safe else 'RECOVERY_REQUIRED')
    assert receipt['paid_runs_started']==(0 if mode in {'connection','unknown'} else 1 if mode in {'customer','counter'} else 2)
    assert receipt['actual_elapsed_seconds']>0 and not policy.LOCK.exists()
    state=policy.read(policy.STATE)
    assert (state['current_run'] is None) is safe
    if mode!='connection':assert events[-1]=='closed' and contexts[0].password is None
    else:assert 'comparison' not in events


@pytest.mark.parametrize('change',['pause','unregistered','configuration','prepared','sources','observer','generator','output'])
def test_local_rejection_never_connects_or_consumes_paid_allowance(prepared,monkeypatch,change):
    events,_=install_transport(monkeypatch)
    if change=='pause':policy.write(policy.PAUSE,{'human_pause':True})
    elif change=='unregistered':
        env=policy.envelope();env['qualified_profiles'].pop();policy.write(policy.ENVELOPE,env)
    elif change=='configuration':
        env=policy.envelope();env['existing_resource_configuration_sha256']='b'*64;policy.write(policy.ENVELOPE,env)
    elif change=='prepared':prepared.package['helpers']['capacity_queue_state.py']='c'*64
    elif change=='observer':prepared.observer_sources['manifest_sha256']='e'*64
    elif change=='generator':prepared.frozen=dict(prepared.frozen);prepared.frozen['foreign']=b'other'
    elif change=='output':prepared.output=new_stage_output()
    else:monkeypatch.setattr(worker,'entry_identity',lambda:'d'*64)
    with pytest.raises(ValueError):worker.execute(prepared,'synthetic-ssh-secret','synthetic-diagnostic-secret')
    assert events==[] and policy.journal(policy.envelope())['experiments']==[] and not policy.LOCK.exists()


@pytest.mark.parametrize('gate',['financial','duplicates','queues','jobs','private_cleanup','restoration','idle','counter'])
def test_finalizer_does_not_infer_missing_gates(prepared,gate):
    report={'run':'synthetic','binding_sha256':policy.digest(prepared.binding),'comparison_attempted':True,'status':'PASSED_RESTORED','pass':True,
            'staging':{'pass':True,'runtime_unchanged':True,'hosts':{h:{'archive_removed':True} for h in ('primary','secondary')}},
            'arms':{'control':arm(),'candidate':arm()}}
    scope={'binding':prepared.binding,'worker_run':'synthetic','paid_runs_started':2,'attempted_paid_arms':['control','candidate']}
    value=report['arms']['candidate']
    if gate in {'financial','duplicates','queues'}:value['stage'][gate]={}
    elif gate=='jobs':value['stage']['job_cleanup']={}
    elif gate=='private_cleanup':value['private_artifact_cleanup_complete']=False
    elif gate=='restoration':value['verify_restoration']={}
    elif gate=='idle':value['restored_idle']={}
    else:scope['paid_runs_started']=1
    assert worker.outcome(report,scope,prepared.binding)==(False,False,False)


def test_registered_cli_routes_only_worker_inputs_and_keeps_readonly_preflight(monkeypatch,tmp_path):
    captured={}
    monkeypatch.setattr(worker,'prepare_files',lambda *args:captured.setdefault('inputs',args))
    monkeypatch.setattr(worker,'execute',lambda *args:{'status':'PASSED_RESTORED'})
    monkeypatch.setattr(entrypoint.sys.stdin,'isatty',lambda:True)
    monkeypatch.setattr(entrypoint.getpass,'getpass',lambda *args:'synthetic')
    import subprocess
    monkeypatch.setattr(subprocess,'run',lambda *args,**kwargs:None)
    result=entrypoint.execute(tmp_path/'config',tmp_path/'archive',tmp_path,profile_name='worker_separation',
                             worker_inputs=tmp_path/'inputs',worker_ca=tmp_path/'ca',diagnostic_target=tmp_path/'target')
    assert result['status']=='PASSED_RESTORED' and len(captured['inputs'])==5


def test_actual_connected_comparison_runs_through_real_envelope_boundary(prepared,monkeypatch):
    import worker_separation_runner as connected
    session=prepared._synthetic_session
    def transport(config,output,password,*,action_guard):
        session.config=config;session.action_guard=action_guard;session.state={}
        session.close=lambda:None
        return session
    monkeypatch.setattr(worker,'Session',transport)
    monkeypatch.setattr(worker,'WorkerComparison',connected.WorkerComparison)
    monkeypatch.setattr(connected,'stage_package',lambda *args,**kwargs:{'pass':True,'runtime_unchanged':True,
        'hosts':{h:{'archive_removed':True} for h in ('primary','secondary')}})
    receipt=worker.execute(prepared,'synthetic-ssh-secret','synthetic-diagnostic-secret')
    assert receipt['status']=='PASSED_RESTORED',json.loads((prepared.output/'worker-envelope-result.json').read_text())
    assert session.paid_arms==['control','candidate'] and session.original_stops==2
    scope=policy.read(policy.STATE)[receipt['ledger']]
    assert scope['attempted_paid_arms']==['control','candidate'] and scope['paid_runs_started']==2
    assert scope['worker_control_authorization']['decision']=='ADR0200'
    assert policy.read(policy.STATE)['current_run'] is None and not policy.LOCK.exists()


@pytest.mark.parametrize('change',['ca','target','source','snapshot','archive','package_keys'])
def test_preparation_rejects_invalid_private_inputs_before_reservation(prepared,change):
    package=copy.deepcopy(prepared.package);ca=prepared.ca_pem;target=dict(prepared.target)
    if change=='ca':ca='other CA'
    elif change=='target':target['host']='10.0.0.99'
    elif change=='source':package['inventory_sources'].pop('api')
    elif change=='snapshot':package['saved']['counts']['api']=3
    elif change=='archive':package['inputs']['archive_receipt']['sha256']='a'*64
    else:package['unexpected']='not permitted'
    with pytest.raises(ValueError):worker.Prepared(prepared.config,package,prepared.archive,ca,target)
    assert policy.journal(policy.envelope())['experiments']==[]


@pytest.mark.parametrize('changed',['run','binding','counter_type','order','stage_pass','staging','dispatch','root_status'])
def test_finalizer_rejects_identity_and_order_drift(prepared,changed):
    report={'run':'synthetic','binding_sha256':policy.digest(prepared.binding),'comparison_attempted':True,
            'status':'PASSED_RESTORED','pass':True,'staging':{'pass':True,'runtime_unchanged':True,
            'hosts':{h:{'archive_removed':True} for h in ('primary','secondary')}},
            'arms':{'control':arm(),'candidate':arm()}}
    scope={'binding':prepared.binding,'worker_run':'synthetic','paid_runs_started':2,'attempted_paid_arms':['control','candidate']}
    if changed=='run':report['run']='foreign'
    elif changed=='binding':scope['binding']={}
    elif changed=='counter_type':scope['paid_runs_started']=True
    elif changed=='order':scope['attempted_paid_arms'].reverse()
    elif changed=='stage_pass':report['arms']['control']['stage']['stage_pass']=False
    elif changed=='staging':report['staging']['hosts']['secondary']['archive_removed']=False
    elif changed=='dispatch':report['arms']['candidate']['stage']['customers_dispatched']=False
    else:report['status']='unknown'
    result=worker.outcome(report,scope,prepared.binding)
    assert result[2] is False
    if changed!='stage_pass':assert result==(False,False,False)


def test_expired_experiment_keeps_durable_success_but_fails_deadline_gate(prepared,monkeypatch):
    install_transport(monkeypatch)
    original=policy.ActionGuard.finish
    monkeypatch.setattr(policy.ActionGuard,'finish',lambda guard,reports,elapsed:original(guard,reports,3601))
    receipt=worker.execute(prepared,'synthetic-ssh-secret','synthetic-diagnostic-secret')
    assert receipt['status']=='FAILED_RESTORED' and receipt['actual_elapsed_seconds']==3601
    assert receipt['paid_runs_started']==2 and policy.read(policy.STATE)['current_run'] is None


def test_prepared_file_reader_is_bounded_and_no_secret_is_echoed(tmp_path):
    path=tmp_path/'input';path.write_text('synthetic-private-secret')
    assert worker.bounded_text(path,64)=='synthetic-private-secret'
    with pytest.raises(ValueError):worker.bounded_text(path,1)
    with pytest.raises(ValueError):worker.prepare_files(path,None,path,path,path)


def test_independent_bootstrap_zero_dispatch_closes_only_with_full_recovery(prepared):
    bootstrap={'status':'FAILED_RESTORED','pass':False,'restoration_complete':True,'zero_dispatch_proven':True,
        'bootstrap_recovery':{'bootstrap_zero_dispatch':{'zero_dispatch':True,'financial_cohort':'not_created'},
         'verify_bootstrap_restoration':dict(RESTORED),'bootstrap_zero_double_booking':{'zero_double_booking':True},
         'bootstrap_restored_queues':dict.fromkeys(('dispatch_stopped','all_queues_zero','kafka_drained'),True),
         'bootstrap_restored_idle':{'generator_idle':True}}}
    report={'run':'synthetic','binding_sha256':policy.digest(prepared.binding),'comparison_attempted':True,
            'status':'FAILED_RESTORED','pass':False,'staging':{'pass':True,'runtime_unchanged':True,
             'hosts':{h:{'archive_removed':True} for h in ('primary','secondary')}},'arms':{'control':bootstrap}}
    scope={'binding':prepared.binding,'worker_run':'synthetic','paid_runs_started':0,'attempted_paid_arms':[]}
    assert worker.outcome(report,scope,prepared.binding)==(True,True,False)
    bootstrap['bootstrap_recovery'].pop('verify_bootstrap_restoration')
    assert worker.outcome(report,scope,prepared.binding)==(False,False,False)


def test_prepared_archive_cannot_reuse_a_consumed_staging_owner(prepared):
    policy.write(prepared.archive.parent/'staging-summary.json',{'pass':False})
    with pytest.raises(ValueError,match='Fresh canonical'):
        worker.Prepared(prepared.config,prepared.package,prepared.archive,prepared.ca_pem,prepared.target)


def test_protected_read_accepts_access_time_updates_but_rejects_replacement(tmp_path,monkeypatch):
    from types import SimpleNamespace
    path=tmp_path/'receipt';path.write_text('private fixture')
    original=Path.stat;info=path.stat();seen=[]
    def observe(self,**kwargs):
        if self!=path:return original(self,**kwargs)
        value=SimpleNamespace(**{k:getattr(info,k) for k in ('st_dev','st_ino','st_size','st_mtime_ns','st_ctime_ns','st_mode')},st_atime_ns=info.st_atime_ns+len(seen))
        seen.append(value)
        return value
    monkeypatch.setattr(Path,'stat',observe)
    assert worker.bounded_text(path,64)=='private fixture' and len(seen)==2
    def replaced(self,**kwargs):
        value=observe(self,**kwargs)
        if self==path and len(seen)%2==0:value.st_ino+=1
        return value
    monkeypatch.setattr(Path,'stat',replaced)
    with pytest.raises(ValueError,match='changed'):worker.bounded_text(path,64)


def test_consumed_preparation_cannot_create_a_second_scope_or_connect_again(prepared,monkeypatch):
    events,_contexts=install_transport(monkeypatch)
    first=worker.execute(prepared,'synthetic-ssh-secret','synthetic-diagnostic-secret')
    assert first['status']=='PASSED_RESTORED'
    with pytest.raises(ValueError,match='Single-use'):
        worker.execute(prepared,'synthetic-ssh-secret','synthetic-diagnostic-secret')
    assert len(policy.journal(policy.envelope())['experiments'])==1
    assert events==['connected','comparison','closed']


def test_diagnostic_root_remains_separate_from_application_principal(prepared):
    package=copy.deepcopy(prepared.package)
    saved=package['saved'];pair=package['inputs']['pair']
    saved['model']['services']['pgbouncer']['environment']['DB_USER']='ticketing'
    for model in (saved['model'],pair['control']['primary'],pair['candidate']['primary'],pair['candidate']['secondary']):
        for service in model['services'].values():
            env=service['environment']
            if 'DATABASE_URL' in env:env['DATABASE_URL']=env['DATABASE_URL'].replace('postgresql://root:','postgresql://ticketing:')
            if 'DB_USER' in env:env['DB_USER']='ticketing'
    package['inputs']['saved_runtime_sha256']=policy.digest(saved)
    package['inputs']['staging_contract']['prepared_pair_sha256']=policy.digest(pair)
    package['inputs']['archive_receipt']['contract_sha256']=policy.digest(package['inputs']['staging_contract'])
    owner=prepared.archive.parent
    for name,value in (('contract.json',package['inputs']['staging_contract']),('archive.json',package['inputs']['archive_receipt'])):policy.write(owner/name,value)
    value=worker.Prepared(prepared.config,package,prepared.archive,prepared.ca_pem,prepared.target)
    assert value.target['user']=='root'
    assert value.saved['model']['services']['pgbouncer']['environment']['DB_USER']=='ticketing'


def test_entrypoint_source_closure_has_no_duplicate_paths():
    assert len(worker.ENTRY_FILES)==len(set(worker.ENTRY_FILES))

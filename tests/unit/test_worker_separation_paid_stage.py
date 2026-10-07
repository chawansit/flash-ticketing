"""ADR0198 synthetic guarded paid-stage integration; no cloud calls."""
import ast
import copy
import importlib.util
import json
import sys
from datetime import UTC, datetime, timedelta
from pathlib import Path
from uuid import uuid4

import pytest

ROOT=Path(__file__).resolve().parents[2]
sys.path.insert(0,str(ROOT/'scripts'))
import work_envelope as policy
import worker_separation_paid_jobs as jobs
import worker_separation_paid_stage as paid
from run_two_host_paid_comparison import frozen_bundle


@pytest.fixture(scope='module')
def frozen():return frozen_bundle()


def setup(tmp_path, monkeypatch, frozen, arm='control'):
    spec=importlib.util.spec_from_file_location('paid_diagnostics_fixture',ROOT/'tests/unit/test_worker_separation_diagnostics.py')
    fixture=importlib.util.module_from_spec(spec);spec.loader.exec_module(fixture)
    diag,engine,events,_uploads=fixture.setup(arm)
    original=engine._observe
    engine._observe=lambda **kwargs:original()
    engine.journal_healthy=True
    engine.runtime.guard.key='synthetic-worker-scope'
    engine.session.config.update(secondary={'repo':'/qualification/secondary'},generator={'repo':'/qualification/generator'})
    engine.session.config['primary']['private_ipv4']='10.0.0.1'
    engine.runtime.output=tmp_path
    engine.runtime.audit_binding={'scope':'synthetic-worker-scope'}
    engine.runtime.guard.binding['worker_paid_stage_contract_sha256']=policy.digest(paid.stage_sources(frozen)[1])
    engine.runtime.guard.binding['worker_audit_expectations']={'expected_orders':18000,'expected_paid':18000,'callbacks':3,'shows':60}
    pinned=policy.digest(engine.runtime.guard.binding)
    def guard(timeout,cleanup=False):
        if policy.digest(engine.runtime.guard.binding)!=pinned:raise ValueError('Synthetic original binding changed')
        if engine.paused and not cleanup:raise ValueError('Synthetic paused')
    engine._guard=guard
    clock=[100.0]
    audit_events=[]
    class Audits:
        execution=engine
        def bind_fixture(self,path,*expected):
            receipt=json.loads(path.read_text());assert expected==(18000,18000,3)
            assert len(receipt['fixture_identity']['show_ids'])==60
            audit_events.append('fixture')
        def payment_durability(self):
            audit_events.append('financial')
            if self.fail=='financial':raise ValueError('Synthetic financial failure')
            return {'post_ttl_complete':True,'payments_durable':True,'ticket_relationships_valid':True}
        def zero_double_booking(self):
            audit_events.append('duplicates')
            if self.fail=='duplicates':raise ValueError('Synthetic duplicate failure')
            return {'zero_double_booking':True}
        def queue_drain(self,binding,cleanup):
            assert binding==engine.runtime.audit_binding and cleanup
            audit_events.append('queues')
            if self.fail=='queues':raise ValueError('Synthetic queue failure')
            return {'dispatch_stopped':True,'all_queues_zero':True,'kafka_drained':True}
        fail=None
    audits=Audits()
    now=datetime.now(UTC)
    fixture_data={'schema_version':1,'environment':'development','fixture_id':str(uuid4()),'created_at':now.isoformat(),
        'sale_ends':(now+timedelta(hours=1)).isoformat(),'shows':60,'seats_per_show':300,'fixture_layout':'distributed',
        'show_ids':[str(uuid4()) for _ in range(60)]}
    diag.prepare=lambda:{'inventory':copy.deepcopy(diag.inventory),'receipt':{'runtime_unchanged':True}}
    engine.session.put=lambda host,path,value,fresh:events.append(('put',{'host':host,'path':path}))
    real_api=engine.session.api
    def remote(site,code,timeout):
        compile(code,'synthetic-paid-program','exec')
        events.append(('remote',{'site':site,'cleanup':engine.session.cleanup_mode}))
        if 'fresh_arm_directory' in code:return {'fresh_arm_directory':True,
            'root':{'device':1,'inode':2,'uid':0,'mode':0o700},'directory':{'device':1,'inode':3,'uid':0,'mode':0o700}}
        if 'frozen_helpers_verified' in code:
            if engine.session.fail=='old_helper':raise ValueError('Synthetic old helper')
            return {'frozen_helpers_verified':True}
        if 'r.check_returncode()' in code:return fixture_data
        if 'jwt.encode' in code:
            assert 'fixture' in audit_events
            return {'viewers':18000,'shows':60}
        if 'raw.decode()' in code:return {'text':json.dumps({'schema_version':1,'environment':'development','origin':'http://10.0.0.1:8000',
            'show_ids':fixture_data['show_ids'],'expires_at':fixture_data['sale_ends'],'fixture_layout':'distributed',
            'seat_offset':0,'seats_per_show':300,'viewer_tokens':['synthetic-'+str(i) for i in range(18000)]})}
        if 'transferred_sources_verified' in code:
            expected=ast.literal_eval(code.split('expected=')[1].split('\nfor name')[0])
            return {'transferred_sources_verified':True,'files':len(expected)}
        return real_api(diag.cid,code,timeout)
    engine.session.call=lambda site,code,timeout=180:remote(site,code,timeout)
    engine.session.api=lambda cid,code,timeout:remote('container',code,timeout)
    component=paid.PaidStage(engine,diag,audits,frozen,clock=lambda:clock[0],wall=lambda:now.timestamp(),
                             sleep=lambda delay:clock.__setitem__(0,clock[0]+delay))
    state=tmp_path/'scope.json'
    monkeypatch.setattr(policy,'STATE',state)
    policy.write(state,{engine.runtime.guard.key:{'binding':engine.runtime.guard.binding,'paid_runs_authorized':2,
        'paid_runs_started':0 if arm=='control' else 1,'attempted_paid_arms':[] if arm=='control' else ['control']}})
    return component,engine,events,audit_events,clock


def test_frozen_whole_manifest_and_workload_remain_exact(frozen):
    files,contract=paid.stage_sources(frozen)
    assert contract['fixed']==paid.FIXED and contract['fixed']['expected_tickets']==18000
    assert set(files)=={*paid.CORE,*paid.IMAGE_HELPERS,paid.SCHEDULER}
    assert files['paid_ticket_sharded_generator.py'].encode()==frozen['scripts/paid_ticket_sharded_generator.py'].replace(b'\r\n',b'\n')
    corrupted=copy.deepcopy(frozen);key=next(k for k in corrupted if k not in ('scripts/'+n for n in files))
    corrupted[key]+=b'\n# unapproved drift\n'
    with pytest.raises(ValueError,match='differs'):paid.stage_sources(corrupted)
    corrupted.pop(key)
    with pytest.raises(ValueError,match='78-file'):paid.stage_sources(corrupted)


@pytest.mark.parametrize('arm',['control','candidate'])
def test_preparation_retains_exact_fixture_before_mint_and_binds_separate_owners(tmp_path,monkeypatch,frozen,arm):
    component,engine,events,audit_events,_=setup(tmp_path,monkeypatch,frozen,arm)
    result=component.prepare()
    assert result['customers_dispatched'] is False and result['pre_dispatch_qualified'] is False
    assert audit_events==['fixture'] and len(json.loads((tmp_path/arm/'fixture-identity.json').read_text())['fixture_identity']['show_ids'])==60
    assert result['frozen_helpers_verified'] and result['transferred_generator_identity']
    assert all(component.diagnostics.artifact_owner_name in value for value in component.locations.values())
    assert component.diagnostics.owner!=engine.configurations.seals['primary']['owner']
    assert not any(k=='paid-dispatch-intent' for k,_ in events)
    with pytest.raises(ValueError,match='replayed'):component.prepare()


@pytest.mark.parametrize('drift',['source','scope','paused','old_helper'])
def test_drift_fails_before_tokens_or_customer_launch(tmp_path,monkeypatch,frozen,drift):
    component,engine,events,audit_events,_=setup(tmp_path,monkeypatch,frozen)
    if drift=='source':component.sources[paid.SCHEDULER]+='\n# changed\n'
    elif drift=='scope':engine.runtime.guard.binding['worker_paid_stage_contract_sha256']='a'*64
    elif drift=='paused':engine.paused=True
    else:engine.session.fail='old_helper'
    with pytest.raises(ValueError):component.prepare()
    assert audit_events==[]
    assert not any(k=='paid-dispatch-intent' for k,_ in events)


def test_claim_is_durable_and_cannot_replay(tmp_path,monkeypatch,frozen):
    component,engine,events,_audit,_=setup(tmp_path,monkeypatch,frozen)
    component.prepare();component.record['scheduled_offered_start_utc']=datetime.now(UTC).isoformat()
    component._claim()
    ledger=policy.read(policy.STATE)[engine.runtime.guard.key]
    assert ledger['paid_runs_started']==1 and ledger['attempted_paid_arms']==['control']
    assert component.record['customers_dispatched'] is True
    assert [k for k,_ in events if k.startswith('paid-dispatch') or k=='paid-allowance-consumed']==['paid-dispatch-intent','paid-allowance-consumed']
    with pytest.raises(ValueError,match='unused paid'):component._claim()


def test_candidate_claim_requires_a_restored_passing_control(tmp_path,monkeypatch,frozen):
    component,engine,_events,_audit,_=setup(tmp_path,monkeypatch,frozen,'candidate')
    component.prepare();component.record['scheduled_offered_start_utc']=datetime.now(UTC).isoformat()
    with pytest.raises(ValueError,match='passing control'):component._claim()
    state=policy.read(policy.STATE);state[engine.runtime.guard.key]['worker_control_pass']=True;policy.write(policy.STATE,state)
    component._claim()
    assert policy.read(policy.STATE)[engine.runtime.guard.key]['paid_runs_started']==2


@pytest.mark.parametrize('audit_failure',[None,'financial','duplicates','queues'])
@pytest.mark.parametrize('interruption',[ValueError,KeyboardInterrupt])
def test_interruption_runs_every_audit_and_never_certifies_restoration(tmp_path,monkeypatch,frozen,audit_failure,interruption):
    component,engine,_events,audit_events,_=setup(tmp_path,monkeypatch,frozen)
    component.audits.fail=audit_failure
    def dispatch():raise interruption('synthetic failure never printed')
    component.dispatch=dispatch
    result=component.run()
    assert audit_events==['fixture','financial','duplicates','queues']
    assert result['stage_pass'] is False and result['pass'] is False and result['restoration_complete'] is False
    assert engine.session.cleanup_mode is False
    with pytest.raises(ValueError,match='replayed'):component.run()


def test_journal_loss_after_consumption_never_reopens_paid_allowance(tmp_path,monkeypatch,frozen):
    component,engine,_events,_audit,_=setup(tmp_path,monkeypatch,frozen)
    component.prepare();component.record['scheduled_offered_start_utc']=datetime.now(UTC).isoformat()
    original=engine.runtime._write
    def write(kind,value):
        if kind=='paid-allowance-consumed':raise OSError('synthetic disk full')
        return original(kind,value)
    engine.runtime._write=write
    with pytest.raises(OSError):component._claim()
    assert policy.read(policy.STATE)[engine.runtime.guard.key]['paid_runs_started']==1
    assert component.record['customers_dispatched'] is True and component.journal_healthy is False
    with pytest.raises(ValueError):component._claim()


def test_job_spec_uses_existing_command_identity_and_rejects_foreign_ack():
    _,expected=jobs.launch_spec(['python','/tmp/owned/worker.py','--seconds','300'],'/tmp/owned')
    actual={**expected,'pid':10,'start_ticks':123}
    assert jobs.validate_job(actual,expected)==actual
    for field,value in [('pid',True),('start_ticks',0),('name','foreign'),('command_sha256','0'*64),('identity_path','/tmp/other')]:
        changed={**actual,field:value}
        with pytest.raises(ValueError):jobs.validate_job(changed,expected)


def job_setup(tmp_path,monkeypatch,frozen):
    stage,engine,events,_audit,clock=setup(tmp_path,monkeypatch,frozen)
    registry=stage.jobs
    stored={}
    def remote(attempt,code,timeout,cleanup=False):
        if 'subprocess.Popen' in code:
            job={**attempt['expected'],'pid':len(stored)+101,'start_ticks':123}
            stored[attempt['expected']['name']]=job
            if engine.session.fail=='lost_launch':raise KeyboardInterrupt('synthetic lost ack')
            if engine.session.fail=='foreign_ack':return {**job,'command_sha256':'a'*64}
            return job
        if 'json.loads(raw)' in code:
            if engine.session.fail=='unknown_launch':raise FileNotFoundError('synthetic no durable identity')
            return stored[attempt['expected']['name']]
        if 'if True and running()' in code:
            if engine.session.fail=='one_stop' and attempt['site']=='generator':raise TimeoutError('synthetic stop failure')
            return {'running':False}
        if 'print(json.dumps' in code and 'returncode' in code:return {'returncode':0}
        return {'running':engine.session.fail=='busy'}
    registry._remote=remote
    return registry,engine,events,clock


@pytest.mark.parametrize('failure',['lost_launch','foreign_ack'])
def test_lost_launch_never_replays_even_when_cleanup_identity_recovered(tmp_path,monkeypatch,frozen,failure):
    registry,engine,_events,_clock=job_setup(tmp_path,monkeypatch,frozen)
    engine.session.fail=failure
    args=['python',registry.locations['generator']+'/customer.py']
    with pytest.raises((KeyboardInterrupt,ValueError)):registry.launch('generator',args)
    assert registry.attempts[0]['job'] is not None and not registry.attempts[0]['acknowledged']
    with pytest.raises(ValueError,match='replayed'):registry.launch('generator',args)
    engine.paused=True
    receipt=registry.stop_all()
    assert receipt['pass'] and receipt['dispatch_stopped']
    assert engine.session.cleanup_mode is False


def test_unknown_launch_and_one_failed_stop_still_attempt_all_other_jobs(tmp_path,monkeypatch,frozen):
    registry,engine,_events,_clock=job_setup(tmp_path,monkeypatch,frozen)
    registry.launch('primary',['python3',registry.locations['primary']+'/cpu.py'])
    registry.launch('generator',['python',registry.locations['generator']+'/customer.py'])
    engine.session.fail='one_stop'
    result=registry.stop_all()
    assert not result['pass'] and not result['dispatch_stopped']
    assert registry.attempts[0]['stopped'] and not registry.attempts[1]['stopped']
    assert engine.session.cleanup_mode is False


def test_bounded_wait_stops_at_deadline_and_rejects_pause(tmp_path,monkeypatch,frozen):
    registry,engine,_events,clock=job_setup(tmp_path,monkeypatch,frozen)
    attempt=registry.launch('generator',['python',registry.locations['generator']+'/customer.py'])
    engine.session.fail='busy'
    with pytest.raises(TimeoutError):registry.wait(attempt,11)
    assert clock[0]<=111
    engine.paused=True
    with pytest.raises(ValueError):registry._guard(1)
    engine.session.fail=None
    assert registry.stop_all()['pass']



def successful_stage(component,monkeypatch,*,failure=None):
    start=component.wall()+30
    customer={'pass':failure!='customer','scheduled':18000,'dispatched':18000,'fulfilled_by_deadline':18000,
              'shards':[{'started_at_utc':datetime.fromtimestamp(start,UTC).isoformat()} for _ in range(2)]}
    if failure=='window':customer['shards'][1]['started_at_utc']=datetime.fromtimestamp(start+3,UTC).isoformat()
    launch_events=[]
    def launch(site,args,**kwargs):
        launch_events.append((site,copy.deepcopy(args)))
        if site=='generator' and failure=='lost_dispatch':raise TimeoutError('Synthetic unknown dispatch')
        return {'site':site}
    component.jobs.launch=launch
    component.jobs.wait=lambda *args,**kwargs:{'returncode':0}
    component.jobs.stop_all=lambda:{'pass':True,'dispatch_stopped':True,'all_jobs_stopped':True}
    original_api=component._api
    def api(code,timeout,**kwargs):
        if "for name in ('pipeline','kafka')" in code:
            return {'pipeline':{'synthetic':True},'kafka':{'utc':datetime.now(UTC).isoformat(),'members':6,'total_lag':0}}
        return original_api(code,timeout,**kwargs)
    component._api=api
    original_read=component._read
    def read(site,name,limit,**kwargs):
        if name=='customer.json':return json.dumps(customer)
        if name=='cpu.json':return json.dumps({'synthetic':True})
        if name.endswith('.jsonl'):return '{}\n'.replace('\\n','\n')
        return original_read(site,name,limit,**kwargs)
    component._read=read
    component.diagnostics.inventory['hosts']={host:{'machine_id_sha256':str(index)*64}
                                             for index,host in enumerate(('primary','secondary'),1)}
    monkeypatch.setattr(paid,'startup',lambda *args:{'pass':failure!='startup'})
    monkeypatch.setattr(paid,'summarize_cpu',lambda *args:{'synthetic':True})
    monkeypatch.setattr(paid,'compare_windows',lambda *args,**kwargs:{'both_hosts_cpu_observed':True,'same_offered_measurement_window':True})
    monkeypatch.setattr(paid,'summarize_worker',lambda *args,**kwargs:{'pass':True})
    monkeypatch.setattr(paid,'summarize_kafka',lambda *args:{'samples':6,'sample_errors':0,'members_min':6,'members_max':6,
        'ownership_observed':True,'max_partitions_per_member':1,'last_member_partition_groups':[[i] for i in range(6)]})
    return launch_events


@pytest.mark.parametrize('failure',[None,'startup','customer','window','lost_dispatch'])
@pytest.mark.parametrize('arm',['control','candidate'])
def test_actual_adapter_ordering_keeps_fixed_dispatch_and_failure_audits(tmp_path,monkeypatch,frozen,failure,arm):
    component,engine,_events,audit_events,_clock=setup(tmp_path,monkeypatch,frozen,arm)
    launches=successful_stage(component,monkeypatch,failure=failure)
    if arm=='candidate':
        state=policy.read(policy.STATE);state[engine.runtime.guard.key]['worker_control_pass']=True;policy.write(policy.STATE,state)
    result=component.run()
    assert audit_events==['fixture','financial','duplicates','queues']
    assert result['stage_pass'] is (failure is None)
    assert result['pass'] is False and result['restoration_complete'] is False
    if failure=='startup':
        assert not any(site=='generator' for site,_ in launches)
        assert result['customers_dispatched'] is False
    else:
        generator=[args for site,args in launches if site=='generator']
        assert len(generator)==1
        assert generator[0]==paid.generator_arguments(component.locations['generator'],component.origin,component.wall()+30)
        assert result['customers_dispatched'] is True
        assert policy.read(policy.STATE)[engine.runtime.guard.key]['paid_runs_started']==(1 if arm=='control' else 2)
    assert engine.session.cleanup_mode is False


@pytest.mark.parametrize('drift',['inventory','fixture'])
def test_retained_stage_identity_drift_blocks_forward_work(tmp_path,monkeypatch,frozen,drift):
    component,_engine,_events,_audit,_clock=setup(tmp_path,monkeypatch,frozen)
    component.prepare()
    if drift=='inventory':component.inventory['arm']='candidate'
    else:(component.local/'fixture-identity.json').write_text('{}')
    with pytest.raises(ValueError,match='changed'):component._guard(1)


def test_primary_helpers_are_verified_without_second_upload(tmp_path,monkeypatch,frozen):
    component,_engine,events,_audit,_clock=setup(tmp_path,monkeypatch,frozen)
    component.prepare()
    primary=[value for kind,value in events if kind=='put' and value['host']=='primary']
    assert primary==[]  # DiagnosticActions already staged these exact host-side files.
    generator=[value['path'] for kind,value in events if kind=='put' and value['host']=='generator']
    assert len(generator)==5 and any(p.endswith('/manifest.private.json') for p in generator)


def test_isolated_linux_paid_job_identity_and_pause_cleanup():
    import os
    import re
    import subprocess

    import stage_status_refresh_images as staging
    import worker_separation_observer_bundle as observer_bundle
    image=os.getenv('ADR0198_LOCAL_LINUX_IMAGE')
    if not image:pytest.skip('Explicit cached immutable Linux image required')
    assert re.fullmatch(r'sha256:[0-9a-f]{64}',image)
    name=staging.new_stage_output().name
    files=observer_bundle.prepare()['files']
    pending=['worker_separation_paid_jobs.py'];visited=set()
    while pending:
        filename=pending.pop()
        if filename in visited:continue
        visited.add(filename)
        source=(ROOT/'scripts'/filename).read_text()
        files[filename]=source
        for node in ast.walk(ast.parse(source)):
            modules=([alias.name.split('.')[0] for alias in node.names] if isinstance(node,ast.Import) else
                     [node.module.split('.')[0]] if isinstance(node,ast.ImportFrom) and node.module and not node.level else [])
            pending.extend(module+'.py' for module in modules if (ROOT/'scripts'/(module+'.py')).is_file())
    assert len(files)<=160 and sum(len(source.encode()) for source in files.values())<=8*1024*1024
    program=r'''import contextlib,io,json,os,sys
from pathlib import Path
from types import SimpleNamespace
p=Path('/tmp/observer');p.mkdir(mode=0o700)
for name,source in FILES.items():
 f=p/name;f.write_text(source);f.chmod(0o600)
sys.path.insert(0,str(p))
from worker_separation_paid_jobs import PaidJobs,process_program
from fetch_status_refresh_parents import owner_path
owner=OWNER
arm='control'
config={host:{'repo':'/tmp/'+host} for host in ('primary','secondary','generator')}
locations={host:owner_path(config[host]['repo'],owner)+'/'+arm for host in config}
locations['container']='/tmp/'+owner+'/'+arm
for value in locations.values():Path(value).mkdir(mode=0o700,parents=True)
class Session:
 cleanup_mode=False
 def __init__(self):self.config=config
 def call(self,host,code,timeout):
  output=io.StringIO()
  with contextlib.redirect_stdout(output):exec(code,{})
  return json.loads(output.getvalue())
session=Session();events=[]
engine=SimpleNamespace(session=session,arm=arm,journal_healthy=True,
 runtime=SimpleNamespace(_write=lambda kind,value:events.append(kind)))
paused=[False]
def guard(timeout,cleanup=False):
 if paused[0] and not cleanup:raise ValueError('Synthetic human pause')
diag=SimpleNamespace(execution=engine,session=session,directory=locations['container'],artifact_owner_name=owner,_guard=guard)
registry=PaidJobs(engine,diag,locations)
child=Path(locations['generator'])/'native_paid_probe.py';child.write_text('import time;time.sleep(60)\n');child.chmod(0o600)
attempt=registry.launch('generator',[sys.executable,str(child)])
assert attempt['acknowledged'] is True and attempt['job']['pid']>0
identity=Path(locations['generator'])/'job-native_paid_probe-identity.json'
assert identity.stat().st_mode&0o777==0o600
assert json.loads(identity.read_text())==attempt['job']
paused[0]=True
result=registry.stop_all()
assert result['pass'] and result['dispatch_stopped'] and result['all_jobs_stopped']
assert session.call('generator',process_program(attempt['job']),45)=={'running':False}
assert events==['paid-job-intent','paid-job-ack','paid-job-stop-intent','paid-job-stopped']
print(json.dumps({'exact_linux_process_identity':True,'identity_persisted':True,'pause_cleanup':True,
 'owned_process_group_removed':True,'cloud_calls':0,'customer_dispatches':0}))
'''.replace('owner=OWNER','owner='+repr(name)).replace('FILES',repr(files))
    command=['docker','run','--pull=never','--name',name,'--label','ticketing.local-paid-job-owner='+name,
        '--rm','-i','--network','none','--read-only','--tmpfs','/tmp:rw,size=32m','--memory','256m','--cpus','0.5',
        image,'python','-c','import sys;exec(sys.stdin.read())']
    try:
        result=subprocess.run(command,input=program,text=True,capture_output=True,timeout=60,check=False)
        assert result.returncode==0,result.stderr[-2000:]
        proof=json.loads(result.stdout.splitlines()[-1])
        assert proof['exact_linux_process_identity'] and proof['pause_cleanup'] and proof['owned_process_group_removed']
        assert proof['cloud_calls']==proof['customer_dispatches']==0
    finally:
        observed=subprocess.run(['docker','inspect',name],text=True,capture_output=True,timeout=10,check=False)
        if observed.returncode==0:
            row=json.loads(observed.stdout)[0]
            assert row['Image']==image and row['Config']['Labels'].get('ticketing.local-paid-job-owner')==name
            subprocess.run(['docker','rm','-f',row['Id']],capture_output=True,check=True,timeout=15)



@pytest.mark.parametrize('value',[0,1,None,'false'])
def test_stop_requires_an_actual_boolean_evidence(tmp_path,monkeypatch,frozen,value):
    registry,engine,_events,_clock=job_setup(tmp_path,monkeypatch,frozen)
    registry.launch('generator',['python',registry.locations['generator']+'/customer.py'])
    registry._remote=lambda *args,**kwargs:{'running':value}
    result=registry.stop_all()
    assert not result['pass'] and not result['dispatch_stopped']
    assert engine.session.cleanup_mode is False


def test_adapter_source_drift_blocks_forward_but_keeps_bound_cleanup(tmp_path,monkeypatch,frozen):
    component,_engine,_events,_audit,_clock=setup(tmp_path,monkeypatch,frozen)
    monkeypatch.setattr(paid,'adapter_identity',lambda:{'changed':'a'*64})
    with pytest.raises(ValueError,match='adapter source changed'):component._guard(1)
    component._guard(1,cleanup=True)


def test_unknown_launch_remains_recovery_required_while_other_jobs_stop(tmp_path,monkeypatch,frozen):
    registry,engine,_events,_clock=job_setup(tmp_path,monkeypatch,frozen)
    registry.launch('primary',['python3',registry.locations['primary']+'/cpu.py'])
    original=registry._remote
    def remote(attempt,code,timeout,cleanup=False):
        if attempt['site']=='generator':raise TimeoutError('Synthetic unknown launch identity')
        return original(attempt,code,timeout,cleanup=cleanup)
    registry._remote=remote
    with pytest.raises(TimeoutError):registry.launch('generator',['python',registry.locations['generator']+'/customer.py'])
    assert registry.attempts[1]['job'] is None
    engine.paused=True
    result=registry.stop_all()
    assert not result['pass'] and not result['dispatch_stopped']
    assert registry.attempts[0]['stopped'] and not registry.attempts[1]['stopped']
    assert engine.session.cleanup_mode is False

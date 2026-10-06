"""Generated remote programs execute only against simulated Docker and POSIX surfaces."""
import builtins
import copy
import importlib.util
import io
import json
import stat
import sys
from contextlib import redirect_stdout
from datetime import UTC, datetime, timedelta
from pathlib import Path
from types import SimpleNamespace

import pytest

ROOT=Path(__file__).resolve().parents[2]
sys.path.insert(0,str(ROOT/'scripts'))
import work_envelope as policy
import worker_separation_runtime as runtime
from worker_separation_staging import PROFILE
from worker_separation_topology import WORKERS

NOW=datetime(2026,10,6,12,tzinfo=UTC)


def module(name):
    spec=importlib.util.spec_from_file_location(name,ROOT/'tests/unit'/f'{name}.py')
    result=importlib.util.module_from_spec(spec);spec.loader.exec_module(result)
    return result


def fixture():
    saved=module('test_worker_separation_lifecycle').saved()
    _,rows,volumes,_=module('test_worker_separation_lifecycle').original()
    for row in rows:row['State']['StartedAt']=NOW.isoformat()
    import hashlib
    saved['bind_sha256']={p:hashlib.sha256(b'nginx-test').hexdigest() for p in saved['bind_sha256']}
    inputs,_=module('test_worker_separation_staging').inputs()
    inputs['saved_runtime_sha256']=policy.digest(saved)
    return saved,rows,volumes,inputs


class Docker:
    DEVNULL=-3
    PIPE=-1
    def __init__(self,rows,volumes):
        self.rows=copy.deepcopy(rows);self.volumes=copy.deepcopy(volumes)
        self.removed=[];self.change=None;self.inspect_calls=0
    def check_output(self,args,**kwargs):
        assert 0<kwargs['timeout']<=10
        if args[1:3]==['ps','-aq']:return '\n'.join(r['Id'] for r in self.rows)
        if args[1:3]==['volume','inspect']:return json.dumps(self.volumes)
        assert args[1]=='inspect'
        selected=[r for r in self.rows if r['Id'] in args[2:]]
        if len(args)==3:
            self.inspect_calls+=1
            if self.change=='started':selected[0]['State']['StartedAt']='changed'
            if self.change=='image':selected[0]['Image']='sha256:'+'f'*64
        return json.dumps(selected)
    def run(self,args,**kwargs):
        assert args[:3]==['docker','rm','-f'] and kwargs['timeout']==5
        row=next(r for r in self.rows if r['Id']==args[3])
        assert row['Config']['Labels']['com.docker.compose.service'] in WORKERS
        self.removed.append(args[3]);self.rows.remove(row)
        if self.change=='partial':raise TimeoutError('Lost simulated stop response')


def execute(code,docker,*,bad_file=False,changed_file=False,clock=None):
    class Stream(io.BytesIO):
        def fileno(self):return 1
    calls=[]
    def metadata(*args,**kwargs):
        calls.append(True)
        return SimpleNamespace(st_mode=stat.S_IFIFO if bad_file else stat.S_IFREG,
            st_size=10,st_dev=1,st_ino=2 if changed_file and len(calls)>1 else 1,
            st_mtime_ns=1,st_ctime_ns=1)
    posix=SimpleNamespace(O_RDONLY=0,O_NOFOLLOW=1,O_NONBLOCK=2,open=lambda *a:1,
        fdopen=lambda *a:Stream(b'nginx-test'),fstat=metadata,stat=metadata)
    original_import=builtins.__import__
    def importer(name,*a,**k):
        if name=='subprocess':return docker
        if name=='os':return posix
        if name=='time' and clock is not None:return SimpleNamespace(monotonic=clock)
        return original_import(name,*a,**k)
    output=io.StringIO()
    with redirect_stdout(output):
        exec(code,{'__builtins__':{**vars(builtins),'__import__':importer}})  # noqa: S102
    return json.loads(output.getvalue())


class Session:
    cleanup_mode=False
    def __init__(self,guard,rows,volumes):
        self.action_guard=guard;self.primary=Docker(rows,volumes);self.secondary=Docker([],[])
        self.calls=[];self.generator_idle=True;self.lost_stop=False;self.bad_file=False
    def call(self,host,code,timeout):
        self.calls.append((host,timeout))
        if host=='generator':return {'generator_idle':self.generator_idle}
        result=execute(code,getattr(self,host),bad_file=self.bad_file)
        if self.lost_stop and 'target_count' in result:raise TimeoutError('Lost acknowledgement')
        return result


def create(monkeypatch,*,provider=True):
    saved,rows,volumes,inputs=fixture()
    binding=runtime.binding_for(inputs)
    guard=object.__new__(policy.ActionGuard);guard.key='synthetic-worker-scope';guard.binding=binding
    checks=[];guard.check=lambda timeout:checks.append(timeout)
    monkeypatch.setattr(policy,'PROFILES',{PROFILE:('synthetic','ADR0188')})
    monkeypatch.setattr(runtime,'check_authority',lambda *a:{'status':'PASS'})
    session=Session(guard,rows,volumes)
    def drain(value):return {'binding_sha256':policy.digest(value),'checked_at':NOW.isoformat(),
        'dispatch_stopped':True,'all_queues_zero':True,'kafka_drained':True}
    adapter=runtime.RuntimeActions(session,guard,inputs,saved,'control',module('test_worker_separation_lifecycle').staging.new_stage_output(),
        drain_provider=drain if provider else None,now=lambda:NOW)
    return adapter,session,checks


def request(adapter,action):return {**adapter.binding,'action':action,'cleanup':False,'timeout_seconds':runtime.LIMITS[action]}


def advance(adapter,n=4):
    return [adapter.perform(request(adapter,action)) for action in runtime.ORDER[:n]]


def test_real_registry_blocks_before_any_transport_or_owned_directory():
    saved,rows,volumes,inputs=fixture()
    guard=object.__new__(policy.ActionGuard);guard.key='unused';guard.binding=runtime.binding_for(inputs)
    guard.check=lambda _:pytest.fail('Unregistered profile must fail before checking scope')
    session=Session(guard,rows,volumes)
    output=module('test_worker_separation_lifecycle').staging.new_stage_output()
    with pytest.raises(ValueError,match='Registered fresh'):runtime.RuntimeActions(session,guard,inputs,saved,'control',output)
    assert session.calls==[] and not output.exists()


def test_ordered_generated_actions_remove_only_exact_five_workers(monkeypatch):
    adapter,session,checks=create(monkeypatch)
    receipts=advance(adapter)
    assert len(session.primary.removed)==5 and session.secondary.removed==[]
    assert all(r['pass'] is True and r['input_sha256']==policy.digest(request(adapter,a)) for a,r in zip(runtime.ORDER,receipts,strict=True))
    assert checks and all(0<t<=120 for t in checks)
    assert len(list(adapter.output.glob('*-intent.json')))==4 and len(list(adapter.output.glob('*-ack.json')))==4
    assert len(list(adapter.output.glob('*-stop-targets.json')))==1
    assert 'nginx-test' not in ''.join(p.read_text() for p in adapter.output.glob('*.json'))
    with pytest.raises(ValueError,match='Single-use'):adapter.perform(request(adapter,runtime.ORDER[-1]))


@pytest.mark.parametrize('change',['order','cleanup','binding','timeout','unsupported'])
def test_unsupported_or_mismatched_requests_make_no_calls(monkeypatch,change):
    adapter,session,_=create(monkeypatch)
    value=request(adapter,runtime.ORDER[0])
    if change=='order':value=request(adapter,runtime.ORDER[1])
    elif change=='cleanup':value['cleanup']=True
    elif change=='binding':value['pair_sha256']='f'*64
    elif change=='timeout':value['timeout_seconds']=999
    else:value['action']='restore_original_runtime'
    with pytest.raises(ValueError):adapter.perform(value)
    assert session.calls==[]


@pytest.mark.parametrize('change',['image','started','partial','lost_ack','pause','journal','replacement','restart','volume','bind'])
def test_failure_blocks_forward_progress_and_never_replays(monkeypatch,change):
    adapter,session,_=create(monkeypatch);advance(adapter,2)
    if change in {'image','started','partial'}:session.primary.change=change
    elif change=='lost_ack':session.lost_stop=True
    elif change=='pause':monkeypatch.setattr(runtime,'check_authority',lambda *a:{'status':'BLOCKED'})
    elif change=='journal':monkeypatch.setattr(adapter,'_write',lambda *a:(_ for _ in ()).throw(OSError('disk full')))
    elif change=='replacement':session.primary.rows[0]['Id']='f'*64
    elif change=='restart':session.primary.rows[0]['State']['StartedAt']='2026-10-06T12:00:01+00:00'
    elif change=='volume':session.primary.volumes[0]['Name']='different'
    else:session.bad_file=True
    with pytest.raises((ValueError,TimeoutError,OSError)):adapter.perform(request(adapter,'stop_original_workers'))
    previous=list(session.primary.removed);calls=len(session.calls)
    with pytest.raises(ValueError,match='Single-use'):adapter.perform(request(adapter,'stop_original_workers'))
    with pytest.raises(ValueError,match='Single-use'):adapter.perform(request(adapter,'verify_all_workers_absent'))
    assert session.primary.removed==previous and len(session.calls)==calls and adapter.failed
    if change not in {'partial','lost_ack'}:assert not session.primary.removed


@pytest.mark.parametrize('change',['missing','stale','future','naive','binding','queue','kafka','dispatch','extra','integer'])
def test_drain_failure_prevents_every_worker_removal(monkeypatch,change):
    adapter,session,_=create(monkeypatch,provider=change!='missing');advance(adapter,2)
    if change!='missing':
        receipt=adapter.drain_provider(adapter.audit_binding)
        if change=='stale':receipt['checked_at']=(NOW-timedelta(seconds=31)).isoformat()
        elif change=='future':receipt['checked_at']=(NOW+timedelta(seconds=1)).isoformat()
        elif change=='naive':receipt['checked_at']=NOW.replace(tzinfo=None).isoformat()
        elif change=='binding':receipt['binding_sha256']='f'*64
        elif change=='extra':receipt['private_token']='never print'
        else:receipt[{'queue':'all_queues_zero','kafka':'kafka_drained','dispatch':'dispatch_stopped','integer':'dispatch_stopped'}[change]]=1 if change=='integer' else False
        adapter.drain_provider=lambda _:receipt
    with pytest.raises((ValueError,TypeError)):adapter.perform(request(adapter,'stop_original_workers'))
    assert not session.primary.removed


@pytest.mark.parametrize('change',['stopped','foreign','secondary','infra','generator'])
def test_absence_is_independent_and_rejects_overlap_or_infrastructure_drift(monkeypatch,change):
    adapter,session,_=create(monkeypatch)
    if change=='generator':
        advance(adapter,1);session.generator_idle=False
        with pytest.raises(ValueError):adapter.perform(request(adapter,'verify_generator_idle'))
        assert not session.primary.removed
        return
    worker=copy.deepcopy(next(r for r in session.primary.rows if r['Config']['Labels']['com.docker.compose.service']=='consumer'))
    advance(adapter,3)
    if change=='stopped':worker['State']['Running']=False;session.primary.rows.append(worker)
    elif change=='foreign':worker['Config']['Labels']['com.docker.compose.project']='other';session.primary.rows.append(worker)
    elif change=='secondary':session.secondary.rows.append(worker)
    else:session.primary.rows[0]['Config']['Cmd']=['changed']
    with pytest.raises(ValueError):adapter.perform(request(adapter,'verify_all_workers_absent'))


def test_generated_observation_rejects_file_change_and_bounded_inventory():
    saved,rows,volumes,_=fixture();docker=Docker(rows,volumes)
    with pytest.raises(ValueError,match='changed'):execute(runtime.observation_program(saved,True),docker,changed_file=True)
    docker.rows=[copy.deepcopy(rows[0]) for _ in range(65)]
    with pytest.raises(ValueError,match='Bounded'):execute(runtime.observation_program(saved,True),docker)


@pytest.mark.parametrize('change',['ack_write','invalid_ack','binding_drift','same_guard','stale_generator'])
def test_guard_and_lost_acknowledgement_fail_closed(monkeypatch,change):
    adapter,session,_=create(monkeypatch)
    if change=='same_guard':
        saved,_rows,_volumes,inputs=fixture()
        session.action_guard=None
        with pytest.raises(ValueError,match='Same active'):
            runtime.RuntimeActions(session,adapter.guard,inputs,saved,'control',ROOT/'tmp'/'adr0153-parents-aaaaaaaaaaaa')
        assert not session.calls
        return
    advance(adapter,2)
    if change=='ack_write':
        previous=adapter._write
        def write(kind,value):
            if kind=='ack':raise OSError('ack write failed')
            return previous(kind,value)
        monkeypatch.setattr(adapter,'_write',write)
    elif change=='invalid_ack':
        previous=session.call
        def call(host,code,timeout):
            result=previous(host,code,timeout)
            return {'owned_workers_stopped':True,'target_count':4} if 'target_count' in result else result
        monkeypatch.setattr(session,'call',call)
    elif change=='binding_drift':adapter.guard.binding['worker_saved_runtime_sha256']='f'*64
    else:session.generator_idle=False
    with pytest.raises((ValueError,OSError)):adapter.perform(request(adapter,'stop_original_workers'))
    if change in {'binding_drift','stale_generator'}:assert not session.primary.removed
    else:assert len(session.primary.removed)==5
    assert adapter.failed
    count=len(session.calls)
    with pytest.raises(ValueError):adapter.perform(request(adapter,'verify_all_workers_absent'))
    assert len(session.calls)==count


def test_generated_shared_deadline_expires_before_a_stop_command():
    saved,rows,volumes,_=fixture();docker=Docker(rows,volumes)
    ticks=iter([0,56])
    program,_=runtime.stop_program(saved,rows)
    with pytest.raises(TimeoutError,match='deadline'):
        execute(program,docker,clock=lambda:next(ticks))
    assert docker.removed==[]


@pytest.mark.parametrize('change',['guard','cleanup_mode'])
def test_session_guard_identity_is_rechecked_before_every_call(monkeypatch,change):
    adapter,session,_=create(monkeypatch);advance(adapter,2)
    if change=='guard':session.action_guard=None
    else:session.cleanup_mode=True
    count=len(session.calls)
    with pytest.raises(ValueError,match='Registered fresh'):adapter.perform(request(adapter,'stop_original_workers'))
    assert len(session.calls)==count and not session.primary.removed



def test_drain_receipt_expiry_during_persistence_blocks_removal(monkeypatch):
    adapter,session,_=create(monkeypatch);advance(adapter,2)
    ticks=iter([NOW,NOW+timedelta(seconds=31)])
    adapter.now=lambda:next(ticks)
    with pytest.raises(ValueError,match='expired before'):adapter.perform(request(adapter,'stop_original_workers'))
    assert not session.primary.removed


@pytest.mark.parametrize('change',['arm','scope','extra_scope_binding'])
def test_drain_receipts_and_guard_bindings_cannot_cross_arms_or_scopes(monkeypatch,change):
    adapter,session,_=create(monkeypatch);advance(adapter,2)
    if change=='extra_scope_binding':adapter.guard.binding['unbound_setting']='changed'
    else:
        binding=copy.deepcopy(adapter.audit_binding)
        binding['lifecycle_binding_sha256' if change=='arm' else 'scope']='other'
        receipt=adapter.drain_provider(binding)
        adapter.drain_provider=lambda _:receipt
    with pytest.raises(ValueError):adapter.perform(request(adapter,'stop_original_workers'))
    assert not session.primary.removed

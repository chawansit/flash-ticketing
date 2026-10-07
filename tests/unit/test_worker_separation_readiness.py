"""ADR0196 synthetic component faults and read-only client behavior; no cloud calls."""
import copy
import hashlib
import importlib.util
import json
import os
import socket
import sys
from pathlib import Path
from types import SimpleNamespace

import pytest

ROOT=Path(__file__).resolve().parents[2]
sys.path.insert(0,str(ROOT/'scripts'))
import work_envelope as policy
import worker_separation_execution as execution
import worker_separation_readiness as readiness

CA='synthetic exact CA bytes\n'


def fixture_module():
    spec=importlib.util.spec_from_file_location('adr0196_fixtures',ROOT/'tests/unit/test_worker_separation_execution.py')
    module=importlib.util.module_from_spec(spec);spec.loader.exec_module(module)
    return module


class Transport:
    cleanup_mode=False
    def __init__(self,values):self.values=values;self.probes=[];self.fail=None;self.bad=None
    def call(self,host,code,timeout):
        assert 0 < timeout <= 60
        if '\nbody=' not in code:return copy.deepcopy(self.values[host])
        self.probes.append((host,timeout))
        if self.fail==host:raise TimeoutError('Synthetic response loss')
        if self.bad=='drift' and host=='primary':self.values[host]['rows'][0]['Image']='sha256:'+'f'*64
        receipt={'checks':{k:True for k in readiness.CHECKS},'probe_removed':True,'runtime_unchanged':True}
        if self.bad in readiness.CHECKS:receipt['checks'][self.bad]=False
        elif self.bad=='extra':receipt['extra']=True
        elif self.bad in {'probe_removed','runtime_unchanged'}:receipt[self.bad]=False
        return receipt


def setup():
    module=fixture_module();saved,_,_,inputs=module.fixture()
    fields={'DB_HOST':'10.0.0.8','DB_PORT':'5432','DB_NAME':'db','DB_USER':'user','DB_PASSWORD':'a$b#',
            'SERVER_TLS_SSLMODE':'verify-full','SERVER_TLS_CA_FILE':'/etc/pgbouncer/readiness-ca.pem'}
    mount={'type':'bind','source':'/qualification/readiness-ca.pem','target':fields['SERVER_TLS_CA_FILE'],'read_only':True}
    saved['model']['services']['pgbouncer']['environment'].update(fields)
    saved['model']['services']['pgbouncer'].setdefault('volumes',[]).append(mount)
    saved['bind_sha256'][mount['source']]=hashlib.sha256(CA.encode()).hexdigest()
    for arm in ('control','candidate'):
        pg=inputs['pair'][arm]['primary']['services']['pgbouncer']
        pg['environment'].update(fields);pg.setdefault('volumes',[]).append(copy.deepcopy(mount))
    events=[]
    runtime=SimpleNamespace(guard=SimpleNamespace(binding={}),audit_binding={'scope':'synthetic'},_write=lambda k,v:events.append((k,copy.deepcopy(v))))
    values={'primary':{'rows':module.rows_for(inputs['pair']['candidate']['primary'],execution.INFRA),
                       'volumes':[saved['broker_volume']],'bind_sha256':saved['bind_sha256']},
            'secondary':{'rows':[],'volumes':[],'bind_sha256':{}}}
    engine=SimpleNamespace(pair=inputs['pair'],saved=saved,arm='candidate',used={'configure_common_infrastructure'},
                           failed=False,runtime=runtime,session=Transport(values),dependency_receipt=None)
    context=readiness.dependency_context(engine,CA)
    runtime.guard.binding={'worker_dependency_context_sha256':policy.digest(context)}
    engine.scope_sha=policy.digest(runtime.guard.binding)
    def guard(timeout):
        if policy.digest(runtime.guard.binding)!=engine.scope_sha:raise ValueError('Synthetic scope drift')
    engine._guard=guard
    return engine,events


def test_two_host_complete_readiness_retains_only_bound_success():
    engine,events=setup();before=copy.deepcopy(engine.session.values)
    component=readiness.ReadinessActions(engine,CA)
    result=component.verify()
    assert result['checks']=={k:True for k in readiness.CHECKS}
    assert engine.dependency_receipt==result
    assert engine.session.values==before and [h for h,_ in engine.session.probes]==['primary','secondary']
    assert [k for k,_ in events]==['dependency-intent','dependency-probe-intent','dependency-probe-ack','dependency-probe-intent','dependency-probe-ack','dependency-ack']
    assert 'a$b#' not in json.dumps(events) and CA not in json.dumps(events)
    with pytest.raises(ValueError,match='Single-use'):component.verify()


@pytest.mark.parametrize('change',['ca','sslmode','password','host','mount','mixed_authority','context_scope'])
def test_context_drift_blocks_before_probe(change):
    engine,_=setup()
    ca=CA
    if change=='ca':ca='different'
    elif change=='sslmode':engine.saved['model']['services']['pgbouncer']['environment']['SERVER_TLS_SSLMODE']='require'
    elif change=='password':engine.saved['model']['services']['pgbouncer']['environment']['DB_PASSWORD']='different'
    elif change=='host':engine.saved['model']['services']['pgbouncer']['environment']['DB_HOST']='8.8.8.8'
    elif change=='mount':engine.saved['model']['services']['pgbouncer']['volumes'][-1]['read_only']=False
    elif change=='mixed_authority':engine.pair['control']['primary']['services']['publisher']['environment']['REDIS_URL']='redis://different:6379'
    else:engine.runtime.guard.binding['worker_dependency_context_sha256']='f'*64
    with pytest.raises((ValueError,KeyError)):readiness.ReadinessActions(engine,ca)
    assert not engine.session.probes


@pytest.mark.parametrize('bad',list(readiness.CHECKS)+['probe_removed','runtime_unchanged','extra','drift'])
def test_failed_check_or_runtime_drift_blocks_start_and_replay(bad):
    engine,_=setup();component=readiness.ReadinessActions(engine,CA);engine.session.bad=bad
    with pytest.raises(ValueError):component.verify()
    assert engine.failed and engine.dependency_receipt is None
    with pytest.raises(ValueError):component.verify()


@pytest.mark.parametrize('host',['primary','secondary'])
def test_lost_probe_response_is_not_replayed(host):
    engine,_=setup();component=readiness.ReadinessActions(engine,CA);engine.session.fail=host
    with pytest.raises(TimeoutError):component.verify()
    assert engine.failed and engine.dependency_receipt is None
    with pytest.raises(ValueError):component.verify()
    assert [h for h,_ in engine.session.probes].count(host)==1


def test_shared_deadline_stops_before_probe():
    engine,_=setup();ticks=iter([0,1,92]);component=readiness.ReadinessActions(engine,CA,monotonic=lambda:next(ticks))
    with pytest.raises(TimeoutError,match='Shared'):component.verify()
    assert engine.failed and not engine.session.probes


def test_failed_ack_persistence_cannot_authorize_start():
    engine,_=setup()
    def write(kind,value):
        if kind=='dependency-ack':raise OSError('Synthetic disk full')
    engine.runtime._write=write
    component=readiness.ReadinessActions(engine,CA)
    with pytest.raises(OSError):component.verify()
    assert engine.failed and engine.dependency_receipt is None


@pytest.mark.parametrize('change',['missing','false','stale','scope','context','extra','probes'])
def test_actual_worker_start_requires_fresh_complete_readiness(monkeypatch,change):
    module=fixture_module();engine,session=module.create(monkeypatch)
    engine.configure_common()
    receipt=engine.dependency_receipt
    if change=='missing':engine.dependency_receipt=None
    elif change=='false':receipt['checks']['private_kafka_ready']=False
    elif change=='stale':receipt['checked_at']='2026-10-07T04:00:00+00:00'
    elif change=='scope':receipt['binding_sha256']='f'*64
    elif change=='context':receipt['context_sha256']='f'*64
    elif change=='extra':receipt['checks']['unknown']=True
    else:receipt['probes_removed']=False
    with pytest.raises(ValueError):engine.start_workers()
    assert all(phase!='start' for phase,_,_ in session.mutations)
    engine.stop_workers();engine.restore()


def clients(monkeypatch,bad=None):
    engine,_=setup();context=readiness.dependency_context(engine,CA)
    context['api_urls']=['http://10.0.0.1:'+str(18000+i)+'/health/ready' for i in range(4)]
    records={'connections':[],'queries':[],'redis_closed':False,'kafka_closed':False,'api':[]}
    monkeypatch.setattr(socket,'getaddrinfo',lambda host,port,**kw:[(socket.AF_INET,socket.SOCK_STREAM,6,'',('8.8.8.8' if bad=='public_dns' else '10.0.0.8',port))])
    class Connection:
        def __enter__(self):return self
        def __exit__(self,*args):pass
        def execute(self,query):records['queries'].append(query);return self
        def fetchone(self):return ('wrong' if bad=='identity' else 'user','db',True,False,bad!='unencrypted')
    def connect(*args,**kw):
        records['connections'].append((args,kw.copy()))
        if 'sslrootcert' in kw:
            assert Path(kw['sslrootcert']).read_text()==CA
            if bad=='tls':raise RuntimeError('Synthetic TLS verification failure')
        return Connection()
    monkeypatch.setitem(sys.modules,'psycopg',SimpleNamespace(connect=connect))
    class Cache:
        @classmethod
        def from_url(cls,*a,**kw):return cls()
        def ping(self):return bad!='redis'
        def close(self):records['redis_closed']=True
    monkeypatch.setitem(sys.modules,'redis',SimpleNamespace(Redis=Cache))
    class Kafka:
        def __init__(self,**kw):
            assert kw['request_timeout_ms']==3000 and kw['api_version_auto_timeout_ms']==3000
        def describe_cluster(self):
            if bad=='kafka':raise TimeoutError('Synthetic Kafka timeout')
            return {'brokers':[{'host':'public' if bad=='metadata' else '10.0.0.1','port':19092}]}
        def close(self):records['kafka_closed']=True
    monkeypatch.setitem(sys.modules,'kafka.admin',SimpleNamespace(KafkaAdminClient=Kafka))
    import urllib.request
    class Response:
        status=200
        def __enter__(self):return self
        def __exit__(self,*a):pass
        def read(self,n):return json.dumps({'status':'unready' if bad=='api' else 'ready'}).encode()
    class Opener:
        def open(self,url,timeout):records['api'].append(url);return Response()
    monkeypatch.setattr(urllib.request,'build_opener',lambda *a:Opener())
    return context,records


def test_dependency_client_paths_use_verified_tls_read_only_and_close(monkeypatch):
    context,records=clients(monkeypatch)
    assert readiness.probe(context)=={k:True for k in readiness.CHECKS}
    assert len(records['connections'])==2 and len(records['api'])==4
    direct=records['connections'][0][1]
    assert direct['sslmode']=='verify-full' and 'default_transaction_read_only=on' in direct['options']
    assert not os.path.exists(direct['sslrootcert'])
    assert 'SET TRANSACTION READ ONLY' in records['queries']
    assert records['redis_closed'] and records['kafka_closed']
    assert all(q.startswith(('SELECT ','SET ')) for q in records['queries'])


@pytest.mark.parametrize('bad',['public_dns','tls','identity','unencrypted','redis','kafka','metadata','api'])
def test_dependency_failures_are_not_successful_receipts(monkeypatch,bad):
    context,records=clients(monkeypatch,bad)
    with pytest.raises((ValueError,RuntimeError,TimeoutError)):readiness.probe(context)
    if bad=='public_dns':assert not records['connections']
    if records['connections'] and 'sslrootcert' in records['connections'][0][1]:
        assert not os.path.exists(records['connections'][0][1]['sslrootcert'])
    if bad in {'kafka','metadata','api'}:assert records['kafka_closed']


def test_dependency_readiness_is_rechecked_immediately_before_start(monkeypatch):
    from datetime import timedelta
    module=fixture_module();engine,session=module.create(monkeypatch)
    engine.configure_common()
    engine.dependency_receipt['checked_at']=(module.NOW-timedelta(seconds=29)).isoformat()
    def drained():
        engine.now=lambda:module.NOW+timedelta(seconds=2)
        return module.NOW
    engine._drain=drained
    with pytest.raises(ValueError,match='readiness expired'):engine.start_workers()
    assert all(phase!='start' for phase,_,_ in session.mutations)


@pytest.mark.parametrize('query', ['connect_timeout=2', 'sslmode=disable&connect_timeout=4', 'connect_timeout=1&sslmode=disable'])
def test_original_bounded_connection_timeout_is_preserved(query):
    engine,_=setup()
    for role in readiness.WORKERS:
        env=engine.pair['control']['primary']['services'][role]['environment']
        env['DATABASE_URL']=env['DATABASE_URL'].split('?')[0]+'?'+query
    context=readiness.dependency_context(engine,CA)
    assert context['database_url'].endswith('?'+query)


@pytest.mark.parametrize('query', ['connect_timeout=0','connect_timeout=5','connect_timeout=-1',
    'connect_timeout=2&connect_timeout=2','sslmode=disable&sslmode=disable','options=-c%20search_path=foreign',
    'host=10.0.0.9','sslmode=require','connect_timeout=','connect_timeout=02'])
def test_pooler_query_overrides_remain_forbidden(query):
    engine,_=setup()
    for role in readiness.WORKERS:
        env=engine.pair['control']['primary']['services'][role]['environment']
        env['DATABASE_URL']=env['DATABASE_URL'].split('?')[0]+'?'+query
    with pytest.raises(ValueError,match='overrides'):
        readiness.dependency_context(engine,CA)


def test_actual_readiness_imports_exist_in_immutable_frozen_image():
    import ast
    import inspect
    import re
    import subprocess

    from stage_status_refresh_images import new_stage_output
    image=os.environ.get('ADR0198_LOCAL_LINUX_IMAGE')
    if image is None:pytest.skip('Explicit immutable local Linux image required')
    assert re.fullmatch(r'sha256:[0-9a-f]{64}',image)
    tree=ast.parse(inspect.getsource(readiness.probe));imports=[ast.unparse(node) for node in tree.body[0].body if isinstance(node,(ast.Import,ast.ImportFrom))]
    body='\n'.join(imports)+'\nprint("readiness_imports_passed")\n'
    result=subprocess.run(['docker','run','--rm','--pull=never','--name',new_stage_output().name+'-readiness-imports',
        '--network=none','--read-only','--user=65534:65534','--cap-drop=ALL','--security-opt=no-new-privileges',
        '--entrypoint=python',image,'-c',body],capture_output=True,text=True,timeout=30,check=False)
    assert result.returncode==0 and result.stdout.strip()=='readiness_imports_passed',result.stderr


@pytest.mark.parametrize('bad,phase', [('public_dns','private_address_resolution'),('tls','direct_rds_tls'),
    ('identity','direct_rds_tls'),('redis','redis'),('kafka','kafka'),('metadata','kafka'),('api','api_readiness')])
def test_safe_failure_receipt_preserves_actual_phase(monkeypatch,bad,phase):
    context,_=clients(monkeypatch,bad)
    receipt=readiness.probe_receipt(context)
    assert set(receipt)=={'failure'} and readiness.valid_failure(receipt['failure'])
    assert receipt['failure']['phase']==phase
    assert context['rds']['password'] not in json.dumps(receipt) and CA not in json.dumps(receipt)


@pytest.mark.parametrize('message,category',[('password=never-publish Connection refused','connection_refused'),
    ('certificate verify failed secret','tls_verification'),('secret timed out','timeout'),('arbitrary secret','check_failed')])
def test_exception_details_are_never_serialized(monkeypatch,message,category):
    def fail(context,progress):
        progress['phase']='pooler'
        raise ConnectionError(message)
    monkeypatch.setattr(readiness,'probe',fail)
    receipt=readiness.probe_receipt({})
    assert receipt=={'failure':{'phase':'pooler','exception_category':'ConnectionError','category':category}}
    assert message not in json.dumps(receipt)


def test_structured_failure_is_retained_then_blocks_workers(monkeypatch):
    engine,events=setup();component=readiness.ReadinessActions(engine,CA)
    original=engine.session.call
    failure={'phase':'pooler','exception_category':'OperationalError','category':'connection_refused'}
    def call(host,code,timeout):
        if '\nbody=' in code:return {'failure':failure,'probe_removed':True,'runtime_unchanged':True}
        return original(host,code,timeout)
    monkeypatch.setattr(engine.session,'call',call)
    with pytest.raises(RuntimeError,match='at pooler'):component.verify()
    assert engine.failed and engine.dependency_receipt is None
    kind,record=events[-1]
    assert kind=='dependency-probe-failure' and record['failure']==failure
    assert record['scope_binding_sha256']==engine.scope_sha
    assert record['context_sha256']==component.context_sha


@pytest.mark.parametrize('change',['secret','cleanup','drift','unknown_phase'])
def test_unverified_or_secret_failure_cannot_be_retained(monkeypatch,change):
    engine,events=setup();component=readiness.ReadinessActions(engine,CA)
    original=engine.session.call
    receipt={'failure':{'phase':'pooler','exception_category':'OperationalError','category':'timeout'},
             'probe_removed':True,'runtime_unchanged':True}
    if change=='secret':receipt['failure']['detail']='password=never-publish'
    elif change=='unknown_phase':receipt['failure']['phase']='password=never-publish'
    else:receipt['probe_removed' if change=='cleanup' else 'runtime_unchanged']=False
    def call(host,code,timeout):
        return receipt if '\nbody=' in code else original(host,code,timeout)
    monkeypatch.setattr(engine.session,'call',call)
    with pytest.raises(ValueError):component.verify()
    assert engine.failed and engine.dependency_receipt is None
    assert not any(k=='dependency-probe-failure' for k,_ in events)

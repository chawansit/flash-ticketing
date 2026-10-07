"""ADR0195 fixed-role execution faults use synthetic observations and a fake transport."""
import ast
import copy
import importlib.util
import sys
from datetime import UTC, datetime
from pathlib import Path

import pytest

ROOT=Path(__file__).resolve().parents[2]
sys.path.insert(0,str(ROOT/'scripts'))
import work_envelope as policy
import worker_separation_configuration as configuration
import worker_separation_execution as execution
import worker_separation_runtime as runtime
import worker_separation_staging as staging

NOW=datetime(2026,10,7,5,tzinfo=UTC)


def module(name):
    spec=importlib.util.spec_from_file_location(name,ROOT/'tests/unit'/f'{name}.py')
    value=importlib.util.module_from_spec(spec);spec.loader.exec_module(value)
    return value


def fixture():
    source=module('test_worker_separation_runtime')
    saved,rows,volumes,inputs=source.fixture()
    inputs,metadata=source.module('test_worker_separation_staging').inputs()
    pair=module('test_worker_separation_observers').pair()
    for arm in ('control','candidate'):
        for host in ('primary','secondary'):
            model=pair[arm][host]
            if model is None:continue
            for service in model['services'].values():
                service['entrypoint']=[]
                service['environment']['PATH']='/image/bin'
            if host=='primary':
                broker=model['services']['kafka'];broker['image']=saved['model']['services']['kafka']['image']
                model['volumes'][broker['volumes'][0]['source']]={'name':saved['broker_volume']['Name'],'external':True}
    inputs['pair']=pair;inputs['saved_runtime_sha256']=policy.digest(saved)
    # Worker images are unchanged by the broker/snapshot cross-binding.
    inputs['staging_contract']=staging.make_contract(pair,inputs['role_sources'],metadata)
    inputs['archive_receipt']['contract_sha256']=policy.digest(inputs['staging_contract'])
    return saved,rows,volumes,inputs


def rows_for(model,counts):
    rows=[];index=100 if model['name']=='flash-ticketing' else 500
    for role,count in counts.items():
        service=model['services'][role]
        for replica in range(count):
            index+=1
            ports={str(p['target'])+'/'+p.get('protocol','tcp'):[{'HostIp':p['host_ip'],'HostPort':str(int(str(p['published']).split('-')[0])+replica)}]
                   for p in service.get('ports',[])}
            mounts=[]
            for m in service.get('volumes',[]):
                mounts.append({'Type':m['type'],'Name':model['volumes'][m['source']]['name'] if m['type']=='volume' else '',
                               'Source':'/docker/volumes/existing' if m['type']=='volume' else m['source'],
                               'Destination':m['target'],'RW':not m.get('read_only',False)})
            rows.append({'Id':f'{index:064x}','Image':service['image'],
                         'Config':{'Env':[k+'='+v for k,v in service['environment'].items()],
                                   'Cmd':service.get('command',[]),'Entrypoint':service.get('entrypoint',[]),
                                   'Labels':{'com.docker.compose.project':model['name'],'com.docker.compose.service':role},
                                   'User':service.get('user',''),'WorkingDir':service.get('working_dir','')},
                         'State':{'Running':True,'StartedAt':NOW.isoformat()},'Mounts':mounts,
                         'HostConfig':{'PortBindings':ports,'NetworkMode':model['name']+'_default',
                                       'RestartPolicy':{'Name':service.get('restart','no'),'MaximumRetryCount':0},
                                       'LogConfig':{'Type':'json-file','Config':{}}}})
    return rows


def literal_assignments(code):
    result={}
    for node in ast.parse(code).body:
        if isinstance(node,ast.Assign) and len(node.targets)==1 and isinstance(node.targets[0],ast.Name):
            try:result[node.targets[0].id]=ast.literal_eval(node.value)
            except (ValueError,TypeError):pass
    return result


class Session:
    cleanup_mode=False
    def __init__(self,guard,saved,rows):
        self.action_guard=guard
        self.config={h:{'repo':'/qualification/repository'} for h in ('primary','secondary')}
        self.values={'primary':{'rows':copy.deepcopy(rows),'volumes':[saved['broker_volume']], 'bind_sha256':saved['bind_sha256']},
                     'secondary':{'rows':[],'volumes':[],'bind_sha256':{}}}
        self.saved=saved;self.calls=[];self.mutations=[];self.fail=None;self.generator_idle=True
    def call(self,host,code,timeout):
        self.calls.append((host,timeout,self.cleanup_mode))
        if host=='generator':return {'generator_idle':self.generator_idle}
        constants=literal_assignments(code)
        if 'up' in constants:
            counts,model=constants['counts'],constants['model']
            phase='common' if counts==execution.INFRA else 'restore' if counts==execution.NORMAL_COUNTS else 'start'
            self.mutations.append((phase,host,constants['up']))
            if phase=='restore':
                restored=copy.deepcopy(self.original_rows)
                for i,row in enumerate(restored):row['Id']=f'{900+i:064x}'
                self.values[host]['rows']=restored
            elif phase=='common':self.values[host]['rows']=rows_for(model,counts)
            else:
                selected=rows_for(model,counts)
                if self.fail=='partial_start':selected=selected[:1]
                # IDs must be distinct from primary infrastructure in the control arm.
                for i,row in enumerate(selected):row['Id']=f'{700+i:064x}'
                self.values[host]['rows']+=selected
            if self.fail==phase or self.fail=='partial_start' and phase=='start':raise TimeoutError('Lost simulated response')
            return {'applied':True,'after':copy.deepcopy(self.values[host])}
        if 'targets' in constants:
            self.mutations.append(('stop',host,constants['targets']))
            ids={t['id'] for t in constants['targets']}
            self.values[host]['rows']=[r for r in self.values[host]['rows'] if r['Id'] not in ids]
            if self.fail=='stop':raise TimeoutError('Lost simulated stop response')
            return {'applied':True,'after':copy.deepcopy(self.values[host])}
        return copy.deepcopy(self.values[host])


def create(monkeypatch,arm='candidate'):
    saved,rows,_,inputs=fixture()
    binding=runtime.binding_for(inputs)
    guard=object.__new__(policy.ActionGuard);guard.key='synthetic-worker-scope';guard.binding=binding;guard.check=lambda _:None
    monkeypatch.setattr(policy,'PROFILES',{staging.PROFILE:('synthetic','ADR0195')})
    monkeypatch.setattr(runtime,'check_authority',lambda *a:{'status':'PASS'})
    session=Session(guard,saved,rows);session.original_rows=copy.deepcopy(rows)
    # Original-stop primitive is already separately qualified; emulate its acknowledged endpoint.
    session.values['primary']['rows']=[r for r in rows if r['Config']['Labels']['com.docker.compose.service'] not in execution.WORKERS]
    cfg=configuration.ConfigurationActions(session,guard,inputs,saved,arm,module('test_worker_separation_lifecycle').staging.new_stage_output())
    cfg.runtime.used=list(runtime.ORDER)
    for host in ('primary','secondary'):
        if inputs['pair'][arm][host] is None:continue
        value=configuration.bundle(inputs['pair'],saved,arm,host,policy.digest(binding))
        owner=configuration.owner_path(session.config[host]['repo'],cfg.runtime.output.name)
        cfg.seals[host]=module('test_worker_separation_configuration').seal(value,owner)
    def drain(bound):return {'binding_sha256':policy.digest(bound),'checked_at':NOW.isoformat(),
                            'dispatch_stopped':True,'all_queues_zero':True,'kafka_drained':True}
    engine=execution.ExecutionActions(cfg,drain_provider=drain,now=lambda:NOW)
    return engine,session


@pytest.mark.parametrize('arm',['control','candidate'])
def test_ordered_common_start_owned_stop_and_exact_restoration(monkeypatch,arm):
    engine,session=create(monkeypatch,arm)
    assert engine.configure_common()=={'broker_volume_retained':True,'common_config_applied':True}
    assert engine.start_workers()=={'exact_arm_workers_started':True}
    execution.verify_phase(engine.pair,engine.saved,arm,session.values,'workers')
    assert engine.stop_workers()['secondary_workers_absent'] is True
    assert engine.restore()['runtime_restored'] is True
    assert [m[0] for m in session.mutations]==['common','start','stop','restore']
    assert session.mutations[1][1]==('primary' if arm=='control' else 'secondary')
    assert all(row[2] is True for row in session.calls[-6:])
    assert session.cleanup_mode is False and not engine.failed
    assert all('--no-deps' in m[2] and '--no-build' in m[2] and '--pull' in m[2]
               for m in session.mutations if m[0]!='stop')
    with pytest.raises(ValueError,match='Single-use'):engine.restore()


@pytest.mark.parametrize('change',['no_stop','generator','queue','kafka','dispatch','stale','scope','image','volume','bind','overlap','seal'])
def test_invalid_preconditions_prevent_common_mutation(monkeypatch,change):
    engine,session=create(monkeypatch)
    if change=='no_stop':engine.runtime.used=[]
    elif change=='generator':session.generator_idle=False
    elif change in {'queue','kafka','dispatch','stale'}:
        value=engine.drain_provider(engine.runtime.audit_binding)
        if change=='stale':value['checked_at']='2026-10-07T04:00:00+00:00'
        else:value[{'queue':'all_queues_zero','kafka':'kafka_drained','dispatch':'dispatch_stopped'}[change]]=False
        engine.drain_provider=lambda _:value
    elif change=='scope':engine.runtime.guard.key='changed'
    elif change=='image':session.values['primary']['rows'][0]['Image']='sha256:'+'f'*64
    elif change=='volume':session.values['primary']['volumes']=[{'Name':'wrong'}]
    elif change=='bind':session.values['primary']['bind_sha256']={'/actual/runtime-nginx.conf':'b'*64}
    elif change=='overlap':session.values['secondary']['rows']=[copy.deepcopy(session.original_rows[0])]
    else:engine.configurations.seals['primary']['files']['arm.compose.json']['sha256']='b'*64
    with pytest.raises((ValueError,KeyError)):engine.configure_common()
    assert session.mutations==[]
    with pytest.raises(ValueError):engine.start_workers()


@pytest.mark.parametrize('failure',['common','partial_start','start'])
def test_lost_response_blocks_forward_but_permits_verified_owned_cleanup(monkeypatch,failure):
    engine,session=create(monkeypatch)
    if failure!='common':engine.configure_common()
    session.fail=failure
    with pytest.raises(TimeoutError):engine.configure_common() if failure=='common' else engine.start_workers()
    assert engine.failed
    before=len(session.mutations)
    with pytest.raises(ValueError):engine.start_workers()
    assert len(session.mutations)==before
    session.fail=None
    assert engine.stop_workers()['owned_workers_stopped']
    assert engine.restore()['runtime_restored']
    assert not session.values['secondary']['rows']


@pytest.mark.parametrize('change',['foreign','overlap','image','restart','network','logging','privileged','health','entrypoint','mount'])
def test_worker_cleanup_rejects_unowned_or_drifted_resources(monkeypatch,change):
    engine,session=create(monkeypatch);engine.configure_common();engine.start_workers()
    row=session.values['secondary']['rows'][0]
    if change=='foreign':row['Config']['Labels']['com.docker.compose.project']='foreign'
    elif change=='overlap':session.values['primary']['rows'].append(copy.deepcopy(row))
    elif change=='image':row['Image']='sha256:'+'f'*64
    elif change=='restart':row['HostConfig']['RestartPolicy']['Name']='always'
    elif change=='network':row['HostConfig']['NetworkMode']='host'
    elif change=='logging':row['HostConfig']['LogConfig']['Config']['max-file']='99'
    elif change=='privileged':row['HostConfig']['Privileged']=True
    elif change=='health':row['Config']['Healthcheck']={'Test':['NONE']}
    elif change=='entrypoint':row['Config']['Entrypoint']=['wrong']
    else:row['Mounts']=[{'Type':'bind','Source':'/unowned','Destination':'/app','RW':True}]
    before=len(session.mutations)
    with pytest.raises(ValueError):engine.stop_workers()
    assert len(session.mutations)==before
    with pytest.raises(ValueError):engine.restore()


def test_pause_and_local_journal_failure_still_attempt_owned_restoration(monkeypatch):
    engine,session=create(monkeypatch);engine.configure_common();engine.start_workers()
    monkeypatch.setattr(engine.runtime,'_authorize',lambda _:(_ for _ in ()).throw(TimeoutError('pause')))
    monkeypatch.setattr(engine.runtime,'_write',lambda *a:(_ for _ in ()).throw(OSError('disk full')))
    with pytest.raises(OSError):engine.stop_workers()
    assert not session.values['secondary']['rows']
    with pytest.raises(OSError):engine.restore()
    assert not engine.journal_healthy and engine.failed and session.cleanup_mode is False
    assert execution.verify_restored(engine.saved,{h:o['rows'] for h,o in session.values.items()},
                                     session.values['primary']['volumes'],session.values['primary']['bind_sha256'])['runtime_restored']


@pytest.mark.parametrize('change',['replicas','role','project','path'])
def test_compose_arguments_reject_budget_or_identity_changes(change):
    _saved,_,_,inputs=fixture();model=inputs['pair']['control']['primary'];counts=dict(execution.INFRA);repo='/qualification/repository'
    if change=='replicas':counts['api']=99
    elif change=='role':counts['unowned']=1
    elif change=='project':model['name']='unowned'
    else:repo='/qualification/../unowned'
    with pytest.raises(ValueError):execution.compose_arguments(repo,model,counts)


def test_generated_program_cannot_select_infrastructure_for_worker_removal():
    saved,rows,_,_=fixture()
    before={'rows':rows,'volumes':[saved['broker_volume']],'bind_sha256':saved['bind_sha256']}
    api=next(r for r in rows if r['Config']['Labels']['com.docker.compose.service']=='api')
    with pytest.raises(ValueError,match='worker targets'):execution.program(saved,before,targets=[execution.row_identity(api)])


def test_real_registry_still_blocks_execution_before_remote_mutation():
    assert staging.PROFILE not in policy.PROFILES
    assert staging.PROFILE not in policy.envelope()['qualified_profiles']


@pytest.mark.parametrize('failure', ['stop', 'restore'])
def test_cleanup_loss_preserves_evidence_and_blocks_replay(monkeypatch, failure):
    engine,session=create(monkeypatch);engine.configure_common();engine.start_workers()
    if failure=='restore':engine.stop_workers()
    session.fail=failure
    with pytest.raises(TimeoutError):engine.stop_workers() if failure=='stop' else engine.restore()
    before=len(session.mutations)
    with pytest.raises(ValueError):engine.stop_workers() if failure=='stop' else engine.restore()
    with pytest.raises(ValueError):engine.start_workers()
    assert len(session.mutations)==before and engine.failed and session.cleanup_mode is False


def test_document_model_drift_rejected_before_remote_program(monkeypatch):
    engine,session=create(monkeypatch)
    model=copy.deepcopy(engine.pair[engine.arm]['primary'])
    model['services']['api']['environment']['DB_POOL_MAX']='99'
    before=session.values['primary']
    with pytest.raises(ValueError,match='Complete intended model'):
        execution.program(engine.saved,before,seal=engine._seal('primary'),repo=session.config['primary']['repo'],
                          model=model,counts=execution.INFRA,filename='arm.compose.json')
    assert not session.mutations


def test_seal_bound_to_other_scope_is_rejected_before_start(monkeypatch):
    engine,session=create(monkeypatch);engine.configure_common()
    engine.configurations.seals['secondary']['manifest']['scope_binding_sha256']='f'*64
    before=len(session.mutations)
    with pytest.raises(ValueError):engine.start_workers()
    assert len(session.mutations)==before


@pytest.mark.parametrize('change', ['worker','volume','bind','unowned_infrastructure'])
def test_restoration_rejects_uncertain_absence_or_persistence(monkeypatch,change):
    engine,session=create(monkeypatch);engine.configure_common();engine.start_workers()
    if change!='worker':engine.stop_workers()
    if change=='volume':session.values['primary']['volumes']=[{'Name':'unowned'}]
    elif change=='bind':session.values['primary']['bind_sha256']={'/unowned':'f'*64}
    elif change=='unowned_infrastructure':session.values['primary']['rows'][0]['Image']='sha256:'+'f'*64
    before=len(session.mutations)
    with pytest.raises(ValueError):engine.restore()
    assert len(session.mutations)==before

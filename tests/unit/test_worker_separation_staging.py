"""ADR0187 fault checks use synthetic images and simulated sessions; no cloud calls."""
import copy
import hashlib
import importlib.util
import io
import json
import sys
from datetime import UTC, datetime, timedelta
from pathlib import Path
from types import SimpleNamespace

import pytest

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT / 'scripts'))
import stage_status_refresh_images as old_stage
import work_envelope as policy
import worker_separation_preload as preload
import worker_separation_staging as stage
import worker_separation_topology as topology

NOW = datetime(2026, 10, 6, 12, tzinfo=UTC)


def module(name):
    spec = importlib.util.spec_from_file_location(name, ROOT / 'tests/unit' / (name + '.py'))
    result = importlib.util.module_from_spec(spec);spec.loader.exec_module(result)
    return result


def inputs():
    model = module('test_worker_separation_topology').model()
    for service in model['services'].values():
        service['environment']['PATH'] = '/image/bin';service['entrypoint'] = []
    pair = topology.prepare_pair(model, primary_ip='10.0.0.1', secondary_ip='10.0.0.2')
    ids = {s['image'] for s in pair['candidate']['secondary']['services'].values()}
    metadata = [{'Id': i, 'Os': 'linux', 'Architecture': 'amd64', 'Size': 100,
                 'Config': {'Env': ['PATH=/image/bin'], 'Cmd': None, 'Entrypoint': [], 'Labels': {}},
                 'RootFS': {'Type': 'layers', 'Layers': ['sha256:' + 'f' * 64]}} for i in sorted(ids)]
    sources = {r: {'src/ticketing/domain.py': 'a' * 64} for r in topology.WORKERS}
    contract = stage.make_contract(pair, sources, metadata)
    return {'pair': pair, 'staging_contract': contract, 'role_sources': sources, 'saved_runtime_sha256': 'b' * 64,
            'archive_receipt': {'contract_sha256': policy.digest(contract), 'bytes': 33, 'sha256': 'f' * 64}}, metadata


def test_contract_binds_every_role_metadata_and_source_without_private_env():
    data, metadata = inputs()
    contract = data['staging_contract']
    assert set(contract['role_images']) == set(topology.WORKERS)
    assert len(contract['image_sizes']) == 7
    assert 'DATABASE_URL' not in json.dumps(contract) and 'synthetic' not in json.dumps(contract)
    changed = copy.deepcopy(metadata);changed[0]['RepoTags'] = ['mutable:tag'];changed[0]['GraphDriver'] = {'host': 'ignored'}
    assert stage.make_contract(data['pair'], data['role_sources'], changed) == contract


@pytest.mark.parametrize('change', ['missing_image', 'extra_image', 'duplicate_image', 'missing_role_source',
                                   'unresolved_env', 'platform', 'size', 'archive_total', 'mutable', 'unsafe_source'])
def test_contract_rejects_drift_before_preparation(change):
    data, metadata = inputs();pair=data['pair'];sources=data['role_sources']
    if change == 'missing_image': metadata.pop()
    elif change == 'extra_image': metadata.append({**metadata[0], 'Id': 'sha256:'+'0'*64})
    elif change == 'duplicate_image': metadata.append(copy.deepcopy(metadata[0]))
    elif change == 'missing_role_source': sources.pop('consumer')
    elif change == 'unresolved_env':
        for arm, host in [('control','primary'),('candidate','secondary')]:pair[arm][host]['services']['consumer']['environment'].pop('PATH')
    elif change == 'platform': metadata[0]['Os']='windows'
    elif change == 'size': metadata[0]['Size']=True
    elif change == 'archive_total':
        for row in metadata:row['Size']=old_stage.MAX_ARCHIVE
    elif change == 'mutable':
        for arm,host in [('control','primary'),('candidate','secondary')]:pair[arm][host]['services']['consumer']['image']='latest'
    else: sources['consumer']={'../secret.py':'a'*64}
    with pytest.raises(ValueError):stage.make_contract(pair,sources,metadata)


def test_same_image_requires_same_source_proof_and_deduplicates_only_that_image():
    data,metadata=inputs();pair=data['pair'];sources=data['role_sources']
    image=pair['control']['primary']['services']['consumer']['image']
    prior=pair['control']['primary']['services']['publisher']['image']
    for arm,host in [('control','primary'),('candidate','secondary')]:pair[arm][host]['services']['publisher']['image']=image
    metadata=[r for r in metadata if r['Id']!=prior]
    assert len(stage.make_contract(pair,sources,metadata)['image_sizes'])==6
    sources['publisher']={'src/ticketing/domain.py':'c'*64}
    with pytest.raises(ValueError,match='Shared'):stage.make_contract(pair,sources,metadata)


class Session:
    def __init__(self, data, archive, output, failures=(), wrong=(), drift=False):
        self.data,self.archive,self.output=data,archive,output
        self.calls,self.remote,self.failures,self.wrong=[],{},set(failures),set(wrong)
        self.config={'primary':{'repo':'/root/primary'},'secondary':{'prepared_directory':'/root/secondary'}}
        _,self.rows,_,_=module('test_worker_separation_lifecycle').original()
        for row in self.rows:row['State']['StartedAt']=NOW.isoformat()
        self.cleanup_mode=False;self.drift=drift
    def phase(self,name):pass
    def begin_cleanup(self):self.cleanup_mode=True
    def call(self,host,code,timeout):
        compile(code,'worker_stage','exec')
        if code==stage.ALL_CONTAINERS:op='inventory';result=copy.deepcopy(self.rows if host=='primary' else [])
        elif code==stage.GENERATOR_IDLE:op='idle';result={'generator_idle':True}
        elif 'Cleanup seal changed' in code:
            op='cleanup';self.remote.pop(host,None);result={'remote_archive_removed':True}
        elif "'image','load'" in code:
            op='load';self.remote[host]='loaded';result={'loaded_images':sorted(self.data['staging_contract']['image_sizes'])}
        elif 'Image cache identities differ' in code:
            op='proof';result={'contract_sha256':policy.digest(self.data['staging_contract']),
                'images_verified':sorted(self.data['staging_contract']['image_sizes']),'source_imports_match':True}
        elif 'Archive digest differs' in code:
            op='seal';owner=stage.owner_path(self.config[host]['repo' if host=='primary' else 'prepared_directory'],self.output.name)
            result={'owner':owner,'sha256':old_stage.hash_file(self.archive),
                    'seal':{'size':self.archive.stat().st_size,'dev':1,'ino':2,'mtime_ns':3,'uid':0,'mode':384}}
        elif 'owner.mkdir' in code:
            op='create';self.remote[host]='created';owner=stage.owner_path(self.config[host]['repo' if host=='primary' else 'prepared_directory'],self.output.name)
            result={'owner':owner,'created':True}
        else:raise AssertionError('Unknown simulated action')
        self.calls.append((host,op))
        if (host,op) in self.failures:raise ConnectionError('private secret must not be echoed')
        if (host,op) in self.wrong:return {'unrecognized':True}
        if self.drift and op=='inventory' and self.cleanup_mode and host=='primary':result.pop()
        return result


def package(tmp_path,monkeypatch):
    data,metadata=inputs();monkeypatch.setattr(old_stage,'ROOT',tmp_path)
    output=old_stage.new_stage_output();output.parent.mkdir();output.mkdir()
    archive=output/'images.tar';archive.write_bytes(b'known synthetic image archive')
    receipt={'contract_sha256':policy.digest(data['staging_contract']),'bytes':archive.stat().st_size,'sha256':old_stage.hash_file(archive)}
    data['archive_receipt']=receipt
    for name,value in [('contract.json',data['staging_contract']),('source-expectations.json',data['role_sources']),('archive.json',receipt)]:
        (output/name).write_text(json.dumps(value))
    return data,metadata,output,archive,receipt


def run_package(tmp_path,monkeypatch,*,failures=(),wrong=(),drift=False,receipt_failure=None):
    data,_,output,archive,receipt=package(tmp_path,monkeypatch)
    session=Session(data,archive,output,failures,wrong,drift)
    guard=SimpleNamespace(check=lambda timeout:None)
    monkeypatch.setattr(stage,'_authorize',lambda guard,contract,timeout,receipt:guard.check(timeout))
    def uploaded(session,host,*args,action_guard):
        action_guard.check(60);session.calls.append((host,'upload'));session.remote[host]='uploaded'
        if (host,'upload') in session.failures:raise TimeoutError('partial transfer')
    monkeypatch.setattr(old_stage,'upload',uploaded)
    monkeypatch.setattr(stage,'validate_archive',lambda *a:None)  # Byte fixtures here simulate transfers; real tar validation is tested separately.
    real_receipt=stage._receipt
    def write(path,value):
        if path.name==receipt_failure:raise OSError('full disk')
        return real_receipt(path,value)
    monkeypatch.setattr(stage,'_receipt',write)
    result=stage.stage_package(session,data['staging_contract'],data['role_sources'],archive,receipt,guard)
    return result,session,output


def test_worker_staging_verifies_both_hosts_and_never_deploys_services(tmp_path,monkeypatch):
    result,session,output=run_package(tmp_path,monkeypatch)
    assert result['pass'] and result['status']=='STAGED_VERIFIED'
    assert session.remote=={} and session.cleanup_mode is False
    assert result['service_deployments']==result['customer_dispatches']==0
    assert all((host,'proof') in session.calls and (host,'cleanup') in session.calls for host in ('primary','secondary'))
    assert all((output/(host+'-owner-intent.json')).exists() for host in ('primary','secondary'))
    assert (output/'staging-summary.json').exists()


@pytest.mark.parametrize('step',['create','upload','seal','load','proof','cleanup'])
def test_staging_failure_does_not_replay_or_delete_unsealed_owner(tmp_path,monkeypatch,step):
    result,session,output=run_package(tmp_path,monkeypatch,failures={('primary',step)})
    assert not result['pass'] and session.calls.count(('primary',step))==1
    assert 'private secret' not in json.dumps(result)
    if step in {'create','upload','seal'}:
        assert result['status']=='RECOVERY_REQUIRED' and ('primary','cleanup') not in session.calls
        assert (output/'primary-owner-intent.json').exists()
    elif step=='cleanup':assert result['status']=='RECOVERY_REQUIRED'
    else:
        assert result['status']=='FAILED_CLEANED' and ('primary','cleanup') in session.calls
    assert ('secondary','create') not in session.calls or step=='cleanup'


@pytest.mark.parametrize('step',['create','seal','load','proof','cleanup'])
def test_wrong_receipts_fail_closed(tmp_path,monkeypatch,step):
    result,session,_=run_package(tmp_path,monkeypatch,wrong={('primary',step)})
    assert not result['pass'] and session.calls.count(('primary',step))==1
    if step=='seal':assert ('primary','load') not in session.calls


@pytest.mark.parametrize('name',['primary-owner-intent.json','primary-seal.json','primary-load-intent.json','primary-verified.json','staging-summary.json'])
def test_receipt_disk_failure_stops_forward_actions_and_preserves_cleanup(tmp_path,monkeypatch,name):
    result,session,_=run_package(tmp_path,monkeypatch,receipt_failure=name)
    assert not result['pass']
    if name=='primary-owner-intent.json':assert ('primary','create') not in session.calls
    if name=='primary-seal.json':assert ('primary','load') not in session.calls and ('primary','cleanup') in session.calls
    if name=='staging-summary.json':assert result['status']=='RECOVERY_REQUIRED'


def test_cleanup_checks_runtime_drift_independently(tmp_path,monkeypatch):
    result,session,_=run_package(tmp_path,monkeypatch,drift=True)
    assert result['status']=='RECOVERY_REQUIRED' and not result['runtime_unchanged']
    assert session.remote=={}


def test_unregistered_registry_rejects_cloud_staging_before_any_remote_call(tmp_path,monkeypatch):
    monkeypatch.setattr(policy,'PROFILES',{k:v for k,v in policy.PROFILES.items() if k!=stage.PROFILE})
    data,_,output,archive,receipt=package(tmp_path,monkeypatch)
    session=Session(data,archive,output)
    guard=object.__new__(policy.ActionGuard)
    with pytest.raises(ValueError,match='registered'):
        stage.stage_package(session,data['staging_contract'],data['role_sources'],archive,receipt,guard)
    assert session.calls==[] and session.remote=={}


def test_generated_image_proof_checks_exact_metadata_and_owned_container_cleanup(monkeypatch,capsys):
    data,metadata=inputs();owner='adr0153-parents-aaaaaaaaaaaa'
    program=stage.image_proof_program(data['staging_contract'],data['role_sources'],owner)
    calls=[];active={}
    def checked(args,**kwargs):
        calls.append(args)
        if args[1:3]==['image','inspect']:return json.dumps(metadata)
        if args[1:3]==['ps','-aq']:return 'a'*64 if active else ''
        if args[1]=='run':
            image=next(i for i in data['staging_contract']['image_sizes'] if i in args)
            labels=dict(args[j+1].split('=',1) for j,v in enumerate(args) if v=='--label')
            active.update({'Id':'a'*64,'Image':image,'Config':{'Labels':labels}})
            assert '--pull=never' in args and '--network=none' in args and '--read-only' in args
            return json.dumps({k:True for k in ('source_hashes_match','app_source_hashes_match','import_source_hashes_match','import_code_matches_source')})
        if args[1]=='inspect':return json.dumps([active.copy()])
        raise AssertionError(args)
    def run(args,**kwargs):
        calls.append(args);assert args==['docker','rm','-f','a'*64];active.clear()
    monkeypatch.setattr(old_stage.command.__globals__['subprocess'],'check_output',checked)
    monkeypatch.setattr(old_stage.command.__globals__['subprocess'],'run',run)
    exec(program,{})  # noqa: S102 - generated program uses simulated subprocesses only.
    proof=json.loads(capsys.readouterr().out)
    assert proof['images_verified']==sorted(data['staging_contract']['image_sizes']) and not active
    assert sum(a[1]=='run' for a in calls)==sum(a[1]=='rm' for a in calls)==7


def mock_preload_policy(monkeypatch,tmp_path,data):
    binding=preload.binding_for(data)
    state={'human_pause':False,'test-scope':{'binding':binding}}
    monkeypatch.setattr(preload,'check_naming',lambda:{'status':'PASS','error_count':0})
    monkeypatch.setattr(policy,'PROFILES',{stage.PROFILE:('unused','ADR0184')})
    monkeypatch.setattr(policy,'envelope',lambda:{'qualified_profiles':[stage.PROFILE]})
    records={'human_pause':False,'experiments':[]}
    monkeypatch.setattr(policy,'journal',lambda envelope:records)
    monkeypatch.setattr(policy,'read',lambda path:state)
    monkeypatch.setattr(policy,'PAUSE',tmp_path/'absent.pause')
    monkeypatch.setattr(policy,'scope_authorized',lambda *args:{'profile':stage.PROFILE})
    receipts={c:{'schema':1,'binding_sha256':policy.digest(binding),'checked_at':NOW.isoformat(),
                 'evidence_sha256':'e'*64,'checks':{k:True for k in keys}} for c,keys in preload.REQUIRED.items()}
    return state,records,receipts


def test_all_ten_categories_are_required_without_authorizing_dispatch(tmp_path,monkeypatch):
    data,_=inputs();_,_,receipts=mock_preload_policy(monkeypatch,tmp_path,data)
    result=preload.evaluate(data,receipts,scope_key='test-scope',now=NOW)
    assert result['ready_for_load'] and len(result['checks'])==10
    assert result['load_authorized'] is False and result['cloud_calls']==0
    assert 'DATABASE_URL' not in json.dumps(result) and 'synthetic' not in json.dumps(result)


@pytest.mark.parametrize('change',['missing','failed','integer_true','stale','future','naive','binding','extra_check','extra_receipt','bad_hash','bad_input','pause','missing_scope','scope_binding','registry','legacy_recovery','other_active'])
def test_preflight_blocks_missing_failed_stale_or_unbound_evidence(tmp_path,monkeypatch,change):
    data,_=inputs();state,records,receipts=mock_preload_policy(monkeypatch,tmp_path,data)
    receipt=receipts['fixture_ownership'];scope='test-scope'
    if change=='missing':receipts.pop('fixture_ownership')
    elif change=='failed':receipt['checks']['identity_receipt_retained']=False
    elif change=='integer_true':receipt['checks']['identity_receipt_retained']=1
    elif change=='stale':receipt['checked_at']=(NOW-timedelta(seconds=121)).isoformat()
    elif change=='future':receipt['checked_at']=(NOW+timedelta(seconds=1)).isoformat()
    elif change=='naive':receipt['checked_at']=NOW.replace(tzinfo=None).isoformat()
    elif change=='binding':receipt['binding_sha256']='f'*64
    elif change=='extra_check':receipt['checks']['unapproved']=True
    elif change=='extra_receipt':receipts['unapproved']={'token':'secret'}
    elif change=='bad_hash':receipt['evidence_sha256']='invalid'
    elif change=='bad_input':data['saved_runtime_sha256']='bad'
    elif change=='pause':records['human_pause']=True
    elif change=='missing_scope':scope=None
    elif change=='scope_binding':state['test-scope']['binding']={}
    elif change=='registry':monkeypatch.setattr(policy,'PROFILES',{})
    elif change=='legacy_recovery':state['bounded_legacy']={'recovery_required':True}
    else:records['experiments'].append({'status':'ACTIVE','ledger':'other-scope'})
    result=preload.evaluate(data,receipts,scope_key=scope,now=NOW)
    assert result['status']=='BLOCKED' and not result['ready_for_load'] and not result['load_authorized']
    assert 'secret' not in json.dumps(result)


def test_local_preflight_preserves_unregistered_profile_blocker(monkeypatch):
    data=policy.envelope();data['qualified_profiles']=[p for p in data['qualified_profiles'] if p!=stage.PROFILE]
    monkeypatch.setattr(policy,'envelope',lambda:data)
    monkeypatch.setattr(preload,'check_naming',lambda:{'status':'PASS'})
    result=preload.evaluate()
    assert result['status']=='BLOCKED'
    blockers=result['checks']['authorization_and_ownership']['blockers']
    assert 'profile_not_registered' in blockers and 'fresh_bound_scope_missing' in blockers


def test_upload_guard_is_checked_before_transfer_and_each_block(tmp_path):
    archive=tmp_path/'archive';archive.write_bytes(b'a'*(1024**2+1))
    checked=[];written=[]
    destination=io.BytesIO();destination.set_pipelined=lambda _n:None;destination.close=lambda:written.append(destination.getvalue())
    sftp=SimpleNamespace(get_channel=lambda:SimpleNamespace(settimeout=lambda _n:None),
        open=lambda *a:destination,chmod=lambda *a:None,close=lambda:None)
    session=SimpleNamespace(clients={'secondary':SimpleNamespace(open_sftp=lambda:sftp)})
    guard=SimpleNamespace(check=lambda timeout:checked.append(timeout))
    old_stage.upload(session,'secondary','/root/repo/tmp/adr0153-parents-aaaaaaaaaaaa',archive,archive.stat().st_size,
                     hashlib.sha256(archive.read_bytes()).hexdigest(),action_guard=guard)
    assert checked==[240,60,60] and written==[archive.read_bytes()]



def test_preexisting_proof_container_blocks_run_and_is_not_deleted(monkeypatch):
    data,metadata=inputs()
    def checked(args,**kwargs):
        if args[1:3]==['image','inspect']:return json.dumps(metadata)
        if args[1:3]==['ps','-aq']:return 'a'*64
        raise AssertionError('Must not start or remove a pre-existing proof container')
    calls=[]
    monkeypatch.setattr(old_stage.command.__globals__['subprocess'],'check_output',checked)
    monkeypatch.setattr(old_stage.command.__globals__['subprocess'],'run',lambda *a,**k:calls.append(a))
    with pytest.raises(ValueError,match='Pre-existing'):
        exec(stage.image_proof_program(data['staging_contract'],data['role_sources'],'adr0153-parents-aaaaaaaaaaaa'),{})  # noqa: S102
    assert calls==[]


def test_guard_pause_mid_upload_does_not_copy_later_blocks(tmp_path):
    archive=tmp_path/'archive';archive.write_bytes(b'a'*(2*1024**2+1))
    written=[];closed=[];checks=[]
    destination=io.BytesIO();destination.set_pipelined=lambda _n:None
    destination.close=lambda:written.append(destination.getvalue())
    sftp=SimpleNamespace(get_channel=lambda:SimpleNamespace(settimeout=lambda _n:None),
        open=lambda *a:destination,chmod=lambda *a:None,close=lambda:closed.append(True))
    session=SimpleNamespace(clients={'secondary':SimpleNamespace(open_sftp=lambda:sftp)})
    def check(timeout):
        checks.append(timeout)
        if len(checks)==3:raise TimeoutError('pause')
    with pytest.raises(TimeoutError):
        old_stage.upload(session,'secondary','/root/repo/tmp/adr0153-parents-aaaaaaaaaaaa',archive,archive.stat().st_size,
            hashlib.sha256(archive.read_bytes()).hexdigest(),action_guard=SimpleNamespace(check=check))
    assert checks==[240,60,60] and len(written[0])==1024**2 and closed==[True]


def test_worker_preflight_cli_is_read_only_and_cannot_be_combined_with_execute(monkeypatch,capsys):
    import run_work_envelope as runner
    monkeypatch.setattr(sys,'argv',['run_work_envelope.py','--worker-preflight'])
    monkeypatch.setattr(preload,'from_files',lambda *a,**k:{'ready_for_load':False,'cloud_calls':0,'status':'BLOCKED'})
    monkeypatch.setattr(runner,'execute',lambda *a,**k:pytest.fail('No cloud execution permitted'))
    with pytest.raises(SystemExit) as exc:runner.main()
    assert exc.value.code==1 and json.loads(capsys.readouterr().out)['status']=='BLOCKED'
    monkeypatch.setattr(sys,'argv',['run_work_envelope.py','--worker-preflight','--execute'])
    with pytest.raises(SystemExit) as exc:runner.main()
    assert exc.value.code==2


def test_worker_evidence_flags_cannot_leak_into_historical_execution(monkeypatch):
    import run_work_envelope as runner
    monkeypatch.setattr(sys,'argv',['run_work_envelope.py','--execute','--worker-inputs','unused.json'])
    monkeypatch.setattr(runner,'execute',lambda *a,**k:pytest.fail('Invalid worker flags must not execute'))
    with pytest.raises(SystemExit) as exc:runner.main()
    assert exc.value.code==2


def test_fresh_guard_must_bind_pair_contract_and_sources(monkeypatch):
    data,_=inputs();contract=data['staging_contract']
    guard=object.__new__(policy.ActionGuard);guard.key='fake-scope'
    guard.binding={'worker_staging_contract_sha256':policy.digest(contract),
                   'worker_pair_sha256':contract['prepared_pair_sha256'],
                   'worker_sources_sha256':contract['source_manifest_sha256'],
                   'worker_archive_receipt_sha256':policy.digest(data['archive_receipt'])}
    checked=[];guard.check=lambda timeout:checked.append(timeout)
    monkeypatch.setattr(policy,'PROFILES',{stage.PROFILE:('unused','ADR0184')})
    monkeypatch.setattr(policy,'read',lambda path:{})
    monkeypatch.setattr(policy,'scope_authorized',lambda *a:{'profile':stage.PROFILE})
    stage._authorize(guard,contract,45,data['archive_receipt'])
    assert checked==[45]
    guard.binding['worker_pair_sha256']='f'*64
    with pytest.raises(ValueError,match='Exact fresh'):stage._authorize(guard,contract,45,data['archive_receipt'])
    assert checked==[45]


def test_receipt_missing_fields_are_not_reported_as_ready(tmp_path,monkeypatch):
    data,_=inputs();_,_,receipts=mock_preload_policy(monkeypatch,tmp_path,data)
    receipts['observation_coverage']={'token':'secret'}
    result=preload.evaluate(data,receipts,scope_key='test-scope',now=NOW)
    assert result['status']=='BLOCKED' and 'secret' not in json.dumps(result)


@pytest.mark.parametrize('change',[None,'extra_image','wrong_config','duplicate_config','unsafe_path','missing_manifest','bad_config_type'])
def test_archive_manifest_checks_real_config_hashes_and_image_coverage(tmp_path,change):
    import tarfile
    path=tmp_path/'images.tar'
    config=b'{"image":"synthetic"}'
    image='sha256:'+hashlib.sha256(config).hexdigest()
    records=[{'Config':'config.json','RepoTags':None,'Layers':[]}]
    if change=='extra_image':records.append({'Config':'other.json','Layers':[]})
    if change=='duplicate_config':records.append({'Config':'config.json','Layers':[]})
    if change=='wrong_config':records[0]['Config']='other.json'
    if change=='bad_config_type':records[0]['Config']=7
    with tarfile.open(path,'w') as archive:
        entries={'config.json':config,'other.json':b'{"different":"image"}'}
        if change!='missing_manifest':entries['manifest.json']=json.dumps(records).encode()
        if change=='unsafe_path':entries['../unowned']=b'preserve'
        for name,content in entries.items():
            member=tarfile.TarInfo(name);member.size=len(content);archive.addfile(member,io.BytesIO(content))
    if change:
        with pytest.raises((ValueError,KeyError,TypeError)):stage.validate_archive(path,{image})
    else:stage.validate_archive(path,{image})


def test_archive_receipt_cannot_change_under_a_fresh_guard(monkeypatch):
    data,_=inputs();contract=data['staging_contract']
    guard=object.__new__(policy.ActionGuard);guard.key='fake-scope'
    guard.binding={'worker_staging_contract_sha256':policy.digest(contract),
                   'worker_pair_sha256':contract['prepared_pair_sha256'],
                   'worker_sources_sha256':contract['source_manifest_sha256'],
                   'worker_archive_receipt_sha256':policy.digest(data['archive_receipt'])}
    guard.check=lambda timeout:None
    monkeypatch.setattr(policy,'PROFILES',{stage.PROFILE:('unused','ADR0184')})
    monkeypatch.setattr(policy,'read',lambda path:{})
    monkeypatch.setattr(policy,'scope_authorized',lambda *a:{'profile':stage.PROFILE})
    changed=copy.deepcopy(data['archive_receipt']);changed['sha256']='a'*64
    with pytest.raises(ValueError,match='Exact fresh'):stage._authorize(guard,contract,45,changed)


@pytest.mark.parametrize('change', [None, 'extra_root', 'duplicate_root', 'altered_blob', 'size',
    'platform', 'duplicate_platform', 'missing_layer', 'missing_attestation', 'compat_layers',
    'compat_config', 'unsupported', 'schema', 'depth', 'layers_limit'])
def test_oci_archive_verifies_pinned_runtime_and_attestation_graph(tmp_path, change):
    import tarfile
    index_type = 'application/vnd.oci.image.index.v1+json'
    manifest_type = 'application/vnd.oci.image.manifest.v1+json'
    entries = {}
    def add(content, media):
        raw = content if isinstance(content, bytes) else json.dumps(content).encode()
        digest = 'sha256:' + hashlib.sha256(raw).hexdigest()
        entries['blobs/sha256/' + digest[7:]] = raw
        return {'digest':digest, 'size':len(raw), 'mediaType':media}
    config = add(b'{"architecture":"amd64","os":"linux"}', 'application/vnd.oci.image.config.v1+json')
    layer = add(b'synthetic-layer', 'application/vnd.oci.image.layer.v1.tar')
    layers = [layer]*129 if change == 'layers_limit' else [layer]
    if change == 'size': layers[0] = dict(layer, size=layer['size']+1)
    runtime = add({'schemaVersion':2, 'mediaType':manifest_type, 'config':config, 'layers':layers}, manifest_type)
    runtime['platform'] = {'os':'linux', 'architecture':'arm64' if change == 'platform' else 'amd64'}
    attest_layer = add(b'synthetic-attestation', 'application/vnd.in-toto+json')
    attest = add({'schemaVersion':2, 'mediaType':manifest_type, 'config':config, 'layers':[attest_layer]}, manifest_type)
    attest['platform'] = {'os':'unknown', 'architecture':'unknown'}
    children = [runtime, attest] + ([runtime] if change == 'duplicate_platform' else [])
    root = add({'schemaVersion':True if change == 'schema' else 2,
                'mediaType':index_type, 'manifests':children}, index_type)
    if change == 'depth':
        for _ in range(5): root = add({'schemaVersion':2, 'mediaType':index_type, 'manifests':[root]}, index_type)
    if change == 'unsupported': root['mediaType'] = 'unsupported'
    roots = [root]
    if change == 'extra_root': roots.append(attest)
    if change == 'duplicate_root': roots.append(root)
    entries['index.json'] = json.dumps({'schemaVersion':2, 'mediaType':index_type, 'manifests':roots}).encode()
    config_path = 'blobs/sha256/' + config['digest'][7:]
    layer_path = 'blobs/sha256/' + layer['digest'][7:]
    entries['manifest.json'] = json.dumps([{'Config':'other.json' if change == 'compat_config' else config_path,
        'Layers':[] if change == 'compat_layers' else [layer_path], 'RepoTags':None}]).encode()
    if change == 'altered_blob': entries[layer_path] = b'corrupted-layer'
    if change == 'missing_layer': del entries[layer_path]
    if change == 'missing_attestation': del entries['blobs/sha256/'+attest_layer['digest'][7:]]
    path = tmp_path/'images.tar'
    with tarfile.open(path,'w') as archive:
        for name,content in entries.items():
            member=tarfile.TarInfo(name);member.size=len(content);archive.addfile(member,io.BytesIO(content))
    if change:
        with pytest.raises((ValueError, KeyError)): stage.validate_archive(path,{root['digest']})
    else: stage.validate_archive(path,{root['digest']})


@pytest.mark.parametrize('change', ['known', 'binding', 'unknown', 'receipt_missing'])
def test_worker_authority_reuses_only_exact_verified_historical_exception(tmp_path, monkeypatch, change):
    import historical_recovery_exception as history
    history_tests = module('test_historical_recovery_exception')
    area = history_tests.area.__wrapped__(tmp_path, monkeypatch)
    history_tests.install(area)
    actual = policy.read(policy.JOURNAL)
    original = area[1]
    assert history.accepted(actual, original, area[0]) is True
    data, _ = inputs()
    _, records, _ = mock_preload_policy(monkeypatch, tmp_path, data)
    records['experiments'].append(copy.deepcopy(original))
    records['historical_exceptions'] = copy.deepcopy(actual['historical_exceptions'])
    if change == 'binding': records['experiments'][-1]['binding_sha256'] = 'f' * 64
    elif change == 'unknown': records['experiments'][-1]['ledger'] = 'unknown-paid-failure'
    elif change == 'receipt_missing': records['historical_exceptions'] = {}
    result = preload.check_authority(preload.binding_for(data), 'test-scope')
    assert ('unresolved_recovery' not in result['blockers']) == (change == 'known')
    assert (result['status'] == 'PASS') == (change == 'known')


@pytest.mark.parametrize('schema',[1,2])
def test_cross_host_size_contracts_keep_historical_semantics(monkeypatch,schema):
    data,metadata=inputs();contract=copy.deepcopy(data['staging_contract'])
    if schema==1:
        contract.update(schema=1,decision='ADR0187')
        contract['image_metadata_sha256']={r['Id']:policy.digest(stage.stable_image(r)) for r in metadata}
    code=stage.image_proof_program(contract,data['role_sources'],'adr0153-parents-aaaaaaaaaaaa')
    rows=copy.deepcopy(metadata)
    for row in rows:row['Size']+=7
    def output(args,**kwargs):
        if args[:3]==['docker','image','inspect']:return json.dumps(rows)
        if args[:2]==['docker','ps']:return ''
        if args[:2]==['docker','run']:return json.dumps({k:True for k in ('source_hashes_match','app_source_hashes_match','import_source_hashes_match','import_code_matches_source')})
        raise AssertionError(args)
    import subprocess
    monkeypatch.setattr(subprocess,'check_output',output)
    if schema==1:
        with pytest.raises(ValueError,match='metadata differs'):exec(code,{})  # noqa: S102 - execute repository-generated proof with a synthetic transport.
    else:exec(code,{})  # noqa: S102 - synthetic transport only.


@pytest.mark.parametrize('change',['Config','RootFS','Os','Architecture','negative_size','oversized','boolean_size'])
def test_portable_identity_still_rejects_content_and_budget_changes(monkeypatch,change):
    data,metadata=inputs();rows=copy.deepcopy(metadata)
    if change=='Config':rows[0]['Config']['Cmd']=['foreign']
    elif change=='RootFS':rows[0]['RootFS']['Layers']=['sha256:'+'0'*64]
    elif change=='Os':rows[0]['Os']='windows'
    elif change=='Architecture':rows[0]['Architecture']='arm64'
    elif change=='negative_size':rows[0]['Size']=-1
    elif change=='oversized':rows[0]['Size']=old_stage.MAX_ARCHIVE+1
    else:rows[0]['Size']=True
    import subprocess
    monkeypatch.setattr(subprocess,'check_output',lambda *args,**kwargs:json.dumps(rows))
    with pytest.raises(ValueError):exec(stage.image_proof_program(data['staging_contract'],data['role_sources'],'adr0153-parents-aaaaaaaaaaaa'),{})  # noqa: S102 - synthetic transport only.


def test_contract_version_cannot_reinterpret_old_receipts():
    data,_=inputs();contract=copy.deepcopy(data['staging_contract']);assert contract['schema']==2 and contract['decision']=='ADR0205'
    contract['schema']=1
    with pytest.raises(ValueError):stage.validate_contract(contract,data['role_sources'])

"""ADR0195 programs execute in isolated Linux with fake Docker; Compose literals get a real local probe."""
import copy
import importlib.util
import json
import os
import re
import subprocess
import sys
from pathlib import Path

import pytest
from test_worker_configuration_posix import linux  # noqa: F401 - shared owned fixture.

ROOT=Path(__file__).resolve().parents[2]
sys.path.insert(0,str(ROOT/'scripts'))
import worker_separation_configuration as configuration
import worker_separation_execution as execution
from stage_status_refresh_images import new_stage_output

pytestmark=pytest.mark.integration


def fixture_module():
    spec=importlib.util.spec_from_file_location('execution_fixtures',ROOT/'tests/unit/test_worker_separation_execution.py')
    value=importlib.util.module_from_spec(spec);spec.loader.exec_module(value)
    return value


@pytest.mark.parametrize('case',['common','start','restore','before_drift','seal_drift','volume_drift',
                                 'compose_drift','partial_apply','stop','stop_replacement','partial_stop'])
def test_generated_program_on_real_posix_with_simulated_docker(linux,case):  # noqa: F811 - pytest fixture injection.
    fixture=fixture_module();saved,original,_,inputs=fixture.fixture()
    # Native filesystem is confined to the owned container's tmpfs.
    saved['bind_sha256']={'/qualification/original-nginx.conf':next(iter(saved['bind_sha256'].values()))}
    for service in saved['model']['services'].values():
        for mount in service.get('volumes',[]):
            if mount['type']=='bind':mount['source']='/qualification/original-nginx.conf'
    for row in original:
        for mount in row['Mounts']:
            if mount['Type']=='bind':mount['Source']='/qualification/original-nginx.conf'
    saved['semantics']={r['Config']['Labels']['com.docker.compose.service']:execution.policy.digest(execution.runtime_semantic(r)) for r in original}
    inputs['saved_runtime_sha256']=execution.policy.digest(saved)
    model=inputs['pair']['candidate']['primary']
    if case in {'stop','stop_replacement','partial_stop','start'}:
        model=inputs['pair']['candidate']['secondary']
        before={'rows':[] if case=='start' else fixture.rows_for(model,execution.WORKERS),'volumes':[],'bind_sha256':{}}
        counts=execution.WORKERS
        after=copy.deepcopy(before)
        after['rows']=fixture.rows_for(model,counts) if case=='start' else []
    else:
        before={'rows':fixture.rows_for(model,execution.INFRA),'volumes':[saved['broker_volume']],'bind_sha256':saved['bind_sha256']}
        if case!='restore':
            before['rows']=[r for r in original if r['Config']['Labels']['com.docker.compose.service'] in execution.INFRA]
        counts=execution.NORMAL_COUNTS if case=='restore' else execution.INFRA
        if case=='restore':model=saved['model']
        after={'rows':original if case=='restore' else fixture.rows_for(model,counts),'volumes':[saved['broker_volume']],'bind_sha256':saved['bind_sha256']}
    owner='/qualification/repository/tmp/'+new_stage_output().name
    prepared=configuration.bundle(inputs['pair'],saved,'candidate','secondary' if case in {'stop','stop_replacement','partial_stop','start'} else 'primary','a'*64)
    install=configuration.install_program(owner,prepared)
    placeholder=fixture.module('test_worker_separation_configuration').seal(prepared,owner)
    if case in {'stop','stop_replacement','partial_stop'}:
        code=execution.program(saved,before,targets=[execution.row_identity(r) for r in before['rows']])
    else:
        code=execution.program(saved,before,seal=placeholder,repo='/qualification/repository',model=model,counts=counts,
                               filename='restore.compose.json' if case=='restore' else 'arm.compose.json')
    script=r'''
import contextlib,copy,io,json,os,sys
from pathlib import Path
from types import SimpleNamespace
case=CASE;before=BEFORE;after=AFTER
owner=OWNER;installation=INSTALL;code=CODE;placeholder=PLACEHOLDER
Path(owner).parent.mkdir(parents=True,exist_ok=True)
Path('/qualification/original-nginx.conf').write_bytes(b'nginx-test')
def execute(program):
 out=io.StringIO()
 with contextlib.redirect_stdout(out):exec(program,{})
 return json.loads(out.getvalue())
seal=execute(installation)
code=code.replace('sealed='+repr(placeholder),'sealed='+repr(seal))
class Docker:
 DEVNULL=-3;PIPE=-1
 def __init__(self):self.rows=copy.deepcopy(before['rows']);self.volumes=copy.deepcopy(before['volumes']);self.up=0;self.rm=[]
 def check_output(self,args,**kwargs):
  assert 0<kwargs['timeout']<=10
  if args[1:3]==['ps','-aq']:return '\n'.join(r['Id'] for r in self.rows)
  if args[1:3]==['volume','inspect']:return json.dumps(self.volumes)
  assert args[1]=='inspect'
  selected=[r for r in self.rows if r['Id'] in args[2:]]
  if case=='stop_replacement' and len(args)==3:selected[0]['Image']='sha256:'+'f'*64
  return json.dumps(selected)
 def run(self,args,**kwargs):
  if args[:3]==['docker','rm','-f']:
   self.rm.append(args[3]);self.rows=[r for r in self.rows if r['Id']!=args[3]]
   if case=='partial_stop':raise TimeoutError('Simulated removal response loss')
   return SimpleNamespace(returncode=0)
  assert args[:2]==['docker','compose'] and args[6:8]==['-f','-']
  if args[-3:]==['config','--format','json']:
   composed=json.loads(kwargs['input'])
   if case=='compose_drift':composed['services']['api']['environment']['DB_POOL_MAX']='99'
   return SimpleNamespace(returncode=0,stdout=json.dumps(composed))
  assert all(flag in args for flag in ('--no-deps','--no-build','--pull'))
  self.up+=1;self.rows=copy.deepcopy(after['rows']);self.volumes=copy.deepcopy(after['volumes'])
  return SimpleNamespace(returncode=1 if case=='partial_apply' else 0)
docker=Docker()
if case=='before_drift':docker.rows[0]['State']['StartedAt']='changed'
if case=='volume_drift':docker.volumes=[{'Name':'unowned'}]
if case=='seal_drift':(Path(owner)/'arm.compose.json').write_text('changed')
sys.modules['subprocess']=docker
failed=False
try:result=execute(code)
except (ValueError,OSError,RuntimeError,TimeoutError):failed=True
if case in {'common','start','restore','stop'}:
 assert not failed and result['applied'] is True
 assert len(result['after']['rows'])==len(after['rows'])
else:
 assert failed
 if case=='partial_apply':assert docker.up==1
 elif case=='partial_stop':assert len(docker.rm)==1
 else:assert docker.up==0 and docker.rm==[]
print(json.dumps({'case':case,'pass':True,'compose_applications':docker.up,'exact_removals':len(docker.rm)}))
'''
    for name,value in {'CASE':case,'BEFORE':before,'AFTER':after,'OWNER':owner,'INSTALL':install,'CODE':code,'PLACEHOLDER':placeholder}.items():
        script=script.replace(name,repr(value))
    result=subprocess.run(['docker','exec','-i',linux,'python','-'],input=script,text=True,capture_output=True,check=False,timeout=30)
    assert result.returncode==0,result.stderr[-1500:]
    assert json.loads(result.stdout)['pass'] is True


def test_real_compose_preserves_literal_environment_and_command():
    image=os.getenv('WORKER_CONFIG_TEST_IMAGE')
    if not image:pytest.skip('Set immutable local Linux Python image for owned Compose probe')
    assert re.fullmatch(r'sha256:[0-9a-f]{64}',image),'Immutable local image required'
    name='flash-literal-'+new_stage_output().name[-12:]
    literal='${UNSET}-$unset-$$-$'
    model={'name':name,'services':{'probe':{'image':image,'pull_policy':'never','network_mode':'none',
            'read_only':True,'user':'65534','cap_drop':['ALL'],'environment':{'SYNTHETIC_LITERAL':literal},
            'entrypoint':['python'],'command':['-c',"import json,os,time;print(json.dumps({'env':os.environ['SYNTHETIC_LITERAL'],'command':'$literal ${UNSET} $$'}),flush=True);time.sleep(30)"]}}}
    payload=json.dumps(execution.literal_model(model))
    base=['docker','compose','--project-name',name,'-f','-']
    attempted=False
    try:
        resolved=subprocess.run([*base,'config','--format','json'],input=payload,text=True,capture_output=True,check=True,timeout=20)
        execution.verify_composed(json.loads(resolved.stdout),model,{'probe':1})
        attempted=True
        subprocess.run([*base,'up','-d','--no-deps','--no-build','--pull','never','probe'],input=payload,text=True,capture_output=True,check=True,timeout=20)
        ids=subprocess.check_output(['docker','ps','-aq','--no-trunc','--filter','label=com.docker.compose.project='+name],text=True,timeout=10).split()
        assert len(ids)==1
        rows=json.loads(subprocess.check_output(['docker','inspect',*ids],text=True,timeout=10))
        assert rows[0]['Image']==image and rows[0]['Config']['Labels']['com.docker.compose.service']=='probe'
        content=subprocess.check_output(['docker','logs',ids[0]],text=True,timeout=10).strip()
        observed=json.loads(content)
        assert observed=={'env':literal,'command':'$literal ${UNSET} $$'}
    finally:
        if attempted:
            ids=subprocess.check_output(['docker','ps','-aq','--no-trunc','--filter','label=com.docker.compose.project='+name],text=True,timeout=10).split()
            if ids:
                rows=json.loads(subprocess.check_output(['docker','inspect',*ids],text=True,timeout=10))
                assert all(r['Image']==image and r['Config']['Labels'].get('com.docker.compose.project')==name
                           and r['Config']['Labels'].get('com.docker.compose.service')=='probe' for r in rows)
                subprocess.run(['docker','rm','-f',*ids],capture_output=True,check=True,timeout=20)

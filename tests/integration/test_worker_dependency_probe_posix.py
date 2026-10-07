"""Generated ADR0196 probe cleanup faults run on Linux with simulated Docker."""
import json
import subprocess
import sys
from pathlib import Path

import pytest
from test_worker_configuration_posix import linux  # noqa: F401 - owned fixture.

ROOT=Path(__file__).resolve().parents[2]
sys.path.insert(0,str(ROOT/'scripts'))
import worker_separation_readiness as readiness
from stage_status_refresh_images import new_stage_output

pytestmark=pytest.mark.integration


@pytest.mark.parametrize('case',['success','create_response_lost','start_timeout','failed_dependency','foreign_probe','preexisting','replacement','post_drift'])
def test_generated_probe_faults_on_real_linux_with_simulated_docker(linux,case):  # noqa: F811 - fixture injection.
    before={'rows':[],'volumes':[],'bind_sha256':{}}
    saved={'broker_volume':{'Name':'unused'},'bind_sha256':{}}
    image='sha256:'+'a'*64
    name=new_stage_output().name+'-dependency-secondary'
    code=readiness.probe_program(saved,before,{'synthetic':True},image,name,'b'*64,45)
    script=r'''
import contextlib,io,json,sys
from types import SimpleNamespace
case=CASE;code=CODE;name=NAME;image=IMAGE
labels={'org.flash-ticketing.dependency-scope':'b'*64,'org.flash-ticketing.dependency-context':CONTEXT_SHA,'org.flash-ticketing.dependency-owner':name}
def row():
 return {'Id':'c'*64,'Image':image,'Name':'/'+name,'State':{'Running':True,'StartedAt':'2026-10-07T00:00:00+00:00'},
  'Config':{'Env':[],'Labels':labels.copy(),'Entrypoint':['python'],'Cmd':['-'],'User':'65534:65534','WorkingDir':'','Healthcheck':{'Test':['NONE']}},
  'Mounts':[], 'HostConfig':{'NetworkMode':'bridge','ReadonlyRootfs':True,'Memory':268435456,'NanoCpus':500000000,'PidsLimit':64,
   'CapDrop':['ALL'],'SecurityOpt':['no-new-privileges'],'RestartPolicy':{'Name':'no','MaximumRetryCount':0},'Tmpfs':{'/tmp':'rw,noexec,nosuid,size=8m,mode=1777'},'PortBindings':{}}}
class Docker:
 DEVNULL=-3;PIPE=-1
 def __init__(self):self.rows=[row()] if case=='preexisting' else [];self.creates=0;self.removes=[]
 def check_output(self,args,**kwargs):
  if args[1:3]==['ps','-aq']:
   return '\n'.join(r['Id'] for r in self.rows if '--filter' not in args or r['Name']=='/'+name)
  assert args[1]=='inspect'
  return json.dumps([r for r in self.rows if r['Id'] in args[2:]])
 def run(self,args,**kwargs):
  if args[:2]==['docker','create']:
   self.creates+=1;self.rows=[row()]
   assert '--pull=never' in args and '--read-only' in args and '--no-healthcheck' in args
   assert not any('synthetic' in value or 'password' in value.lower() for value in args)
   if case=='create_response_lost':raise TimeoutError('Synthetic creation response loss')
   return SimpleNamespace(returncode=0,stdout='c'*64+'\n')
  if args[:3]==['docker','start','-ai']:
   if case=='foreign_probe':self.rows[0]['Config']['Labels']['org.flash-ticketing.dependency-scope']='f'*64
   if case=='replacement':self.rows[0]['Id']='d'*64
   if case=='start_timeout':raise TimeoutError('Synthetic probe timeout')
   return SimpleNamespace(returncode=1 if case=='failed_dependency' else 0,stdout=json.dumps({k:True for k in ('verified_rds_tls','redis_ready','pooler_ready','private_kafka_ready','all_four_apis_reachable')}))
  assert args[:3]==['docker','rm','-f']
  self.removes.append(args[3]);self.rows=[]
  if case=='post_drift':
   self.rows=[row()];self.rows[0]['Id']='d'*64;self.rows[0]['Name']='/unowned'
  return SimpleNamespace(returncode=0)
docker=Docker();sys.modules['subprocess']=docker
failed=False
try:
 out=io.StringIO()
 with contextlib.redirect_stdout(out):exec(code,{})
 result=json.loads(out.getvalue())
except (ValueError,RuntimeError,TimeoutError):failed=True
if case=='success':assert not failed and result['probe_removed'] and docker.removes==['c'*64]
else:
 assert failed
 if case in {'preexisting','foreign_probe','replacement'}:assert docker.removes==[] and docker.rows
 else:assert docker.removes==['c'*64]
 if case=='preexisting':assert docker.creates==0
print(json.dumps({'case':case,'pass':True}))
'''
    for key,value in {'CASE':case,'CODE':code,'NAME':name,'IMAGE':image,'CONTEXT_SHA':readiness.policy.digest({'synthetic':True})}.items():
        script=script.replace(key,repr(value))
    result=subprocess.run(['docker','exec','-i',linux,'python','-'],input=script,text=True,capture_output=True,timeout=30,check=False)
    assert result.returncode==0,result.stderr[-1600:]
    assert json.loads(result.stdout)['pass'] is True

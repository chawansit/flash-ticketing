"""ADR0198 exact POSIX artifact programs, qualified in an isolated cached Linux image."""
import json
import os
import re
import subprocess
import sys
from pathlib import Path

import pytest

ROOT=Path(__file__).resolve().parents[2]
sys.path.insert(0,str(ROOT/'scripts'))
import stage_status_refresh_images as staging
import worker_separation_artifacts as artifacts


@pytest.mark.parametrize('change',['boolean','inode','mode','uid','keys','path'])
def test_seal_rejects_missing_or_weak_directory_identity(change):
    value={'fresh_arm_directory':True,'root':{'device':1,'inode':1,'uid':0,'mode':0o700},
           'directory':{'device':1,'inode':2,'uid':0,'mode':0o700}}
    path='/tmp/owner/control'
    if change=='boolean':value['fresh_arm_directory']=1
    elif change=='keys':value['directory']['extra']=True
    elif change=='path':path='/tmp/owner/../control'
    else:value['directory'][change]={'inode':False,'mode':0o755,'uid':1}[change]
    with pytest.raises(ValueError):artifacts.validate_seal(value,path)


@pytest.mark.parametrize('files',[[],['../secret'],['.'],['x/x'],['x','x'],['x;rm'],['x']*251])
def test_cleanup_requires_explicit_bounded_names(files):
    seal={'fresh_arm_directory':True,'root':{'device':1,'inode':1,'uid':0,'mode':0o700},
          'directory':{'device':1,'inode':2,'uid':0,'mode':0o700}}
    with pytest.raises(ValueError):artifacts.cleanup_program('/tmp/owner/control',seal,files)


def test_linux_artifact_identity_and_nonrecursive_cleanup():
    image=os.environ.get('ADR0198_LOCAL_LINUX_IMAGE')
    if not image:pytest.skip('Cached immutable local Linux image required')
    assert re.fullmatch(r'sha256:[0-9a-f]{64}',image)
    owner=staging.new_stage_output().name
    source=(ROOT/'scripts/worker_separation_artifacts.py').read_text()
    program=r'''import contextlib,io,json,os,sys
from pathlib import Path
ns={};exec(SOURCE,ns)
base=Path('/tmp/qualification');base.mkdir(mode=0o700)
def execute(code):
 output=io.StringIO()
 with contextlib.redirect_stdout(output):exec(code,{})
 return json.loads(output.getvalue())
checked=[]
for case in ('success','unexpected','symlink','hardlink','mode','oversize','arm_replaced','root_replaced','ancestor_link','stale_seal'):
 p=base/case;p.mkdir(mode=0o700);root=p/'owner';directory=root/'control'
 seal=execute(ns['directories_program'](str(root),'control'))
 file=directory/'known.json';file.write_text('private synthetic data');file.chmod(0o600)
 if case=='unexpected':(directory/'foreign.json').write_text('foreign')
 elif case=='symlink':file.unlink();file.symlink_to(p/'outside')
 elif case=='hardlink':os.link(file,p/'outside')
 elif case=='mode':file.chmod(0o644)
 elif case=='oversize':
  with file.open('r+b') as f:f.truncate(16777217)
 elif case=='arm_replaced':directory.rename(root/'old-control');directory.mkdir(mode=0o700)
 elif case=='root_replaced':root.rename(p/'old-owner');root.mkdir(mode=0o700);directory.mkdir(mode=0o700)
 elif case=='ancestor_link':root.rename(p/'old-owner');root.symlink_to(p/'old-owner',target_is_directory=True)
 elif case=='stale_seal':seal['directory']['inode']+=100
 if case=='success':
  assert execute(ns['cleanup_program'](str(directory),seal,{'known.json'},remove=False))=={'owned_artifacts_verified':True}
  assert file.exists()
  assert execute(ns['cleanup_program'](str(directory),seal,{'known.json'}))=={'owned_artifacts_removed':True}
  assert not root.exists()
 else:
  try:execute(ns['cleanup_program'](str(directory),seal,{'known.json'}))
  except (OSError,ValueError):pass
  else:raise AssertionError('Unsafe case accepted: '+case)
  assert root.exists(),'Unsafe cleanup deleted root: '+case
  if case not in ('arm_replaced','root_replaced'):assert file.exists() or file.is_symlink()
 checked.append(case)
print(json.dumps({'checked':checked,'cloud_calls':0,'customer_dispatches':0}))
'''.replace('SOURCE',repr(source))
    command=['docker','run','--pull=never','--name',owner,'--label','ticketing.local-artifacts-owner='+owner,
        '--rm','-i','--network','none','--read-only','--user','0','--tmpfs','/tmp:rw,size=32m',
        '--memory','256m','--cpus','0.5',image,'python','-c','import sys;exec(sys.stdin.read())']
    try:
        result=subprocess.run(command,input=program,text=True,capture_output=True,timeout=30,check=False)
        assert result.returncode==0,result.stderr[-2000:]
        value=json.loads(result.stdout.splitlines()[-1])
        assert len(value['checked'])==10 and value['cloud_calls']==value['customer_dispatches']==0
    finally:
        observed=subprocess.run(['docker','inspect',owner],text=True,capture_output=True,timeout=10,check=False)
        if observed.returncode==0:
            row=json.loads(observed.stdout)[0]
            assert row['Image']==image and row['Config']['Labels'].get('ticketing.local-artifacts-owner')==owner
            subprocess.run(['docker','rm','-f',row['Id']],capture_output=True,check=True,timeout=15)

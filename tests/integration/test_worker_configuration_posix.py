"""Opt-in generated-program qualification in an owned, network-isolated Linux container."""
import importlib.util
import json
import os
import re
import subprocess
import sys
from pathlib import Path
from uuid import uuid4

import pytest

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT / 'scripts'))
import worker_separation_configuration as config

pytestmark = pytest.mark.integration


@pytest.fixture(scope='module')
def linux():
    image = os.getenv('WORKER_CONFIG_TEST_IMAGE')
    if not image:
        pytest.skip('Set WORKER_CONFIG_TEST_IMAGE to an available immutable Linux Python image')
    if not re.fullmatch(r'sha256:[0-9a-f]{64}', image):
        pytest.fail('Immutable local image required')
    name = 'flash-worker-configuration-test-' + uuid4().hex[:12]
    attempted = False
    try:
        attempted = True
        subprocess.run(['docker', 'run', '-d', '--name', name, '--network', 'none', '--read-only',
                        '--tmpfs', '/qualification:rw,noexec,nosuid,size=16m', '--user', '0', '--pull', 'never',
                        '--entrypoint', 'python', image, '-c', 'import time;time.sleep(300)'],
                       check=True, capture_output=True, timeout=30)
        yield name
    finally:
        if attempted:
            subprocess.run(['docker', 'rm', '-f', name], check=True, capture_output=True, timeout=30)


def value():
    spec = importlib.util.spec_from_file_location('configuration_fixtures', ROOT / 'tests/unit/test_worker_separation_configuration.py')
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module.prepared()


@pytest.mark.parametrize('case', ['success', 'existing', 'extra', 'content', 'file_replacement', 'directory_replacement',
                                 'file_mode', 'directory_mode', 'symlink', 'hardlink', 'uid', 'partial_write',
                                 'parent_symlink', 'parent_writable'])
def test_real_posix_file_identity_failure_and_cleanup(linux, case):
    from stage_status_refresh_images import new_stage_output
    owner = '/qualification/repository/tmp/' + new_stage_output().name
    prepared = value()
    installation = config.install_program(owner, prepared)
    # Each case has a fresh owner. Tmpfs contents disappear with this exact owned container.
    script = r'''
import contextlib,io,json,os
from pathlib import Path
owner=Path(OWNER)
case=CASE
owner.parent.mkdir(parents=True,exist_ok=True)
def execute(code):
 output=io.StringIO()
 with contextlib.redirect_stdout(output):exec(code,{})
 return json.loads(output.getvalue())
def rejected(code):
 try:execute(code)
 except (OSError,ValueError):return
 raise AssertionError('Unsafe action unexpectedly succeeded')
'''.replace('OWNER', repr(owner)).replace('CASE', repr(case))
    script += '\ninstallation=' + repr(installation) + '\nhelpers=' + repr(config.POSIX)
    script += r'''
if case=='existing':
 owner.mkdir();(owner/'unowned').write_text('keep');rejected(installation)
 assert (owner/'unowned').read_text()=='keep'
elif case=='parent_symlink':
 linked=owner.parent/'link';linked.mkdir(exist_ok=True)
 altered=installation.replace(str(owner),str(linked/owner.name))
 linked.rmdir();linked.symlink_to(owner.parent,target_is_directory=True)
 try:rejected(altered)
 finally:linked.unlink()
 assert not owner.exists()
elif case=='parent_writable':
 owner.parent.chmod(0o777)
 try:rejected(installation)
 finally:owner.parent.chmod(0o755)
 assert not owner.exists()
elif case=='partial_write':
 real_write=os.write;writes=[]
 def failing_write(fd,data):
  if writes:raise OSError('Simulated disk full')
  writes.append(True);return real_write(fd,data)
 os.write=failing_write
 try:rejected(installation)
 finally:os.write=real_write
 assert owner.exists() and (owner/'arm.compose.json').exists()
else:
 seal=execute(installation)
 base=helpers+'\nsealed='+repr(seal)
 verify=base+"\nparent,name=open_parent(sealed['owner'])\ntry:\n assert observe(parent,name,sealed['manifest'])=={'directory':sealed['directory'],'files':sealed['files']}\n print(json.dumps({'verified':True}))\nfinally:os.close(parent)\n"
 # CLEANUP contains the actual generated production cleanup body, supplied below.
 cleanup=CLEANUP.replace('SEAL_VALUE',repr(seal))
 if case=='success':
  assert execute(verify)=={'configuration_matches':True}
  assert execute(cleanup)=={'owned_configuration_removed':True}
  assert not owner.exists()
 else:
  file=owner/'arm.compose.json'
  if case=='extra':(owner/'unowned').write_text('keep')
  elif case=='content':file.write_text('changed')
  elif case=='file_replacement':
   replacement=owner/'replacement';replacement.write_bytes(file.read_bytes());replacement.chmod(0o600)
   file.unlink();replacement.rename(file)
  elif case=='directory_replacement':
   old=owner.with_name(owner.name+'-saved');owner.rename(old);owner.mkdir(mode=0o700)
   for filename in seal['files']:
    target=owner/filename;target.write_bytes((old/filename).read_bytes());target.chmod(0o600)
  elif case=='file_mode':file.chmod(0o644)
  elif case=='directory_mode':owner.chmod(0o755)
  elif case=='symlink':
   target=owner.with_name(owner.name+'-unowned');target.write_text('keep');file.unlink();file.symlink_to(target)
  elif case=='hardlink':
   target=owner.with_name(owner.name+'-unowned');target.write_bytes(file.read_bytes());target.chmod(0o600)
   file.unlink();os.link(target,file)
  elif case=='uid':os.chown(file,1234,1234)
  rejected(cleanup)
  assert owner.exists() and file.exists()
  if case in {'symlink','hardlink'}:assert target.exists()
print(json.dumps({'case':case,'pass':True}))
'''
    # Use the real cleanup generator with a structurally valid placeholder seal.
    manifest = prepared['manifest']
    placeholder = {'owner': owner, 'manifest': manifest,
                   'directory': {'device': 1, 'inode': 1, 'uid': 0, 'mode': 0o700},
                   'files': {name: {'device': 1, 'inode': 2, 'uid': 0, 'mode': 0o600, **record}
                             for name, record in manifest['files'].items()}}
    generated = config.sealed_program(placeholder, cleanup=True)
    template = generated.replace('sealed=' + repr(placeholder), 'sealed=SEAL_VALUE')
    verify_template = config.sealed_program(placeholder).replace('sealed=' + repr(placeholder), 'sealed=SEAL_VALUE')
    script = script.replace('CLEANUP', repr(template))
    # Exercise the production verifier as well as its production cleanup program.
    script = script.replace("if case=='success':", 'verify=' + repr(verify_template) + ".replace('SEAL_VALUE',repr(seal))\n if case=='success':")
    result = subprocess.run(['docker', 'exec', '-i', linux, 'python', '-'], input=script, text=True,
                            capture_output=True, timeout=30, check=False)
    # Do not emit source/payloads on failure; stderr is fixed exception diagnostics only.
    assert result.returncode == 0, result.stderr[-2000:]
    assert json.loads(result.stdout) == {'case': case, 'pass': True}

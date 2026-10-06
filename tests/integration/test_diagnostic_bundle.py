"""ADR0180 native POSIX credential ownership/privacy checks; no network."""
import json
import os
import subprocess
from pathlib import Path
from uuid import uuid4

import pytest

pytestmark = pytest.mark.integration
IMAGE = "sha256:7136a0b6386c6af001b765d4b6aa0915be1c04a2e13c361a0950260956adee1e"
PROGRAM = r"""
import tempfile
from pathlib import Path
with tempfile.TemporaryDirectory(prefix='adr0180-bundle-') as temporary:
 directory=Path(temporary);os.chmod(directory,0o700)
 path=directory/'diagnostic.private.json';ca=directory/'diagnostic-ca.pem'
 value={'host':'10.0.0.1','port':5432,'dbname':'ticketing','user':'monitor','password':'isolated-fixture','sslmode':'verify-full','sslrootcert':str(ca)}
 raw=json.dumps(value).encode();path.write_bytes(raw);os.chmod(path,0o600)
 ca.write_bytes(b'-----BEGIN CERTIFICATE-----\nfixture\n-----END CERTIFICATE-----');os.chmod(ca,0o600)
 kwargs={'owned_directory':directory,'expected_sha256':hashlib.sha256(raw).hexdigest(),'expected_database':'ticketing','expected_identity':identity('monitor','ticketing'),'expected_endpoint':('10.0.0.1',5432),'expected_ca_sha256':hashlib.sha256(ca.read_bytes()).hexdigest()}
 spec=load_bundle(path,**kwargs);assert 'isolated-fixture' not in repr(spec)
 cases=0
 def reject(target=path,**overrides):
  global cases
  try:load_bundle(target,**dict(kwargs,**overrides))
  except (ValueError,OSError):cases+=1
  else:raise AssertionError('Unsafe bundle accepted')
 reject(expected_sha256='0'*64)
 reject(expected_database='other')
 reject(expected_identity='0'*64)
 reject(expected_endpoint=('10.0.0.2',5432))
 link=directory/'alias';link.symlink_to(path);reject(link);link.unlink()
 os.chmod(path,0o644);reject();os.chmod(path,0o600)
 os.chmod(ca,0o644);reject();os.chmod(ca,0o600)
 os.chmod(directory,0o755);reject();os.chmod(directory,0o700)
 os.chown(path,1,1);reject();os.chown(path,os.geteuid(),os.getegid())
 link.hardlink_to(path);reject();link.unlink()
 ca.unlink();ca.symlink_to(path);reject();ca.unlink();ca.write_bytes(b'BEGIN CERTIFICATE');os.chmod(ca,0o600)
 path.write_bytes(b'x'*16385);reject();path.write_bytes(raw)
 changed=dict(value,options='-c default_transaction_read_only=off');modified=json.dumps(changed).encode();path.write_bytes(modified)
 reject(expected_sha256=hashlib.sha256(modified).hexdigest());path.write_bytes(raw)
 ca.write_bytes(b'not a certificate');reject()
 assert cases==14
 print(json.dumps({'unsafe_cases_rejected':cases,'secret_repr_protected':True}))
"""


def test_native_owned_bundle_rejects_permissions_symlinks_rebinding_and_oversize():
    image = os.environ.get("TEST_DIAGNOSTIC_BUNDLE_IMAGE")
    if not image:
        pytest.skip("Explicit cached isolated diagnostic bundle image required")
    assert image == IMAGE
    owner = "adr0180-local-bundle-" + uuid4().hex[:12]
    source = (Path(__file__).resolve().parents[2] / "scripts/diagnostic_connection.py").read_text(encoding="utf-8")
    container = None
    try:
        container = subprocess.check_output(['docker','create','-i','--network','none','--user','0','--name',owner,
            '--label','ticketing.diagnostics.owner='+owner,'--entrypoint','python',image,'-'],text=True,timeout=20).strip()
        result = subprocess.run(['docker','start','-ai',container],input=source+'\n'+PROGRAM,
                                text=True,capture_output=True,timeout=30,check=False)
        assert result.returncode == 0, result.stderr
        assert json.loads(result.stdout)['unsafe_cases_rejected'] == 14
    finally:
        if container:
            row=json.loads(subprocess.check_output(['docker','inspect',container],text=True,timeout=20))[0]
            assert row['Image']==image and row['Config']['Labels']['ticketing.diagnostics.owner']==owner
            subprocess.run(['docker','rm','-f','-v',container],check=True,capture_output=True,timeout=20)

"""ADR0181 real POSIX partial credential cleanup; explicitly cached native image."""
import json
import os
import subprocess
import sys
from pathlib import Path
from uuid import uuid4

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[2] / "scripts"))
from diagnostic_runner_connection import cleanup_program

pytestmark = pytest.mark.integration
IMAGE = "sha256:7136a0b6386c6af001b765d4b6aa0915be1c04a2e13c361a0950260956adee1e"


def test_native_partial_cleanup_and_unsafe_ownership_rejection():
    if not os.environ.get("TEST_DIAGNOSTIC_BUNDLE_IMAGE"):
        pytest.skip("Explicit isolated cached diagnostic image required")
    assert os.environ["TEST_DIAGNOSTIC_BUNDLE_IMAGE"] == IMAGE
    owner = "adr0181-cleanup-" + uuid4().hex[:12]
    container = None
    program = ("import json,os,tempfile;from pathlib import Path\n"
               "with tempfile.TemporaryDirectory() as root:\n"
               " p=Path(root)/'owned';p.mkdir(mode=0o700)\n"
               " code=" + repr(cleanup_program("/tmp/placeholder")) + "\n"
               " code=code.replace(" + repr(repr("/tmp/placeholder")) + ",repr(str(p)))\n"
               " for names in ((),('diagnostic-ca.pem',),('diagnostic.private.json',),('diagnostic-ca.pem','diagnostic.private.json')):\n"
               "  for name in names:(p/name).write_text('partial-secret');os.chmod(p/name,0o600)\n"
               "  exec(code);assert not list(p.iterdir())\n"
               " keep=p/'unrelated';keep.write_text('preserved')\n"
               " exec(code);assert keep.exists()\n"
               " bad=p/'diagnostic.private.json';bad.symlink_to(keep)\n"
               " try:exec(code)\n"
               " except ValueError:pass\n"
               " else:raise AssertionError('Unsafe link accepted')\n"
               " assert bad.is_symlink() and keep.exists();bad.unlink()\n"
               " bad.write_text('secret');os.chmod(bad,0o644)\n"
               " try:exec(code)\n"
               " except ValueError:pass\n"
               " else:raise AssertionError('Unsafe mode accepted')\n"
               " os.chmod(bad,0o600);os.chown(bad,1,1)\n"
               " try:exec(code)\n"
               " except ValueError:pass\n"
               " else:raise AssertionError('Unsafe owner accepted')\n"
               " os.chown(bad,os.geteuid(),os.getegid());exec(code)\n"
               "print(json.dumps({'partial_states':4,'unsafe_states_rejected':3,'unrelated_preserved':True}))")
    try:
        container = subprocess.check_output(["docker", "create", "-i", "--network", "none", "--user", "0",
            "--name", owner, "--label", "ticketing.diagnostics.owner=" + owner,
            "--entrypoint", "python", IMAGE, "-"], text=True, timeout=20).strip()
        result = subprocess.run(["docker", "start", "-ai", container], input=program,
                                text=True, capture_output=True, timeout=30, check=False)
        assert result.returncode == 0, result.stderr
        proof = json.loads(result.stdout.splitlines()[-1])
        assert proof == {"partial_states": 4, "unsafe_states_rejected": 3, "unrelated_preserved": True}
    finally:
        if container:
            row = json.loads(subprocess.check_output(["docker", "inspect", container], text=True, timeout=20))[0]
            assert row["Image"] == IMAGE and row["Config"]["Labels"]["ticketing.diagnostics.owner"] == owner
            subprocess.run(["docker", "rm", "-f", "-v", container], capture_output=True, check=True, timeout=20)

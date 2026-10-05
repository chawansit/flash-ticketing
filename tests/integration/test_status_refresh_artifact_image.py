"""Explicit offline Docker fixture qualification; never a cloud deployment receipt."""

import json
import os
import sys
from pathlib import Path
from uuid import uuid4

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[2] / "scripts"))
import prepare_status_refresh_artifacts as artifacts


@pytest.mark.integration
def test_offline_derivation_verifies_both_source_roots_and_inherited_runtime():
    local_parent = os.environ.get("ADR0152_SMOKE_PARENT")
    if not local_parent:
        pytest.skip("Explicit local fixture parent image required; no automatic image pull")
    inspected = artifacts.inspect_image(local_parent)
    output, data, _receipt = artifacts.prepare(artifacts.ROOT / "tmp" / ("adr0152-smoke-" + uuid4().hex[:12]))
    fixture = output / "fixture-context"
    fixture.mkdir()
    for path in [*data["parent_runtime_source_sha256"], *data["dependency_input_sha256"]]:
        target = fixture / path
        target.parent.mkdir(parents=True, exist_ok=True)
        target.write_bytes(artifacts.command(["git", "show", artifacts.REVISION + ":" + path]))
    # This constructs a deliberately synthetic frozen-source parent from a local test image.
    # It is never passed to validate_parents or recorded as the original cloud parent.
    fixture_code = r"""import base64,csv,hashlib,importlib.metadata,shutil,sysconfig
from pathlib import Path
payload=Path('/tmp/adr0152-fixture')
dist=importlib.metadata.distribution('flash-ticketing')
installed=Path(dist.locate_file('ticketing'))
app=Path('/app/src/ticketing')
if installed.resolve()!=Path(sysconfig.get_path('purelib')).resolve()/'ticketing' or app.resolve()!=app or installed.is_symlink():raise ValueError('Fixture roots differ')
for root in (app,installed):
 shutil.rmtree(root)
 shutil.copytree(payload/'src/ticketing',root)
for name in ('pyproject.toml','requirements.lock'):shutil.copyfile(payload/name,Path('/app')/name)
record=Path(dist.locate_file(next(f for f in dist.files if f.as_posix().endswith('.dist-info/RECORD'))))
rows=[r for r in csv.reader(record.read_text().splitlines()) if not r[0].startswith('ticketing/')]
for p in installed.rglob('*.py'):
 raw=p.read_bytes();h=base64.urlsafe_b64encode(hashlib.sha256(raw).digest()).rstrip(b'=').decode()
 rows.append(['ticketing/'+p.relative_to(installed).as_posix(),'sha256='+h,str(len(raw))])
with record.open('w',newline='') as f:csv.writer(f).writerows(sorted(rows))
"""
    (fixture / "fixture.py").write_text(fixture_code)
    tag = "flash-ticketing-adr0152-fixture-base:" + local_parent.removeprefix("sha256:")
    artifacts.command(["docker", "tag", local_parent, tag])
    user = inspected["Config"].get("User", "")
    restore = "USER " + user + "\n" if user else ""
    (fixture / "Dockerfile").write_text(
        "FROM "
        + tag
        + "\nUSER root\nCOPY . /tmp/adr0152-fixture/\nRUN python /tmp/adr0152-fixture/fixture.py && rm -rf /tmp/adr0152-fixture\n"
        + restore
    )
    image_file = output / "fixture-image-id.txt"
    artifacts.command(
        ["docker", "build", "--network=none", "--pull=false", "--iidfile", str(image_file), str(fixture)],
        log=output / "fixture-build.log",
        timeout=180,
    )
    fixture_id = image_file.read_text().strip()
    folder = output / "candidate-build"
    folder.mkdir()
    image_id, proof = artifacts.build_one(folder, data, fixture_id, timeout=180)
    assert proof["app_and_installed_modules_verified"] == 19
    assert proof["source_identity"]["import_code_matches_source"] is True
    parent, candidate = artifacts.inspect_image(fixture_id), artifacts.inspect_image(image_id)
    source_manifest = artifacts.digest(
        {"base_revision": artifacts.REVISION, "runtime_source_sha256": data["runtime_source_sha256"]}
    )
    artifacts.verify_derived(parent, candidate, source_manifest)
    assert candidate["Config"]["User"] == parent["Config"]["User"]
    record_program = r"""import base64,csv,hashlib,importlib.metadata,json
from pathlib import Path
dist=importlib.metadata.distribution('flash-ticketing')
record=Path(dist.locate_file(next(f for f in dist.files if f.as_posix().endswith('.dist-info/RECORD'))))
rows=[r for r in csv.reader(record.read_text().splitlines()) if r[0].startswith('ticketing/')]
if len(rows)!=19:raise ValueError('Installed record source count differs')
for relative,expected,size in rows:
 raw=Path(dist.locate_file(relative)).read_bytes();digest='sha256='+base64.urlsafe_b64encode(hashlib.sha256(raw).digest()).rstrip(b'=').decode()
 if digest!=expected or str(len(raw))!=size:raise ValueError('Installed RECORD differs')
print(json.dumps({'installed_record_verified':len(rows)}))
"""
    record_proof = json.loads(
        artifacts.command(
            [
                "docker",
                "run",
                "--rm",
                "--network=none",
                "--read-only",
                "--entrypoint",
                "python",
                image_id,
                "-c",
                record_program,
            ],
            log=output / "record-proof.json",
        )
    )
    assert record_proof["installed_record_verified"] == 19
    with pytest.raises(ValueError, match="frozen cloud API"):
        artifacts.validate_parents(dict.fromkeys(artifacts.ROLES, fixture_id))
    assert not (output / "artifact-receipt.json").exists()
    summary = {
        "kind": "synthetic_offline_image_qualification",
        "pass": True,
        "fixture_parent_image": fixture_id,
        "candidate_image": image_id,
        "runtime_modules_verified": 19,
        "installed_record_verified": 19,
        "inherited_configuration_verified": True,
        "cloud_deployment_receipt_created": False,
        "cloud_calls": 0,
        "evidence_directory": output.relative_to(artifacts.ROOT).as_posix(),
    }
    (output / "smoke-summary.json").write_text(json.dumps(summary, indent=2) + "\n")
    print(json.dumps(summary))

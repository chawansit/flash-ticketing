"""ADR0198 sealed observer source closure, including a real isolated Linux import check."""
import json
import os
import re
import subprocess
import sys
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT / 'scripts'))
import stage_status_refresh_images as staging
import worker_separation_observer_bundle as bundle
from worker_separation_inventory import digest


def test_closure_includes_conditional_imports_and_cycles_but_no_vendor_source(tmp_path):
    (tmp_path / bundle.ENTRY).write_text('import one\nif False:\n from two import x\nimport psycopg\n')
    (tmp_path / 'one.py').write_text('import two\n')
    (tmp_path / 'two.py').write_text('import one\n')
    result = bundle.prepare(tmp_path)
    assert set(result['files']) == {bundle.ENTRY, 'one.py', 'two.py'}
    assert bundle.validate(result, tmp_path) == result['manifest_sha256']
    assert bundle.prepare(tmp_path) == result
    result['files'].pop('two.py');result['manifest'].pop('two.py')
    result['manifest_sha256'] = digest(result['manifest'])
    with pytest.raises(ValueError, match='dependency omitted'):bundle.validate(result, tmp_path)


@pytest.mark.parametrize('change', ['bytes', 'hash', 'path', 'source', 'missing', 'syntax'])
def test_sealed_transfer_rejects_changed_sources_or_unsafe_names(change):
    value = bundle.prepare()
    if change == 'bytes':value['manifest'][bundle.ENTRY]['bytes'] += 1
    elif change == 'hash':value['manifest_sha256'] = 'a' * 64
    elif change == 'path':value['files']['../outside.py'] = 'pass\n'
    elif change == 'source':value['files'][bundle.ENTRY] += '\n# changed\n'
    elif change == 'missing':value['files'].pop('diagnostic_connection.py')
    else:value['files'][bundle.ENTRY] = 'def (\n'
    with pytest.raises((ValueError, SyntaxError)):bundle.verification_program(value, '/tmp/owned/arm')


def test_bound_real_closure_includes_qualified_endpoints_diagnostics_and_source_checks():
    value = bundle.prepare()
    assert {'observe_worker_pipeline.py', 'worker_separation_pipeline.py', 'worker_separation_inventory.py',
            'diagnostic_connection.py', 'database_wait_evidence.py', 'runtime_source_identity.py'} <= set(value['files'])
    assert bundle.validate(value) == value['manifest_sha256']
    assert len(value['files']) <= bundle.MAX_FILES
    assert 'password' not in value['manifest']


def test_isolated_linux_bundle_import_and_owner_only_diagnostic_binding():
    image = os.getenv('ADR0198_LOCAL_LINUX_IMAGE')
    if not image:pytest.skip('Explicit local immutable Linux image required')
    assert re.fullmatch(r'sha256:[0-9a-f]{64}',image), 'Exact cached immutable Linux image required'
    value = bundle.prepare()
    # Fixture is synthetic. No ECS, RDS, DCS or load-generator endpoints are contacted.
    spec = __import__('importlib.util', fromlist=['']).spec_from_file_location('local_inventory_fixture', ROOT / 'tests/unit/test_worker_separation_observers.py')
    fixture = __import__('importlib.util', fromlist=['']).module_from_spec(spec);spec.loader.exec_module(fixture)
    data = fixture.inventory()
    program = '''import hashlib,io,json,os,sys
from pathlib import Path
from datetime import datetime
p=Path('/tmp/observer');p.mkdir(mode=0o700)
for name,source in FILES.items():
 path=p/name;path.write_text(source);path.chmod(0o600)
VERIFY
sys.path.insert(0,str(p))
import observe_worker_pipeline as observer
import diagnostic_connection as diagnostic
inventory=INVENTORY
certificate=b'-----BEGIN CERTIFICATE-----\\nsynthetic local trust binding\\n-----END CERTIFICATE-----\\n'
ca=p/'diagnostic-ca.pem';ca.write_bytes(certificate);ca.chmod(0o600)
parameters={'host':'10.0.0.4','port':5432,'dbname':'ticketing','user':'observer','password':'synthetic',
 'sslmode':'verify-full','sslrootcert':str(ca)}
raw=json.dumps(parameters,sort_keys=True).encode()
private=p/'diagnostic.private.json';private.write_bytes(raw);private.chmod(0o600)
identity=diagnostic.identity('observer','ticketing')
inventory['diagnostic_connection_binding']={'decision':'ADR0180','database':'ticketing','identity_sha256':identity,
 'endpoint':['10.0.0.4',5432],'bundle_sha256':hashlib.sha256(raw).hexdigest(),'ca_sha256':hashlib.sha256(certificate).hexdigest()}
from worker_separation_inventory import digest
sha=digest(inventory)
load=lambda:observer.diagnostic_spec(inventory,inventory_path=p/'inventory.private.json',bundle_path=private,
 approved_sha256=sha,source_dsn='dbname=ticketing')
spec=load()
module=observer.configure(inventory,Path('/app/scripts/observe_paid_pipeline.py'),approved_sha256=sha,
 diagnostic=spec,source_dsn='dbname=ticketing',now=datetime.fromisoformat(inventory['captured_at']),
 fetch=lambda *a,**k:io.BytesIO(b'process_start_time_seconds 1000\\n'))
assert len(module.api_replicas())==4
assert module.worker_counters('confirmation','confirm_one','http://confirmation:9101/metrics')['confirmation_replicas']==1
rejected=[]
for case in ('bundle_mode','ca_mode','bundle_bytes','ca_bytes','hardlink'):
 private.write_bytes(raw);private.chmod(0o600);ca.write_bytes(certificate);ca.chmod(0o600)
 if case=='bundle_mode':private.chmod(0o644)
 elif case=='ca_mode':ca.chmod(0o644)
 elif case=='bundle_bytes':private.write_bytes(raw+b' ')
 elif case=='ca_bytes':ca.write_bytes(certificate+b' ')
 else:os.link(private,p/'private-alias')
 try:load()
 except ValueError:rejected.append(case)
 else:raise AssertionError('Invalid protected binding accepted')
 if case=='hardlink':(p/'private-alias').unlink()
print(json.dumps({'linux_import':True,'frozen_observer_verified':True,'confirmation_observed':True,
 'trust_file_rejections':rejected,'cloud_calls':0,'customer_dispatches':0}))
'''.replace('FILES', repr(value['files'])).replace('VERIFY', bundle.verification_program(value, '/tmp/observer')).replace('INVENTORY', repr(data))
    name = staging.new_stage_output().name
    args = ['docker', 'run', '--pull=never', '--name', name, '--label', 'ticketing.local-observer-owner=' + name,
            '--rm', '-i', '--network', 'none', '--read-only', '--tmpfs', '/tmp:rw,size=32m',
            '--memory', '256m', '--cpus', '0.5', image, 'python', '-c', 'import sys;exec(sys.stdin.read())']
    try:
        result = subprocess.run(args, input=program, text=True, capture_output=True, timeout=60, check=False)
        assert result.returncode == 0, result.stderr[-2000:]
        proof = json.loads(result.stdout.splitlines()[-1])
        assert proof['linux_import'] and proof['frozen_observer_verified'] and proof['confirmation_observed']
        assert len(proof['trust_file_rejections']) == 5 and proof['cloud_calls'] == proof['customer_dispatches'] == 0
    finally:
        found = subprocess.run(['docker', 'inspect', name], text=True, capture_output=True, timeout=10, check=False)
        if found.returncode == 0:
            observed = json.loads(found.stdout)[0]
            assert observed['Image'] == image and observed['Config']['Labels']['ticketing.local-observer-owner'] == name
            subprocess.run(['docker', 'rm', '-f', observed['Id']], check=True, capture_output=True, timeout=15)

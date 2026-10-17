"""ADR0346 simulator transaction local image preparation; no registry/deployment/load calls."""
import argparse
import ast
import json
import subprocess
from pathlib import Path
from uuid import uuid4

import cce_fulfillment_batch_profile as measured
import prepare_fulfillment_batch_image as builder
import prepare_simulator_roundtrip_image as simulator

ROOT = Path(__file__).resolve().parents[1]
MODULE = 'src/ticketing/workers.py'
CALLBACK = 'src/ticketing/infrastructure/reservations.py'
API = 'src/ticketing/api.py'
CONFIG = 'src/ticketing/config.py'
MODULES = {MODULE, CALLBACK, API, CONFIG}
REVIEWED_CALLBACK_SHA256 = '99a31778374aa0324c70b6519c865e7c6300f9a6413edbfbf218af85dba8e0d1'


def callback_parts(raw):
    nodes = [n for n in ast.walk(ast.parse(raw)) if isinstance(n, ast.FunctionDef) and n.name == 'apply_callback']
    if len(nodes) != 1:
        raise ValueError('Exactly one callback method required')
    n = nodes[0]
    lines = raw.splitlines(keepends=True)
    return b''.join(lines[:n.lineno-1]), b''.join(lines[n.lineno-1:n.end_lineno]), b''.join(lines[n.end_lineno:])


def replace_once(raw, old, new):
    if raw.count(old) != 1:
        raise ValueError('Exact immutable-parent callback insertion point required')
    return raw.replace(old, new)


def overlay(files):
    # Reuse the verified ADR0344 worker-only overlay; never copy the whole checkout.
    result = simulator.overlay(files)
    old = b'    payment_db.payment_write_pipeline = settings.payment_write_pipeline\n'
    result[API] = replace_once(files[API], old, old + b'    payment_db.payment_callback_write_pipeline = settings.payment_callback_write_pipeline\n')
    old = b'    payment_write_pipeline: bool ='
    new = b'    payment_callback_write_pipeline: bool = os.getenv("PAYMENT_CALLBACK_WRITE_PIPELINE", "0") == "1"\n' + old
    config = replace_once(files[CONFIG], old, new)
    old = b'    def validate(self):\n'
    new = old + b'        if self.payment_callback_write_pipeline and self.payment_confirmation_async:\n            raise RuntimeError("PAYMENT_CALLBACK_WRITE_PIPELINE requires synchronous confirmation")\n'
    result[CONFIG] = replace_once(config, old, new)
    if result[CONFIG] != (ROOT/CONFIG).read_bytes().replace(b'\r\n', b'\n'):
        raise ValueError('Checkout configuration differs beyond reviewed callback flag')
    before, _, after = callback_parts(files[CALLBACK])
    _, callback, _ = callback_parts((ROOT/CALLBACK).read_bytes().replace(b'\r\n', b'\n'))
    if builder.sha(callback) != REVIEWED_CALLBACK_SHA256:
        raise ValueError('Unreviewed callback method')
    result[CALLBACK] = before + callback + after
    if {name for name in result if result[name] != files[name]} != MODULES:
        raise ValueError('Exactly four reviewed confirmation modules must differ')
    return result


def prepare(output):
    previous = builder.parent_profile, builder.overlay
    try:
        builder.parent_profile, builder.overlay = measured, overlay
        manifest = builder.prepare(output)
    finally:
        builder.parent_profile, builder.overlay = previous
    manifest.update(decision='ADR0346', changed_modules=sorted(MODULES), changed_factor='confirmation_and_successful_callback_roundtrips')
    (output / 'manifest.json').write_bytes((json.dumps(manifest, indent=2) + '\n').encode())
    path = output / 'Dockerfile'
    source = path.read_bytes()
    if source.count(b'ADR0324') != 1:
        raise ValueError('Exact existing builder label required')
    path.write_bytes(source.replace(b'ADR0324', b'ADR0346'))
    return manifest


VERIFY = """
import hashlib, inspect, json
from pathlib import Path
import ticketing.workers as workers
m=json.loads(Path('/app/shared-image.json').read_text())
assert m['decision']=='ADR0346' and m['changed_modules']==sorted(['src/ticketing/workers.py','src/ticketing/api.py','src/ticketing/config.py','src/ticketing/infrastructure/reservations.py'])
assert len(m['runtime_source_sha256'])==22
for name,expected in m['runtime_source_sha256'].items():
    assert hashlib.sha256((Path('/app')/name).read_bytes()).hexdigest()==expected
assert 'WITH candidate AS' in inspect.getsource(workers.simulate_one)
assert 'p.due_at<=statement_timestamp()' in inspect.getsource(workers.simulate_one)
assert inspect.getsource(workers.simulate_one).count('db.transaction(pipeline=True)')==2
assert 'p.lease_until<clock_timestamp()' in inspect.getsource(workers.simulate_one)
assert hashlib.sha256(Path(workers.__file__).read_bytes()).hexdigest()==m['runtime_source_sha256']['src/ticketing/workers.py']
import ticketing.api as api
from ticketing.config import Settings
from ticketing.infrastructure.reservations import PostgresReservations
assert not inspect.iscoroutinefunction(api.actor)
assert not Settings().payment_callback_write_pipeline
assert 'payment_callback_write_pipeline' in inspect.getsource(PostgresReservations.apply_callback)
print(json.dumps({'modules_verified':22,'imported_worker_matches':True,'lease_clock_preserved':True}))
"""


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--receipt', type=Path, required=True)
    parser.add_argument('--build', action='store_true')
    args = parser.parse_args()
    if not args.receipt.resolve().is_relative_to((ROOT / 'tmp').resolve()) or args.receipt.exists():
        raise ValueError('Use a fresh receipt path inside workspace tmp')
    output = ROOT / 'tmp' / ('adr0346-image-' + uuid4().hex[:12])
    manifest = prepare(output)
    tag = 'flash-ticketing-shared:adr0346-' + manifest['source_identity_sha256'][:12]
    receipt = {**manifest, 'local_tag': tag, 'payload_path': str(output),
               'preparation_recipe_sha256': builder.sha(Path(__file__).read_bytes()),
               'built': False, 'registry_published': False, 'cloud_deployed': False,
               'cloud_load_started': False, 'capacity_improvement_measured': False}
    if args.build:
        log = output.with_suffix('.build.log')
        with log.open('w', encoding='utf-8') as stream:
            subprocess.run(['docker', 'buildx', 'build', '--platform=linux/amd64', '--network=none',
                            '--provenance=false', '--sbom=false', '--output=type=docker,oci-mediatypes=false',
                            '--load', '-t', tag, str(output)], stdout=stream, stderr=stream, check=True)
        receipt['validation'] = json.loads(subprocess.check_output(
            ['docker', 'run', '--rm', '--network=none', '--entrypoint', 'python', tag, '-c', VERIFY], text=True))
        info = json.loads(builder.docker('image', 'inspect', tag))[0]
        if (info['Os'], info['Architecture'], info['Config']['User']) != ('linux', 'amd64', 'ticketing'):
            raise ValueError('Expected linux/amd64 unprivileged image')
        receipt.update(built=True, local_image_id=info['Id'])
    args.receipt.parent.mkdir(parents=True, exist_ok=True)
    args.receipt.write_bytes((json.dumps(receipt, indent=2) + '\n').encode())
    print(json.dumps({'built': receipt['built'], 'local_tag': tag,
                      'validation': receipt.get('validation'), 'cloud_calls': 0}))


if __name__ == '__main__':
    main()

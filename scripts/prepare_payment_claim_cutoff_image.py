"""ADR0341 reproducible local image preparation; no registry/deployment/load calls."""
import argparse
import json
import subprocess
from pathlib import Path
from uuid import uuid4

import cce_fulfillment_batch_profile as measured
import prepare_fulfillment_batch_image as builder

ROOT = Path(__file__).resolve().parents[1]
MODULE = 'src/ticketing/workers.py'
OLD = b'p.due_at<=clock_timestamp()'
NEW = b'p.due_at<=statement_timestamp()'


def overlay(files):
    parent = measured.image()
    if {name: builder.sha(raw) for name, raw in files.items()} != parent['runtime_source_sha256']:
        raise ValueError('Complete measured ADR0324 parent source required')
    if files[MODULE].count(OLD) != 1:
        raise ValueError('Exact single claim eligibility predicate required')
    candidate = {**files, MODULE: files[MODULE].replace(OLD, NEW)}
    if (ROOT / MODULE).read_bytes().replace(b'\r\n', b'\n') != candidate[MODULE]:
        raise ValueError('Checkout differs from sole reviewed predicate replacement')
    return candidate


def prepare(output):
    previous = builder.parent_profile, builder.overlay
    try:
        builder.parent_profile, builder.overlay = measured, overlay
        manifest = builder.prepare(output)
    finally:
        builder.parent_profile, builder.overlay = previous
    manifest.update(decision='ADR0341', changed_modules=[MODULE], changed_factor='payment_due_statement_cutoff')
    (output / 'manifest.json').write_bytes((json.dumps(manifest, indent=2) + '\n').encode())
    path = output / 'Dockerfile'
    source = path.read_bytes()
    if source.count(b'ADR0324') != 1:
        raise ValueError('Exact existing builder label required')
    path.write_bytes(source.replace(b'ADR0324', b'ADR0341'))
    return manifest


VERIFY = """
import hashlib, inspect, json
from pathlib import Path
import ticketing.workers as workers
m=json.loads(Path('/app/shared-image.json').read_text())
assert m['decision']=='ADR0341' and m['changed_modules']==['src/ticketing/workers.py']
assert len(m['runtime_source_sha256'])==22
for name,expected in m['runtime_source_sha256'].items():
    assert hashlib.sha256((Path('/app')/name).read_bytes()).hexdigest()==expected
assert 'p.due_at<=statement_timestamp()' in inspect.getsource(workers.simulate_one)
assert 'p.lease_until<clock_timestamp()' in inspect.getsource(workers.simulate_one)
assert hashlib.sha256(Path(workers.__file__).read_bytes()).hexdigest()==m['runtime_source_sha256']['src/ticketing/workers.py']
print(json.dumps({'modules_verified':22,'imported_worker_matches':True,'lease_clock_preserved':True}))
"""


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--receipt', type=Path, required=True)
    parser.add_argument('--build', action='store_true')
    args = parser.parse_args()
    if not args.receipt.resolve().is_relative_to((ROOT / 'tmp').resolve()) or args.receipt.exists():
        raise ValueError('Use a fresh receipt path inside workspace tmp')
    output = ROOT / 'tmp' / ('adr0341-image-' + uuid4().hex[:12])
    manifest = prepare(output)
    tag = 'flash-ticketing-shared:adr0341-' + manifest['source_identity_sha256'][:12]
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

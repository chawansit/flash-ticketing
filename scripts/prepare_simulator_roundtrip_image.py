"""ADR0344 simulator transaction local image preparation; no registry/deployment/load calls."""
import argparse
import ast
import json
import subprocess
from pathlib import Path
from uuid import uuid4

import cce_fulfillment_batch_profile as measured
import prepare_fulfillment_batch_image as builder

ROOT = Path(__file__).resolve().parents[1]
MODULE = 'src/ticketing/workers.py'
REVIEWED_FUNCTION_SHA256 = 'e89f728d7dc12d121d7738cdcf961cd71ad62d104444535d05f03f8f0ba78ac3'


def function_parts(raw):
    tree = ast.parse(raw)
    nodes = [n for n in tree.body if isinstance(n, ast.FunctionDef) and n.name == 'simulate_one']
    if len(nodes) != 1:
        raise ValueError('Exactly one simulator function required')
    node = nodes[0]
    start = min([node.lineno] + [d.lineno for d in node.decorator_list]) - 1
    lines = raw.splitlines(keepends=True)
    return b''.join(lines[:start]), b''.join(lines[start:node.end_lineno]), b''.join(lines[node.end_lineno:])


def overlay(files):
    parent = measured.image()
    if {name: builder.sha(raw) for name, raw in files.items()} != parent['runtime_source_sha256']:
        raise ValueError('Complete measured ADR0324 parent source required')
    current = (ROOT / MODULE).read_bytes().replace(b'\r\n', b'\n')
    before, _, after = function_parts(files[MODULE])
    current_before, function, current_after = function_parts(current)
    if (before, after) != (current_before, current_after):
        raise ValueError('Only simulate_one may change')
    if builder.sha(function) != REVIEWED_FUNCTION_SHA256:
        raise ValueError('Unreviewed simulator function')
    return {**files, MODULE: current}


def prepare(output):
    previous = builder.parent_profile, builder.overlay
    try:
        builder.parent_profile, builder.overlay = measured, overlay
        manifest = builder.prepare(output)
    finally:
        builder.parent_profile, builder.overlay = previous
    manifest.update(decision='ADR0344', changed_modules=[MODULE], changed_factor='simulator_atomic_claim_and_transaction_pipeline')
    (output / 'manifest.json').write_bytes((json.dumps(manifest, indent=2) + '\n').encode())
    path = output / 'Dockerfile'
    source = path.read_bytes()
    if source.count(b'ADR0324') != 1:
        raise ValueError('Exact existing builder label required')
    path.write_bytes(source.replace(b'ADR0324', b'ADR0344'))
    return manifest


VERIFY = """
import hashlib, inspect, json
from pathlib import Path
import ticketing.workers as workers
m=json.loads(Path('/app/shared-image.json').read_text())
assert m['decision']=='ADR0344' and m['changed_modules']==['src/ticketing/workers.py']
assert len(m['runtime_source_sha256'])==22
for name,expected in m['runtime_source_sha256'].items():
    assert hashlib.sha256((Path('/app')/name).read_bytes()).hexdigest()==expected
assert 'WITH candidate AS' in inspect.getsource(workers.simulate_one)
assert 'p.due_at<=statement_timestamp()' in inspect.getsource(workers.simulate_one)
assert inspect.getsource(workers.simulate_one).count('db.transaction(pipeline=True)')==2
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
    output = ROOT / 'tmp' / ('adr0344-image-' + uuid4().hex[:12])
    manifest = prepare(output)
    tag = 'flash-ticketing-shared:adr0344-' + manifest['source_identity_sha256'][:12]
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

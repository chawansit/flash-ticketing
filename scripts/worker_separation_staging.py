"""ADR0187 bounded worker image packages; cloud entry is blocked until profile registration."""
import copy
import hashlib
import json
import os
import re
import shutil
import sys
import tarfile
from collections import Counter
from pathlib import Path, PurePosixPath

import stage_status_refresh_images as staging
import work_envelope as policy
from fetch_status_refresh_parents import cleanup_program, owner_path
from prepare_status_refresh_artifacts import command, inspect_image, owned_output
from qualify_two_host_deployment import GENERATOR_IDLE
from runtime_source_identity import source_identity_program
from two_host_topology import NORMAL_COUNTS
from worker_separation_inventory import ALL_CONTAINERS
from worker_separation_retained import verify_retained
from worker_separation_snapshot import bind_images, runtime_semantic
from worker_separation_topology import WORKERS, validate_pair

PROFILE = 'worker_separation'
HASH = re.compile(r'[0-9a-f]{64}$')
IMAGE = re.compile(r'sha256:[0-9a-f]{64}$')


def stable_image(row):
    if (not IMAGE.fullmatch(row.get('Id', '')) or row.get('Os') != 'linux' or row.get('Architecture') != 'amd64'
            or type(row.get('Size')) is not int or not 0 < row['Size'] <= staging.MAX_ARCHIVE
            or not isinstance(row.get('Config'), dict) or not isinstance(row.get('RootFS'), dict)):
        raise ValueError('Exact bounded immutable Linux amd64 metadata required')
    return {k: copy.deepcopy(row[k]) for k in ('Id', 'Os', 'Architecture', 'Size', 'Config', 'RootFS')}


def make_contract(pair, sources, metadata):
    validate_pair(pair)
    if not isinstance(sources, dict) or set(sources) != set(WORKERS):
        raise ValueError('Every worker role needs exact source expectations')
    for value in sources.values(): source_identity_program(value)
    workers = {r: pair['candidate']['secondary']['services'][r] for r in WORKERS}
    images = {r: s['image'] for r, s in workers.items()}
    rows = {r['Id']: stable_image(r) for r in metadata}
    if len(rows) != len(metadata) or set(rows) != set(images.values()):
        raise ValueError('Exact distinct worker image metadata required')
    if sum(r['Size'] for r in rows.values()) > staging.MAX_ARCHIVE:
        raise ValueError('Worker image archive exceeds bounded budget')
    model = {'services': copy.deepcopy(workers)}
    if bind_images(model, list(rows.values())) != model:
        raise ValueError('Image defaults must be materialized before pair preparation')
    per_image = {}
    for role, image in images.items():
        if image in per_image and per_image[image] != sources[role]:
            raise ValueError('Shared images cannot have different source expectations')
        per_image[image] = sources[role]
    return {'schema': 2, 'decision': 'ADR0205', 'placement_decision': 'ADR0184',
            'prepared_pair_sha256': policy.digest(pair), 'source_manifest_sha256': policy.digest(sources),
            'role_images': images, 'image_metadata_sha256': {i: policy.digest({k:v for k,v in r.items() if k!='Size'}) for i, r in rows.items()},
            'image_sizes': {i: r['Size'] for i, r in rows.items()},
            'image_sources_sha256': {i: policy.digest(s) for i, s in per_image.items()}}


def validate_contract(contract, sources):
    if (set(contract) != {'schema', 'decision', 'placement_decision', 'prepared_pair_sha256',
            'source_manifest_sha256', 'role_images', 'image_metadata_sha256', 'image_sizes', 'image_sources_sha256'}
            or type(contract['schema']) is not int or contract['schema'] not in {1,2}
            or contract['decision'] != {1:'ADR0187',2:'ADR0205'}[contract['schema']]
            or contract['placement_decision'] != 'ADR0184' or not HASH.fullmatch(contract['prepared_pair_sha256'])
            or set(contract['role_images']) != set(WORKERS) or set(sources) != set(WORKERS)
            or policy.digest(sources) != contract['source_manifest_sha256']):
        raise ValueError('Exact staging contract and role-source bindings required')
    for value in sources.values(): source_identity_program(value)
    ids = set(contract['role_images'].values())
    if any(not IMAGE.fullmatch(i) for i in ids): raise ValueError('Immutable staging images required')
    for key in ('image_metadata_sha256', 'image_sources_sha256', 'image_sizes'):
        if set(contract[key]) != ids: raise ValueError('Exact distinct image coverage required')
    for image in ids:
        if (not HASH.fullmatch(contract['image_metadata_sha256'][image])
                or any(policy.digest(sources[r]) != contract['image_sources_sha256'][image]
                       for r, i in contract['role_images'].items() if i == image)
                or type(contract['image_sizes'][image]) is not int or contract['image_sizes'][image] <= 0):
            raise ValueError('Exact bounded image/source metadata required')
    if sum(contract['image_sizes'].values()) > staging.MAX_ARCHIVE:
        raise ValueError('Worker images exceed archive budget')


def image_proof_program(contract, sources, owner_name):
    validate_contract(contract, sources)
    owner_path('/qualification/repository', owner_name)
    unique = {image: sources[role] for role, image in contract['role_images'].items()}
    proofs = {i: source_identity_program(s) for i, s in unique.items()}
    return r'''import hashlib,json,subprocess
expected=EXPECTED
proofs=PROOFS
owner=OWNER
def digest(value):return hashlib.sha256(json.dumps(value,sort_keys=True,separators=(',',':'),allow_nan=False).encode()).hexdigest()
images=sorted(expected['image_metadata_sha256'])
rows=json.loads(subprocess.check_output(['docker','image','inspect',*images],text=True,timeout=30))
if len(rows)!=len(images) or {r['Id'] for r in rows}!=set(images):raise ValueError('Image cache identities differ')
for row in rows:
 if type(row.get('Size')) is not int or not 0<row['Size']<=MAX_IMAGE_SIZE:raise ValueError('Remote image size is unbounded')
 keys=('Id','Os','Architecture','Size','Config','RootFS') if expected['schema']==1 else ('Id','Os','Architecture','Config','RootFS')
 metadata={k:row[k] for k in keys}
 if digest(metadata)!=expected['image_metadata_sha256'][row['Id']]:raise ValueError('Image metadata differs')
for index,image in enumerate(images):
 name=owner+'-proof-'+str(index)
 labels={'org.flash-ticketing.stage-owner':owner,'org.flash-ticketing.staging-contract':digest(expected)}
 def ids():return subprocess.check_output(['docker','ps','-aq','--no-trunc','--filter','name=^/'+name+'$'],text=True,timeout=5).split()
 if ids():raise ValueError('Pre-existing proof container blocks replay')
 try:
  args=['docker','run','--pull=never','--name',name,'--network=none','--read-only','--cap-drop=ALL','--security-opt=no-new-privileges','--entrypoint','python']
  for key,value in labels.items():args+=['--label',key+'='+value]
  result=json.loads(subprocess.check_output([*args,image,'-c',proofs[image]],text=True,timeout=20))
  if any(result.get(k) is not True for k in ('source_hashes_match','app_source_hashes_match','import_source_hashes_match','import_code_matches_source')):raise ValueError('Image source/import proof differs')
 finally:
  current=ids()
  if current:
   if len(current)!=1:raise ValueError('Ambiguous proof identity')
   row=json.loads(subprocess.check_output(['docker','inspect',current[0]],text=True,timeout=5))[0]
   if row['Id']!=current[0] or row['Image']!=image or any(row['Config']['Labels'].get(k)!=v for k,v in labels.items()):raise ValueError('Unowned proof container preserved')
   subprocess.run(['docker','rm','-f',current[0]],check=True,stdout=subprocess.DEVNULL,stderr=subprocess.PIPE,timeout=10)
   if ids():raise ValueError('Proof container cleanup incomplete')
print(json.dumps({'contract_sha256':digest(expected),'images_verified':images,'source_imports_match':True}))
'''.replace('EXPECTED', repr(contract)).replace('PROOFS', repr(proofs)).replace('OWNER', repr(owner_name)).replace('MAX_IMAGE_SIZE',repr(staging.MAX_ARCHIVE))



def validate_archive_receipt(contract, receipt):
    if (not isinstance(receipt, dict) or set(receipt) != {'contract_sha256', 'bytes', 'sha256'}
            or receipt['contract_sha256'] != policy.digest(contract)
            or type(receipt['bytes']) is not int or not 0 < receipt['bytes'] <= staging.MAX_ARCHIVE
            or not isinstance(receipt['sha256'], str) or not HASH.fullmatch(receipt['sha256'])):
        raise ValueError('Exact bounded archive receipt required')


def _oci_archive(archive, records, images):
    """Verify bounded OCI descriptor graphs, including pinned attestation siblings."""
    indexes = {'application/vnd.oci.image.index.v1+json', 'application/vnd.docker.distribution.manifest.list.v2+json'}
    manifests = {'application/vnd.oci.image.manifest.v1+json', 'application/vnd.docker.distribution.manifest.v2+json'}
    verified, walked, active = {}, {}, set()
    edges = 0

    def document(name):
        member = archive.getmember(name)
        if not member.isfile() or not 0 < member.size <= 1024**2:
            raise ValueError('Bounded OCI document required')
        value = json.loads(archive.extractfile(member).read())
        if not isinstance(value, dict) or type(value.get('schemaVersion')) is not int or value['schemaVersion'] != 2:
            raise ValueError('OCI schema version 2 required')
        return value

    def blob(descriptor):
        nonlocal edges
        edges += 1
        if edges > 4096: raise ValueError('OCI graph edge budget exceeded')
        if (not isinstance(descriptor, dict) or not isinstance(descriptor.get('digest'), str)
                or not IMAGE.fullmatch(descriptor['digest']) or type(descriptor.get('size')) is not int
                or not 0 < descriptor['size'] <= staging.MAX_ARCHIVE):
            raise ValueError('Exact bounded SHA-256 descriptor required')
        digest = descriptor['digest']
        name = 'blobs/sha256/' + digest[7:]
        member = archive.getmember(name)
        if not member.isfile() or member.size != descriptor['size']:
            raise ValueError('OCI descriptor size differs')
        if digest not in verified:
            if len(verified) >= 256: raise ValueError('OCI graph node budget exceeded')
            hasher = hashlib.sha256()
            with archive.extractfile(member) as stream:
                for chunk in iter(lambda: stream.read(1024**2), b''): hasher.update(chunk)
            if hasher.hexdigest() != digest[7:]: raise ValueError('OCI blob digest differs')
            verified[digest] = member.size
        return name

    def visit(descriptor, depth=0):
        if depth > 4: raise ValueError('OCI graph depth exceeded')
        name = blob(descriptor)
        media = descriptor.get('mediaType')
        if media not in indexes | manifests: raise ValueError('Unsupported OCI graph type')
        if name in active: raise ValueError('Cyclic OCI graph')
        if name in walked:
            if walked[name]['mediaType'] != media: raise ValueError('OCI media type differs')
            return walked[name]
        value = document(name)
        if value.get('mediaType') != media: raise ValueError('OCI media type differs')
        active.add(name)
        if media in indexes:
            children = value.get('manifests')
            if not isinstance(children, list) or not 0 < len(children) <= 64:
                raise ValueError('Bounded OCI child descriptors required')
            for child in children: visit(child, depth + 1)
        else:
            blob(value.get('config'))
            layers = value.get('layers')
            if not isinstance(layers, list) or len(layers) > 128:
                raise ValueError('Bounded OCI layers required')
            for layer in layers: blob(layer)
        active.remove(name)
        walked[name] = value
        return value

    top = document('index.json')
    roots = top.get('manifests')
    if (not isinstance(roots, list) or len(roots) != len(images)
            or any(not isinstance(r, dict) for r in roots)
            or {r.get('digest') for r in roots} != set(images)):
        raise ValueError('Exact OCI root image set required')
    expected = {}
    for root in roots:
        value = visit(root)
        if root['mediaType'] in indexes:
            candidates = [d for d in value['manifests'] if isinstance(d.get('platform'), dict)
                          and d['platform'].get('os') == 'linux' and d['platform'].get('architecture') == 'amd64']
            if len(candidates) != 1 or candidates[0].get('mediaType') not in manifests:
                raise ValueError('One Linux amd64 runtime manifest required')
            value = document(blob(candidates[0]))
        elif (not isinstance(root.get('platform'), dict) or root['platform'].get('os') != 'linux'
              or root['platform'].get('architecture') != 'amd64'):
            raise ValueError('Linux amd64 runtime platform required')
        config = blob(value['config'])
        if config in expected: raise ValueError('Duplicate runtime image config')
        expected[config] = [blob(layer) for layer in value['layers']]
    if (len({r['Config'] for r in records}) != len(records)
            or {r['Config'] for r in records} != set(expected)
            or any(r.get('Layers') != expected[r['Config']] for r in records)):
        raise ValueError('Docker compatibility manifest differs from OCI runtime')


def validate_archive(path, images):
    """Verify classic Docker or OCI immutable identities before image load."""
    if not images or any(not IMAGE.fullmatch(i) for i in images):
        raise ValueError('Exact archive image set required')
    with tarfile.open(path, 'r:') as archive:
        members = archive.getmembers()
        if (len(members) > 50000 or len({m.name for m in members}) != len(members)
                or sum(m.size for m in members) > staging.MAX_ARCHIVE
                or any(PurePosixPath(m.name).is_absolute() or '..' in PurePosixPath(m.name).parts
                       or not (m.isfile() or m.isdir()) for m in members)):
            raise ValueError('Unsafe or oversized Docker image archive')
        manifest = archive.getmember('manifest.json')
        if not manifest.isfile() or not 0 < manifest.size <= 1024**2:
            raise ValueError('Bounded image manifest required')
        records = json.loads(archive.extractfile(manifest).read())
        if not isinstance(records, list) or len(records) != len(images):
            raise ValueError('Extra or missing image in archive manifest')
        if any(not isinstance(r, dict) or not isinstance(r.get('Config'), str) for r in records):
            raise TypeError('Exact image config entry required')
        if 'index.json' in {m.name for m in members}:
            _oci_archive(archive, records, images)
            return
        actual = set()
        for record in records:
            if not isinstance(record, dict) or not isinstance(record.get('Config'), str):
                raise TypeError('Exact image config entry required')
            member = archive.getmember(record['Config'])
            if not member.isfile() or not 0 < member.size <= 1024**2:
                raise ValueError('Bounded image config required')
            image = 'sha256:' + hashlib.sha256(archive.extractfile(member).read()).hexdigest()
            if image in actual: raise ValueError('Duplicate archive image config')
            actual.add(image)
        if actual != set(images): raise ValueError('Archive config identities differ')


def _receipt(path, value):
    with path.open('x', encoding='utf-8') as stream:
        stream.write(json.dumps(value, sort_keys=True, allow_nan=False) + '\n')
        stream.flush()
        os.fsync(stream.fileno())


def prepare_package(pair, sources, output):
    """Local only. Call with new_stage_output(); no image build/pull or service deployment."""
    validate_pair(pair)
    ids = sorted({s['image'] for s in pair['candidate']['secondary']['services'].values()})
    metadata = [inspect_image(i) for i in ids]
    contract = make_contract(pair, sources, metadata)
    output = Path(output).absolute()
    owner_path('/qualification/repository', output.name)
    if output.parent != (staging.ROOT / 'tmp').absolute() or output.resolve() != output:
        raise ValueError('Canonical shared staging output required')
    output.parent.mkdir(exist_ok=True)
    if shutil.disk_usage(output.parent).free < sum(contract['image_sizes'].values()) + 512 * 1024**2:
        raise ValueError('Insufficient local archive headroom')
    output = owned_output(output)
    _receipt(output / 'contract.json', contract)
    _receipt(output / 'source-expectations.json', sources)
    _receipt(output / 'image-proof-intent.json', {'owner': output.name, 'contract_sha256': policy.digest(contract)})
    command([sys.executable, '-c', image_proof_program(contract, sources, output.name)], log=output / 'image-proof.log', timeout=240)
    archive = output / 'images.tar'
    command(['docker', 'image', 'save', '--output', str(archive), *ids], log=output / 'save.log', timeout=180)
    if archive.is_symlink() or not archive.is_file() or not 0 < archive.stat().st_size <= staging.MAX_ARCHIVE:
        raise ValueError('Exact bounded saved worker archive required')
    validate_archive(archive, set(contract['image_sizes']))
    receipt = {'contract_sha256': policy.digest(contract), 'bytes': archive.stat().st_size,
               'sha256': staging.hash_file(archive)}
    _receipt(output / 'archive.json', receipt)
    return contract, archive, receipt


def _authorize(guard, contract, timeout, receipt):
    validate_archive_receipt(contract, receipt)
    if not isinstance(guard, policy.ActionGuard) or PROFILE not in policy.PROFILES:
        raise ValueError('Worker staging requires a registered guarded profile')
    item = policy.scope_authorized(policy.read(policy.STATE), guard.key, guard.binding)
    expected = {'worker_staging_contract_sha256': policy.digest(contract),
                'worker_pair_sha256': contract['prepared_pair_sha256'],
                'worker_sources_sha256': contract['source_manifest_sha256'],
                'worker_archive_receipt_sha256': policy.digest(receipt)}
    if item['profile'] != PROFILE or any(guard.binding.get(k) != v for k, v in expected.items()):
        raise ValueError('Exact fresh worker staging scope required')
    guard.check(timeout)


def _runtime(session, before_call=None, saved=None):
    signature = {}
    for host in ('primary', 'secondary'):
        if before_call is not None: before_call(45)
        rows = session.call(host, ALL_CONTAINERS, 45)
        rows = verify_retained(rows, (saved or {}).get('retained_inactive_containers',{}) if host=='primary' else {})
        counts = Counter(r['Config']['Labels'].get('com.docker.compose.service') for r in rows)
        if ((host == 'secondary' and rows) or (host == 'primary' and dict(counts) != NORMAL_COUNTS)
                or any(r['State']['Running'] is not True or r['Config']['Labels'].get('com.docker.compose.project') != 'flash-ticketing' for r in rows)):
            raise ValueError('Original primary and empty secondary required before staging')
        signature[host] = sorted((r['Id'], r['Image'], r['State']['StartedAt'], policy.digest(runtime_semantic(r))) for r in rows)
    if before_call is not None: before_call(30)
    if session.call('generator', GENERATOR_IDLE, 30).get('generator_idle') is not True:
        raise ValueError('Dedicated generator must remain idle')
    return signature


def sealed_cleanup_program(seal):
    proof = staging.seal_program(seal['owner'], seal['seal']['size'], seal['sha256'])
    proof = proof[:proof.rindex('print(json.dumps(')]
    return proof + '\nif seal!=' + repr(seal['seal']) + ":raise ValueError('Cleanup seal changed')\n" + cleanup_program(seal)


def stage_package(session, contract, sources, archive, receipt, guard, *, saved=None):
    """Guarded staging hook; only the registered fresh worker scope can enter."""
    validate_contract(contract, sources)
    if saved is not None and guard.binding.get('worker_saved_runtime_sha256') != policy.digest(saved):
        raise ValueError('Exact saved runtime including retained inventory required')
    _authorize(guard, contract, 360, receipt)
    archive = Path(archive).absolute()
    output = archive.parent
    owner_path('/qualification/repository', output.name)
    if (archive.name != 'images.tar' or archive.resolve() != archive or archive.is_symlink()
            or output.parent != (staging.ROOT / 'tmp').absolute()
            or receipt != {'contract_sha256': policy.digest(contract), 'bytes': archive.stat().st_size,
                           'sha256': staging.hash_file(archive)}
            or not 0 < receipt['bytes'] <= staging.MAX_ARCHIVE):
        raise ValueError('Exact owned package archive and durable receipt required')
    for name, expected in [('contract.json', contract), ('source-expectations.json', sources), ('archive.json', receipt)]:
        path = output / name
        if path.is_symlink() or path.resolve() != path or policy.read(path) != expected:
            raise ValueError('Durable package receipt differs')
    validate_archive(archive, set(contract['image_sizes']))
    result = {'decision': 'ADR0187', 'contract_sha256': policy.digest(contract), 'pass': False,
              'status': 'RECOVERY_REQUIRED', 'hosts': {}, 'service_deployments': 0, 'customer_dispatches': 0}
    before, completed = None, False
    prior_cleanup_mode = getattr(session, 'cleanup_mode', False)
    try:
        _authorize(guard, contract, 120, receipt)
        session.phase('worker-image-staging')
        before = _runtime(session, lambda timeout: _authorize(guard, contract, timeout, receipt), saved)
        for host in ('primary', 'secondary'):
            repo = session.config[host]['repo'] if host == 'primary' else session.config[host]['prepared_directory']
            owner = owner_path(repo, output.name)
            state = result['hosts'][host] = {'owner': owner, 'seal': None, 'archive_removed': False}
            _receipt(output / (host + '-owner-intent.json'), {'owner': owner, 'contract_sha256': policy.digest(contract)})
            _authorize(guard, contract, 30, receipt)
            state['creation_attempted'] = True
            created = session.call(host, staging.create_owner_program(owner, receipt['bytes']), 30)
            if created != {'owner': owner, 'created': True}: raise ValueError('Exact owner creation receipt required')
            _authorize(guard, contract, 240, receipt)
            staging.upload(session, host, owner, archive, receipt['bytes'], receipt['sha256'], action_guard=guard)
            _authorize(guard, contract, 60, receipt)
            seal = session.call(host, staging.seal_program(owner, receipt['bytes'], receipt['sha256']), 60)
            if (seal.get('owner') != owner or seal.get('sha256') != receipt['sha256']
                    or seal.get('seal', {}).get('size') != receipt['bytes']):
                raise ValueError('Exact owned archive seal required')
            state['seal'] = seal  # Retain identity before a potentially failed local receipt write.
            _receipt(output / (host + '-seal.json'), seal)
            _receipt(output / (host + '-load-intent.json'), {'seal_sha256': policy.digest(seal)})
            _authorize(guard, contract, 240, receipt)
            loaded = session.call(host, staging.load_program(seal, sorted(contract['image_sizes'])), 240)
            if loaded != {'loaded_images': sorted(contract['image_sizes'])}: raise ValueError('Image load receipt differs')
            _authorize(guard, contract, 240, receipt)
            _receipt(output / (host + '-proof-intent.json'), {'owner': output.name, 'contract_sha256': policy.digest(contract)})
            proof = session.call(host, image_proof_program(contract, sources, output.name), 240)
            if proof != {'contract_sha256': policy.digest(contract), 'images_verified': sorted(contract['image_sizes']), 'source_imports_match': True}:
                raise ValueError('Exact loaded image/source proof required')
            _receipt(output / (host + '-verified.json'), proof)
        _authorize(guard, contract, 120, receipt)
        if before != _runtime(session, lambda timeout: _authorize(guard, contract, timeout, receipt), saved): raise ValueError('Runtime changed during image staging')
        completed = True
    except BaseException as exc:  # noqa: BLE001 - interruption must retain ownership and attempt exact cleanup.
        result['failure_type'] = type(exc).__name__
    finally:
        session.begin_cleanup()
        for host, state in result['hosts'].items():
            if state['seal'] is None: continue  # Unknown/partial owner must not be inferred or deleted.
            try:
                if session.call(host, sealed_cleanup_program(state['seal']), 60) != {'remote_archive_removed': True}:
                    raise ValueError('Exact cleanup acknowledgement required')
                state['archive_removed'] = True
            except BaseException as exc:  # noqa: BLE001 - attempt cleanup on every exactly owned host.
                state['cleanup_failure_type'] = type(exc).__name__
        runtime_unchanged = False
        if before is not None:
            try:
                runtime_unchanged = before == _runtime(session, saved=saved)
            except BaseException as exc:  # noqa: BLE001 - unknown runtime after cleanup blocks progression.
                result['runtime_failure_type'] = type(exc).__name__
        result['runtime_unchanged'] = runtime_unchanged
        clean = runtime_unchanged and all(not s.get('creation_attempted') or s['archive_removed'] for s in result['hosts'].values())
        result.update({'pass': completed and clean, 'status': 'STAGED_VERIFIED' if completed and clean else
                       'FAILED_CLEANED' if clean else 'RECOVERY_REQUIRED'})
        try: _receipt(output / 'staging-summary.json', result)
        except BaseException as exc:  # noqa: BLE001 - lost final evidence blocks progression.
            result.update({'pass': False, 'status': 'RECOVERY_REQUIRED', 'receipt_failure_type': type(exc).__name__})
        session.cleanup_mode = prior_cleanup_mode
    return result

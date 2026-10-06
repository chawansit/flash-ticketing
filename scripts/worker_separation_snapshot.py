"""ADR0186 private in-memory image bindings and full broker-aware restoration snapshots."""
import copy
import re
from collections import Counter
from pathlib import PurePosixPath

from two_host_topology import NORMAL_COUNTS, bindings, environment, semantic
from worker_separation_inventory import digest
from worker_separation_topology import FORBIDDEN

IMAGE = re.compile(r'sha256:[0-9a-f]{64}$')
IDENTITY = re.compile(r'[0-9a-f]{64}$')


def bind_images(model, image_rows):
    """Materialize inherited defaults; no I/O, credentials or metadata are printed."""
    expected = {s['image'] for s in model['services'].values()}
    if any(not IMAGE.fullmatch(i) for i in expected):
        raise ValueError('Immutable image identifiers required')
    images = {r['Id']: r for r in image_rows}
    if len(images) != len(image_rows) or set(images) != expected:
        raise ValueError('Exact distinct image metadata required')
    result = copy.deepcopy(model)
    for service in result['services'].values():
        config = images[service['image']]['Config']
        defaults = config.get('Env') or []
        if any(not isinstance(v, str) or '=' not in v for v in defaults):
            raise ValueError('Literal image environment required')
        inherited = dict(v.split('=', 1) for v in defaults)
        if len(inherited) != len(defaults) or '' in inherited:
            raise ValueError('Duplicate or empty image environment key')
        explicit = service.get('environment')
        if not isinstance(explicit, dict) or any(not isinstance(k, str) or not k or not isinstance(v, str)
                                                for k, v in explicit.items()):
            raise ValueError('Resolved literal service environment required')
        service['environment'] = {**inherited, **explicit}
        if service.get('entrypoint') is not None and service.get('command') is None:
            service['command'] = []  # Explicit entrypoint suppresses the image CMD in Compose.
        for field, key in [('command', 'Cmd'), ('entrypoint', 'Entrypoint')]:
            # Empty arrays explicitly disable image defaults; null inherits in Compose.
            if service.get(field) is None:
                service[field] = copy.deepcopy(config.get(key) or [])
            if not isinstance(service[field], list) or any(not isinstance(v, str) for v in service[field]):
                raise ValueError('Exact resolved command and entrypoint arrays required')
    return result


def _path(value):
    p = PurePosixPath(value)
    if not p.is_absolute() or str(p) != value or '..' in p.parts:
        raise ValueError('Exact absolute runtime mount path required')
    return value


def runtime_semantic(row):
    result = semantic(row)
    result['mounts'] = sorted((m['Type'], m.get('Name', ''), m['Source'], m['Destination'], m['RW'],
                               m.get('Propagation', ''), m.get('Mode', ''))
                              for m in row.get('Mounts', []))
    host = row['HostConfig']
    result['host_options'] = {k: host.get(k) for k in (
        'NetworkMode', 'ReadonlyRootfs', 'Privileged', 'SecurityOpt', 'CapAdd', 'CapDrop',
        'Tmpfs', 'Memory', 'NanoCpus', 'PidsLimit', 'Ulimits', 'LogConfig',
    )}
    result['user'] = row['Config'].get('User', '')
    result['working_dir'] = row['Config'].get('WorkingDir', '')
    return result


def _groups(rows):
    groups, ids = {}, set()
    for row in rows:
        labels = row['Config']['Labels']
        cid = row['Id']
        if (not IDENTITY.fullmatch(cid) or cid in ids or row['State']['Running'] is not True
                or labels.get('com.docker.compose.project') != 'flash-ticketing'
                or not IMAGE.fullmatch(row['Image'])):
            raise ValueError('Exact running original project and immutable identities required')
        ids.add(cid)
        groups.setdefault(labels.get('com.docker.compose.service'), []).append(row)
    if {r: len(v) for r, v in groups.items()} != NORMAL_COUNTS:
        raise ValueError('Original topology differs; stopped or unrelated resources rejected')
    return groups


def _broker_volume(groups, volumes):
    mounts = groups['kafka'][0].get('Mounts', [])
    if len(mounts) != 1 or mounts[0]['Type'] != 'volume' or mounts[0]['RW'] is not True:
        raise ValueError('Exact writable Kafka named volume required')
    mount = mounts[0]
    env = environment(groups['kafka'][0])
    if mount['Destination'] != env.get('KAFKA_LOG_DIRS') or not mount.get('Name'):
        raise ValueError('Kafka log directory and actual volume name required')
    if len(volumes) != 1 or volumes[0].get('Name') != mount['Name']:
        raise ValueError('Exact existing broker volume inspection required')
    if volumes[0].get('Driver') != 'local' or volumes[0].get('Options') not in (None, {}):
        raise ValueError('Custom persistent volume drivers need separate qualification')
    return copy.deepcopy(volumes[0])


def capture_runtime(model, rows, volumes, bind_hashes):
    """Input rows must come from all-container observations, including stopped rows."""
    if model.get('name') != 'flash-ticketing':
        raise ValueError('Original project required')
    groups = _groups(rows)
    volume = _broker_volume(groups, volumes)
    actual_binds = {_path(m['Source']) for r in rows for m in r.get('Mounts', []) if m['Type'] == 'bind'}
    if set(bind_hashes) != actual_binds or any(not IDENTITY.fullmatch(v) for v in bind_hashes.values()):
        raise ValueError('Every original bind file needs exact content identity')
    result = copy.deepcopy(model)
    result['services'] = {r: result['services'][r] for r in groups}
    # Existing volume only: Compose must never create a replacement for a missing one.
    result['volumes'] = {'owned-kafka-data': {'name': volume['Name'], 'external': True}}
    semantics = {}
    for role, replicas in groups.items():
        if len({digest(runtime_semantic(r)) for r in replicas}) != 1:
            raise ValueError('Different original replica semantics within a role')
        row, service = replicas[0], result['services'][role]
        if service.get('networks') not in (None, {}, {'default': None}, {'default': {}}):
            raise ValueError('Only original default service network supported')
        service.pop('networks', None)
        if set(service) & (FORBIDDEN - {'build', 'env_file'}):
            raise ValueError('Unsupported original execution options')
        for key in ('build', 'env_file', 'depends_on', 'profiles'):
            service.pop(key, None)
        host = row['HostConfig']
        if (host.get('Privileged') or host.get('ReadonlyRootfs') or host.get('SecurityOpt')
                or host.get('CapAdd') or host.get('CapDrop') or host.get('Tmpfs')
                or host.get('Memory', 0) or host.get('NanoCpus', 0)
                or host.get('PidsLimit') not in (None, 0) or host.get('Ulimits')):
            raise ValueError('Unsupported runtime options need separate restoration qualification')
        mode = host.get('NetworkMode')
        if mode not in (None, 'flash-ticketing_default'):
            raise ValueError('Exact original Compose default network required')
        service.update(image=row['Image'], pull_policy='never', environment=environment(row),
                       command=copy.deepcopy(row['Config']['Cmd'] or []),
                       entrypoint=copy.deepcopy(row['Config'].get('Entrypoint') or []),
                       ports=[{'target': p, 'protocol': proto, 'host_ip': ip, 'published': str(public)}
                              for p, proto, ip, public in bindings(row)])
        if row['Config'].get('Healthcheck') is not None:
            health = row['Config']['Healthcheck']
            if set(health) - {'Test', 'Interval', 'Timeout', 'Retries', 'StartPeriod', 'StartInterval'}:
                raise ValueError('Unsupported original health check')
            service['healthcheck'] = {k: copy.deepcopy(health[v]) for k, v in {
                'test': 'Test', 'interval': 'Interval', 'timeout': 'Timeout', 'retries': 'Retries',
                'start_period': 'StartPeriod', 'start_interval': 'StartInterval'}.items() if v in health}
            for k in ('interval', 'timeout', 'start_period', 'start_interval'):
                if k in service['healthcheck']:
                    service['healthcheck'][k] = str(service['healthcheck'][k]) + 'ns'
        else:
            service.pop('healthcheck', None)
        restart = host['RestartPolicy']
        name, retries = restart['Name'], restart.get('MaximumRetryCount', 0)
        if name not in {'no', 'always', 'unless-stopped', 'on-failure'} or type(retries) is not int or retries < 0:
            raise ValueError('Exact original restart policy required')
        service['restart'] = name + (':' + str(retries) if name == 'on-failure' and retries else '')
        log = host.get('LogConfig')
        if log is not None:
            if set(log) != {'Type', 'Config'} or not isinstance(log['Config'], dict):
                raise ValueError('Exact original logging configuration required')
            service['logging'] = {'driver': log['Type'], 'options': copy.deepcopy(log['Config'])}
        service['user'] = row['Config'].get('User', '')
        service['working_dir'] = row['Config'].get('WorkingDir', '')
        service['volumes'] = []
        for mount in row.get('Mounts', []):
            if mount.get('Propagation', '') not in ('', 'rprivate') or mount.get('Mode', '') not in ('', 'ro', 'rw'):
                raise ValueError('Unsupported runtime mount propagation or mode')
            if mount['Type'] == 'volume' and role == 'kafka' and mount['Name'] == volume['Name']:
                source = 'owned-kafka-data'
            elif mount['Type'] == 'bind':
                source = _path(mount['Source'])
            else:
                raise ValueError('Unsupported runtime mount')
            service['volumes'].append({'type': mount['Type'], 'source': source, 'target': mount['Destination'],
                                       'read_only': not mount['RW']})
        semantics[role] = digest(runtime_semantic(row))
    return {'schema': 1, 'decision': 'ADR0186', 'model': result, 'counts': dict(NORMAL_COUNTS),
            'semantics': semantics, 'broker_volume': volume, 'bind_sha256': dict(bind_hashes),
            'original_containers': sorted(r['Id'] for r in rows)}


def verify_restored(saved, rows_by_host, volumes, bind_hashes):
    if saved.get('decision') != 'ADR0186' or saved.get('schema') != 1 or rows_by_host.get('secondary') != []:
        raise ValueError('Exact saved runtime and empty secondary required')
    groups = _groups(rows_by_host['primary'])
    if (dict(Counter(r['Config']['Labels']['com.docker.compose.service'] for r in rows_by_host['primary'])) != saved['counts']
            or any(digest(runtime_semantic(row)) != saved['semantics'][role] for role, rs in groups.items() for row in rs)
            or _broker_volume(groups, volumes) != saved['broker_volume'] or bind_hashes != saved['bind_sha256']):
        raise ValueError('Original runtime, broker volume or private file restoration differs')
    return {'runtime_restored': True, 'broker_volume_retained': True, 'bind_files_restored': True,
            'secondary_empty': True}

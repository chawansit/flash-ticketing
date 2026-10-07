"""ADR0186 private in-memory image bindings and full broker-aware restoration snapshots."""
import copy
import re
from collections import Counter
from pathlib import PurePosixPath

from two_host_topology import NORMAL_COUNTS, bindings, environment, semantic
from worker_separation_inventory import digest
from worker_separation_retained import capture_retained
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
        for field,key in [('user','User'),('working_dir','WorkingDir')]:
            if service.get(field) is None and (field in service or config.get(key)):
                service[field]=config.get(key) or ''
            if field in service and not isinstance(service[field],str):
                raise TypeError('Exact literal image user and working directory required')
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
                               m.get('Propagation', ''), m.get('Mode') or ('rw' if m['RW'] else 'ro'))
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


def broker_records(saved):
    auxiliary=saved.get('broker_auxiliary_volumes',[])
    if not isinstance(auxiliary,list) or len(auxiliary)>2:
        raise ValueError('Bounded existing broker volume receipt required')
    records=[saved['broker_volume'],*auxiliary]
    names=[r.get('Name') for r in records]
    if (len(set(names))!=len(names) or any(not isinstance(n,str) or not re.fullmatch(r'[A-Za-z0-9][A-Za-z0-9_.-]*',n) for n in names)
            or any(r.get('Driver')!='local' or r.get('Options') not in (None,{}) for r in records)):
        raise ValueError('Exact distinct local broker volumes required')
    return records


def _broker_inventory(groups, volumes):
    row=groups['kafka'][0];mounts=row.get('Mounts',[])
    if (not 1<=len(mounts)<=3 or any(m['Type']!='volume' or m['RW'] is not True for m in mounts)
            or len({m['Destination'] for m in mounts})!=len(mounts)
            or len({m.get('Name') for m in mounts})!=len(mounts)):
        raise ValueError('Exact bounded writable Kafka volumes required')
    directory=environment(row).get('KAFKA_LOG_DIRS')
    data=[m for m in mounts if m['Destination']==directory]
    auxiliary=[m for m in mounts if m['Destination']!=directory]
    declared=row['Config'].get('Volumes') or {}
    if (len(data)!=1 or any(m['Destination'] not in {'/etc/kafka/secrets','/mnt/shared/config'}
                            or m['Destination'] not in declared for m in auxiliary)):
        raise ValueError('Exact log volume and declared auxiliary destinations required')
    rows={v['Name']:v for v in volumes}
    if len(rows)!=len(volumes) or set(rows)!={m.get('Name') for m in mounts}:
        raise ValueError('Every existing broker volume must be inspected')
    main=copy.deepcopy(rows[data[0]['Name']])
    aux=[copy.deepcopy(rows[m['Name']]) for m in sorted(auxiliary,key=lambda m:m['Name'])]
    broker_records({'broker_volume':main,'broker_auxiliary_volumes':aux})
    return main,aux


def _broker_volume(groups, volumes):
    return _broker_inventory(groups,volumes)[0]


def capture_runtime(model, rows, volumes, bind_hashes):
    """Input rows must come from all-container observations, including stopped rows."""
    if model.get('name') != 'flash-ticketing':
        raise ValueError('Original project required')
    rows, retained = capture_retained(rows)
    groups = _groups(rows)
    volume, auxiliary_volumes = _broker_inventory(groups, volumes)
    actual_binds = {_path(m['Source']) for r in rows for m in r.get('Mounts', []) if m['Type'] == 'bind'}
    if set(bind_hashes) != actual_binds or any(not IDENTITY.fullmatch(v) for v in bind_hashes.values()):
        raise ValueError('Every original bind file needs exact content identity')
    result = copy.deepcopy(model)
    result['services'] = {r: result['services'][r] for r in groups}
    # Existing volume only: Compose must never create a replacement for a missing one.
    volume_keys={volume['Name']:'owned-kafka-data',**{v['Name']:'owned-kafka-aux-'+str(i) for i,v in enumerate(auxiliary_volumes)}}
    result['volumes'] = {volume_keys[v['Name']]:{'name':v['Name'],'external':True} for v in [volume,*auxiliary_volumes]}
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
                or host.get('PidsLimit') not in (None, 0)):
            raise ValueError('Unsupported runtime options need separate restoration qualification')
        limits=host.get('Ulimits')
        expected_limits=[{'Name':'nofile','Soft':65535,'Hard':65535}]
        if limits not in (None,[]) and (role!='load-balancer' or limits!=expected_limits):
            raise ValueError('Only the exact original load-balancer nofile limits are supported')
        service.pop('ulimits',None)
        if limits:service['ulimits']={'nofile':{'soft':65535,'hard':65535}}
        mode = host.get('NetworkMode')
        if mode not in (None, 'flash-ticketing_default'):
            raise ValueError('Exact original Compose default network required')
        service.update(image=row['Image'], pull_policy='never', environment=environment(row),
                       command=copy.deepcopy(row['Config']['Cmd'] or []),
                       entrypoint=copy.deepcopy(row['Config'].get('Entrypoint')),
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
            if mount['Type'] == 'volume' and role == 'kafka' and mount['Name'] in volume_keys:
                source = volume_keys[mount['Name']]
            elif mount['Type'] == 'bind':
                source = _path(mount['Source'])
            else:
                raise ValueError('Unsupported runtime mount')
            service['volumes'].append({'type': mount['Type'], 'source': source, 'target': mount['Destination'],
                                       'read_only': not mount['RW']})
        semantics[role] = digest(runtime_semantic(row))
    return {'schema': 1, 'decision': 'ADR0186', 'model': result, 'counts': dict(NORMAL_COUNTS),
            'semantics': semantics, 'broker_volume': volume, 'bind_sha256': dict(bind_hashes),
            'original_containers': sorted(r['Id'] for r in rows),
            **({'broker_auxiliary_volumes':auxiliary_volumes} if auxiliary_volumes else {}),
            **({'retained_inactive_containers': retained} if retained else {})}


def verify_restored(saved, rows_by_host, volumes, bind_hashes):
    if saved.get('decision') != 'ADR0186' or saved.get('schema') != 1 or rows_by_host.get('secondary') != []:
        raise ValueError('Exact saved runtime and empty secondary required')
    groups = _groups(rows_by_host['primary'])
    if (dict(Counter(r['Config']['Labels']['com.docker.compose.service'] for r in rows_by_host['primary'])) != saved['counts']
            or any(digest(runtime_semantic(row)) != saved['semantics'][role] for role, rs in groups.items() for row in rs)
            or _broker_inventory(groups, volumes) != (saved['broker_volume'],saved.get('broker_auxiliary_volumes',[]))
            or bind_hashes != saved['bind_sha256']):
        raise ValueError('Original runtime, broker volume or private file restoration differs')
    return {'runtime_restored': True, 'broker_volume_retained': True, 'bind_files_restored': True,
            'secondary_empty': True}

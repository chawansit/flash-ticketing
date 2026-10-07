"""ADR0184 read-only, host-aware identity inventory for the worker split."""
import copy
import hashlib
import json
import math
import re
from collections import Counter
from datetime import UTC, datetime

from collect_two_host_inventory import container_call, host_program
from runtime_source_identity import container_identity_program, source_identity_program
from two_host_topology import bindings, environment, fingerprint
from worker_separation_topology import PROJECT, validate_pair

ALL_CONTAINERS = "import json,subprocess;ids=subprocess.check_output(['docker','ps','-aq','--no-trunc'],text=True).split();print(json.dumps(json.loads(subprocess.check_output(['docker','inspect',*ids],text=True)) if ids else []))"
IMAGE_ENV = {'PATH', 'LANG', 'PYTHON_VERSION', 'PYTHON_SHA256', 'PYTHONUNBUFFERED', 'PYTHONDONTWRITEBYTECODE'}


def digest(value):
    return hashlib.sha256(json.dumps(value, sort_keys=True, separators=(',', ':')).encode()).hexdigest()


def expected_counts(pair, arm, host):
    validate_pair(pair)
    if arm not in {'control', 'candidate'} or host not in {'primary', 'secondary'}:
        raise ValueError('Exact worker placement arm/host required')
    return copy.deepcopy(pair[arm]['counts'][host])


def _port_matches(row, service, used):
    actual = bindings(row)
    expected = service.get('ports', [])
    if len(actual) != len(expected):
        raise ValueError('Exact private port publication required')
    for target, protocol, address, port in actual:
        matches = []
        for entry in expected:
            first, *end = map(int, str(entry['published']).split('-'))
            if (target == entry['target'] and protocol == entry.get('protocol', 'tcp')
                    and address == entry['host_ip'] and first <= port <= (end[0] if end else first)):
                matches.append(entry)
        if len(matches) != 1 or (address, port, protocol) in used:
            raise ValueError('Unexpected or duplicate private endpoint')
        used.add((address, port, protocol))
    return actual


def inspect_layout(pair, arm, rows_by_host):
    """Validate supplied observations; authenticity comes from collect()."""
    validate_pair(pair)
    if arm not in {'control', 'candidate'} or set(rows_by_host) != {'primary', 'secondary'}:
        raise ValueError('Complete exact two-host observations required')
    result, ids, ports = [], set(), set()
    for host, rows in rows_by_host.items():
        counts = Counter()
        planned = pair[arm][host]
        for row in rows:
            cid = row.get('Id', '')
            if not re.fullmatch(r'[0-9a-f]{64}', cid) or cid in ids:
                raise ValueError('Distinct full container identities required')
            ids.add(cid)
            labels = row['Config']['Labels']
            role = labels.get('com.docker.compose.service')
            project = 'flash-ticketing' if host == 'primary' else PROJECT
            if (labels.get('com.docker.compose.project') != project or not row['State']['Running']
                    or planned is None or role not in planned['services']):
                raise ValueError('Unexpected, stopped or overlapping host resource')
            service = planned['services'][role]
            env = environment(row)
            if (row['Image'] != service['image'] or any(env.get(k) != v for k, v in service['environment'].items())
                    or set(env) - set(service['environment']) - IMAGE_ENV
                    or ('command' in service and row['Config']['Cmd'] != service['command'])
                    or ('entrypoint' in service and row['Config'].get('Entrypoint') != service['entrypoint'])):
                raise ValueError('Image, command or settings drift')
            stamp = datetime.fromisoformat(row['State']['StartedAt'])
            if stamp.tzinfo is None:
                raise ValueError('Aware runtime start identity required')
            actual_ports = _port_matches(row, service, ports)
            mounts = row.get('Mounts', [])
            expected_mounts = []
            for mount in service.get('volumes', []):
                name = (planned.get('volumes', {}).get(mount['source'], {}).get('name')
                        if mount['type'] == 'volume' else mount['source'])
                expected_mounts.append((mount['type'], name, mount['target'], not mount.get('read_only', False)))
            actual_mounts = [(m['Type'], m.get('Name') if m['Type'] == 'volume' else m['Source'],
                              m['Destination'], m['RW']) for m in mounts]
            if sorted(actual_mounts) != sorted(expected_mounts):
                raise ValueError('Runtime mount or broker volume differs')
            counts[role] += 1
            result.append({'host_role': host, 'role': role, 'container_id': cid, 'image_id': row['Image'],
                           'started_at': row['State']['StartedAt'], 'project': project,
                           'runtime_fingerprint': fingerprint(row),
                           'ports': [list(p) for p in actual_ports]})
        if dict(counts) != expected_counts(pair, arm, host):
            raise ValueError('Exact per-host role counts required')
    return sorted(result, key=lambda r: (r['host_role'], r['role'], r['container_id']))


def process_start_program(port):
    if type(port) is not int or not 1024 <= port <= 65535:
        raise ValueError('Bounded local metrics port required')
    return ("import json,math,urllib.request\n"
            + "with urllib.request.urlopen('http://127.0.0.1:" + str(port) + "/metrics',timeout=2) as response:raw=response.read(4194305)\n"
            + "assert len(raw)<=4194304\nvalues=[float(line.split()[1]) for line in raw.decode().splitlines() if line.startswith('process_start_time_seconds ')]\n"
            + "assert len(values)==1 and math.isfinite(values[0]) and values[0]>0\nprint(json.dumps({'process_start_time_seconds':values[0]}))")


def collect(session, pair, arm, sources):
    """Compatibility entry for a single common source map; historical behavior retained."""
    source_identity_program(sources)
    from worker_separation_topology import WORKERS
    result = collect_roles(session, pair, arm, {role: sources for role in ('api', *WORKERS)})
    result['source_manifest_sha256'] = digest(sources)
    return result


def collect_roles(session, pair, arm, role_sources):
    """Read-only preflight. Never dispatch, deploy, classify recovery or authorize load."""
    from worker_separation_topology import WORKERS
    if not isinstance(role_sources, dict) or set(role_sources) != {'api', *WORKERS}:
        raise ValueError('Exact source expectations for every API and worker role required')
    role_sources = copy.deepcopy(role_sources)
    for sources in role_sources.values():source_identity_program(sources)
    before = {host: session.call(host, ALL_CONTAINERS, 45) for host in ('primary', 'secondary')}
    entries = inspect_layout(pair, arm, before)
    hosts = {role: session.call(role, host_program(), 45) for role in ('primary', 'secondary', 'generator')}
    if (len({h['machine_id_sha256'] for h in hosts.values()}) != 3
            or any(session.config[role]['private_ipv4'] not in h['addresses'] for role, h in hosts.items())
            or hosts['generator']['api_processes'] != 0 or hosts['generator']['load_processes'] != 0):
        raise ValueError('Distinct configured machines and idle dedicated generator required')
    for role in ('primary', 'secondary'):
        if hosts[role]['vcpus'] != 4 or not 7 * 2**30 <= hosts[role]['memory_bytes'] <= 9 * 2**30:
            raise ValueError('Existing matching ECS specifications required')
    memory = [hosts[role]['memory_bytes'] for role in ('primary', 'secondary')]
    if abs(memory[0] - memory[1]) > min(memory) * 0.05:
        raise ValueError('Matching ECS memory required')
    for role in ('primary', 'secondary'):
        if hosts[role]['api_processes'] != expected_counts(pair, arm, role).get('api', 0) or hosts[role]['load_processes'] != 0:
            raise ValueError('Unexpected native API or generator process')
    by_id = {r['Id']: r for rows in before.values() for r in rows}
    for entry in entries:
        if entry['role'] in {'kafka', 'pgbouncer', 'load-balancer'}:
            continue
        host, cid = entry['host_role'], entry['container_id']
        proof = session.call(host, container_identity_program(by_id[cid], entry['role'], role_sources[entry['role']],
                                                             readiness=entry['role'] == 'api'), 50)
        if (proof['container_id'] != cid or proof['image_id'] != entry['image_id']
                or proof['started_at'] != entry['started_at'] or proof['role'] != entry['role']
                or any(proof['source_identity'].get(k) is not True for k in ('source_hashes_match',
                    'app_source_hashes_match', 'import_source_hashes_match', 'import_code_matches_source'))
                or (entry['role'] == 'api' and proof['source_identity'].get('ready') is not True)):
            raise ValueError('Exact live source/import/readiness identity required')
        entry['source_identity_sha256'] = digest(proof['source_identity'])
        entry['source_expectations_sha256'] = digest(role_sources[entry['role']])
        port = 8000 if entry['role'] == 'api' else int(environment(by_id[cid]).get('WORKER_METRICS_PORT', '9101'))
        start = container_call(session, host, cid, process_start_program(port), 30)['process_start_time_seconds']
        if isinstance(start, bool) or not isinstance(start, (int, float)) or not math.isfinite(start) or start <= 0:
            raise ValueError('Exact metric process start required')
        target = [p for p in entry['ports'] if p[0] == port and p[1] == 'tcp']
        if len(target) != 1 or target[0][2] != session.config[host]['private_ipv4']:
            raise ValueError('Exactly one published metrics endpoint required')
        entry.update(process_start_time_seconds=start, metrics_container_port=port,
                     metrics_url='http://' + target[0][2] + ':' + str(target[0][3]) + '/metrics')
    after = {host: session.call(host, ALL_CONTAINERS, 45) for host in ('primary', 'secondary')}
    repeated = inspect_layout(pair, arm, after)
    comparable = [{k: v for k, v in entry.items() if k in repeated[0]} for entry in entries] if repeated else []
    if comparable != repeated:
        raise ValueError('Runtime changed during inventory qualification')
    report = {'schema': 1, 'decision': 'ADR0184', 'arm': arm, 'captured_at': datetime.now(UTC).isoformat(),
              'prepared_pair_sha256': digest(pair), 'source_manifest_sha256': digest(role_sources),
              'hosts': {role: {k: v for k, v in h.items() if k != 'addresses'} for role, h in hosts.items()},
              'containers': entries, 'budgets': copy.deepcopy(pair['budgets']), 'runtime_unchanged': True,
              'scope': 'Read-only identity preflight only; queue/financial/customer gates and live runner pending.'}
    return report


def cpu_spec(inventory, host):
    if host not in {'primary', 'secondary'} or inventory.get('decision') != 'ADR0184':
        raise ValueError('Explicit qualified worker inventory required')
    result = {'schema': 1, 'decision': 'ADR0184', 'placement': 'worker-separation', 'arm': inventory['arm'],
              'host_role': host, 'instance_uuid_sha256': inventory['hosts'][host]['machine_id_sha256'],
              'inventory_sha256': digest(inventory),
              'containers': [{'id': r['container_id'], 'role': r['role'], 'image_id': r['image_id'],
                              'started_at': r['started_at'], 'project': r['project']}
                             for r in inventory['containers'] if r['host_role'] == host]}
    from observe_two_host_cpu import validate_spec
    validate_spec(result)
    return result

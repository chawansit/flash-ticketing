import copy
import importlib.util
import io
import sys
from datetime import UTC, datetime, timedelta
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT / 'scripts'))
import observe_two_host_cpu as cpu
import observe_two_host_pipeline as frozen_loader
import worker_separation_inventory as inv
import worker_separation_pipeline as pipe
import worker_separation_topology as topology

NOW = datetime(2026, 10, 6, 12, tzinfo=UTC)


def pair():
    spec = importlib.util.spec_from_file_location('split_model_fixture', ROOT / 'tests/unit/test_worker_separation_topology.py')
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    model = module.model()
    for role, target, published in [('api', 8000, '8101-8104'), ('pgbouncer', 5432, '5432'), ('load-balancer', 8000, '8000')]:
        model['services'][role]['ports'] = [{'target': target, 'published': published, 'host_ip': '10.0.0.1', 'protocol': 'tcp'}]
    return topology.prepare_pair(model, primary_ip='10.0.0.1', secondary_ip='10.0.0.2')


def observed(pair, arm):
    hosts = {'primary': [], 'secondary': []}
    cid = 0
    for host, host_rows in hosts.items():
        for role, count in pair[arm]['counts'][host].items():
            service = pair[arm][host]['services'][role]
            for index in range(count):
                cid += 1
                ports = {}
                for entry in service.get('ports', []):
                    first = int(entry['published'].split('-')[0])
                    ports[str(entry['target']) + '/tcp'] = [{'HostIp': entry['host_ip'], 'HostPort': str(first + index)}]
                mounts = []
                for m in service.get('volumes', []):
                    mounts.append({'Type': m['type'], 'Name': pair[arm][host]['volumes'][m['source']]['name'],
                                   'Source': '/docker/data', 'Destination': m['target'], 'RW': True})
                host_rows.append({'Id': f'{cid:064x}', 'Image': service['image'],
                                   'Config': {'Env': [k + '=' + v for k, v in service['environment'].items()],
                                              'Cmd': service.get('command'), 'Entrypoint': None,
                                              'Labels': {'com.docker.compose.project': pair[arm][host]['name'],
                                                         'com.docker.compose.service': role}},
                                   'State': {'Running': True, 'StartedAt': NOW.isoformat()}, 'Mounts': mounts,
                                   'HostConfig': {'PortBindings': ports, 'RestartPolicy': {'Name': 'unless-stopped'}}})
    return hosts


def inventory(arm='candidate'):
    plan = pair()
    entries = inv.inspect_layout(plan, arm, observed(plan, arm))
    for entry in entries:
        if entry['role'] in {'kafka', 'pgbouncer', 'load-balancer'}:
            continue
        endpoint = entry['ports'][0]
        entry.update(source_identity_sha256='d' * 64, process_start_time_seconds=1000,
                     metrics_container_port=endpoint[0], metrics_url=f'http://{endpoint[2]}:{endpoint[3]}/metrics')
    return {'schema': 1, 'decision': 'ADR0184', 'arm': arm, 'captured_at': NOW.isoformat(),
            'runtime_unchanged': True, 'containers': entries, 'budgets': plan['budgets'],
            'hosts': {r: {'machine_id_sha256': x * 64} for r, x in [('primary', 'a'), ('secondary', 'b'), ('generator', 'c')]}}


@pytest.mark.parametrize('arm', ['control', 'candidate'])
def test_exact_layout_observes_all_replicas_without_publishing_secrets(arm):
    import json
    plan = pair()
    result = inv.inspect_layout(plan, arm, observed(plan, arm))
    assert len(result) == 21 and 'synthetic' not in json.dumps(result)
    assert sum(r['role'] == 'consumer' for r in result) == 6
    assert {r['host_role'] for r in result if r['role'] == 'consumer'} == {'primary' if arm == 'control' else 'secondary'}


@pytest.mark.parametrize('change', ['overlap', 'missing', 'stopped', 'image', 'command', 'env', 'unknown_env', 'project', 'public_port', 'volume'])
def test_layout_rejects_drift_and_unowned_resources(change):
    plan = pair()
    rows = observed(plan, 'candidate')
    row = rows['secondary'][0]
    if change == 'overlap':
        rows['primary'].append(copy.deepcopy(row))
    elif change == 'missing':
        rows['secondary'].pop()
    elif change == 'stopped':
        row['State']['Running'] = False
    elif change == 'image':
        row['Image'] = 'sha256:' + 'f' * 64
    elif change == 'command':
        row['Config']['Cmd'] = ['wrong']
    elif change == 'env':
        row['Config']['Env'] = [v.replace('DB_POOL_MAX=8', 'DB_POOL_MAX=9') for v in row['Config']['Env']]
    elif change == 'unknown_env':
        row['Config']['Env'].append('UNKNOWN_APPLICATION_OPTION=1')
    elif change == 'project':
        row['Config']['Labels']['com.docker.compose.project'] = 'unowned'
    elif change == 'public_port':
        row['HostConfig']['PortBindings']['9101/tcp'][0]['HostIp'] = '0.0.0.0'
    else:
        next(r for r in rows['primary'] if r['Config']['Labels']['com.docker.compose.service'] == 'kafka')['Mounts'][0]['Name'] = 'wrong-volume'
    with pytest.raises(ValueError):
        inv.inspect_layout(plan, 'candidate', rows)


@pytest.mark.parametrize('arm', ['control', 'candidate'])
def test_bound_cpu_specs_preserve_exact_counts_and_old_restrictions(arm):
    data = inventory(arm)
    for host in ['primary', 'secondary']:
        spec = inv.cpu_spec(data, host)
        assert cpu.validate_spec(spec) == spec['containers']
        assert spec['inventory_sha256'] == inv.digest(data)
    spec = inv.cpu_spec(data, 'primary')
    spec['containers'].pop()
    with pytest.raises(ValueError):
        cpu.validate_spec(spec)
    spec = inv.cpu_spec(data, 'secondary')
    spec.pop('decision')
    with pytest.raises(ValueError):
        cpu.validate_spec(spec)


def payload(counter=1, start=1000):
    return (f'process_start_time_seconds {start}\n'
            f'ticketing_worker_operations_total{{operation="consume_event",outcome="ok"}} {counter}\n'
            f'ticketing_worker_busy_seconds_total{{operation="consume_event"}} {counter}\n').encode()


def installed(data=None, fetch=None):
    data = data or inventory()
    module = frozen_loader.load_frozen(ROOT / 'scripts/observe_paid_pipeline.py')
    original = (module.sample, module.worker_counters)
    endpoints = pipe.install_adapter(module, data, approved_inventory_sha256=inv.digest(data), now=NOW,
                                    fetch=fetch or (lambda *args, **kwargs: io.BytesIO(payload())))
    assert (module.sample, module.worker_counters) == original
    return module, endpoints


def test_worker_parser_scrapes_every_exact_remote_replica_once():
    seen = []
    def fetch(url, **kwargs):
        seen.append(url)
        return io.BytesIO(payload())
    module, endpoints = installed(fetch=fetch)
    result = module.worker_counters('consumer', 'consume_event', 'http://consumer:9101/metrics')
    assert result['consumer_replicas'] == 6 and result['consumer_calls'] == 6
    assert len(seen) == len(set(seen)) == 6
    assert set(seen) == {r['metrics_url'] for r in endpoints.values() if r['role'] == 'consumer'}
    assert len(module.api_replicas()) == 4
    assert len(module.api_replicas('confirmation', 9101)) == 1
    with pytest.raises(ValueError):
        module.api_replicas('unowned', 9101)


@pytest.mark.parametrize('raw', [payload(start=1001), payload(counter=0), payload(counter=float('nan')), b'', b'x' * (pipe.MAX_PAYLOAD + 1)], ids=['restart', 'reset', 'nan', 'missing', 'oversized'])
def test_metric_identity_counter_or_payload_failure_stays_failed(raw):
    replies = iter([payload(), raw, payload()])
    module, _ = installed(fetch=lambda *args, **kwargs: io.BytesIO(next(replies)))
    label = module.api_replicas('consumer', 9101)[0]
    module.urlopen(f'http://{label}:9101/metrics')
    with pytest.raises((ValueError, UnicodeError)):
        module.urlopen(f'http://{label}:9101/metrics')
    with pytest.raises(ValueError, match='Previously failed'):
        module.urlopen(f'http://{label}:9101/metrics')


@pytest.mark.parametrize('change', ['digest', 'stale', 'missing', 'endpoint', 'source'])
def test_adapter_rejects_unqualified_inventory(change):
    data = inventory()
    approved = inv.digest(data)
    if change == 'digest':
        data['budgets']['api_connections'] += 1
    elif change == 'stale':
        data['captured_at'] = (NOW - timedelta(seconds=121)).isoformat()
    elif change == 'missing':
        data['containers'].pop()
    elif change == 'endpoint':
        next(r for r in data['containers'] if r['role'] == 'consumer')['metrics_url'] = 'http://8.8.8.8:9101/metrics'
    else:
        next(r for r in data['containers'] if r['role'] == 'consumer').pop('source_identity_sha256')
    if change != 'digest':
        approved = inv.digest(data)
    module = frozen_loader.load_frozen(ROOT / 'scripts/observe_paid_pipeline.py')
    with pytest.raises(ValueError):
        pipe.install_adapter(module, data, approved_inventory_sha256=approved, now=NOW)


def test_api_diagnostic_family_is_required_and_failure_is_sticky():
    module, _ = installed()
    label = module.api_replicas()[0]
    with pytest.raises(ValueError, match='diagnostic metric family'):
        module.api_metrics(label)
    with pytest.raises(ValueError, match='Previously failed'):
        module.api_metrics(label)


def test_api_diagnostics_retained_without_changing_financial_sample():
    raw = payload() + b'# TYPE ticketing_db_acquisition_failures_total counter\n'
    module, _ = installed(fetch=lambda *args, **kwargs: io.BytesIO(raw))
    value = module.api_metrics(module.api_replicas()[0])
    assert value['process_start_time_seconds'] == 1000
    assert value['acquisition_failure:payment:native_limit'] == 0


@pytest.mark.parametrize('arm', ['control', 'candidate'])
def test_cpu_roundtrip_preserves_inventory_identity_and_worker_host(arm):
    data = inventory(arm)
    reports = []
    for host in ('primary', 'secondary'):
        spec = inv.cpu_spec(data, host)
        result = {**spec, 'seconds': 5, 'interval': 5, 'requested_start_utc': NOW.isoformat(),
                  'samples': [{'utc': (NOW + timedelta(seconds=i)).isoformat(), 'elapsed_seconds': i,
                               'host_ticks': [100 + i, 0, 0, 100 + i, 0, 0, 0, 0],
                               'container_cpu_usec': {r['id']: i * 1000 for r in spec['containers']}}
                              for i in (0, 5)]}
        reports.append(cpu.summarize(result))
    comparison = cpu.compare_windows(*reports, offered_start_utc=NOW.isoformat(),
                                    offered_end_utc=(NOW + timedelta(seconds=5)).isoformat())
    assert comparison['same_offered_measurement_window'] is True
    assert reports[0]['inventory_sha256'] == reports[1]['inventory_sha256'] == inv.digest(data)
    assert ('consumer' in reports[1]['cpu_cores_by_role']) == (arm == 'candidate')
    reports[1]['inventory_sha256'] = 'f' * 64
    with pytest.raises(ValueError):
        cpu.compare_windows(*reports, offered_start_utc=NOW.isoformat(),
                            offered_end_utc=(NOW + timedelta(seconds=5)).isoformat())


def test_read_only_collector_proves_sources_on_the_actual_host():
    import re
    plan = pair()
    rows = observed(plan, 'candidate')
    seen = []
    sources = {'src/ticketing/config.py': 'a' * 64}
    by_id = {r['Id']: (host, r) for host, values in rows.items() for r in values}
    class Session:
        def __init__(self):
            self.config = {r: {'private_ipv4': ip} for r, ip in
                           [('primary', '10.0.0.1'), ('secondary', '10.0.0.2'), ('generator', '10.0.0.3')]}
        def call(self, host, code, timeout):
            if code == inv.ALL_CONTAINERS:
                return copy.deepcopy(rows[host])
            if code == inv.host_program():
                return {'machine_id_sha256': {'primary': 'a', 'secondary': 'b', 'generator': 'c'}[host] * 64,
                        'vcpus': 4, 'memory_bytes': 8 * 2**30, 'addresses': [self.config[host]['private_ipv4']],
                        'api_processes': 4 if host == 'primary' else 0, 'load_processes': 0}
            cid = re.search(r"[0-9a-f]{64}", code)[0]
            expected_host, row = by_id[cid]
            assert expected_host == host
            seen.append((host, row['Config']['Labels']['com.docker.compose.service']))
            if "['docker', 'exec'" in code:
                return {'process_start_time_seconds': 1000}
            return {'container_id': cid, 'image_id': row['Image'], 'started_at': row['State']['StartedAt'],
                    'role': row['Config']['Labels']['com.docker.compose.service'],
                    'source_identity': {'source_hashes_match': True, 'app_source_hashes_match': True,
                                        'import_source_hashes_match': True, 'import_code_matches_source': True, 'ready': True}}
    result = inv.collect(Session(), plan, 'candidate', sources)
    assert result['runtime_unchanged'] is True
    assert len([r for r in result['containers'] if 'metrics_url' in r]) == 18
    assert {host for host, role in seen if role == 'consumer'} == {'secondary'}
    assert {host for host, role in seen if role == 'api'} == {'primary'}


def test_invalid_confirmation_gauge_is_sticky():
    raw = payload() + b'ticketing_payment_confirmation_pending nan\n'
    replies = iter([raw, payload()])
    module, _ = installed(fetch=lambda *args, **kwargs: io.BytesIO(next(replies)))
    label = module.api_replicas('confirmation', 9101)[0]
    with pytest.raises(ValueError, match='confirmation metric'):
        module.urlopen(f'http://{label}:9101/metrics')
    with pytest.raises(ValueError, match='Previously failed'):
        module.urlopen(f'http://{label}:9101/metrics')

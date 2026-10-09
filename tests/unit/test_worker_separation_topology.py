import copy
import json
import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[2] / 'scripts'))
import worker_separation_topology as topology


def model():
    services = {}
    for index, role in enumerate([*topology.INFRA, *topology.WORKERS]):
        env = {'DB_POOL_MAX': '12', 'DATABASE_URL': 'postgresql://user:a%24b%23@pgbouncer:5432/db?sslmode=disable',
               'REDIS_URL': 'redis://:synthetic@10.0.0.3:6379/0', 'KAFKA_BOOTSTRAP': 'kafka:9092',
               'API_URL': 'http://api:8000', 'PAYMENT_CONFIRMATION_ASYNC': '0'}
        services[role] = {'image': 'sha256:' + f'{index + 1:064x}', 'environment': env,
                          'command': ['python', '-m', 'ticketing.workers', role],
                          'depends_on': {'pgbouncer': {'condition': 'service_started'}}}
    services['api']['environment'].update(DB_POOL_MAX='4', API_PAYMENT_POOL_MAX='2')
    services['consumer']['environment']['DB_POOL_MAX'] = '8'
    services['simulator']['environment']['DB_POOL_MAX'] = '10'
    services['confirmation']['environment']['DB_POOL_MAX'] = '2'
    services['pgbouncer']['environment'].update(DEFAULT_POOL_SIZE='24', RESERVE_POOL_SIZE='0', MAX_CLIENT_CONN='160')
    services['kafka']['environment'] = {
        'KAFKA_LISTENERS': 'PLAINTEXT://:9092,CONTROLLER://:9093',
        'KAFKA_ADVERTISED_LISTENERS': 'PLAINTEXT://kafka:9092',
        'KAFKA_LISTENER_SECURITY_PROTOCOL_MAP': 'CONTROLLER:PLAINTEXT,PLAINTEXT:PLAINTEXT',
        'KAFKA_LOG_DIRS': '/var/lib/kafka/data', 'KAFKA_CONTROLLER_QUORUM_VOTERS': '1@kafka:9093',
    }
    services['kafka']['volumes'] = [{'type': 'volume', 'source': 'kafka-data', 'target': '/var/lib/kafka/data'}]
    return {'name': 'flash-ticketing', 'services': services,
            'volumes': {'kafka-data': {'name': 'flash-ticketing_kafka-data','external':True}}}


def pair():
    return topology.prepare_pair(model(), primary_ip='10.0.0.1', secondary_ip='10.0.0.2')


def test_pair_is_one_factor_without_mutating_input_or_duplicating_workers():
    source = model()
    original = copy.deepcopy(source)
    result = topology.prepare_pair(source, primary_ip='10.0.0.1', secondary_ip='10.0.0.2')
    assert source == original
    assert result['control']['counts']['primary']['api'] == result['candidate']['counts']['primary']['api'] == 4
    assert set(result['candidate']['primary']['services']) == set(topology.INFRA)
    assert result['candidate']['counts']['secondary'] == topology.WORKERS
    assert result['budgets']['api_connections'] == 24
    assert result['budgets']['callback_pool_slots'] == 8
    assert result['budgets']['pgbouncer_server_connections'] == 24
    assert result['live_qualified'] is False and result['load_authorized'] is False


def test_workers_share_identical_private_routes_preserving_credentials_and_db_options():
    result = pair()
    for arm in ('control', 'candidate'):
        workers = result[arm]['primary']['services'] if arm == 'control' else result[arm]['secondary']['services']
        for role in topology.WORKERS:
            env = workers[role]['environment']
            assert env['DATABASE_URL'] == 'postgresql://user:a%24b%23@10.0.0.1:5432/db?sslmode=disable'
            assert env['KAFKA_BOOTSTRAP'] == '10.0.0.1:19092'
            assert env['API_URL'] == 'http://10.0.0.1:8000'
            assert 'depends_on' not in workers[role]


def test_broker_volume_and_controller_retained_and_listener_is_private():
    original = model()
    for arm in ('control', 'candidate'):
        prepared = pair()[arm]['primary']
        broker = prepared['services']['kafka']
        assert prepared['volumes'] == original['volumes']
        assert broker['volumes'] == original['services']['kafka']['volumes']
        assert broker['environment']['KAFKA_CONTROLLER_QUORUM_VOTERS'] == '1@kafka:9093'
        assert broker['ports'] == [{'target': 19092, 'published': '19092', 'host_ip': '10.0.0.1', 'protocol': 'tcp'}]
        assert broker['environment']['KAFKA_ADVERTISED_LISTENERS'] == 'PLAINTEXT://kafka:9092,PRIVATE://10.0.0.1:19092'


def test_each_worker_has_unique_private_metric_port_range():
    workers = pair()['candidate']['secondary']['services']
    used = set()
    for role, service in workers.items():
        binding = service['ports'][0]
        first, *rest = map(int, binding['published'].split('-'))
        ports = set(range(first, (rest[0] if rest else first) + 1))
        assert len(ports) == topology.WORKERS[role] and not ports & used
        used.update(ports)
        assert binding['host_ip'] == '10.0.0.2' and binding['target'] == 9101


@pytest.mark.parametrize('change', ['mutable_image', 'worker_mount', 'worker_network', 'extra_role', 'direct_db',
                                   'local_redis', 'pool_budget', 'broker_volume', 'broker_listener', 'env_file'])
def test_unsupported_input_fails_before_deployment(change):
    source = model()
    worker = source['services']['consumer']
    if change == 'mutable_image':
        worker['image'] = 'ticketing:latest'
    elif change == 'worker_mount':
        worker['volumes'] = [{'type': 'bind', 'source': '/arbitrary', 'target': '/app', 'read_only': True}]
    elif change == 'worker_network':
        worker['networks'] = ['unexpected']
    elif change == 'extra_role':
        source['services']['postgres'] = copy.deepcopy(worker)
    elif change == 'direct_db':
        worker['environment']['DATABASE_URL'] = worker['environment']['DATABASE_URL'].replace('pgbouncer', '10.0.0.9')
    elif change == 'local_redis':
        worker['environment']['REDIS_URL'] = 'redis://redis:6379/0'
    elif change == 'pool_budget':
        worker['environment']['DB_POOL_MAX'] = '9'
    elif change == 'broker_volume':
        source['volumes'].clear()
    elif change == 'broker_listener':
        source['services']['kafka']['environment']['KAFKA_ADVERTISED_LISTENERS'] = 'PLAINTEXT://0.0.0.0:9092'
    else:
        worker['env_file'] = ['/secrets/unbound']
    with pytest.raises(ValueError):
        topology.prepare_pair(source, primary_ip='10.0.0.1', secondary_ip='10.0.0.2')


@pytest.mark.parametrize('change', ['image', 'pool', 'replicas', 'callback_route', 'overlap', 'load_permission'])
def test_pair_validation_rejects_candidate_drift(change):
    result = pair()
    remote = result['candidate']['secondary']['services']
    if change == 'image':
        remote['consumer']['image'] = 'sha256:' + 'f' * 64
    elif change == 'pool':
        remote['consumer']['environment']['DB_POOL_MAX'] = '9'
    elif change == 'replicas':
        result['candidate']['counts']['secondary']['consumer'] = 7
    elif change == 'callback_route':
        remote['simulator']['environment']['API_URL'] = 'http://api:8000'
    elif change == 'overlap':
        result['candidate']['primary']['services']['consumer'] = copy.deepcopy(remote['consumer'])
    else:
        result['load_authorized'] = True
    with pytest.raises(ValueError):
        topology.validate_pair(result)


@pytest.mark.parametrize('address', ['0.0.0.0', '127.0.0.1', '203.0.113.1', '8.8.8.8', '::1'])
def test_only_private_existing_hosts_supported(address):
    with pytest.raises(ValueError):
        topology.prepare_pair(model(), primary_ip='10.0.0.1', secondary_ip=address)


def test_equal_hosts_rejected():
    with pytest.raises(ValueError):
        topology.prepare_pair(model(), primary_ip='10.0.0.1', secondary_ip='10.0.0.1')


def test_lifecycle_requires_recovery_drains_absence_and_restoration():
    steps = topology.lifecycle_steps('candidate')
    assert steps.index('verify_consumed_paid_failure_recovery') < steps.index('bind_fresh_registered_scope')
    assert steps.index('prove_original_workers_absent') < steps.index('start_exact_arm_workers')
    assert steps.index('retain_fixture_identity_before_dispatch') < steps.index('dispatch_bounded_paid_stage')
    assert steps.index('stop_arm_workers_and_prove_absence') < steps.index('restore_original_broker_volume_and_runtime_semantics')
    assert 'secret' not in json.dumps(steps)


def test_resolved_compose_default_network_is_supported():
    source = model()
    for role in topology.WORKERS:
        source['services'][role]['networks'] = {'default': None}
    result = topology.prepare_pair(source, primary_ip='10.0.0.1', secondary_ip='10.0.0.2')
    assert all('networks' not in v for v in result['candidate']['secondary']['services'].values())


def test_remote_workers_cannot_be_spread_across_unbound_hosts():
    result = pair()
    result['candidate']['secondary']['services']['consumer']['ports'][0]['host_ip'] = '10.0.0.4'
    with pytest.raises(ValueError):
        topology.validate_pair(result)

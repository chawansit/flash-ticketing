"""ADR0198 observer composition tests; no cloud transport or customer dispatch."""
import importlib.util
import io
import sys
from datetime import timedelta
from pathlib import Path
from types import SimpleNamespace

import pytest

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT / 'scripts'))
import diagnostic_connection as diagnostic
import observe_worker_pipeline as observer
import worker_separation_inventory as inventory_module


def fixture_module():
    spec = importlib.util.spec_from_file_location('worker_observer_fixtures', ROOT / 'tests/unit/test_worker_separation_observers.py')
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


F = fixture_module()


def data_and_spec():
    data = F.inventory()
    identity = diagnostic.identity('observer', 'ticketing')
    data['diagnostic_connection_binding'] = {'decision': 'ADR0180', 'database': 'ticketing',
        'identity_sha256': identity, 'endpoint': ['10.0.0.4', 5432], 'bundle_sha256': 'a' * 64, 'ca_sha256': 'b' * 64}
    parameters = {'host': '10.0.0.4', 'port': 5432, 'dbname': 'ticketing', 'user': 'observer',
                  'password': 'synthetic-local-test', 'sslmode': 'verify-full', 'sslrootcert': str(ROOT / 'tmp/diagnostic-ca.pem'),
                  'autocommit': True, 'connect_timeout': 5, 'prepare_threshold': None,
                  'options': diagnostic.READ_ONLY_OPTIONS}
    return data, diagnostic.ConnectionSpec(parameters, identity)


def row(data, elapsed=0):
    value = {'utc': (F.NOW + timedelta(seconds=elapsed)).isoformat(), 'extended_diagnostics': True,
             'issued_tickets': elapsed, 'database_wait_diagnostics': {'complete': True},
             'host_cpu_ticks': [100 + elapsed, 0, 0, 100 + elapsed, 0, 0, 0, 0],
             'pgbouncer': {'stats': {'xacts': elapsed}, 'pools': {'active': 0}, 'maxwait_seconds': 0}}
    for role, count in {'api': 4, **observer.ROLE_COUNTS}.items():
        actual_role = 'reservation-writer' if role == 'writer' else role
        metrics = {entry['container_id']: {'process_start_time_seconds': 1000,
                    'process_cpu_seconds_total': elapsed, 'business_http_requests_total': elapsed}
                   for entry in data['containers'] if entry['role'] == actual_role}
        value['api_replicas' if role == 'api' else role + '_db_replicas'] = metrics
        if role != 'api':
            value[role + '_replicas'] = count
    return value


def configured(data=None, spec=None):
    default_data, default_spec = data_and_spec()
    return observer.configure(data or default_data, ROOT / 'scripts/observe_paid_pipeline.py',
        approved_sha256=inventory_module.digest(data or default_data), diagnostic=spec or default_spec,
        source_dsn='dbname=ticketing host=10.0.0.1 port=5432', now=F.NOW,
        fetch=lambda *args, **kwargs: io.BytesIO(F.payload() + b'# TYPE ticketing_db_acquisition_failures_total counter\n'))


def test_composition_isolates_parsers_driver_and_confirmation_from_other_imports():
    import psycopg
    first, second = configured(), configured()
    assert first is not second and first.METRICS is not second.METRICS
    assert first.psycopg is not psycopg and first.psycopg.connect.original is psycopg.connect
    assert first.psycopg.connect is not second.psycopg.connect
    assert len(first.api_replicas()) == 4
    assert first.worker_counters('confirmation', 'confirm_one', 'http://confirmation:9101/metrics')['confirmation_replicas'] == 1
    assert first.sample.__module__ == 'database_wait_evidence'
    assert first.worker_counters.__module__ == 'frozen_paid_pipeline'
    assert 'confirmation' not in F.frozen_loader.load_frozen(ROOT / 'scripts/observe_paid_pipeline.py').METRICS


@pytest.mark.parametrize('field,value', [('host', '10.0.0.5'), ('dbname', 'other'), ('sslmode', 'disable'),
    ('options', ''), ('autocommit', False), ('connect_timeout', 30)])
def test_library_connection_cannot_change_identity_tls_or_read_only_options(field, value):
    data, spec = data_and_spec()
    changed = dict(spec.parameters, **{field: value})
    with pytest.raises(ValueError):
        configured(data, diagnostic.ConnectionSpec(changed, spec.identity_sha256))


@pytest.mark.parametrize('role', ['api', *observer.ROLE_COUNTS])
@pytest.mark.parametrize('change', ['missing', 'replaced', 'restart', 'no_cpu', 'nan_cpu'])
def test_each_process_must_be_present_with_bound_identity_and_finite_cpu(role, change):
    data, _ = data_and_spec()
    sample = row(data)
    assert observer.startup(sample, data)['pass'] is True
    metrics = sample['api_replicas' if role == 'api' else role + '_db_replicas']
    first = next(iter(metrics))
    if change == 'missing':metrics.pop(first)
    elif change == 'replaced':metrics['f' * 64] = metrics.pop(first)
    elif change == 'restart':metrics[first]['process_start_time_seconds'] += 1
    elif change == 'no_cpu':metrics[first].pop('process_cpu_seconds_total')
    else:metrics[first]['process_cpu_seconds_total'] = float('nan')
    assert observer.startup(sample, data)['pass'] is False


@pytest.mark.parametrize('change', ['diagnostics', 'extended', 'error', 'bool_count'])
def test_startup_retains_existing_and_full_visibility_gates(change):
    data, _ = data_and_spec()
    sample = row(data)
    if change == 'diagnostics':sample['database_wait_diagnostics']['complete'] = False
    elif change == 'extended':sample['extended_diagnostics'] = False
    elif change == 'error':sample['confirmation_metrics_error'] = 'ValueError'
    else:sample['confirmation_replicas'] = True
    assert observer.startup(sample, data)['pass'] is False


def test_summary_extends_frozen_gates_and_does_not_claim_capacity():
    data, _ = data_and_spec()
    samples = [row(data, i) for i in range(6)]
    result = observer.summarize(samples, data, offered_start_utc=F.NOW.isoformat(),
                                offered_end_utc=(F.NOW + timedelta(seconds=5)).isoformat())
    assert result['pass'] is True and result['role_database']['confirmation']['observed_replicas'] == 1
    assert result['distribution']['per_replica_traffic_distribution'] is True
    assert samples[0]['api_replicas'].keys() == row(data)['api_replicas'].keys()
    assert not any('capacity' in key for key in result)


def test_confirmation_reset_or_missing_sample_cannot_be_hidden_by_old_summary():
    data, _ = data_and_spec()
    samples = [row(data, i) for i in range(6)]
    target = next(iter(samples[-1]['confirmation_db_replicas'].values()))
    target['process_cpu_seconds_total'] = 0
    args = {'offered_start_utc': F.NOW.isoformat(), 'offered_end_utc': (F.NOW + timedelta(seconds=5)).isoformat()}
    assert observer.summarize(samples, data, **args)['pass'] is False
    samples = [row(data, i) for i in range(6)]
    samples[2]['confirmation_db_replicas'] = {}
    assert observer.summarize(samples, data, **args)['pass'] is False


@pytest.mark.parametrize('change', ['gap', 'no_business', 'wrong_keys', 'restart'])
def test_distribution_keeps_historical_offered_window_requirements(change):
    data, _ = data_and_spec()
    samples = [row(data, i) for i in range(6)]
    if change == 'gap':samples = samples[:1] + samples[3:]
    elif change == 'no_business':
        target = next(iter(samples[0]['api_replicas']))
        for sample in samples:sample['api_replicas'][target]['business_http_requests_total'] = 0
    elif change == 'wrong_keys':samples[2]['api_replicas'].pop(next(iter(samples[2]['api_replicas'])))
    else:next(iter(samples[2]['api_replicas'].values()))['process_start_time_seconds'] += 1
    with pytest.raises(ValueError):
        observer.distribution(samples, data, offered_start_utc=F.NOW.isoformat(),
                              offered_end_utc=(F.NOW + timedelta(seconds=5)).isoformat())


def test_cli_restores_argv_on_frozen_failure_without_global_driver_mutation(tmp_path, monkeypatch):
    import json
    data, spec = data_and_spec()
    path = tmp_path / 'inventory.private.json'
    path.write_text(json.dumps(data))
    monkeypatch.setenv('TEST_DATABASE_URL', 'dbname=ticketing')
    monkeypatch.setattr(observer, 'diagnostic_spec', lambda *a, **k: spec)
    called = []
    def main():
        called.append(sys.argv)
        raise RuntimeError('synthetic frozen failure')
    monkeypatch.setattr(observer, 'configure', lambda *a, **k: SimpleNamespace(main=main))
    previous = sys.argv
    with pytest.raises(RuntimeError):
        observer.main(['--inventory', str(path), '--frozen-observer', 'frozen.py',
                      '--approved-inventory-sha256', inventory_module.digest(data),
                      '--diagnostic-connection-bundle', str(tmp_path / 'diagnostic.private.json'),
                      '--manifest', 'owned.private.json', '--seconds', '5'])
    assert sys.argv is previous
    assert called == [['frozen.py', '--manifest', 'owned.private.json', '--seconds', '5']]

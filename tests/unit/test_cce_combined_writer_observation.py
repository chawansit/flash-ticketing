"""Exercise combined identity through actual observer startup and summary gates."""
import copy
import sys
from pathlib import Path
from types import SimpleNamespace

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[2] / 'scripts'))
import cce_paid_observers as observations
import cce_simulator_roundtrip_profile as profile
import cce_worker_identity as identity
import cce_writer_scale_verification as verification
import diagnostic_runner_connection as diagnostics
from cce_paid_profiles import RATE_PUSH_160
from observe_paid_pipeline import pipeline_startup_view
from summarize_paid_pipeline import summarize_resources
from test_cce_combined_inventory_renewal import (  # noqa: F401 - shared autouse fixture
    arguments,
    inactive_authorization,
)
from test_observe_cce_paid_pipeline import bundle as api_bundle


@pytest.fixture
def row():
    counts = {'writer': 4, 'maintenance': 1, 'reconciler': 1,
              'simulator': 1, 'publisher': 1, 'consumer': 6}
    value = {'utc': '2026-10-17T12:00:00+00:00', 'issued_tickets': 0,
             'api_replicas': {str(i): {} for i in range(4)}, 'extended_diagnostics': True,
             'host_cpu_ticks': [100] * 8, 'database_wait_diagnostics': {'complete': True},
             'pgbouncer': {'stats': {'total_xact_count': 1}, 'pools': {'sv_active': 1},
                           'maxwait_seconds': 0}}
    for role, count in counts.items():
        value[role + '_replicas'] = count
        value[role + '_db_replicas'] = {str(i): {'process_cpu_seconds_total': 0}
                                       for i in range(count)}
    return value


@pytest.mark.parametrize('arm', ['control', 'candidate'])
@pytest.mark.parametrize('fault', [None, 'writer', 'cpu', 'pool', 'identity'])
def test_combined_declared_gates_preserve_exact_evidence(monkeypatch, row, arm, fault):
    _, _, kwargs, _ = arguments(monkeypatch, arm)
    bundle = kwargs['workers_before']
    assert not pipeline_startup_view(row, 6)['pass']  # Reproduce historical fallback.
    if fault == 'writer':
        row['writer_replicas'] = 3
    elif fault == 'cpu':
        row['writer_db_replicas']['3'].pop('process_cpu_seconds_total')
    elif fault == 'pool':
        row.pop('pgbouncer')
    elif fault == 'identity':
        bundle['sources']['src/ticketing/api.py'] = '0' * 64
        with pytest.raises(ValueError):
            verification.startup(row, bundle, pipeline_startup_view)
        return
    assert verification.startup(row, bundle, pipeline_startup_view)['pass'] is (fault is None)
    second = copy.deepcopy(row)
    second['host_cpu_ticks'][0] += 10
    summary = verification.summary([row, second], bundle,
                                  lambda rows: {'resources': summarize_resources(rows)})
    assert summary['resources']['pass'] is (fault is None)
    pipeline = {'api': {'observed_replicas': 4}, 'consumer': {'max_replicas': 6},
                'api_metrics_errors': 0, 'database_errors': 0, 'resources': summary['resources'],
                'role_database': {'writer': {'observed_replicas': 4, 'counter_reset_detected': False}},
                'writer_phases': {'counter_reset_detected': False}}
    assert verification.pipeline(pipeline, bundle, lambda value: False) is (fault is None)
    pipeline['writer_phases']['counter_reset_detected'] = True
    assert not verification.pipeline(pipeline, bundle, lambda value: True)


@pytest.mark.parametrize('arm', ['control', 'candidate'])
@pytest.mark.parametrize('fault', [None, 'writer', 'database_wait'])
def test_actual_startup_reaches_contract_with_combined_bundle(monkeypatch, tmp_path, row, arm, fault):
    goal, _, kwargs, _ = arguments(monkeypatch, arm)
    image = profile.image(goal)
    api = copy.deepcopy(api_bundle())
    settings = observations.cce.contract()
    api.update(run=kwargs['run'], api_sources=image['runtime_source_sha256'],
               manifest_digest=image['registry_manifest_digest'], resources=settings['resources'])
    for receipt in api['receipts']:
        receipt.update(image_id=image['registry_image'], resources=settings['resources'])
        receipt['startup_proof'].update(run=api['run'], sources=api['api_sources'])
    if fault == 'writer':
        row['writer_replicas'] = 3
    elif fault == 'database_wait':
        row['database_wait_diagnostics']['complete'] = False
    monkeypatch.setattr(observations, 'api_upload', lambda *args: None)
    monkeypatch.setattr(diagnostics, 'prepare', lambda *args: {})
    monkeypatch.setattr(diagnostics, 'qualify_bound_inventory', lambda *args: None)
    launched, verified = [], []
    def call(cid, program, timeout):
        compile(program, 'offline-startup', 'exec')
        if 'native_helpers_verified' in program:
            return {'native_helpers_verified': True}
        if 'read_text().splitlines()[0]' in program:
            kafka = {'utc': row['utc'], 'members': 6, 'total_lag': 0}
            projection = {'utc': row['utc'], 'members': 1, 'total_lag': 0,
                          'assigned_partitions': 6, 'group': 'ticketing-seat-projection-v1',
                          'partitions': [{'partition': i} for i in range(6)]}
            return {'pipeline': row, 'kafka': kafka, 'projection-kafka': projection}
        return {'fresh': True}
    session = SimpleNamespace(api=call, call=lambda *args: {'fresh': True})
    stage = SimpleNamespace(profile=RATE_PUSH_160, session=session, run=api['run'], cid='c' * 64,
                            manifest={'environment': 'development', 'show_ids': ['synthetic']},
                            output=tmp_path, record={}, job_list=[], check=lambda *args: None,
                            checkpoint=lambda: None, guard=SimpleNamespace(binding={}),
                            jobs=SimpleNamespace(launch=lambda *args, **kw: launched.append(True)))
    observer = observations.Observers(stage, {},
        SimpleNamespace(verify_pipeline_startup=lambda value: verified.append(value)), None, '/offline')
    observer.native_workers = kwargs['workers_before']
    assert len(identity.validate_bundle(observer.native_workers)) == 15
    if fault:
        with pytest.raises(ValueError, match='startup failed'):
            observer.start(api['receipts'])
        assert not observer.started and not verified
    else:
        observer.start(api['receipts'])
        assert observer.started and verified == [row] and len(launched) == 3
        assert observer.record['pipeline_startup']['writer_count'] == 4

import json
import sys
from datetime import UTC, datetime, timedelta
from pathlib import Path
from uuid import uuid4

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[2] / 'scripts'))
import fixture_identity_evidence as evidence

NOW = datetime(2026, 10, 6, 11, tzinfo=UTC)


def fixture(count=1):
    return {'schema_version': 1, 'environment': 'development', 'fixture_id': str(uuid4()),
            'created_at': NOW.isoformat(), 'sale_ends': (NOW + timedelta(minutes=59)).isoformat(),
            'shows': count, 'seats_per_show': 300, 'fixture_layout': 'distributed',
            'show_ids': [str(uuid4()) for _ in range(count)], 'viewer_tokens': ['must-not-retain'],
            'DATABASE_URL': 'must-not-retain', 'origin': 'must-not-retain'}


def stage(tmp_path):
    local = tmp_path / 'candidate'
    local.mkdir()
    return local, {'arm': 'candidate', 'customers_dispatched': False, 'pass': False}


@pytest.mark.parametrize('count', [1, 60, 84])
def test_identity_is_minimal_exact_and_independent(count):
    source = fixture(count)
    result = evidence.fixture_identity(source, count, now=NOW)
    assert len(result['show_ids']) == count and 'must-not-retain' not in json.dumps(result)
    source['show_ids'].clear()
    assert len(result['show_ids']) == count


def test_identity_persisted_and_bound_in_stage_before_return(tmp_path):
    local, record = stage(tmp_path)
    source = fixture()
    receipt = evidence.retain_fixture_identity(local, record, source, 1, now=NOW)
    assert json.loads((local / 'fixture-identity.json').read_text()) == receipt
    assert json.loads((local / 'stage.private.json').read_text())['fixture_identity'] == receipt
    assert len(receipt['fixture_identity_sha256']) == 64
    with pytest.raises(FileExistsError):
        evidence.retain_fixture_identity(local, record, fixture(), 1, now=NOW)
    assert json.loads((local / 'fixture-identity.json').read_text()) == receipt


@pytest.mark.parametrize('change', ['duplicate', 'missing', 'count', 'layout', 'environment', 'seats',
                                   'canonical', 'stale', 'future', 'closed', 'naive', 'boolean_schema'])
def test_bad_fixture_stops_before_receipt_creation(tmp_path, change):
    local, record = stage(tmp_path)
    value = fixture(60)
    if change == 'duplicate':
        value['show_ids'][1] = value['show_ids'][0]
    elif change == 'missing':
        value['show_ids'].pop()
    elif change == 'count':
        value['shows'] = 61
    elif change == 'layout':
        value['fixture_layout'] = 'single-show'
    elif change == 'environment':
        value['environment'] = 'production'
    elif change == 'seats':
        value['seats_per_show'] = 301
    elif change == 'canonical':
        value['fixture_id'] = value['fixture_id'].upper()
    elif change == 'stale':
        value['created_at'] = (NOW - timedelta(seconds=121)).isoformat()
    elif change == 'future':
        value['created_at'] = (NOW + timedelta(seconds=1)).isoformat()
    elif change == 'closed':
        value['sale_ends'] = NOW.isoformat()
    elif change == 'naive':
        value['created_at'] = NOW.replace(tzinfo=None).isoformat()
    else:
        value['schema_version'] = True
    with pytest.raises(ValueError):
        evidence.retain_fixture_identity(local, record, value, 60, now=NOW)
    assert not list(local.iterdir())


def test_stage_write_failure_preserves_receipt_and_stops_dispatch(tmp_path):
    local, record = stage(tmp_path)
    value = fixture()
    (local / 'stage.private.json').write_text('preexisting')
    with pytest.raises(FileExistsError):
        evidence.retain_fixture_identity(local, record, value, 1, now=NOW)
    assert record['customers_dispatched'] is False
    assert record['fixture_identity']['fixture_identity']['show_ids'] == value['show_ids']
    assert (local / 'stage.private.json').read_text() == 'preexisting'
    assert (local / 'fixture-identity.json').exists()


def test_dispatched_stage_and_wrong_directory_rejected(tmp_path):
    local, record = stage(tmp_path)
    record['customers_dispatched'] = True
    with pytest.raises(ValueError):
        evidence.retain_fixture_identity(local, record, fixture(), 1, now=NOW)
    record.update(arm='control', customers_dispatched=False)
    with pytest.raises(ValueError):
        evidence.retain_fixture_identity(local, record, fixture(), 1, now=NOW)


def test_helper_source_is_bound_and_retention_precedes_minting():
    import inspect

    import run_two_host_paid_comparison as runner
    assert 'scripts/fixture_identity_evidence.py' in runner.adapter_identity()
    source = inspect.getsource(runner.Stages.__call__)
    assert source.index('event_ids = fixture["show_ids"]') < source.index('retain_fixture_identity(') < source.index('mint = (')
    assert source.index('retain_fixture_identity(') < source.index('customers_dispatched"] = True')


def test_real_stage_retires_exact_fixture_when_receipt_write_fails(monkeypatch, tmp_path):
    import run_two_host_paid_comparison as runner

    value = fixture(60)
    api_calls = []
    class Session:
        def __init__(self):
            self.config = {'primary': {'private_ipv4': '10.0.0.1'}, 'generator': {'repo': '/generator'}}
            self.state = {}
        def call(self, *args):
            return {'fresh': True, 'private_manifests_removed': True}
        def api(self, cid, program, timeout):
            api_calls.append(program)
            if "r=subprocess.run(['python','/app/scripts/prepare_capacity_fixture.py'" in program:
                return value
            if 'UPDATE events SET sale_ends=' in program:
                assert repr(value['show_ids']) in program
                return {'count': 60}
            return {'fresh': True, 'frozen_helpers_verified': True}
        def checkpoint(self):
            pass

    def fail_write(*args, **kwargs):
        raise PermissionError('synthetic disk write failure')

    monkeypatch.setattr(runner, 'observe', lambda *args, **kwargs: ({}, {}, [], []))
    monkeypatch.setattr(runner, 'retain_fixture_identity', fail_write)
    names = ('observe_paid_pipeline.py', 'kafka_lag_observe.py', 'prepare_capacity_fixture.py',
             'audit_checkout_smoke.py', 'capacity_queue_state.py')
    bundle = {'scripts/' + name: b'synthetic' for name in names}
    stages = runner.Stages(True, bundle)
    roles = ('api', 'consumer', 'reservation-writer', 'maintenance', 'publisher', 'reconciler', 'simulator')
    saved = {'model': {'services': {r: {'image': 'sha256:' + 'a' * 64, 'environment': {}} for r in roles}}}
    session = Session()
    with pytest.raises(PermissionError, match='disk write'):
        stages(session, 'candidate', [{'host_role': 'primary', 'container_id': 'a' * 64}],
               saved, '/owned', tmp_path)
    record = stages.results['candidate']
    assert record['retired_shows'] == 60 and record['private_cleanup_pass'] is True
    assert record['customers_dispatched'] is False
    assert not any('jwt.encode' in program for program in api_calls)
    assert (tmp_path / 'candidate' / 'stage.private.json').is_file()

"""Strict clock resampling retains every rejected observation; no cloud calls."""
import ast
import copy
import sys
from pathlib import Path
from types import SimpleNamespace

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[2] / 'scripts'))
import cce_fixture_clock as clock
from test_fixture_identity_evidence import NOW, fixture


def stage_for(monkeypatch, latencies, fault=None):
    snapshots, calls = [], []
    wall, mono = [], []
    for index, latency in enumerate(latencies):
        wall.extend([NOW.timestamp()-latency, NOW.timestamp()])
        mono.extend([100+index*100, 100+index*100+latency])
    wi, mi = iter(wall), iter(mono)
    monkeypatch.setattr(clock.time, 'time', lambda: next(wi))
    monkeypatch.setattr(clock.time, 'monotonic', lambda: next(mi, mono[-1]))
    def api(cid, source, timeout):
        assert cid == 'owned' and timeout == 30
        tree = ast.parse(source)
        dictionary = next(n for n in ast.walk(tree) if isinstance(n, ast.Dict))
        nonce = ast.literal_eval(dictionary.values[0])
        calls.append(nonce)
        if fault == 'transport': raise TimeoutError('synthetic transport failure')
        if fault == 'nonce': nonce = 'wrong'
        return {'nonce': nonce, 'utc': 'invalid' if fault == 'utc' else NOW.isoformat()}
    stage = SimpleNamespace(session=SimpleNamespace(api=api), cid='owned', record={})
    stage.checkpoint = lambda: snapshots.append(copy.deepcopy(stage.record))
    return stage, calls, snapshots


def test_rejected_then_valid_probe_keeps_raw_evidence_and_strict_bounds(monkeypatch):
    stage, calls, snapshots = stage_for(monkeypatch, [6, 0.2])
    value = fixture()
    result = clock.sample_creator(stage, value)
    attempts = stage.record['fixture_clock_attempts'][value['fixture_id']]
    assert [r['status'] for r in attempts] == ['REJECTED_UNCERTAINTY', 'ACCEPTED']
    assert len(set(calls)) == 2 and result.tzinfo is not None
    assert attempts[0]['mono_end']-attempts[0]['mono_start'] == 6
    assert any(s['fixture_clock_attempts'][value['fixture_id']][0]['status'] == 'OBSERVED' for s in snapshots)
    assert clock.contract()['maximum_clock_offset_seconds'] == 2
    assert clock.contract()['maximum_probe_round_trip_seconds'] == 5


def test_persistent_uncertainty_stops_after_three_without_proof(monkeypatch):
    stage, calls, _ = stage_for(monkeypatch, [6, 6, 6])
    value = fixture()
    with pytest.raises(ValueError, match='uncertainty'): clock.sample_creator(stage, value)
    assert len(calls) == 3 and len(set(calls)) == 3
    assert 'fixture_clock_evidence' not in stage.record
    assert all(r['status'] == 'REJECTED_UNCERTAINTY' for r in stage.record['fixture_clock_attempts'][value['fixture_id']])


@pytest.mark.parametrize('fault', ['nonce', 'utc', 'transport'])
def test_malformed_or_transport_failure_is_never_resampled(monkeypatch, fault):
    stage, calls, _ = stage_for(monkeypatch, [0.2], fault)
    with pytest.raises((ValueError, TimeoutError)): clock.sample_creator(stage, fixture())
    assert len(calls) == 1 and 'fixture_clock_evidence' not in stage.record
    assert next(iter(stage.record['fixture_clock_attempts'].values()))[0]['status'] in {'MALFORMED', 'TRANSPORT_FAILED'}


def test_success_is_one_probe_and_replay_is_rejected(monkeypatch):
    stage, calls, _ = stage_for(monkeypatch, [0.2])
    value = fixture()
    clock.sample_creator(stage, value)
    before = copy.deepcopy(stage.record)
    with pytest.raises(ValueError, match='no replay'): clock.sample_creator(stage, value)
    assert len(calls) == 1 and stage.record == before

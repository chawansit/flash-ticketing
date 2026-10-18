"""ADR0353 admission boundaries without cloud traffic or live authorization."""
import sys
from datetime import UTC, datetime, timedelta
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[2] / 'scripts'))
import cce_simulator_roundtrip_profile as profile
from test_cce_simulator_roundtrip_profile import control_fixture, goal


def timed(arm='control', preview=False):
    value = goal(arm, preview)
    value.update(simulator_roundtrip_timing_decision='ADR0353', replacement_rds_decision='ADR0352',
                 payment_slot_snapshot_path='tmp/adr0352-new-rds/snapshot.private.json')
    return value


@pytest.mark.parametrize('arm,limit', [('control', 3600), ('candidate', 7200)])
def test_fixed_deadline_and_reserve_boundaries(arm, limit):
    value = timed(arm)
    start = datetime(2026, 10, 18, tzinfo=UTC)
    value['simulator_roundtrip_pair']['started_at_utc'] = start.isoformat()
    assert profile.require_pair_time(value, 900, now=start+timedelta(seconds=limit-900))
    with pytest.raises(ValueError, match='PAIR_TIME_BUDGET_EXHAUSTED'):
        profile.require_pair_time(value, 900, now=start+timedelta(seconds=limit-899))
    assert profile.require_pair_time(value, now=start+timedelta(seconds=limit))
    with pytest.raises(ValueError, match='PAIR_TIME_BUDGET_EXHAUSTED'):
        profile.require_pair_time(value, now=start+timedelta(seconds=limit+1))


@pytest.mark.parametrize('arm', ['control', 'candidate'])
def test_historical_pair_never_gains_extra_hour(arm):
    value = goal(arm, preview=False)
    now = datetime.now(UTC)
    value['simulator_roundtrip_pair']['started_at_utc'] = (now-timedelta(seconds=3601)).isoformat()
    with pytest.raises(ValueError, match='PAIR_TIME_BUDGET_EXHAUSTED'):
        profile.require_pair_time(value, now=now)


@pytest.mark.parametrize('fault', ['decision', 'replacement', 'preview', 'naive', 'future', 'negative', 'bool'])
def test_malformed_timing_fails_closed(fault):
    value = timed(preview=fault == 'preview')
    now = datetime.now(UTC)
    seconds = 0
    if fault == 'decision': value['simulator_roundtrip_timing_decision'] = 'ADR0354'
    if fault == 'replacement': value.pop('replacement_rds_decision')
    if fault == 'naive': value['simulator_roundtrip_pair']['started_at_utc'] = now.replace(tzinfo=None).isoformat()
    if fault == 'future': value['simulator_roundtrip_pair']['started_at_utc'] = (now+timedelta(seconds=1)).isoformat()
    if fault == 'negative': seconds = -1
    if fault == 'bool': seconds = True
    with pytest.raises(ValueError): profile.require_pair_time(value, seconds, now=now)


def test_timing_binding_rejects_old_control(monkeypatch):
    value = timed('candidate')
    binding = control_fixture(monkeypatch, value)
    assert profile.require_control(value)[1] == binding
    binding['cce_simulator_roundtrip_contract'].pop('pair_timing')
    binding['cce_simulator_roundtrip_contract']['pair_seconds_limit'] = 3600
    with pytest.raises(ValueError, match='another pair'):
        profile.require_control(value)


def test_timing_does_not_authorize_preview_reservation(monkeypatch):
    value = timed(preview=True)
    assert profile.selected(value)
    monkeypatch.setattr(profile, 'registry_image', dict)
    with pytest.raises(ValueError, match='LOCAL_ONLY'): profile.require_live(value)

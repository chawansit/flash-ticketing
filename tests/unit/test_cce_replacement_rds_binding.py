"""Replacement RDS cannot broaden snapshot/target or paired-control permissions."""
import copy
import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[2] / 'scripts'))
import cce_replacement_rds_binding as replacement
import cce_simulator_roundtrip_profile as profile
import work_envelope as policy
from test_cce_simulator_roundtrip_profile import goal


def prepared(arm='control'):
    value = goal(arm, preview=True)
    value.update(replacement_rds_decision='ADR0352', payment_slot_snapshot_path=replacement.NEW)
    return value


def test_exact_captured_replacement_snapshot_and_target():
    value = replacement.contract(prepared())
    assert value['restoration_endpoint'] == '10.1.21.48'
    assert value['snapshot_sha256'] == replacement.NEW_SHA
    assert replacement.validate_binding(prepared(), {'diagnostic_target_sha256': value['diagnostic_target_sha256']}) == value
    with pytest.raises(ValueError):
        replacement.validate_binding(prepared(), {'diagnostic_target_sha256': '0' * 64})


@pytest.mark.parametrize('fault', ['snapshot', 'decision', 'source', 'target', 'certificate'])
def test_changed_replacement_binding_is_rejected(monkeypatch, fault):
    value = prepared()
    read = policy.read
    def altered(path):
        result = copy.deepcopy(read(path))
        if Path(path).resolve() == (policy.ROOT / replacement.NEW).resolve() and fault == 'source':
            result['saved']['model']['services']['api']['environment']['DB_POOL_MAX'] = '99'
        if Path(path).resolve() == (policy.ROOT / replacement.TARGET).resolve():
            if fault == 'target':result['host'] = '10.1.228.129'
            elif fault == 'certificate':result['ca_sha256'] = '0' * 64
        return result
    monkeypatch.setattr(policy, 'read', altered)
    if fault == 'snapshot':value['payment_slot_snapshot_path'] = replacement.OLD
    elif fault == 'decision':value['replacement_rds_decision'] = 'unknown'
    with pytest.raises(ValueError):replacement.contract(value)


@pytest.mark.parametrize('arm', ['control', 'candidate'])
def test_preview_uses_published_image_without_paid_permission(arm):
    value = prepared(arm)
    selected = profile.image(value)
    assert selected['registry_image'].startswith('swr.ap-southeast-2.myhuaweicloud.com/')
    binding = profile.binding_contract(value)
    assert binding['replacement_rds']['snapshot_sha256'] == replacement.NEW_SHA
    with pytest.raises(ValueError, match='LOCAL_ONLY'):
        profile.require_pair_time(value)


def test_old_profile_has_no_replacement_qualification():
    assert not replacement.selected(goal('control', preview=True))
    assert 'replacement_rds' not in profile.binding_contract(goal('control', preview=True))

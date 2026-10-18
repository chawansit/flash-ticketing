"""ADR0355 strict workload ladder, sealed generator and prerequisite rejection."""
import copy
import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[2] / 'scripts'))
import cce_rate_qualification as qualification
import cce_simulator_roundtrip_profile as profile
import prepare_rate_qualification as prepare
import work_envelope as policy
from cce_paid_profiles import RATE_PUSH_180, RATE_PUSH_200


def actual(rate=180):
    base = policy.read(policy.ROOT/'tmp/adr0354-control-goal.prepared.private.json')
    ref = policy.read(policy.ROOT/'tmp/adr0355-180-reference.private.json')
    if rate == 200: ref['rate'] = 180
    pair = {'id':'3'*32,'started_at_utc':None,'ledger_start_index':0}
    return prepare.goal_for(base,rate,ref,pair)


def test_actual_passed_candidate_is_required_lower_reference():
    goal = actual()
    report,binding = qualification.require_authority(goal)
    assert report['pass'] and binding['cce_simulator_roundtrip_contract']['arm'] == 'candidate'
    assert profile.image(goal)['registry_manifest_digest'] == profile.registry_image()['registry_manifest_digest']
    assert qualification.contract(goal)['expected_journeys'] == 54000


@pytest.mark.parametrize('rate,expected',[(180,54000),(200,60000)])
def test_exact_profile_and_workload_contracts(rate,expected):
    import cce_api_client_headroom as clients
    import cce_generator_process_scaling as generator
    import cce_throughput_push as push
    goal = actual(rate)
    assert qualification.selected(goal) and profile.selected(goal)
    assert (RATE_PUSH_180 if rate == 180 else RATE_PUSH_200).expected == expected
    assert push.bounds(goal) == (rate,rate,expected,expected-600)
    assert generator.contract(goal)['rate'] == clients.contract(goal)['offered_rate'] == rate
    assert generator.contract(goal)['total_concurrency'] == 800
    assert profile.binding_contract(goal)['pair_seconds_limit'] == 3600
    with pytest.raises(ValueError,match='LOCAL_ONLY'):profile.require_pair_time(goal)


@pytest.mark.parametrize('field,value',[
    ('rate_qualification_rate',220),('rate_qualification_rate',True),
    ('rate_qualification_decision','unknown'),('simulator_roundtrip_arm','control'),
    ('replacement_rds_decision','unknown')])
def test_unreviewed_qualification_is_rejected(field,value):
    goal=actual();goal[field]=value
    with pytest.raises(ValueError): qualification.selected(goal)


def test_skipping_180_cannot_admit_200():
    goal=actual();goal['rate_qualification_rate']=200
    with pytest.raises(ValueError): qualification.selected(goal)


def test_failed_lower_result_cannot_admit_rate(monkeypatch):
    monkeypatch.setattr(profile,'_retained',lambda *a,**k:(_ for _ in ()).throw(ValueError('failed lower run')))
    with pytest.raises(ValueError,match='failed lower'):qualification.require_previous(actual())


def test_source_bound_budget_drift_is_rejected():
    goal=actual();goal['generator_concurrency']=1000
    with pytest.raises(ValueError):profile.selected(goal)


def test_lower_reference_digest_mutation_is_rejected():
    goal=actual();goal['rate_qualification_reference']['sha256']='0'*64
    with pytest.raises(ValueError):qualification.require_previous(goal)


def test_prerequisite_bound_settings_cannot_drift():
    goal=actual();_,before=qualification.require_previous(goal)
    binding=copy.deepcopy(before);binding['cce_api_client_pool_max']=8
    with pytest.raises(ValueError,match='resource budgets'):qualification.validate_binding(goal,binding)


@pytest.mark.parametrize('rate', [180, 200])
def test_actual_sealed_coordinator_accepts_only_exact_new_rate(rate):
    import asyncio
    from types import ModuleType, SimpleNamespace

    import cce_generator_process_scaling as generator
    import cce_recovery_headroom as headroom
    from test_cce_recovery_headroom import parent, reads
    goal = actual(rate)
    bundle, coordinator = parent()
    bundle, coordinator = reads(bundle, coordinator)
    bundle, coordinator = headroom.qualify_workload(bundle, coordinator, goal)
    changed, sealed = generator.qualify_workload(bundle, coordinator, goal)
    source = changed['scripts/paid_ticket_sharded_generator.py']
    assert ("(" + str(rate) + ", 300, 800, 8, 1, 1, 3, 'distributed')").encode() in source
    import hashlib
    assert hashlib.sha256(source).hexdigest().encode() in sealed
    module = ModuleType('qualified_generator')
    exec(compile(source, 'qualified_generator.py', 'exec'), module.__dict__)  # noqa: S102 - verified sealed test source
    wrong = SimpleNamespace(rate=rate+20, seconds=300, concurrency=800, http_client_count=8,
                            poll_seconds=1, duplicates=1, recovery_max_attempts=3)
    with pytest.raises(ValueError, match='Exact ADR0333'):
        asyncio.run(module.run(wrong, {'fixture_layout':'distributed'}))

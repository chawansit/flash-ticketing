"""Synthetic admission cases, plus real local identity/manifests; no cloud proof claimed."""
import base64
import copy
import json
import os
import sys
from datetime import UTC, datetime, timedelta
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[2] / 'scripts'))
import cce_native_workers as workers
import cce_simulator_roundtrip_profile as profile
import cce_transaction_profile as transaction
import prepare_simulator_roundtrip_comparison as prepare
import work_envelope as policy
from cce_paid_profiles import RATE_PUSH_160
from test_cce_api_client_headroom import goal as client_goal
from test_cce_payment_claim_binding import inputs


@pytest.fixture(autouse=True)
def inactive_authorization(monkeypatch):
    """Unit identity checks must not inherit a consumed host-specific paid goal."""
    data = copy.deepcopy(policy.envelope())
    data['spending']['temporary_cce_pilot_exception']['goal_bounded_authorization'] = {}
    monkeypatch.setattr(policy, 'envelope', lambda: data)


def goal(arm='control', preview=True):
    pair = {'id': '1'*32, 'started_at_utc': None, 'ledger_start_index': 10}
    value = prepare.goal_for(client_goal(), pair, arm)
    value['simulator_roundtrip_preview'] = preview
    if not preview:
        value['simulator_roundtrip_pair']['started_at_utc'] = datetime.now(UTC).isoformat()
    return value


def test_explicit_discriminator_and_exact_local_source():
    assert not profile.selected(None) and not profile.selected({})
    with pytest.raises(ValueError):
        profile.selected({'simulator_roundtrip_arm': 'candidate'})
    for arm in ('control', 'candidate'):
        value = goal(arm)
        assert profile.selected(value)
        assert profile.image(value)['runtime_sources_sha256']
    assert profile.local_image()['source_identity_sha256'] == profile.SOURCE


@pytest.mark.parametrize('field,value', [
    ('offered_journeys_per_second', 180), ('maximum_paid_stages', 2),
    ('generator_concurrency', 1000), ('acquisition_budget', 24),
    ('maximum_experiment_seconds', 5400), ('pod_vcpu', 2),
    ('simulator_roundtrip_arm', 'unknown'), ('simulator_roundtrip_preview', 1),
    ('simulator_roundtrip_contract_sha256', '0'*64),
])
def test_changed_budgets_or_discriminator_fail_closed(field, value):
    actual = goal()
    actual[field] = value
    with pytest.raises(ValueError):
        profile.selected(actual)


def test_real_unpublished_receipt_rejects_both_live_images():
    for arm in ('control', 'candidate'):
        with pytest.raises(ValueError, match='REGISTRY_PROOF_REQUIRED'):
            profile.image(goal(arm, preview=False))


def test_preview_cannot_execute_or_check_paid_stage(monkeypatch):
    import run_cce_paid_comparison as core
    from cce_paid_stage import PaidStage
    value = goal()
    monkeypatch.setattr(transaction, 'active', lambda: value)
    with pytest.raises(ValueError, match='LOCAL_ONLY'):
        core.execute(*([None]*10), profile=RATE_PUSH_160)
    stage = object.__new__(PaidStage)
    with pytest.raises(ValueError, match='LOCAL_ONLY'):
        stage.check()


def test_preview_cannot_reserve_even_in_isolated_approved_envelope(monkeypatch, tmp_path):
    value = goal()
    monkeypatch.setattr(policy, 'envelope', lambda: {'spending': {'temporary_cce_pilot_exception': {'goal_bounded_authorization': value}}})
    lock = tmp_path / 'lock.json'
    lock.write_text(json.dumps({'pid': os.getpid(), 'run': 'adr0151-034500000000'}))
    monkeypatch.setattr(policy, 'LOCK', lock)
    with pytest.raises(ValueError, match='LOCAL_ONLY'):
        policy.reserve({'cce_simulator_roundtrip_contract': profile.binding_contract(value)}, {}, profile='cce_ticket_target')


def test_existing_bootstrap_and_native_environment_semantics_are_unchanged(monkeypatch):
    import run_cce_paid_comparison as core
    from cce_recovery_headroom import runtime_view
    _, service, environments, address = inputs()
    value = goal('candidate')
    view = runtime_view(value)
    monkeypatch.setattr(transaction, 'active', lambda: view)
    assert core.worker_contract().shared['registry_image'] == profile.parent.image()['registry_image']
    for role, env in environments.items():
        assert workers.environment(role, env, address, 'ADR0345') == workers.environment(role, env, address, 'ADR0324')
    native = workers.objects('adr0151-034500000000', environments, address, 'ADR0345', profile=RATE_PUSH_160)
    apis = core.native.objects('adr0151-034500000000', service, address, 'preview', 'preview',
                               acquisition_budget=20, profile=RATE_PUSH_160)
    pods = [v for v in native + apis if v['kind'] == 'Pod']
    assert len(pods) == 19
    assert all(v['spec']['containers'][0]['image'] == profile.local_image()['registry_image'] for v in pods)
    for obj in native:
        if obj['kind'] == 'Secret' and obj['metadata']['name'] == 'simulator-env':
            assert base64.b64decode(obj['data']['DB_POOL_MAX']) == b'10'
            assert base64.b64decode(obj['data']['SIMULATOR_CONCURRENCY']) == b'16'
    assert workers.identity.specification('ADR0345')['source'] == profile.SOURCE
    assert workers.identity.specification('ADR0324-bootstrap')['image_id'] == profile.parent.pins.IMAGE_ID


@pytest.mark.parametrize('kind', ['expired', 'future', 'naive', 'missing'])
def test_shared_pair_deadline_fails_closed(kind):
    value = goal(preview=False)
    now = datetime.now(UTC)
    stamps = {'expired': (now-timedelta(seconds=3601)).isoformat(),
              'future': (now+timedelta(seconds=1)).isoformat(),
              'naive': now.replace(tzinfo=None).isoformat(), 'missing': None}
    value['simulator_roundtrip_pair']['started_at_utc'] = stamps[kind]
    with pytest.raises(ValueError):
        profile.require_pair_time(value, now=now)


def test_deadline_reserves_required_remaining_time():
    value = goal(preview=False)
    now = datetime.now(UTC)
    value['simulator_roundtrip_pair']['started_at_utc'] = (now-timedelta(seconds=2500)).isoformat()
    assert profile.require_pair_time(value, now=now)
    with pytest.raises(ValueError, match='PAIR_TIME_BUDGET_EXHAUSTED'):
        profile.require_pair_time(value, 1200, now=now)


def control_fixture(monkeypatch, actual):
    contract = {**profile.binding_contract(actual), 'arm': 'control', 'fresh_control': None}
    binding = {'cce_simulator_roundtrip_contract': contract}
    monkeypatch.setattr(profile, '_retained', lambda *args: ({'pass': True}, binding, 10))
    actual['simulator_roundtrip_control'] = {'path': 'tmp/adr0151-123456789abc/cce-comparison.private.json',
                                          'ledger': 'bounded_cce_ticket_target__123456789abc', 'sha256': 'a'*64}
    return binding


def test_candidate_requires_fresh_same_pair_control(monkeypatch):
    actual = goal('candidate', preview=False)
    with pytest.raises(ValueError, match='Fresh paired control'):
        profile.require_control(actual)
    binding = control_fixture(monkeypatch, actual)
    assert profile.require_control(actual)[1] == binding
    binding['cce_simulator_roundtrip_contract']['pair']['id'] = '2'*32
    with pytest.raises(ValueError, match='another pair'):
        profile.require_control(actual)


@pytest.mark.parametrize('change', ['old_boundary', 'preview_control', 'different_sources', 'not_control'])
def test_historical_or_changed_control_cannot_admit_candidate(monkeypatch, change):
    actual = goal('candidate', preview=False)
    binding = control_fixture(monkeypatch, actual)
    previous = binding['cce_simulator_roundtrip_contract']
    if change == 'old_boundary':
        actual['simulator_roundtrip_pair']['ledger_start_index'] = 11
    elif change == 'preview_control':
        previous['preview'] = True
    elif change == 'different_sources':
        previous['source'] = '0'*64
    else:
        previous['arm'] = 'candidate'
    with pytest.raises(ValueError, match='another pair'):
        profile.require_control(actual)


def valid_publication():
    local = profile.local_image()
    manifest, config = 'sha256:'+'a'*64, 'sha256:'+'b'*64
    return {'decision': profile.DECISION, 'registry_published': True, 'registry_pulled_runtime_verified': True,
            'local_image_id': profile.LOCAL_IMAGE, 'source_identity_sha256': profile.SOURCE,
            'runtime_source_sha256': local['runtime_source_sha256'],
            'dependency_input_sha256': local['dependency_input_sha256'],
            'platform': {'os': 'linux', 'architecture': 'amd64'}, 'runtime_user': 'ticketing',
            'registry_manifest_digest': manifest, 'registry_configuration_digest': config,
            'registry_image': 'swr.ap-southeast-2.myhuaweicloud.com/chawansit/flash-ticketing@'+manifest}


@pytest.mark.parametrize('change', ['unpublished', 'old_source', 'source_map', 'manifest', 'configuration', 'pulled', 'dependencies'])
def test_synthetic_publication_gate_rejects_tampering(monkeypatch, change):
    value = valid_publication()
    if change == 'unpublished': value['registry_published'] = False
    elif change == 'old_source': value['source_identity_sha256'] = '0'*64
    elif change == 'source_map': value['runtime_source_sha256'] = {}
    elif change == 'manifest': value['registry_manifest_digest'] = 'sha256:'+'c'*64
    elif change == 'configuration': value['registry_configuration_digest'] = value['registry_manifest_digest']
    elif change == 'pulled': value['registry_pulled_runtime_verified'] = False
    else: value['dependency_input_sha256'] = {}
    original = policy.read
    monkeypatch.setattr(policy, 'read', lambda path: value if Path(path) == policy.ROOT/profile.PUBLICATION else original(path))
    with pytest.raises(ValueError, match='REGISTRY_PROOF_REQUIRED'):
        profile.registry_image()


def test_consumed_arm_cannot_gain_a_second_paid_allowance(monkeypatch):
    value = goal(preview=False)
    monkeypatch.setattr(profile, 'registry_image', valid_publication)
    monkeypatch.setattr(policy, 'envelope', dict)
    entries = [{} for _ in range(10)] + [{'ledger': 'owned', 'status': 'FAILED_RESTORED'}]
    state = {'owned': {'paid_runs_started': 1, 'binding': {'cce_simulator_roundtrip_contract': profile.binding_contract(value)}}}
    monkeypatch.setattr(policy, 'journal', lambda envelope: {'experiments': entries})
    original = policy.read
    monkeypatch.setattr(policy, 'read', lambda path: state if path == policy.STATE else original(path))
    with pytest.raises(ValueError, match='already consumed'):
        profile.require_live(value)


def test_preview_binding_cannot_be_passed_as_live(monkeypatch):
    value = goal()
    with pytest.raises(ValueError, match='LOCAL_ONLY'):
        profile.validate_binding(value, {})
    with pytest.raises(ValueError, match='explicit local preview'):
        profile.validate_binding(goal(preview=False), {}, preparation_only=True)


def binding_fixture(monkeypatch):
    import cce_api_client_headroom as client
    import cce_generator_process_scaling as generator
    import cce_paid_stage as paid
    import cce_primary_bootstrap as bootstrap
    import cce_recovery_headroom as headroom
    import cce_throughput_push as push
    import cce_verification_contract as verification
    import run_cce_paid_comparison as core
    actual = goal()
    binding = {'baseline_sha256': profile.REFERENCE[2],
               'cce_simulator_roundtrip_contract': profile.binding_contract(actual),
               'cce_transaction_pair_sha256': profile.sha(profile.PUBLICATION),
               'cce_paid_entry_sources': core.identity(), 'cce_paid_core_sources': paid.identity(),
               'cce_api_client_pool_contract': client.contract(actual),
               'cce_headroom_contract': headroom.contract(actual),
               'cce_throughput_push_contract': push.contract(actual),
               'cce_generator_process_contract': generator.contract(actual),
               'cce_primary_bootstrap_contract': bootstrap.contract(),
               'cce_verification_contract_sha256': policy.digest(verification.contract()),
               'cce_api_client_pool_max': 6, 'cce_payment_pool_max': 3, 'cce_simulator_concurrency': 16,
               'cce_manifest_sha256': 'synthetic-manifests', 'saved_api_service_sha256': 'same-service'}
    old = copy.deepcopy(binding)
    old.pop('cce_simulator_roundtrip_contract')
    sources = copy.deepcopy(old)
    for key in ('cce_paid_entry_sources', 'cce_paid_core_sources'):
        for name in profile.INPUTS:
            sources[key].pop(name)
        for name in profile.CHANGED & sources[key].keys():
            sources[key][name] = 'previous-reviewed-source'
    monkeypatch.setattr(profile, '_retained', lambda *args: ({'pass': True}, old, 0))
    monkeypatch.setattr(profile, 'source_reference', lambda: sources)
    monkeypatch.setattr(generator, 'validate_binding', lambda *args: True)
    return actual, binding


def test_preview_binding_validates_all_declared_sources(monkeypatch):
    actual, binding = binding_fixture(monkeypatch)
    assert profile.validate_binding(actual, binding, preparation_only=True)


@pytest.mark.parametrize('field,value', [
    ('cce_api_client_pool_max', 8), ('cce_payment_pool_max', 4),
    ('cce_simulator_concurrency', 32), ('saved_api_service_sha256', 'different'),
    ('cce_transaction_pair_sha256', '0'*64), ('cce_generator_process_contract', {}),
    ('cce_simulator_roundtrip_contract', {}),
])
def test_binding_budget_and_contract_drift_is_rejected(monkeypatch, field, value):
    actual, binding = binding_fixture(monkeypatch)
    binding[field] = value
    with pytest.raises(ValueError):
        profile.validate_binding(actual, binding, preparation_only=True)


@pytest.mark.parametrize('key', ['cce_paid_entry_sources', 'cce_paid_core_sources'])
def test_binding_cannot_substitute_unreviewed_generator_sources(monkeypatch, key):
    actual, binding = binding_fixture(monkeypatch)
    protected = next(k for k in binding[key] if k.endswith('customer_gateway_read_bundle.py'))
    binding[key][protected] = '0'*64
    with pytest.raises(ValueError, match='source drift'):
        profile.validate_binding(actual, binding, preparation_only=True)


@pytest.mark.parametrize('fault', [None, 'failed_load', 'not_restored', 'financial', 'ttl',
                                   'drops', 'queues', 'issuance', 'measurement', 'ledger_status'])
def test_retained_control_quality_and_restoration_are_mandatory(monkeypatch, fault):
    report = {'pass': True, 'restoration_complete': True, 'integrity_verified': True,
              'transport_credentials_cleared': True, 'binding_sha256': policy.digest({}),
              'native': {'cleanup_complete': True, 'measurement_gates': {str(i): True for i in range(12)}},
              'paid_stage': {'customer': {'scheduled': 48000, 'dispatched': 48000, 'generator_drops': 0},
                             'financial': {'pass': True, 'hold_deadlines_elapsed': True},
                             'global_queues': {'pass': True}, 'ticket_target_issuance': {'pass': True}}}
    if fault == 'failed_load': report['pass'] = False
    elif fault == 'not_restored': report['restoration_complete'] = False
    elif fault == 'financial': report['paid_stage']['financial']['pass'] = False
    elif fault == 'ttl': report['paid_stage']['financial']['hold_deadlines_elapsed'] = False
    elif fault == 'drops': report['paid_stage']['customer']['generator_drops'] = 1
    elif fault == 'queues': report['paid_stage']['global_queues']['pass'] = False
    elif fault == 'issuance': report['paid_stage']['ticket_target_issuance']['pass'] = False
    elif fault == 'measurement': report['native']['measurement_gates']['0'] = False
    digest = policy.digest(report)
    path, ledger = 'tmp/adr0151-123456789abc/cce-comparison.private.json', 'synthetic-ledger'
    state = {ledger: {'binding': {}, 'paid_runs_started': 1, 'active_run': None, 'cce_paid_result_sha256': digest}}
    entry = {'ledger': ledger, 'status': 'FAILED_RESTORED' if fault == 'ledger_status' else 'PASSED_RESTORED',
             'result_sha256': digest}
    original = policy.read
    monkeypatch.setattr(policy, 'envelope', dict)
    monkeypatch.setattr(policy, 'journal', lambda _: {'experiments': [entry]})
    monkeypatch.setattr(policy, 'read', lambda p: state if p == policy.STATE else report if p == policy.ROOT/path else original(p))
    if fault:
        with pytest.raises(ValueError, match='Fully passed and restored'):
            profile._retained(path, ledger, digest)
    else:
        assert profile._retained(path, ledger, digest)[0]['pass'] is True


def test_synthetic_valid_publication_selects_distinct_live_arm_images(monkeypatch):
    publication = valid_publication()
    original = policy.read
    monkeypatch.setattr(policy, 'read', lambda path: publication if path == policy.ROOT/profile.PUBLICATION else original(path))
    assert profile.image(goal('candidate', preview=False))['registry_image'] == publication['registry_image']
    assert profile.image(goal('control', preview=False))['registry_image'] == profile.parent.image()['registry_image']

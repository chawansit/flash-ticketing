"""ADR0347 matched160/s combined confirmation source; preview can never admit paid work."""
import copy
import hashlib
import json
import re
from datetime import UTC, datetime

import cce_fulfillment_batch_profile as parent
import work_envelope as policy

DECISION = 'ADR0347'
LOCAL = 'docs/capacity/cce/callback-write-pipeline-local-2026-10-17.json'
LOCAL_SHA = 'c0ed6437f5709b5c871faf53a816ae5cafda1b690043146cce1485b65ff1a813'
PUBLICATION = 'docs/capacity/cce/callback-write-pipeline-registry-2026-10-17.json'
SOURCE = '19da6fb554d1e592e3eba59a52427b10510c213ab62c2a80181b8d5d27123925'
LOCAL_IMAGE = 'sha256:b114f6b15e4e07f11da19e98a82a21d910c39879b7aadf1bc96e8e11a74595ba'
REFERENCE = ('tmp/adr0151-c83348917efc/cce-comparison.private.json',
             'bounded_cce_ticket_target__ee0f97a1a26d',
             '5ec10ec764bc349911b28e2c5d20883eb7c2f1df58a02bc55e792b181ccaf05b')
SOURCE_REFERENCE = ('tmp/adr0151-a0d63f35960f/cce-comparison.private.json',
                    'bounded_cce_ticket_target__03bf2cac8c9b',
                    'de6e008fad525f67d93fd15b83f0a8ddb105110f9806d31bcc8bdfa6d09ec06c')
KEYS = {'simulator_roundtrip_decision', 'simulator_roundtrip_arm', 'simulator_roundtrip_contract_sha256',
        'simulator_roundtrip_pair', 'simulator_roundtrip_preview', 'simulator_roundtrip_timing_decision'}
INPUTS = {LOCAL, PUBLICATION, 'scripts/cce_replacement_rds_binding.py', 'docs/capacity/cce/rds-endpoint-switch-2026-10-18.json', 'scripts/cce_combined_worker_identity.py', 'scripts/cce_simulator_roundtrip_profile.py',
          'scripts/prepare_simulator_roundtrip_image.py', 'scripts/prepare_simulator_roundtrip_comparison.py',
          'scripts/prepare_callback_write_pipeline_image.py',
          'docs/capacity/cce/simulator-roundtrip-local-2026-10-17.json',
          'docs/capacity/cce/simulator-roundtrip-registry-2026-10-17.json'}
CHANGED = {'scripts/cce_fixture_clock.py','scripts/cce_dependency_probe.py','scripts/cce_writer_scale_verification.py','scripts/observe_cce_paid_pipeline.py','scripts/observe_cce_workers.py','scripts/cce_inventory_renewal.py', 'scripts/cce_paid_observers.py','scripts/cce_api_adapter.py','scripts/cce_transaction_profile.py', 'scripts/run_cce_paid_comparison.py',
           'scripts/cce_recovery_headroom.py', 'scripts/cce_throughput_push.py',
           'scripts/cce_native_workers.py', 'scripts/cce_worker_identity.py',
           'scripts/cce_worker_transition.py', 'scripts/cce_paid_stage.py',
           'scripts/cce_reproduction_overlay.py', 'scripts/work_envelope.py',
           'artifacts/cce-transaction-runner/manifest.json'}


def sha(path):
    path = policy.ROOT / path
    if path.is_symlink():
        raise ValueError('Regular reviewed roundtrip input required')
    return hashlib.sha256(path.read_bytes().replace(b'\r\n', b'\n')).hexdigest()


def local_image():
    if sha(LOCAL) != LOCAL_SHA:
        raise ValueError('Exact executed local qualification receipt required')
    receipt = policy.read(policy.ROOT / LOCAL)
    item = receipt
    control = parent.image()
    sources = item['runtime_source_sha256']
    if (receipt.get('decision') != 'ADR0346' or receipt.get('cloud_load_started') is not False
            or receipt.get('registry_published') is not False
            or item.get('local_image_id') != LOCAL_IMAGE or item.get('source_identity_sha256') != SOURCE
            or set(sources) != set(control['runtime_source_sha256'])
            or {k for k in sources if sources[k] != control['runtime_source_sha256'][k]} != {'src/ticketing/workers.py', 'src/ticketing/api.py',
                                                                 'src/ticketing/config.py', 'src/ticketing/infrastructure/reservations.py'}
            or hashlib.sha256(json.dumps(sources, sort_keys=True).encode()).hexdigest() != SOURCE
            or item.get('parent_registry_image') != control['registry_image']
            or item.get('dependency_input_sha256') != control['dependency_input_sha256']
            or item.get('preparation_recipe_sha256') != sha('scripts/prepare_callback_write_pipeline_image.py')
            or item.get('validation') != {'modules_verified': 22, 'imported_worker_matches': True, 'lease_clock_preserved': True}):
        raise ValueError('Exact immutable-parent locally qualified simulator required')
    return {**item, 'decision': 'ADR0346', 'registry_image': item['local_tag'],
            'runtime_sources_sha256': sources, 'registry_manifest_digest': None, 'local_only': True}


def registry_image():
    expected = local_image()
    value = policy.read(policy.ROOT / PUBLICATION)
    checks = {'decision': DECISION, 'registry_published': True, 'registry_pulled_runtime_verified': True,
              'local_image_id': LOCAL_IMAGE, 'source_identity_sha256': SOURCE,
              'runtime_source_sha256': expected['runtime_source_sha256'],
              'dependency_input_sha256': expected['dependency_input_sha256'],
              'platform': {'os': 'linux', 'architecture': 'amd64'}, 'runtime_user': 'ticketing'}
    manifest, config = value.get('registry_manifest_digest', ''), value.get('registry_configuration_digest', '')
    if (any(type(value.get(k)) is not type(v) or value.get(k) != v for k, v in checks.items())
            or not all(isinstance(v, str) and re.fullmatch(r'sha256:[0-9a-f]{64}', v) for v in (manifest, config))
            or manifest == config
            or value.get('registry_image') != 'swr.ap-southeast-2.myhuaweicloud.com/chawansit/flash-ticketing@' + manifest):
        raise ValueError('REGISTRY_PROOF_REQUIRED: actual immutable publication and pulled-source verification required')
    return {**expected, **value, 'decision': 'ADR0346', 'local_only': False,
            'runtime_sources_sha256': expected['runtime_source_sha256']}


def contract():
    return {'decision': DECISION, 'source': SOURCE, 'local_image': LOCAL_IMAGE,
            'local_receipt_sha256': LOCAL_SHA, 'publication_sha256': sha(PUBLICATION),
            'offered_rate': 160, 'seconds': 300, 'expected_journeys': 48000,
            'api_pods': 4, 'worker_pods': 15, 'api_client_slots': [3, 3],
            'physical_backends': 24, 'physical_reserve': 0, 'shared_admission': 20,
            'acquisition_wait_ms': 500, 'simulator_delivery_slots': 16, 'simulator_database_clients': 10,
            'generator_processes': 4, 'active_journeys': 800, 'http_connections': 500, 'http_clients': 16,
            'candidate_api_callback_write_pipeline': '1', 'control_api_callback_write_pipeline': 'absent',
            'maximum_paid_stages_per_arm': 1, 'pair_seconds_limit': 3600,
            'source_reference_result_sha256': SOURCE_REFERENCE[2],
            'sources': {name: sha(name) for name in sorted(INPUTS | CHANGED)}}


def selected(goal):
    if goal is None or not KEYS.intersection(goal):
        return False
    timing_contract(goal)
    from cce_api_client_headroom import selected as clients_selected
    expected = {'simulator_roundtrip_decision': DECISION,
                'simulator_roundtrip_contract_sha256': policy.digest(contract()),
                'throughput_push_decision': 'ADR0329'}
    pair = goal.get('simulator_roundtrip_pair', {})
    if (any(type(goal.get(k)) is not type(v) or goal.get(k) != v for k, v in expected.items())
            or goal.get('simulator_roundtrip_arm') not in {'control', 'candidate'}
            or (goal.get('simulator_roundtrip_arm') == 'control' and goal.get('simulator_roundtrip_control') is not None)
            or type(goal.get('simulator_roundtrip_preview')) is not bool
            or not isinstance(pair, dict) or set(pair) != {'id', 'started_at_utc', 'ledger_start_index'}
            or not re.fullmatch(r'[0-9a-f]{32}', pair.get('id', ''))
            or type(pair.get('ledger_start_index')) is not int or pair['ledger_start_index'] < 0
            or not clients_selected(goal)):
        raise ValueError('Exact source-bound160/s simulator pair required')
    return True


def image(goal=None):
    if goal is None:
        from cce_transaction_profile import active
        goal = active()
    if not selected(goal):
        raise ValueError('Explicit simulator-roundtrip image selection required')
    import cce_replacement_rds_binding as replacement
    published = not goal['simulator_roundtrip_preview'] or replacement.selected(goal)
    if published:
        registry_image()  # Both live arms require the fully published pair before starting.
    if goal['simulator_roundtrip_arm'] == 'control':
        value = parent.image()
        return {**value, 'runtime_sources_sha256': value['runtime_source_sha256']}
    return registry_image() if published else local_image()


def binding_contract(goal):
    selected(goal)
    import cce_replacement_rds_binding as replacement
    return {**contract(), 'arm': goal['simulator_roundtrip_arm'],
            'preview': goal['simulator_roundtrip_preview'], 'pair': copy.deepcopy(goal['simulator_roundtrip_pair']),
            'fresh_control': copy.deepcopy(goal.get('simulator_roundtrip_control')),
            **({'pair_timing': timing_contract(goal), 'pair_seconds_limit': 7200}
               if timing_contract(goal) else {}),
            **({'replacement_rds': replacement.contract(goal)} if replacement.selected(goal) else {})}


def extend_plan(goal, plan):
    if not selected(goal):
        return plan
    return {**plan, 'kind': 'simulator_roundtrip_matched_160',
            'single_changed_factor': 'combined_simulator_and_successful_callback_roundtrips',
            'baseline_evidence': REFERENCE[0], 'baseline_sha256': REFERENCE[2],
            'baseline_is_passing_control': True, 'matched_performance_comparison': True,
            'simulator_roundtrip_contract': binding_contract(goal),
            'duration_reason': ('160/s300s per arm; fresh passed/restored control; fixed7200s pair/3600s control ceilings.'
                                if timing_contract(goal) else
                                '160/s300s per arm; fresh passed/restored control required before candidate; shared3600s ceiling.')}


def timing_contract(goal):
    """Optional fresh-pair scheduling; never changes a historical allowance."""
    if 'simulator_roundtrip_timing_decision' not in goal:
        return None
    if (goal['simulator_roundtrip_timing_decision'] != 'ADR0353'
            or goal.get('replacement_rds_decision') != 'ADR0352'):
        raise ValueError('Exact replacement-RDS ADR0353 timing required')
    return {'decision': 'ADR0353', 'pair_seconds_limit': 7200,
            'control_seconds_limit': 3600, 'clock': 'immutable_pair_start',
            'fresh_same_pair_control_required': True}


def require_pair_time(goal, seconds=0, now=None):
    selected(goal)
    if goal['simulator_roundtrip_preview']:
        raise ValueError('LOCAL_ONLY: preview cannot reserve, deploy or dispatch')
    raw = goal['simulator_roundtrip_pair']['started_at_utc']
    try:
        start = datetime.fromisoformat(raw)
    except (TypeError, ValueError) as error:
        raise ValueError('Fresh aware pair start required') from error
    current = datetime.now(UTC) if now is None else now
    timing = timing_contract(goal)
    limit = (timing['control_seconds_limit'] if goal['simulator_roundtrip_arm'] == 'control'
             else timing['pair_seconds_limit']) if timing else 3600
    if (type(seconds) is not int or seconds < 0 or current.tzinfo is None
            or start.tzinfo is None or not 0 <= (current-start).total_seconds() <= limit-seconds):
        raise ValueError('PAIR_TIME_BUDGET_EXHAUSTED')
    return True


def _retained(path, ledger, digest, *, expected=48000):
    if not re.fullmatch(r'tmp/adr0151-[0-9a-f]{12}/cce-comparison\.private\.json', path):
        raise ValueError('Canonical owned control report required')
    report = policy.read(policy.ROOT / path)
    state = policy.read(policy.STATE).get(ledger, {})
    rows = policy.journal(policy.envelope())['experiments']
    matches = [(i, v) for i, v in enumerate(rows) if v['ledger'] == ledger]
    gates = report.get('native', {}).get('measurement_gates', {})
    stage = report.get('paid_stage', {})
    if (len(matches) != 1 or policy.digest(report) != digest
            or matches[0][1].get('status') != 'PASSED_RESTORED'
            or matches[0][1].get('result_sha256') != digest
            or state.get('cce_paid_result_sha256') != digest or state.get('active_run')
            or state.get('paid_runs_started') != 1
            or report.get('binding_sha256') != policy.digest(state.get('binding', {}))
            or any(report.get(k) is not True for k in ('pass', 'restoration_complete', 'integrity_verified', 'transport_credentials_cleared'))
            or report.get('native', {}).get('cleanup_complete') is not True
            or len(gates) != 12 or any(v is not True for v in gates.values())
            or any(stage.get('customer', {}).get(k) != expected for k in ('scheduled', 'dispatched'))
            or stage.get('customer', {}).get('generator_drops') != 0
            or stage.get('financial', {}).get('pass') is not True
            or stage.get('financial', {}).get('hold_deadlines_elapsed') is not True
            or stage.get('global_queues', {}).get('pass') is not True
            or stage.get('ticket_target_issuance', {}).get('pass') is not True):
        raise ValueError('Fully passed and restored160/s control required')
    return report, state['binding'], matches[0][0]


def require_control(goal):
    if goal['simulator_roundtrip_arm'] != 'candidate':
        return None
    ref = goal.get('simulator_roundtrip_control')
    if not isinstance(ref, dict) or set(ref) != {'path', 'ledger', 'sha256'}:
        raise ValueError('Fresh paired control reference required')
    report, binding, index = _retained(ref['path'], ref['ledger'], ref['sha256'])
    old = binding.get('cce_simulator_roundtrip_contract', {})
    wanted = {**binding_contract(goal), 'arm': 'control', 'fresh_control': None}
    if index < goal['simulator_roundtrip_pair']['ledger_start_index'] or old != wanted:
        raise ValueError('Control belongs to another pair, source or journal boundary')
    return report, binding


def source_reference():
    path, ledger, digest = SOURCE_REFERENCE
    raw = policy.read(policy.ROOT / path)
    scope = policy.read(policy.STATE)[ledger]
    rows = [v for v in policy.journal(policy.envelope())['experiments'] if v['ledger'] == ledger]
    recovery = policy.read(policy.ROOT / 'docs/capacity/cce/claim-cutoff-180-control-recovery-2026-10-17.json')
    if (policy.digest(raw) != digest or len(rows) != 1 or rows[0].get('status') != 'FAILED_RESTORED'
            or rows[0].get('result_sha256') != digest or scope.get('cce_paid_result_sha256') != digest
            or raw.get('binding_sha256') != policy.digest(scope['binding']) or scope.get('active_run')
            or recovery.get('original_result_sha256') != digest or recovery.get('pass') is not True
            or rows[0].get('recovery', {}).get('sha256') != policy.digest(recovery)):
        raise ValueError('Exact reconciled orchestration source reference required; not a passing load permit')
    return scope['binding']


def require_live(goal, seconds=0):
    if not selected(goal):
        return False
    require_pair_time(goal, seconds)
    registry_image()
    require_control(goal)
    # A second reservation for an already consumed arm is never a retry allowance.
    pair = goal['simulator_roundtrip_pair']
    state = policy.read(policy.STATE)
    entries = policy.journal(policy.envelope())['experiments']
    for entry in entries[pair['ledger_start_index']:]:
        item = state.get(entry['ledger'], {})
        bound = item.get('binding', {}).get('cce_simulator_roundtrip_contract', {})
        if (bound.get('pair') == pair and bound.get('arm') == goal['simulator_roundtrip_arm']
                and (entry.get('status') != 'ACTIVE' or item.get('paid_runs_started', 0) > 1)):
            raise ValueError('Pair arm already consumed; no automatic paid retry')
    return True


def validate_binding(goal, binding, *, preparation_only=False):
    if not selected(goal):
        raise ValueError('Explicit simulator-roundtrip binding required')
    if preparation_only:
        if not goal['simulator_roundtrip_preview']:
            raise ValueError('Only an explicit local preview may bypass live qualification')
    else:
        require_live(goal, 1200)
    _, original, _ = _retained(*REFERENCE)
    source_original = source_reference()
    import cce_generator_process_scaling as generator
    import cce_paid_stage as paid
    import cce_primary_bootstrap as bootstrap
    import cce_recovery_headroom as headroom
    import cce_throughput_push as push
    import cce_verification_contract as verification
    import run_cce_paid_comparison as core
    from cce_api_client_headroom import contract as client_contract
    varied = {'cce_paid_entry_sources', 'cce_paid_core_sources', 'cce_manifest_sha256', 'baseline_sha256',
              'cce_fixture_clock_contract', 'cce_headroom_contract', 'cce_throughput_push_contract', 'cce_generator_process_contract',
              'cce_api_client_pool_contract', 'cce_primary_bootstrap_contract', 'cce_verification_contract_sha256',
              'cce_transaction_pair_sha256', 'cce_simulator_roundtrip_contract', 'cce_inventory_renewal_contract', 'cce_metric_admission_contract_sha256'}
    import cce_replacement_rds_binding as replacement
    if replacement.selected(goal):
        replacement.validate_binding(goal, binding)
        varied.add('diagnostic_target_sha256')
    if any(original.get(k) != binding.get(k) for k in original.keys() | binding.keys() if k not in varied):
        raise ValueError('Roundtrip comparison changed unrelated resources, routes, settings or recovery')
    from cce_inventory_renewal import contract as renewal_contract
    from cce_metric_admission import contract as metric_contract
    expected = {'cce_fixture_clock_contract': __import__('cce_fixture_clock').contract(),
                'cce_metric_admission_contract_sha256': policy.digest(metric_contract()),
                'cce_inventory_renewal_contract': renewal_contract(), 'baseline_sha256': REFERENCE[2], 'cce_simulator_roundtrip_contract': binding_contract(goal),
                'cce_transaction_pair_sha256': sha(PUBLICATION), 'cce_api_client_pool_contract': client_contract(goal),
                'cce_headroom_contract': headroom.contract(goal), 'cce_throughput_push_contract': push.contract(goal),
                'cce_generator_process_contract': generator.contract(goal), 'cce_primary_bootstrap_contract': bootstrap.contract(),
                'cce_verification_contract_sha256': policy.digest(verification.contract())}
    if any(binding.get(k) != value for k, value in expected.items()):
        raise ValueError('Exact roundtrip source-bound image/settings contracts required')
    for key, current in (('cce_paid_entry_sources', core.identity()), ('cce_paid_core_sources', paid.identity())):
        before, after = source_original[key], binding.get(key, {})
        drift = sorted(k for k in before if k not in CHANGED and before[k] != after.get(k))
        if (after != current or set(after) != set(before) | INPUTS
                or drift):
            raise ValueError('Runner source drift outside reviewed roundtrip integration: ' + ','.join(drift))
    before, after = source_original['cce_primary_bootstrap_contract'], binding['cce_primary_bootstrap_contract']
    if ({k:v for k,v in before.items() if k != 'sources'} != {k:v for k,v in after.items() if k != 'sources'}
            or set(before['sources']) != set(after['sources'])
            or any(before['sources'][k] != after['sources'][k] for k in before['sources'] if k not in CHANGED)):
        raise ValueError('Exact unchanged common primary bootstrap required')
    generator.validate_binding(goal, binding)
    if not preparation_only and goal['simulator_roundtrip_arm'] == 'candidate':
        _, control = require_control(goal)
        # Source contracts must be identical between the fresh measured arms.
        for key in ('cce_paid_entry_sources', 'cce_paid_core_sources', 'configuration_sha256', 'saved_api_service_sha256'):
            if binding.get(key) != control.get(key):
                raise ValueError('Fresh paired control uses different sources or configuration')
    return True

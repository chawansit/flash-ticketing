"""ADR0345 matched160/s simulator source; preview can never admit paid work."""
import copy
import hashlib
import json
import re
from datetime import UTC, datetime

import cce_fulfillment_batch_profile as parent
import work_envelope as policy

DECISION = 'ADR0345'
LOCAL = 'docs/capacity/cce/simulator-roundtrip-local-2026-10-17.json'
LOCAL_SHA = 'cb51f23a9863d9be99954128d09af4702d12903afe81ffdbaead01b6615d9e26'
PUBLICATION = 'docs/capacity/cce/simulator-roundtrip-registry-2026-10-17.json'
SOURCE = '56af658a57e6f230467b0b775dea09dac0c7cc82008926f83a3693db4ec20374'
LOCAL_IMAGE = 'sha256:a3002fda9ae20d52a40e776374d87c34a0ec536ba6325a0b343fd2b8b9ae4d78'
REFERENCE = ('tmp/adr0151-c83348917efc/cce-comparison.private.json',
             'bounded_cce_ticket_target__ee0f97a1a26d',
             '5ec10ec764bc349911b28e2c5d20883eb7c2f1df58a02bc55e792b181ccaf05b')
SOURCE_REFERENCE = ('tmp/adr0151-a0d63f35960f/cce-comparison.private.json',
                    'bounded_cce_ticket_target__03bf2cac8c9b',
                    'de6e008fad525f67d93fd15b83f0a8ddb105110f9806d31bcc8bdfa6d09ec06c')
KEYS = {'simulator_roundtrip_decision', 'simulator_roundtrip_arm', 'simulator_roundtrip_contract_sha256',
        'simulator_roundtrip_pair', 'simulator_roundtrip_preview'}
INPUTS = {LOCAL, PUBLICATION, 'scripts/cce_simulator_roundtrip_profile.py',
          'scripts/prepare_simulator_roundtrip_image.py', 'scripts/prepare_simulator_roundtrip_comparison.py'}
CHANGED = {'scripts/cce_transaction_profile.py', 'scripts/run_cce_paid_comparison.py',
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
    item = receipt['local_image']
    control = parent.image()
    sources = item['runtime_source_sha256']
    if (receipt.get('decision') != 'ADR0344' or receipt.get('cloud_load_started') is not False
            or receipt.get('registry_published') is not False
            or item.get('local_image_id') != LOCAL_IMAGE or item.get('source_identity_sha256') != SOURCE
            or set(sources) != set(control['runtime_source_sha256'])
            or {k for k in sources if sources[k] != control['runtime_source_sha256'][k]} != {'src/ticketing/workers.py'}
            or hashlib.sha256(json.dumps(sources, sort_keys=True).encode()).hexdigest() != SOURCE
            or item.get('parent_registry_image') != control['registry_image']
            or item.get('dependency_input_sha256') != control['dependency_input_sha256']
            or item.get('preparation_recipe_sha256') != sha('scripts/prepare_simulator_roundtrip_image.py')
            or item.get('validation') != {'modules_verified': 22, 'imported_worker_matches': True, 'lease_clock_preserved': True}):
        raise ValueError('Exact immutable-parent locally qualified simulator required')
    return {**item, 'decision': 'ADR0344', 'registry_image': item['local_tag'],
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
    return {**expected, **value, 'decision': 'ADR0344', 'local_only': False,
            'runtime_sources_sha256': expected['runtime_source_sha256']}


def contract():
    return {'decision': DECISION, 'source': SOURCE, 'local_image': LOCAL_IMAGE,
            'local_receipt_sha256': LOCAL_SHA, 'publication_sha256': sha(PUBLICATION),
            'offered_rate': 160, 'seconds': 300, 'expected_journeys': 48000,
            'api_pods': 4, 'worker_pods': 15, 'api_client_slots': [3, 3],
            'physical_backends': 24, 'physical_reserve': 0, 'shared_admission': 20,
            'acquisition_wait_ms': 500, 'simulator_delivery_slots': 16, 'simulator_database_clients': 10,
            'generator_processes': 4, 'active_journeys': 800, 'http_connections': 500, 'http_clients': 16,
            'maximum_paid_stages_per_arm': 1, 'pair_seconds_limit': 3600,
            'source_reference_result_sha256': SOURCE_REFERENCE[2],
            'sources': {name: sha(name) for name in sorted(INPUTS | CHANGED)}}


def selected(goal):
    if goal is None or not KEYS.intersection(goal):
        return False
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
    if not goal['simulator_roundtrip_preview']:
        registry_image()  # Both live arms require the fully published pair before starting.
    if goal['simulator_roundtrip_arm'] == 'control':
        value = parent.image()
        return {**value, 'runtime_sources_sha256': value['runtime_source_sha256']}
    return local_image() if goal['simulator_roundtrip_preview'] else registry_image()


def binding_contract(goal):
    selected(goal)
    return {**contract(), 'arm': goal['simulator_roundtrip_arm'],
            'preview': goal['simulator_roundtrip_preview'], 'pair': copy.deepcopy(goal['simulator_roundtrip_pair']),
            'fresh_control': copy.deepcopy(goal.get('simulator_roundtrip_control'))}


def extend_plan(goal, plan):
    if not selected(goal):
        return plan
    return {**plan, 'kind': 'simulator_roundtrip_matched_160',
            'single_changed_factor': 'simulator_atomic_claim_and_transaction_pipeline',
            'baseline_evidence': REFERENCE[0], 'baseline_sha256': REFERENCE[2],
            'baseline_is_passing_control': True, 'matched_performance_comparison': True,
            'simulator_roundtrip_contract': binding_contract(goal),
            'duration_reason': '160/s300s per arm; fresh passed/restored control required before candidate; shared3600s ceiling.'}


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
    if start.tzinfo is None or not 0 <= (current-start).total_seconds() <= 3600-seconds:
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
              'cce_headroom_contract', 'cce_throughput_push_contract', 'cce_generator_process_contract',
              'cce_api_client_pool_contract', 'cce_primary_bootstrap_contract', 'cce_verification_contract_sha256',
              'cce_transaction_pair_sha256', 'cce_simulator_roundtrip_contract'}
    if any(original.get(k) != binding.get(k) for k in original.keys() | binding.keys() if k not in varied):
        raise ValueError('Roundtrip comparison changed unrelated resources, routes, settings or recovery')
    expected = {'baseline_sha256': REFERENCE[2], 'cce_simulator_roundtrip_contract': binding_contract(goal),
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

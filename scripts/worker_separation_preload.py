"""ADR0187 local pre-load checklist; never stages, deploys, reserves or dispatches."""
from datetime import UTC, datetime
from pathlib import Path

import check_repository_names as names
import work_envelope as policy
from worker_separation_staging import PROFILE, validate_archive_receipt, validate_contract
from worker_separation_topology import WORKERS, validate_pair

REQUIRED = {
    'artifact_identity': ('revision_pinned', 'images_and_sources_match', 'configuration_and_schema_match'),
    'topology_and_budgets': ('exact_layout', 'no_worker_overlap', 'connection_budgets_match'),
    'private_connectivity': ('verified_rds_tls', 'redis_and_kafka_ready', 'all_four_callback_apis_reachable'),
    'service_and_generator_health': ('services_ready', 'no_unexplained_restarts', 'disk_headroom', 'generator_ready', 'clocks_aligned'),
    'fixture_ownership': ('identity_receipt_retained', 'sale_window_covers_run_and_audit', 'tokens_and_authorization_valid'),
    'queue_baseline': ('all_queues_zero', 'kafka_drained', 'exclusive_worker_ownership'),
    'observation_coverage': ('all_replicas_observed', 'host_cpu_and_database_waits', 'no_missing_or_reset_metrics'),
    'safety_and_restoration': ('concurrency_and_replay_verified', 'payment_and_ticket_audits_ready',
                              'post_ttl_and_full_drain_ready', 'exact_rollback_qualified'),
}


def binding_for(inputs):
    if set(inputs) != {'pair', 'staging_contract', 'role_sources', 'saved_runtime_sha256', 'archive_receipt'}:
        raise ValueError('Exact prepared worker inputs required')
    pair, contract, sources = inputs['pair'], inputs['staging_contract'], inputs['role_sources']
    validate_pair(pair)
    validate_contract(contract, sources)
    validate_archive_receipt(contract, inputs['archive_receipt'])
    if (contract['prepared_pair_sha256'] != policy.digest(pair)
            or contract['role_images'] != {r: pair['candidate']['secondary']['services'][r]['image'] for r in WORKERS}
            or not isinstance(inputs['saved_runtime_sha256'], str)
            or len(inputs['saved_runtime_sha256']) != 64
            or any(c not in '0123456789abcdef' for c in inputs['saved_runtime_sha256'])):
        raise ValueError('Exact prepared image, source and saved runtime bindings required')
    return {'worker_pair_sha256': policy.digest(pair), 'worker_staging_contract_sha256': policy.digest(contract),
            'worker_sources_sha256': contract['source_manifest_sha256'],
            'worker_archive_receipt_sha256': policy.digest(inputs['archive_receipt']),
            'worker_saved_runtime_sha256': inputs['saved_runtime_sha256']}


def check_authority(binding, scope_key):
    """Read current policy independently; supplied evidence cannot override it."""
    blockers = []
    try:
        data = policy.envelope()
        records, state = policy.journal(data), policy.read(policy.STATE)
        if records['human_pause'] or state.get('human_pause') is True or policy.PAUSE.exists():
            blockers.append('human_pause')
        from pre_dispatch_abort_recovery import resolved
        if any(r['status'] == 'RECOVERY_REQUIRED' and not resolved(records, r, policy.ROOT) for r in records['experiments']):
            blockers.append('unresolved_recovery')
        if any(r['status'] == 'ACTIVE' and r['ledger'] != scope_key for r in records['experiments']):
            blockers.append('other_active_experiment')
        if any(isinstance(v, dict) and v.get('recovery_required') for k, v in state.items() if k.startswith('bounded_')):
            blockers.append('legacy_recovery_required')
        if state.get('cloud_load_requires_resume') and state.get('cloud_load_stop_kind') != 'scope_exhausted':
            blockers.append('unclassified_legacy_pause')
        if PROFILE not in policy.PROFILES or PROFILE not in data['qualified_profiles']:
            blockers.append('profile_not_registered')
        if not scope_key or not binding:
            blockers.append('fresh_bound_scope_missing')
        elif not blockers:
            active = state.get(scope_key, {}).get('binding', {})
            if any(active.get(k) != v for k, v in binding.items()):
                blockers.append('scope_binding_mismatch')
            else:
                item = policy.scope_authorized(state, scope_key, active)
                if item['profile'] != PROFILE: blockers.append('scope_profile_mismatch')
    except Exception as exc:  # noqa: BLE001 - malformed local policy must fail closed without exposing secrets.
        blockers.append('policy_validation_' + type(exc).__name__)
    return {'status': 'BLOCKED' if blockers else 'PASS', 'blockers': blockers}


def check_naming():
    try:
        paths, data = names.snapshot()
        failures = names.validate(paths, data)
        return {'status': 'BLOCKED' if failures else 'PASS', 'error_count': len(failures)}
    except Exception as exc:  # noqa: BLE001 - incomplete naming coverage fails preflight.
        return {'status': 'BLOCKED', 'failure_type': type(exc).__name__}


def evaluate(inputs=None, receipts=None, *, scope_key=None, now=None):
    now = now or datetime.now(UTC)
    binding, input_failure = None, None
    try:
        if inputs is None: raise ValueError('Prepared inputs unavailable')
        binding = binding_for(inputs)
    except Exception as exc:  # noqa: BLE001 - report absent/invalid inputs; do not echo private data.
        input_failure = type(exc).__name__
    binding_sha = policy.digest(binding) if binding else None
    checks = {'authorization_and_ownership': check_authority(binding, scope_key), 'repository_naming': check_naming()}
    receipts = receipts if isinstance(receipts, dict) else {}
    for category, required in REQUIRED.items():
        status, reason = 'UNKNOWN', 'missing_receipt'
        receipt = receipts.get(category)
        if receipt is not None:
            status, reason = 'BLOCKED', 'invalid_receipt'
            try:
                stamp = datetime.fromisoformat(receipt['checked_at'])
                if (set(receipt) == {'schema', 'binding_sha256', 'checked_at', 'checks', 'evidence_sha256'}
                        and type(receipt['schema']) is int and receipt['schema'] == 1
                        and binding_sha is not None and receipt['binding_sha256'] == binding_sha
                        and stamp.tzinfo is not None and now.tzinfo is not None
                        and 0 <= (now - stamp).total_seconds() <= 120
                        and isinstance(receipt['evidence_sha256'], str) and len(receipt['evidence_sha256']) == 64
                        and all(c in '0123456789abcdef' for c in receipt['evidence_sha256'])
                        and isinstance(receipt['checks'], dict) and set(receipt['checks']) == set(required)
                        and all(receipt['checks'][k] is True for k in required)):
                    status, reason = 'PASS', None
            except (KeyError, TypeError, ValueError):
                pass
        checks[category] = {'status': status, 'reason': reason}
    unknown_receipts = bool(set(receipts) - set(REQUIRED))
    ready = (input_failure is None and not unknown_receipts and all(c['status'] == 'PASS' for c in checks.values()))
    return {'decision': 'ADR0187', 'placement_decision': 'ADR0184', 'profile': PROFILE,
            'status': 'READY' if ready else 'BLOCKED', 'ready_for_load': ready,
            'load_authorized': False, 'cloud_calls': 0, 'binding_sha256': binding_sha,
            'input_failure_type': input_failure, 'unexpected_receipts': unknown_receipts,
            'checks': checks, 'scope': 'Diagnostic pre-load report only; registered guards still govern every action.'}


def from_files(inputs_path=None, receipts_path=None, *, scope_key=None):
    try:
        inputs = policy.read(Path(inputs_path)) if inputs_path else None
        receipts = policy.read(Path(receipts_path)) if receipts_path else None
        return evaluate(inputs, receipts, scope_key=scope_key)
    except Exception as exc:  # noqa: BLE001 - bounded runner reports file/parse failures without private contents.
        report = evaluate()
        report['input_failure_type'] = type(exc).__name__
        return report

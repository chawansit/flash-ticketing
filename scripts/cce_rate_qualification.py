"""ADR0355 explicit five-minute qualification of the measured combined image."""
import copy
import re

import work_envelope as policy

KEY = 'rate_qualification_decision'
APPROVAL = 'tmp/capacity-200-preapproval-20261019.private.json'


def selected(goal):
    if goal is None or KEY not in goal:
        return False
    ref = goal.get('rate_qualification_reference', {})
    rate = goal.get('rate_qualification_rate')
    if (goal[KEY] != 'ADR0355' or type(rate) is not int or rate not in {180, 200}
            or goal.get('simulator_roundtrip_arm') != 'candidate'
            or goal.get('replacement_rds_decision') != 'ADR0352'
            or goal.get('simulator_roundtrip_timing_decision') != 'ADR0353'
            or not isinstance(ref, dict) or set(ref) != {'path', 'ledger', 'sha256', 'rate'}
            or type(ref['rate']) is not int or ref['rate'] != rate-20
            or not re.fullmatch(r'tmp/adr0151-[0-9a-f]{12}/cce-comparison\.private\.json', ref.get('path', ''))
            or not re.fullmatch(r'bounded_cce_ticket_target__[0-9a-f]{12}', ref.get('ledger', ''))
            or not re.fullmatch(r'[0-9a-f]{64}', ref.get('sha256', ''))):
        raise ValueError('Exact ADR0355 candidate qualification and lower-rate reference required')
    return True


def rate(goal, default=160):
    return goal['rate_qualification_rate'] if selected(goal) else default


def contract(goal):
    if not selected(goal):
        raise ValueError('Explicit rate qualification required')
    return {'decision': 'ADR0355', 'offered_rate': rate(goal), 'seconds': 300,
            'expected_journeys': rate(goal)*300, 'minimum_unique_inside_window': rate(goal)*300-600,
            'previous': copy.deepcopy(goal['rate_qualification_reference']),
            'selected_arm': 'candidate', 'single_changed_factor': 'offered_rate',
            'maximum_paid_stages': 1, 'experiment_seconds_limit': 3600,
            'approval_sha256': policy.digest(policy.read(policy.ROOT / APPROVAL))}


def require_previous(goal):
    if not selected(goal):
        raise ValueError('Explicit rate qualification required')
    from cce_replacement_rds_binding import contract as replacement_contract
    from cce_simulator_roundtrip_profile import SOURCE, _retained
    ref = goal['rate_qualification_reference']
    report, binding, _ = _retained(ref['path'], ref['ledger'], ref['sha256'], expected=ref['rate']*300)
    previous = binding.get('cce_simulator_roundtrip_contract', {})
    if (previous.get('arm') != 'candidate' or previous.get('source') != SOURCE
            or previous.get('offered_rate') != ref['rate']
            or previous.get('replacement_rds') != replacement_contract(goal)
            or any(report['paid_stage']['customer'].get(k) != 0 for k in
                   ('generator_drops', 'first_attempt_error_journeys', 'final_customer_failures'))):
        raise ValueError('Clean lower-rate combined image on exact replacement RDS required')
    if ref['rate'] == 180 and previous.get('rate_qualification', {}).get('decision') != 'ADR0355':
        raise ValueError('200/s requires the passed180/s qualification')
    return report, binding


def require_authority(goal):
    approval = policy.read(policy.ROOT / APPROVAL)
    if (approval.get('user_authorized') is not True or approval.get('instruction') != 'pre-approved until clean 200/s'
            or approval.get('offered_seconds_each') != 300 or approval.get('rates') != [160, 180, 200]
            or approval.get('preserve_correctness_financial_drain_restoration') is not True
            or approval.get('preserve_consumed_attempts') is not True
            or approval.get('no_reset_existing_pair') is not True):
        raise ValueError('Explicit user preapproval through clean200/s required')
    return require_previous(goal)


def validate_binding(goal, binding):
    _, before = require_previous(goal)
    changed = {'baseline_sha256', 'cce_paid_entry_sources', 'cce_paid_core_sources', 'cce_manifest_sha256',
               'cce_headroom_contract', 'cce_throughput_push_contract', 'cce_generator_process_contract',
               'cce_api_client_pool_contract', 'cce_primary_bootstrap_contract', 'cce_verification_contract_sha256',
               'cce_simulator_roundtrip_contract', 'cce_inventory_renewal_contract', 'cce_metric_admission_contract_sha256'}
    import cce_fixed_worker_allocation as allocation
    if allocation.selected(goal):
        changed |= {'cce_writer_scale_contract', 'cce_worker_placement_factor'}
        allocation.validate_binding(goal, binding, before)
    if any(before.get(k) != binding.get(k) for k in before.keys() | binding.keys() if k not in changed):
        raise ValueError('Rate qualification changed image context, routes, settings or resource budgets')
    return True

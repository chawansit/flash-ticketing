"""ADR0357 explicit redistribution inside unchanged aggregate worker resources."""
import copy

from cce_combined_worker_identity import allocation_specification

KEY = 'fixed_worker_allocation_decision'


def selected(goal):
    if goal is None or KEY not in goal:
        return False
    from cce_rate_qualification import selected as qualification
    if goal[KEY] != 'ADR0357' or not qualification(goal):
        raise ValueError('Explicit fixed-budget rate qualification allocation required')
    return True


def specification():
    from cce_worker_identity import specification as original
    return allocation_specification(original('ADR0324'))


def contract(goal):
    if not selected(goal):
        raise ValueError('Explicit fixed worker allocation required')
    spec = specification()
    return {'decision': 'ADR0357', 'counts': copy.deepcopy(spec['counts']),
            'resources': copy.deepcopy(spec['resources']), 'worker_pods': 15,
            'worker_cpu_millicores': 3750, 'worker_memory_mib': 7680,
            'api_pods': 4, 'physical_backends': 24, 'shared_admission': 20,
            'application_image_unchanged': True}


def validate_binding(goal, binding, before):
    value = contract(goal)
    if (goal['rate_qualification_reference']['rate'] == 180
            and before.get('cce_simulator_roundtrip_contract', {}).get('worker_allocation') != value):
        raise ValueError('200/s requires clean180/s with the same worker allocation')
    wanted_factor = {**before['cce_worker_placement_factor'], 'worker_counts': value['counts'],
                     'worker_resources': value['resources']}
    wanted_scale = {**before['cce_writer_scale_contract'], 'decision': 'ADR0357',
                    'native_counts': value['counts'], 'worker_resources': value['resources']}
    if (binding.get('cce_worker_placement_factor') != wanted_factor
            or binding.get('cce_writer_scale_contract') != wanted_scale):
        raise ValueError('Exact fixed-total worker role/resource redistribution required')
    return True


def kafka_members(bundle):
    if bundle is None or bundle.get('decision') != 'ADR0357':
        return 6
    from cce_worker_identity import validate_transferred_bundle
    validate_transferred_bundle(bundle)
    return 5


def kafka_pass(summary, bundle, legacy):
    if kafka_members(bundle) == 6:
        return legacy(summary)
    groups = summary.get('last_member_partition_groups', [])
    return (summary.get('samples', 0) >= 5 and summary.get('sample_errors') == 0
            and summary.get('members_min') == summary.get('members_max') == 5
            and summary.get('ownership_observed') is True
            and summary.get('max_partitions_per_member') == 2
            and len(groups) == 5 and sorted(len(g) for g in groups) == [1,1,1,1,2]
            and all(type(p) is int for g in groups for p in g)
            and sorted(p for g in groups for p in g) == list(range(6)))


def apply_contract(value, goal):
    if not selected(goal):
        return value
    from cce_event_lane_contract import PROJECTION_AUDIT
    changed = PROJECTION_AUDIT.replace("result['kafka_members']==6", "result['kafka_members'] in (5,6)")
    if value.global_audit.count(PROJECTION_AUDIT) != 1 or changed == PROJECTION_AUDIT:
        raise ValueError('Exact projection-lane audit required before allocation')
    value.global_audit = value.global_audit.replace(PROJECTION_AUDIT, changed)
    return value


def native_drain_options(session):
    from cce_transaction_profile import active
    if selected(active()) and not getattr(session, 'cleanup_mode', False):
        return {'expected_members': 5}
    return {}

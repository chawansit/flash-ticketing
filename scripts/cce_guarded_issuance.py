"""ADR0365 explicitly approved full and guarded interval qualification."""
import work_envelope as policy
from cce_guarded_issuance_proposal import proposed_program

KEY = 'guarded_issuance_decision'
APPROVAL = 'docs/capacity/cce/guarded-window-approval-2026-10-19.json'


def selected(goal):
    if goal is None or KEY not in goal:
        return False
    import cce_persistent_uuid as image
    if goal[KEY] != 'ADR0365' or not image.selected(goal):
        raise ValueError('Explicit ADR0365 fixed-resource UUID qualification required')
    return True


def require_authority():
    value = policy.read(policy.ROOT / APPROVAL)
    expected = {'decision':'ADR0365','user_authorized':True,
                'instruction':'Approve correction and fresh 180 → conditional 200',
                'rates':[180,200],'offered_seconds':300,'guard_seconds_each':2,
                'guarded_seconds':296,'full_minima':[53400,59400],
                'guarded_minima':[53280,59200],'fresh_180_required_before_200':True,
                'preserve_all_other_gates':True,'historical_failures_remain_failed':True,
                'resources_unchanged':True}
    if value != expected:
        raise ValueError('Exact explicit guarded interval approval required')
    return value


def contract(goal):
    if not selected(goal):
        raise ValueError('Explicit guarded issuance selection required')
    require_authority()
    rate = goal['rate_qualification_rate']
    return {'decision':'ADR0365','seconds':300,'guard_seconds_each':2,
            'guarded_seconds':296,'minimum_full_count':rate*300-600,
            'minimum_guarded_count':rate*296,'approval_sha256':policy.digest(require_authority()),
            'historical_failures_remain_failed':True,'all_other_gates_unchanged':True}


def require_previous(goal, previous):
    if selected(goal) and goal['rate_qualification_rate'] == 200:
        expected = contract({**goal,'rate_qualification_rate':180})
        if previous.get('guarded_issuance') != expected:
            raise ValueError('200/s requires fresh clean180 under the approved guarded mode')


def program(events, start, goal):
    contract(goal)
    return proposed_program(events,start,goal['rate_qualification_rate'])

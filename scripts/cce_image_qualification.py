"""Explicit mutually exclusive image experiments; no inferred mode selection."""


def for_goal(goal):
    import cce_projection_refresh_pipeline as projection
    import cce_reservation_claim_lookup as claim
    keys=goal or {}
    if claim.KEY in keys and projection.KEY in keys:
        raise ValueError('Exactly one reviewed image experiment may be selected')
    return projection if projection.KEY in keys else claim


def selected(goal):
    return for_goal(goal).selected(goal)


def environment(role,values,goal):
    return for_goal(goal).environment(role,values,goal)

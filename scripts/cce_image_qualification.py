"""Explicit mutually exclusive image experiments; no inferred mode selection."""


def for_goal(goal):
    import cce_persistent_uuid as identifiers
    import cce_projection_refresh_pipeline as projection
    import cce_reservation_claim_lookup as claim
    import cce_simulator_claim_batch as batch
    keys=goal or {}
    if batch.KEY in keys:
        if claim.KEY in keys or projection.KEY in keys or identifiers.KEY not in keys:
            raise ValueError('Batch claims extend only the reviewed UUID image')
        return batch
    if sum(mode.KEY in keys for mode in (claim,projection,identifiers)) > 1:
        raise ValueError('Exactly one reviewed image experiment may be selected')
    if identifiers.KEY in keys:return identifiers
    return projection if projection.KEY in keys else claim


def selected(goal):
    return for_goal(goal).selected(goal)


def environment(role,values,goal):
    return for_goal(goal).environment(role,values,goal)


def api_environment(values,goal):
    mode=for_goal(goal)
    return mode.api_environment(values,goal) if hasattr(mode,'api_environment') else values


def normalize_api_environment(values,goal):
    mode=for_goal(goal)
    return mode.normalize_api_environment(values,goal) if hasattr(mode,'normalize_api_environment') else values

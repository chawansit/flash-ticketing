"""Exact one-factor experiment, offline only."""
import copy

import cce_payment_dispatch_profile as dispatch
import cce_transaction_profile as transaction
import pytest
from cce_shared_worker_contract import SharedWorkerContract


def goal():
    return {"decision": "ADR0228", "extension_decision": "ADR0263",
            "profile": "cce_paid_comparison", "comparison_arm": "correction",
            "acquisition_budget": 20, "candidate_factor": dispatch.factor()}


def test_single_correction_fixed_image_resources_and_workload():
    value = dispatch.plan(goal())
    assert value["simulator_concurrency"] == 16 and value["simulator_database_pool_max"] == 10
    assert value["pooler_server_connections"] == 24 and value["acquisition_budget"] == 20
    assert value["replicas"] == 4 and value["connections_per_api"] == 4
    assert value["common"] == {"buyer_journeys_per_second": 84, "duration_seconds": 300}
    assert value["gateway_delay_changes"] is False
    assert value["baseline_is_passing_control"] is False
    envelope = {"spending": {"temporary_cce_pilot_exception": {"goal_bounded_authorization": goal()}}}
    assert transaction.active(envelope) == goal()
    assert transaction.image_for(goal()) == dispatch.parent.image()


@pytest.mark.parametrize("key,value", [("candidate", 20), ("baseline", 8),
                                       ("simulator_database_pool_max", 16),
                                       ("gateway_delay_unchanged", False),
                                       ("database_connections_unchanged", False)])
def test_mixed_factors_rejected(key, value):
    changed = copy.deepcopy(goal())
    changed["candidate_factor"][key] = value
    with pytest.raises(ValueError):dispatch.active(changed)


def test_only_delivery_slots_change_in_worker_contract():
    old, new = SharedWorkerContract(), SharedWorkerContract(dispatch_slots=16)
    assert old.images == new.images
    for role in old.roles:
        a, b = old.settings(role), new.settings(role)
        if role == "simulator":
            assert a.pop("SIMULATOR_CONCURRENCY") == "12"
            assert b.pop("SIMULATOR_CONCURRENCY") == "16"
            assert b["DB_POOL_MAX"] == "10"
        assert a == b and old.source_map(role) == new.source_map(role)
    assert new.inventory_marker()["dispatch_correction_decision"] == "ADR0263"
    assert new.background["simulator"]["pool_per_replica"] == 10


@pytest.mark.parametrize("slots", [True, 8, 17, 32, "16"])
def test_unreviewed_dispatch_slots_rejected(slots):
    with pytest.raises(ValueError):SharedWorkerContract(dispatch_slots=slots)


def test_runner_keeps_payment_reservation_and_recovery_budgets(monkeypatch):
    import cce_api_adapter as native
    import run_cce_paid_comparison as runner
    from customer_recovery_bundle import enabled
    monkeypatch.setattr(transaction, "active", lambda *args: goal())
    current = runner.worker_contract()
    assert current.settings("simulator")["SIMULATOR_CONCURRENCY"] == "16"
    assert current.settings("simulator")["DB_POOL_MAX"] == "10"
    assert enabled(goal())
    assert native.contract()["workload"]["customer_retries"] == 3
    assert native.contract()["resources"]["limits"] == {"cpu": "1", "memory": "2Gi"}

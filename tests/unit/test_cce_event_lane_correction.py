"""Event-lane runner rejects partial identity, missing projection and budget drift."""
import copy
import json
from functools import lru_cache

import cce_event_lane_identity as identity
import cce_event_lane_profile as profile
import cce_transaction_profile as transaction
import pytest
from cce_event_lane_contract import PROJECTION_AUDIT, EventLaneContract


def goal():
    return {"decision": "ADR0228", "extension_decision": "ADR0266", "profile": "cce_paid_comparison",
            "comparison_arm": "correction", "acquisition_budget": 20, "candidate_factor": profile.factor()}


@lru_cache(maxsize=1)
def prepared_contract():
    return EventLaneContract()


def inventory():
    contract = copy.deepcopy(prepared_contract())
    sources = profile.image()["runtime_source_sha256"]
    proof = {k: True for k in ("source_hashes_match", "app_source_hashes_match", "import_source_hashes_match", "import_code_matches_source")}
    proof["resolved_imports"] = {k: {"sha256": v, "code_matches_source": True} for k, v in sources.items()}
    rows = []
    for role, count in identity.COUNTS.items():
        for _ in range(count):
            rows.append({"role": role, "container_id": str(len(rows)), "image_id": identity.IMAGE_ID,
                         "settings": contract.settings(role), "source_identity": copy.deepcopy(proof)})
    return {"worker_sources": rows, "status_refresh_contract": contract.inventory_marker(),
            "background": contract.background}


def test_fixed_budget_plan_and_actual_workers():
    value = profile.plan(goal())
    assert value["common"] == {"buyer_journeys_per_second": 84, "duration_seconds": 300}
    assert value["pooler_server_connections"] == 24 and value["acquisition_budget"] == 20
    assert value["replicas"] == value["connections_per_api"] == 4
    assert value["simulator_concurrency"] == 16 and value["simulator_database_pool_max"] == 10
    assert value["baseline_is_passing_control"] is False
    contract = copy.deepcopy(prepared_contract())
    assert sum(contract.background[r]["replicas"] * contract.background[r]["pool_per_replica"] for r in ("consumer", "projection-consumer")) == 48
    assert contract.settings("consumer")["EVENT_CONSUMER_SEPARATION"] == "1"
    assert contract.settings("projection-consumer")["EVENT_CONSUMER_SEPARATION"] == "1"
    assert contract.settings("simulator")["EVENT_CONSUMER_SEPARATION"] == "0"
    compile(contract.image_program(contract.roles), "<proof>", "exec")
    assert transaction.image_for(goal())["local_image_id"] == identity.IMAGE_ID
    data = inventory()
    assert identity.verify_inventory(data, identity.digest(data))


@pytest.mark.parametrize("fault", ["image", "source", "bytecode", "missing", "duplicate", "flag", "pool", "marker", "seal"])
def test_real_inventory_rejects_fault_before_historical_adapter(fault):
    data = inventory()
    row = next(w for w in data["worker_sources"] if w["role"] == "projection-consumer")
    if fault == "image": row["image_id"] = "sha256:" + "0" * 64
    elif fault == "source": next(iter(row["source_identity"]["resolved_imports"].values()))["sha256"] = "0" * 64
    elif fault == "bytecode": row["source_identity"]["import_code_matches_source"] = False
    elif fault == "missing": data["worker_sources"].remove(row)
    elif fault == "duplicate": row["container_id"] = data["worker_sources"][0]["container_id"]
    elif fault == "flag": row["settings"]["EVENT_CONSUMER_SEPARATION"] = "0"
    elif fault == "pool": row["settings"]["DB_POOL_MAX"] = "8"
    elif fault == "marker": data["status_refresh_contract"]["event_consumer_pool_budget"] = 50
    with pytest.raises(ValueError):
        identity.historical_view(data, "0" * 64 if fault == "seal" else identity.digest(data))


def test_historical_view_keeps_actual_retained_evidence():
    data = inventory()
    before = json.dumps(data, sort_keys=True)
    legacy, sealed = identity.historical_view(data, identity.digest(data))
    assert json.dumps(data, sort_keys=True) == before
    assert "projection-consumer" not in legacy["background"]
    assert len(legacy["worker_sources"]) == 14
    assert legacy["background"]["consumer"]["pool_per_replica"] == 8
    assert sealed == identity.digest(legacy)


@pytest.mark.parametrize("lag,members,partitions,expected", [(0, 1, 6, True), (1, 1, 6, False), (0, 0, 6, False), (0, 1, 5, False)])
def test_split_global_drain_requires_projection(lag, members, partitions, expected):
    class Observer:
        def sample(self, group):
            assert group == "ticketing-seat-projection-v1"
            return {"total_lag": lag, "members": members, "assigned_partitions": partitions}
        def close(self): self.closed = True
    result = {"kafka_members": 6, "pass": True}
    namespace = {"result": result, "os": __import__("os"), "python_observer": lambda *args: Observer()}
    exec(PROJECTION_AUDIT, namespace)  # noqa: S102 - Execute the exact read-only remote audit against fakes.
    assert result["pass"] is expected


def test_unavailable_projection_does_not_become_zero():
    class Observer:
        def sample(self, group): raise ValueError("Missing group")
        def close(self): pass
    with pytest.raises(ValueError):
        exec(PROJECTION_AUDIT, {"result": {"kafka_members": 6, "pass": True}, "os": __import__("os"), "python_observer": lambda *args: Observer()})  # noqa: S102 - Verify failure propagation from the deployed audit.


def test_normal_restoration_does_not_claim_split_lane_qualification():
    result = {"kafka_members": 1, "pass": True}
    exec(PROJECTION_AUDIT, {"result": result, "python_observer": lambda *args: pytest.fail("Dormant group must not be queried")})  # noqa: S102 - Verify normal-mode restoration.
    assert result == {"kafka_members": 1, "pass": True, "event_lane_mode": "normal"}


def test_projection_trace_requires_complete_ownership_and_final_zero():
    summary = {"samples": 300, "sample_errors": 0, "members_min": 1, "members_max": 1,
               "ownership_observed": True, "max_partitions_per_member": 6,
               "last_member_partition_groups": [list(range(6))], "last_total_lag": 0}
    assert identity.projection_summary_pass(summary)
    for field, bad in (("sample_errors", 1), ("members_min", 0), ("last_total_lag", 1), ("last_member_partition_groups", [[0, 1]])):
        assert not identity.projection_summary_pass({**summary, field: bad})


def test_runner_uses_same_api_and_gateway_budgets(monkeypatch):
    import cce_api_adapter as api
    import run_cce_paid_comparison as runner
    monkeypatch.setattr(transaction, "active", lambda *args: goal())
    contract = runner.worker_contract()
    assert isinstance(contract, EventLaneContract)
    assert api.admission_budget() == 20
    assert api.contract()["resources"]["limits"] == {"cpu": "1", "memory": "2Gi"}
    assert api.contract()["workload"]["customer_retries"] == 3
    assert api.contract()["api_settings"]["ORDER_STATUS_READ_PIPELINE"] == "0"


def test_projection_command_and_image_are_explicit():
    contract = copy.deepcopy(prepared_contract())
    model = {"services": {role: {"environment": {}, "image": "unused"} for role in contract.roles if role != "projection-consumer"}}
    rendered = contract.primary_model(model)
    assert rendered["services"]["projection-consumer"]["command"] == ["python", "-m", "ticketing.workers", "projection-consumer"]
    assert rendered["services"]["projection-consumer"]["image"] == identity.IMAGE_ID
    assert rendered["services"]["consumer"]["environment"]["DB_POOL_MAX"] == "7"
    assert rendered["services"]["projection-consumer"]["environment"]["DB_POOL_MAX"] == "6"
    assert "projection-consumer" not in model["services"]


def test_rejected_readiness_retains_actual_inventory_and_queue_evidence(tmp_path):
    from types import SimpleNamespace
    contract = copy.deepcopy(prepared_contract())
    observed = inventory()
    queues = {"pass": False, "projection_kafka_total_lag": 100}
    contract.retain_pre_safety_observation(SimpleNamespace(output=tmp_path), observed, queues)
    retained = json.loads((tmp_path / "event-lane-readiness.private.json").read_text(encoding="utf-8"))
    assert retained == {"inventory": observed, "queues": queues}
    assert retained["inventory"]["status_refresh_contract"]["shared_image_decision"] == "ADR0267"


def test_readiness_reason_preserved_without_weakening_rejection(monkeypatch):
    from types import SimpleNamespace

    from status_refresh_contract import StatusRefreshContract
    def reject(*args):
        raise ValueError("Pre-safety candidate contract failed")
    monkeypatch.setattr(StatusRefreshContract, "pre_safety", reject)
    saved = []
    session = SimpleNamespace(state={}, checkpoint=lambda: saved.append(True))
    with pytest.raises(ValueError, match="Pre-safety candidate"):
        copy.deepcopy(prepared_contract()).pre_safety(session, [], {})
    assert session.state["event_lane_readiness_rejection"] == "Pre-safety candidate contract failed"
    assert saved == [True]

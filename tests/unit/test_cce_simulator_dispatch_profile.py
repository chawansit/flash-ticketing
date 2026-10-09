"""Worker-only image isolation and unchanged CCE acquisition budgets."""
import copy
import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[2] / "scripts"))
import cce_api_adapter as api
import cce_simulator_dispatch_profile as simulator
import cce_transaction_profile as transaction
import generator_completion_probe_contract as baseline
import run_cce_paid_comparison as core
import work_envelope as policy


def goal():
    return {"decision": "ADR0228", "profile": "cce_paid_comparison", "extension_decision": "ADR0251",
            "comparison_arm": "correction", "acquisition_budget": 20,
            "candidate_factor": {"name": simulator.FACTOR, "comparison_arm": "correction", "baseline": 8,
                 "candidate": 12, "simulator_database_pool_max": 10,
                 "image_pair_receipt_sha256": transaction.PROOF_SHA256,
                 "simulator_image_receipt_sha256": simulator.PROOF_SHA256, "database_connections_unchanged": True}}


def selected(monkeypatch):
    envelope = copy.deepcopy(policy.envelope())
    envelope["spending"]["temporary_cce_pilot_exception"]["goal_bounded_authorization"] = goal()
    monkeypatch.setattr(policy, "envelope", lambda: envelope)


def test_only_simulator_role_changes_and_normal_restore_parents_remain_exact(monkeypatch):
    selected(monkeypatch)
    original = baseline.GeneratorCompletionProbeContract(baseline.plan()["artifact_receipt"], "candidate", api.legacy_contract()["api_sources"])
    worker = core.worker_contract()
    assert worker.background["simulator"]["concurrency"] == 12
    assert original.background["simulator"]["concurrency"] == 8
    assert worker.parents == original.parents
    assert worker.original_parents == original.original_parents
    assert worker.staging_contracts() == (worker,)
    for role in original.roles:
        if role != "simulator":
            assert worker.images[role] == original.images[role]
            assert worker.settings(role) == original.settings(role)
            assert worker.source_map(role) == original.source_map(role)
            assert worker.image_manifest(role) == original.image_manifest(role)
        else:
            assert worker.images[role] != original.images[role]
            assert worker.settings(role) == {**original.settings(role), "SIMULATOR_CONCURRENCY": "12",
                                           "SIMULATOR_DISPATCH_MODE": "refill", "DB_POOL_MAX": "10"}
            before, after = original.source_map(role), worker.source_map(role)
            assert [name for name in before if before[name] != after[name]] == ["src/ticketing/config.py"]
    worker.verify_worker_settings("simulator", worker.settings("simulator"))
    with pytest.raises(ValueError):
        worker.verify_worker_settings("simulator", {**worker.settings("simulator"), "DB_POOL_MAX": "12"})


def test_api_image_resources_and_payment_budgets_are_unchanged(monkeypatch):
    selected(monkeypatch)
    assert api.api_image() == transaction.image_pair()["images"]["control"]["registry_image"]
    assert api.admission_budget() == 20
    assert transaction.payment_connections(goal()) == 2
    plan = core.plan()
    assert plan["common"] == {"buyer_journeys_per_second": 84, "duration_seconds": 300}
    assert plan["baseline_is_passing_control"] is False
    assert plan["simulator_database_pool_max"] == 10
    assert plan["connections_per_api"] == 4
    assert plan["paid_runs_authorized"] == 1
    assert plan["single_changed_factor"] == simulator.FACTOR


@pytest.mark.parametrize("fault", ["arm", "pool", "concurrency", "proof"])
def test_changed_factor_rejected(fault):
    value = goal()
    if fault == "arm":
        value["comparison_arm"] = "candidate"
    elif fault == "pool":
        value["candidate_factor"]["simulator_database_pool_max"] = 12
    elif fault == "concurrency":
        value["candidate_factor"]["candidate"] = 16
    else:
        value["candidate_factor"]["simulator_image_receipt_sha256"] = "0" * 64
    with pytest.raises(ValueError, match="discriminator"):
        simulator.active(value)


def test_proof_drift_rejected(monkeypatch, tmp_path):
    p = tmp_path / simulator.PROOF
    p.parent.mkdir(parents=True)
    p.write_bytes(b"{}")
    monkeypatch.setattr(policy, "ROOT", tmp_path)
    with pytest.raises(ValueError, match="image receipt"):
        simulator.receipt()


def correction_observer_inventory():
    worker = simulator.worker_contract()
    return {"arm": "candidate", "status_refresh_contract": worker.inventory_marker(),
            "background": worker.background,
            "worker_sources": [{"role": "simulator", "image_id": worker.images["simulator"],
                "settings": worker.settings("simulator"), "source_identity": {"source_hashes_match": True,
                "resolved_imports": {"src/ticketing/config.py": {"sha256": worker.source_map("simulator")["src/ticketing/config.py"]}}}}]}


def test_observer_verifies_correction_before_legacy_projection_and_retains_actual():
    from observe_slot_paid_pipeline import correction_inventory
    data = correction_observer_inventory()
    original = copy.deepcopy(data)
    view, digest = correction_inventory(data, policy.digest(data))
    assert data == original
    assert data["background"]["simulator"]["concurrency"] == 12
    assert view["background"]["simulator"]["concurrency"] == 8
    assert "correction_decision" not in view["status_refresh_contract"]
    assert view["worker_sources"] == data["worker_sources"]
    assert digest == policy.digest(view)


@pytest.mark.parametrize("fault", ["pool", "concurrency", "source", "image", "proof", "digest"])
def test_observer_rejects_unqualified_correction(fault):
    from observe_slot_paid_pipeline import correction_inventory
    data = correction_observer_inventory()
    if fault == "pool": data["worker_sources"][0]["settings"]["DB_POOL_MAX"] = "12"
    elif fault == "concurrency": data["background"]["simulator"]["concurrency"] = 16
    elif fault == "source": data["worker_sources"][0]["source_identity"]["resolved_imports"]["src/ticketing/config.py"]["sha256"] = "0" * 64
    elif fault == "image": data["worker_sources"][0]["image_id"] = "sha256:" + "0" * 64
    elif fault == "proof": data["status_refresh_contract"]["simulator_receipt_sha256"] = "0" * 64
    approved = "0" * 64 if fault == "digest" else policy.digest(data)
    with pytest.raises(ValueError): correction_inventory(data, approved)


def test_per_action_receipt_checks_do_not_reconstruct_historical_plan(monkeypatch):
    import prepare_simulator_dispatch_sources as builder
    monkeypatch.setattr(builder, "source_plan", lambda: (_ for _ in ()).throw(AssertionError("historical reconstruction in action guard")))
    for _ in range(10):
        assert simulator.receipt()["runtime_settings"]["simulator_concurrency"] == 12


def test_worker_preparation_still_requires_complete_source_plan(monkeypatch):
    import prepare_simulator_dispatch_sources as builder
    monkeypatch.setattr(builder, "source_plan", lambda: (None, {"runtime_source_sha256": {}, "parent_runtime_source_sha256": {}}))
    with pytest.raises(ValueError, match="prepared simulator source plan"):
        simulator.worker_contract()

"""Same published image, explicit activation and fail-closed comparison admission."""
import copy

import cce_api_adapter as api
import cce_event_lane_identity as old_pins
import cce_payment_pipeline_identity as pins
import cce_payment_pipeline_profile as profile
import cce_transaction_profile as transaction
import cce_verification_contract as verification
import cce_worker_identity as workers
import cce_worker_rebalance_profile as placement
import pytest
import work_envelope as policy
from cce_payment_pipeline_contract import PaymentPipelineContract
from observe_slot_paid_pipeline import correction_inventory
from test_cce_event_lane_correction import inventory


def goal(arm="control"):
    return {"decision": "ADR0228", "extension_decision": "ADR0271", "profile": "cce_paid_comparison",
            "comparison_arm": "correction", "acquisition_budget": 20, "candidate_factor": placement.factor(),
            "offered_journeys_per_second": 84, "offered_seconds": 300, "maximum_paid_stages": 1,
            "payment_pipeline_decision": "ADR0284", "payment_pipeline_arm": arm,
            "payment_pipeline_receipt_sha256": pins.RECEIPT_SHA256,
            "verification_decision": "ADR0285", "verification_contract_sha256": verification.digest(verification.contract())}


@pytest.mark.parametrize("arm", ["control", "candidate"])
def test_exact_same_image_only_measured_api_flag_changes(monkeypatch, arm):
    value = goal(arm)
    envelope = copy.deepcopy(policy.envelope())
    envelope["spending"]["temporary_cce_pilot_exception"]["goal_bounded_authorization"] = value
    monkeypatch.setattr(policy, "envelope", lambda: envelope)
    assert profile.selected(value)
    assert transaction.image_for(value)["local_image_id"] == pins.IMAGE_ID
    assert transaction.proof_digest(value) == pins.RECEIPT_SHA256
    actual = api.contract()
    assert actual["api_settings"]["PAYMENT_WRITE_PIPELINE"] == ("1" if arm == "candidate" else "0")
    assert actual["resources"]["limits"] == {"cpu": "1", "memory": "2Gi"}
    assert actual["api_settings"]["API_PAYMENT_POOL_MAX"] == "2"
    assert api.admission_budget() == 20
    contract = PaymentPipelineContract(arm=arm)
    assert contract.shared["local_image_id"] == pins.IMAGE_ID
    assert all(contract.settings(r)["PAYMENT_WRITE_PIPELINE"] == "0" for r in contract.roles)
    assert contract.api_settings["PAYMENT_WRITE_PIPELINE"] == "0"  # Unmeasured ECS bootstrap.
    assert workers.specification("ADR0285")["source"] == pins.SOURCE
    assert workers.specification("ADR0271")["source"] == old_pins.SOURCE
    assert sum(workers.specification("ADR0285")["counts"].values()) == 14
    compile(contract.image_program(contract.roles), "<image-proof>", "exec")


@pytest.mark.parametrize("fault", ["receipt", "image", "source", "bytecode", "worker_flag", "bootstrap_flag", "marker", "seal"])
def test_reject_drift_before_legacy_observer_adapter(fault):
    data = inventory()
    contract = PaymentPipelineContract(arm="candidate", status_refresh_reuse=False)
    data["status_refresh_contract"] = contract.inventory_marker()
    for api_row in data["apis"]:
        api_row["settings"] = copy.deepcopy(contract.api_settings)
    source = profile.image()["runtime_source_sha256"]
    for row in data["worker_sources"]:
        row["image_id"] = pins.IMAGE_ID
        row["settings"] = contract.settings(row["role"])
        row["source_identity"]["resolved_imports"] = {k: {"sha256": v, "code_matches_source": True} for k, v in source.items()}
    baseline = copy.deepcopy(data)
    legacy, digest = correction_inventory(data, old_pins.digest(data))
    assert data == baseline and digest == old_pins.digest(legacy)
    row = data["worker_sources"][0]
    if fault == "receipt": data["status_refresh_contract"]["shared_image_receipt_sha256"] = old_pins.RECEIPT_SHA256
    elif fault == "image": row["image_id"] = old_pins.IMAGE_ID
    elif fault == "source": next(iter(row["source_identity"]["resolved_imports"].values()))["sha256"] = "0" * 64
    elif fault == "bytecode": row["source_identity"]["import_code_matches_source"] = False
    elif fault == "worker_flag": row["settings"]["PAYMENT_WRITE_PIPELINE"] = "1"
    elif fault == "bootstrap_flag": data["apis"][0]["settings"]["PAYMENT_WRITE_PIPELINE"] = "1"
    elif fault == "marker": data["status_refresh_contract"]["payment_write_pipeline"] = "0"
    with pytest.raises(ValueError):
        correction_inventory(data, "0" * 64 if fault == "seal" else old_pins.digest(data))


def test_candidate_requires_fully_passed_restored_same_contract_control():
    with pytest.raises(ValueError, match="control ledger"):
        profile.require_control(goal("candidate"))


@pytest.mark.parametrize("key,value", [("offered_seconds", 3600), ("offered_journeys_per_second", 100),
    ("payment_pipeline_receipt_sha256", "0" * 64), ("verification_contract_sha256", "0" * 64)])
def test_reject_unapproved_scope_or_changed_verification(key, value):
    changed = goal()
    changed[key] = value
    with pytest.raises(ValueError):
        profile.selected(changed)

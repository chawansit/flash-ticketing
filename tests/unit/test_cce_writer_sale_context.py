import copy
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[2] / "scripts"))

import cce_committed_status_profile as parent
import cce_native_workers as native_workers
import cce_recovery_headroom as headroom
import cce_transaction_profile as transaction
import cce_worker_identity as worker
import cce_writer_sale_context_profile as context
import pytest
import work_envelope as policy
from cce_committed_status_contract import CommittedStatusContract
from cce_paid_profiles import HEADROOM
from test_cce_committed_status_comparison import goal as parent_goal


def goal():
    value = parent_goal("candidate")
    value.update(decision="ADR0277", extension_decision="ADR0277", profile="cce_ticket_target",
        offered_journeys_per_second=102, maximum_pods=19, payment_slot_arm="control",
        headroom_decision="ADR0312", headroom_contract_sha256=policy.digest(headroom.contract()),
        headroom_slot_correction_decision="ADR0314", headroom_slot_failure_sha256=headroom.FAILURE_SHA256,
        reservation_writer_scale_decision="ADR0315", reservation_writer_failure_sha256=headroom.SLOT_SHA256,
        gateway_read_recovery_decision="ADR0311", baseline_sha256=headroom.WRITER_SHA256,
        writer_sale_context_decision="ADR0321", writer_sale_context_receipt_sha256=context.pins.RECEIPT_SHA256,
        writer_sale_context_contract_sha256=policy.digest(context.contract()))
    return value


def test_exact_image_and_only_one_source_delta_preserve_parent():
    corrected, original = context.image(), parent.image()
    assert corrected["registry_manifest_digest"] == context.pins.IMAGE_ID
    assert [k for k in original["runtime_source_sha256"] if original["runtime_source_sha256"][k]
            != corrected["runtime_source_sha256"][k]] == ["src/ticketing/infrastructure/reservations.py"]
    assert corrected["dependency_input_sha256"] == original["dependency_input_sha256"]
    assert transaction.image_for(goal())["registry_image"] == corrected["registry_image"]
    assert transaction.proof_digest(goal()) == context.pins.RECEIPT_SHA256
    assert parent.image() == original


@pytest.mark.parametrize("key,value", [
    ("writer_sale_context_receipt_sha256", "0" * 64), ("writer_sale_context_contract_sha256", "0" * 64),
    ("offered_journeys_per_second", 103), ("offered_seconds", 301), ("maximum_paid_stages", 2),
    ("generator_concurrency", 501), ("acquisition_budget", 21), ("maximum_pods", 20),
    ("baseline_sha256", "0" * 64), ("backend_partition_decision", "ADR0318"),
    ("writer_batch_decision", "ADR0316"), ("payment_pipeline_decision", "ADR0285")])
def test_partial_or_confounded_scope_is_rejected(key, value):
    with pytest.raises(ValueError, match="fixed-budget"):
        context.selected({**goal(), key: value})


def test_normal_bootstrap_and_native_counts_share_corrected_image_without_shrinking_writer_gate():
    normal, native = worker.specification("ADR0321-bootstrap"), worker.specification("ADR0321")
    assert normal["counts"]["reservation-writer"] == 3 and sum(normal["counts"].values()) == 14
    assert native["counts"]["reservation-writer"] == 4 and sum(native["counts"].values()) == 15
    assert normal["image_id"] == native["image_id"] == context.pins.IMAGE_ID
    assert worker.verify_sources(context.image()["runtime_source_sha256"], "ADR0321")
    assert worker.specification("ADR0315")["image_id"] == parent.pins.IMAGE_ID


def test_native_worker_image_and_consumer_activation_match_api_scope(monkeypatch):
    value = goal()
    envelope = copy.deepcopy(policy.envelope())
    envelope["spending"]["temporary_cce_pilot_exception"]["goal_bounded_authorization"] = value
    monkeypatch.setattr(policy, "envelope", lambda: envelope)
    monkeypatch.setattr(headroom, "require_short", lambda g: ("unit-only", {}))
    assert headroom.selected(value) and context.selected(headroom.runtime_view(value))
    contract = CommittedStatusContract(arm="candidate", identity=context.pins)
    assert contract.inventory_marker()["shared_image_decision"] == "ADR0321"
    assert all(contract.images[role] == context.pins.IMAGE_ID for role in contract.background)
    compile(contract.image_program(contract.background), "<context-proof>", "exec")
    env = {role: {"DATABASE_URL": "postgresql://ticketing:dummy@pgbouncer:5432/ticketing",
                  "API_URL": "http://load-balancer:8000", "RESERVATION_WRITER_BATCH_SIZE": "4", **contract.settings(role)}
           for role in worker.specification("ADR0321")["counts"]}
    objects = native_workers.objects("adr0151-123456abcdef", env, "10.1.137.69", "ADR0321", profile=HEADROOM)
    pods = [o for o in objects if o["kind"] == "Pod"]
    assert len(pods) == 15
    assert all(p["spec"]["containers"][0]["image"] == context.image()["registry_image"] for p in pods)
    assert native_workers.environment("consumer", env["consumer"], "10.1.137.69", "ADR0321")[
        "ORDER_STATUS_COMMITTED_SNAPSHOT"] == "1"
    assert native_workers.environment("reservation-writer", env["reservation-writer"], "10.1.137.69", "ADR0321")[
        "RESERVATION_WRITER_BATCH_SIZE"] == "4"


@pytest.mark.parametrize("fault", [None, "old_image", "old_source", "active_bootstrap"])
def test_candidate_inventory_is_validated_before_historical_projection(fault):
    from observe_slot_paid_pipeline import correction_inventory
    from test_cce_event_lane_correction import inventory
    data = inventory()
    contract = CommittedStatusContract(arm="candidate", identity=context.pins)
    data["status_refresh_contract"] = contract.inventory_marker()
    for api in data["apis"]:
        api["settings"] = copy.deepcopy(contract.api_settings)
    sources = context.image()["runtime_source_sha256"]
    for row in data["worker_sources"]:
        row["image_id"] = context.pins.IMAGE_ID
        row["settings"] = contract.settings(row["role"])
        row["source_identity"]["resolved_imports"] = {
            k: {"sha256": v, "code_matches_source": True} for k, v in sources.items()}
    if fault == "old_image":
        data["worker_sources"][0]["image_id"] = parent.pins.IMAGE_ID
    elif fault == "old_source":
        row = data["worker_sources"][0]
        row["source_identity"]["resolved_imports"] = {
            k: {"sha256": v, "code_matches_source": True}
            for k, v in parent.image()["runtime_source_sha256"].items()}
    elif fault == "active_bootstrap":
        data["worker_sources"][0]["settings"]["ORDER_STATUS_COMMITTED_SNAPSHOT"] = "1"
    original = copy.deepcopy(data)
    if fault:
        with pytest.raises(ValueError):
            correction_inventory(data, policy.digest(data))
    else:
        legacy, digest = correction_inventory(data, policy.digest(data))
        assert digest == policy.digest(legacy)
        assert "committed_status_decision" not in legacy["status_refresh_contract"]
    assert data == original

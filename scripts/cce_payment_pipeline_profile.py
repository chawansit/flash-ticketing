"""ADR0285 short off/on comparison using one published shared image."""
import hashlib
import json

import cce_payment_pipeline_identity as pins
import work_envelope as policy


def image():
    path = policy.ROOT / pins.RECEIPT
    if path.is_symlink() or hashlib.sha256(path.read_bytes().replace(b"\r\n", b"\n")).hexdigest() != pins.RECEIPT_SHA256:
        raise ValueError("Exact reviewed payment pipeline publication receipt required")
    value = json.loads(path.read_text(encoding="utf-8"))
    if (value.get("decision") != pins.DECISION or value.get("source_identity_sha256") != pins.SOURCE
            or value.get("registry_manifest_digest") != pins.IMAGE_ID
            or value.get("local_image_id") != pins.IMAGE_ID
            or value.get("registry_configuration_digest") != pins.CONFIG
            or value.get("registry_image") != "swr.ap-southeast-2.myhuaweicloud.com/chawansit/flash-ticketing@" + pins.IMAGE_ID
            or value.get("registry_pulled_runtime_verified") is not True or value.get("registry_published") is not True
            or value.get("feature_default") is not False or value.get("verified_modules") != 22):
        raise ValueError("Published immutable payment pipeline runtime required")
    return {**value, "runtime_sources_sha256": value["runtime_source_sha256"]}


def selected(goal):
    if goal is None or "payment_pipeline_decision" not in goal:
        return False
    from cce_verification_contract import selected as verification_selected
    if (goal.get("payment_pipeline_decision") != pins.DECISION
            or goal.get("payment_pipeline_arm") not in {"control", "candidate"}
            or goal.get("payment_pipeline_receipt_sha256") != pins.RECEIPT_SHA256
            or goal.get("extension_decision") != "ADR0271"
            or goal.get("offered_journeys_per_second") != 84 or goal.get("offered_seconds") != 300
            or goal.get("maximum_paid_stages") != 1 or not verification_selected(goal)):
        raise ValueError("Exact bounded payment pipeline comparison required")
    image()
    return True


def require_control(goal):
    if not selected(goal) or goal["payment_pipeline_arm"] != "candidate":
        raise ValueError("Candidate-only passing control requirement")
    import cce_transaction_profile as transaction
    relative, report = transaction.control_receipt(goal)
    binding = policy.read(policy.STATE)[goal["control_ledger"]]["binding"]
    if (binding.get("cce_payment_pipeline_arm") != "control"
            or binding.get("cce_payment_write_pipeline") != "0"
            or binding.get("cce_verification_contract_sha256") != goal["verification_contract_sha256"]
            or binding.get("cce_payment_pipeline_receipt_sha256") != pins.RECEIPT_SHA256):
        raise ValueError("Passing same-image, same-verification pipeline-off control required")
    return relative, report


def extend_plan(plan, goal):
    if not selected(goal):
        return plan
    value = image()
    from cce_verification_contract import contract
    arm = goal["payment_pipeline_arm"]
    baseline = "docs/capacity/cce/observer-cost-paid-result-2026-10-12.json"
    if arm == "candidate":
        baseline, _ = require_control(goal)
    return {**plan, "kind": "payment_write_pipeline_comparison", "single_changed_factor": "API_PAYMENT_WRITE_PIPELINE",
            "payment_pipeline_decision": pins.DECISION, "payment_pipeline_arm": arm,
            "payment_write_pipeline": "1" if arm == "candidate" else "0",
            "verification_contract": contract(), "image_pair_receipt_sha256": pins.RECEIPT_SHA256,
            "arm_manifest_digest": pins.IMAGE_ID, "arm_configuration_digest": pins.CONFIG,
            "arm_api_source_sha256": value["runtime_sources_sha256"], "baseline_evidence": baseline,
            "baseline_sha256": policy.digest(policy.read(policy.ROOT / baseline)),
            "baseline_is_passing_control": arm == "candidate",
            "duration_reason": "One five-minute same-image payment pipeline arm with complete audit and restoration."}

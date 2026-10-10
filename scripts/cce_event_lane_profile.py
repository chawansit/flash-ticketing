"""ADR0266 one fixed-budget isolated consumer-lane correction."""
import hashlib
import json

import cce_event_lane_identity as pins
import cce_payment_dispatch_profile as parent
import work_envelope as policy

BASELINE = "docs/capacity/cce/payment-dispatch-16-result-2026-10-10.json"


def image():
    path = policy.ROOT / pins.RECEIPT
    if path.is_symlink() or hashlib.sha256(path.read_bytes().replace(b"\r\n", b"\n")).hexdigest() != pins.RECEIPT_SHA256:
        raise ValueError("Exact reviewed event-lane receipt required")
    value = json.loads(path.read_text(encoding="utf-8"))
    if (value.get("decision") != "ADR0267" or value.get("source_identity_sha256") != pins.SOURCE
            or value.get("registry_configuration_digest") != pins.CONFIG
            or value.get("local_image_id") != pins.IMAGE_ID
            or value.get("registry_manifest_digest") != pins.IMAGE_ID
            or value.get("registry_pulled_runtime_verified") is not True):
        raise ValueError("Immutable published and pulled event-lane image required")
    return {**value, "runtime_sources_sha256": value["runtime_source_sha256"]}


def factor():
    return {"name": "independent_event_consumer_progress", "comparison_arm": "correction",
            "fulfillment_consumers": 6, "projection_consumers": 1,
            "fulfillment_pool_per_replica": 7, "projection_pool_per_replica": 6,
            "consumer_local_pool_budget": 48, "image_receipt_sha256": pins.RECEIPT_SHA256,
            "database_connections_unchanged": True, "gateway_delay_unchanged": True}


def active(goal):
    if (goal.get("extension_decision") != "ADR0266" or goal.get("decision") != "ADR0228"
            or goal.get("profile") != "cce_paid_comparison" or goal.get("comparison_arm") != "correction"
            or goal.get("acquisition_budget") != 20 or goal.get("candidate_factor") != factor()):
        raise ValueError("Exact independent-event-progress correction required")
    image()
    return goal


def plan(goal):
    active(goal)
    legacy = {**goal, "extension_decision": "ADR0263", "candidate_factor": parent.factor()}
    result = parent.plan(legacy)
    value = image()
    return {**result, "extension_decision": "ADR0266", "kind": "event_lane_progress_correction",
            "single_changed_factor": "independent_event_consumer_progress",
            "baseline_evidence": BASELINE, "baseline_sha256": policy.digest(policy.read(policy.ROOT / BASELINE)),
            "baseline_is_passing_control": False, "image_pair_receipt_sha256": pins.RECEIPT_SHA256,
            "arm_manifest_digest": value["registry_manifest_digest"],
            "arm_configuration_digest": pins.CONFIG, "arm_api_source_sha256": value["runtime_sources_sha256"],
            "consumer_counts": {"fulfillment": 6, "projection": 1},
            "consumer_local_pool_budget": 48,
            "duration_reason": "One five-minute independent-progress correction; both groups must drain before restoration."}

"""ADR0298 same-image short comparison with existing correctness gates."""
import hashlib
import json

import cce_committed_status_identity as pins
import work_envelope as policy


def image():
    path = policy.ROOT / pins.RECEIPT
    if path.is_symlink() or hashlib.sha256(path.read_bytes().replace(b"\r\n", b"\n")).hexdigest() != pins.RECEIPT_SHA256:
        raise ValueError("Exact committed-status publication receipt required")
    value = json.loads(path.read_text(encoding="utf-8"))
    if (value.get("decision") != pins.DECISION or value.get("source_identity_sha256") != pins.SOURCE
            or value.get("registry_manifest_digest") != pins.IMAGE_ID
            or value.get("registry_configuration_digest") != pins.CONFIG
            or value.get("registry_pulled_runtime_verified") is not True
            or value.get("registry_published") is not True or value.get("feature_default") is not False
            or value.get("verified_modules") != 22
            or value.get("registry_image") != "swr.ap-southeast-2.myhuaweicloud.com/chawansit/flash-ticketing@" + pins.IMAGE_ID):
        raise ValueError("Verified immutable committed-status image required")
    return {**value, "runtime_sources_sha256": value["runtime_source_sha256"]}


def selected(goal):
    keys = {"committed_status_decision", "committed_status_arm", "committed_status_receipt_sha256"}
    if goal is None or not keys.intersection(goal):
        return False
    if (goal.get("committed_status_decision") != "ADR0298"
            or goal.get("committed_status_arm") not in {"control", "candidate"}
            or goal.get("committed_status_receipt_sha256") != pins.RECEIPT_SHA256
            or goal.get("extension_decision") != "ADR0271" or goal.get("profile") != "cce_paid_comparison"
            or goal.get("offered_journeys_per_second") != 84 or goal.get("offered_seconds") != 300
            or goal.get("maximum_paid_stages") != 1 or goal.get("maximum_experiment_seconds") != 3600
            or goal.get("acquisition_budget") != 20 or goal.get("generator_concurrency") != 500
            or goal.get("ingress_route_arm") != "candidate"
            or goal.get("pod_vcpu") != 1 or goal.get("pod_memory_gib") != 2
            or goal.get("maximum_pods") != 18
            or goal.get("worker_resources") != {"cpu": "250m", "memory": "512Mi"}):
        raise ValueError("Exact bounded committed-status comparison required")
    from cce_metric_admission import selected as metric_selected
    from cce_nginx_ingress import selected as ingress_selected
    from cce_verification_contract import selected as verification_selected
    if not verification_selected(goal) or not metric_selected(goal) or not ingress_selected(goal):
        raise ValueError("Unchanged verification, admission capture and route required")
    for key in ("offered_journeys_per_second", "offered_seconds", "maximum_paid_stages",
                "maximum_experiment_seconds", "acquisition_budget", "generator_concurrency",
                "pod_vcpu", "pod_memory_gib", "maximum_pods"):
        if type(goal[key]) is not int:
            raise ValueError("Explicit integer comparison bounds required")
    image()
    return True


def require_control(goal):
    if not selected(goal) or goal["committed_status_arm"] != "candidate":
        raise ValueError("Candidate-only control prerequisite")
    import cce_transaction_profile as transaction
    relative, report = transaction.control_receipt(goal)
    binding = policy.read(policy.STATE)[goal["control_ledger"]]["binding"]
    if (binding.get("cce_committed_status_arm") != "control"
            or binding.get("cce_committed_status_snapshot") != "0"
            or binding.get("cce_committed_status_receipt_sha256") != pins.RECEIPT_SHA256
            or binding.get("cce_verification_contract_sha256") != goal["verification_contract_sha256"]
            or binding.get("cce_ingress_route_arm") != "candidate"):
        raise ValueError("Passed same-image, same-route and same-verification control required")
    return relative, report


def extend_plan(plan, goal):
    if not selected(goal):
        return plan
    value = image()
    baseline = "docs/capacity/cce/existing-ingress-hourly-result-2026-10-13.json"
    if goal["committed_status_arm"] == "candidate":
        baseline, _ = require_control(goal)
    return {**plan, "kind": "committed_fulfillment_status_comparison",
            "single_changed_factor": "consumer_ORDER_STATUS_COMMITTED_SNAPSHOT",
            "committed_status_decision": "ADR0298", "committed_status_arm": goal["committed_status_arm"],
            "committed_status_snapshot": "1" if goal["committed_status_arm"] == "candidate" else "0",
            "image_pair_receipt_sha256": pins.RECEIPT_SHA256, "arm_manifest_digest": pins.IMAGE_ID,
            "arm_configuration_digest": pins.CONFIG, "arm_api_source_sha256": value["runtime_sources_sha256"],
            "baseline_evidence": baseline, "baseline_sha256": policy.digest(policy.read(policy.ROOT / baseline)),
            "baseline_is_passing_control": goal["committed_status_arm"] == "candidate",
            "duration_reason": "One five-minute same-image consumer publication arm; long-run stability remains unqualified."}

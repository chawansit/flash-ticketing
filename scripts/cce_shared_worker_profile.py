"""ADR0259 explicit bounded shared-image placement profile."""
import hashlib

import cce_shared_worker_comparison as comparison
import work_envelope as policy

BASELINE = "docs/capacity/cce/customer-recovery-hourly-2026-10-10.json"


def proof_digest():
    return hashlib.sha256(comparison.RECEIPT.read_bytes().replace(b"\r\n", b"\n")).hexdigest()


def image():
    value = comparison.selected_receipt()
    return {**value, "runtime_sources_sha256": value["runtime_source_sha256"]}


def active(goal):
    arm = goal.get("comparison_arm")
    expected = {"name": "worker_placement", "comparison_arm": arm,
                "placement_plan_sha256": comparison.digest(comparison.plan()),
                "shared_image_receipt_sha256": proof_digest(),
                "database_connections_unchanged": True}
    if (arm not in {"control", "candidate"} or goal.get("decision") != "ADR0228"
            or goal.get("profile") != "cce_paid_comparison" or goal.get("acquisition_budget") != 20
            or goal.get("candidate_factor") != expected):
        raise ValueError("Exact ADR0259 shared-image placement discriminator required")
    comparison.selected_receipt()
    return goal


def plan(goal):
    active(goal)
    from cce_transaction_profile import control_receipt
    if goal["comparison_arm"] == "candidate":
        path, evidence = control_receipt(goal)
    else:
        path, evidence = BASELINE, policy.read(policy.ROOT / BASELINE)
    value = image()
    return {"decision": "ADR0228", "arms": ["candidate"], "extension_decision": "ADR0259",
            "comparison_arm": goal["comparison_arm"], "kind": "shared_image_worker_placement",
            "single_changed_factor": "worker_placement",
            "common": {"buyer_journeys_per_second": 84, "duration_seconds": 300},
            "diagnostic_connection_decision": "ADR0180", "baseline_evidence": path,
            "baseline_sha256": policy.digest(evidence),
            "baseline_is_passing_control": goal["comparison_arm"] == "candidate",
            "image_pair_receipt_sha256": proof_digest(),
            "arm_manifest_digest": value["registry_manifest_digest"],
            "arm_configuration_digest": value["registry_configuration_digest"],
            "arm_api_source_sha256": value["runtime_sources_sha256"],
            "common_environment_changes": {"DB_FAILURE_DIAGNOSTICS": "1"},
            "connections_per_api": 4, "payment_connections_per_api": 2, "general_connections_per_api": 2,
            "pod_resources": {k: {"cpu": "1", "memory": "2Gi"} for k in ("requests", "limits")},
            "worker_resources": {k: {"cpu": "250m", "memory": "512Mi"} for k in ("requests", "limits")},
            "replicas": 4, "pooler_server_connections": 24, "acquisition_budget": 20,
            "bridge_cpu_limit": 1, "simulator_concurrency": 12, "simulator_database_pool_max": 10,
            "customer_recovery_max_attempts": 3,
            "recovery_bundle_sha256": comparison.plan()["recovery_bundle_sha256"],
            "qualification_runs_authorized": 1, "paid_runs_authorized": 1, "safety_tickets_authorized": 2,
            "duration_reason": "One fresh five-minute arm; candidate requires fully passed restored control with identical role behavior."}

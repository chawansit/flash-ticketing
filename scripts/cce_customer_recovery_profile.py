"""ADR0254/0255 fixed recovery control and status-pipeline comparison."""

import hashlib
import re

import work_envelope as policy
from customer_recovery_bundle import MANIFEST_SHA256 as GENERATOR_SHA256

PROOF = "docs/capacity/cce/customer-recovery-image-2026-10-10.json"
PROOF_SHA256 = "06cdc522b719f888906de7aa90704e6ef65b222e81b4997283a2d4bea3dff888"
BASELINE = "docs/capacity/cce/simulator-dispatch-comparison-2026-10-10.json"
FACTOR = "order_status_read_pipeline"


def receipt(*, require_registry=True):
    path = policy.ROOT / PROOF
    if path.is_symlink() or hashlib.sha256(path.read_bytes().replace(b"\r\n", b"\n")).hexdigest() != PROOF_SHA256:
        raise ValueError("Reviewed recovery image receipt required")
    data = policy.read(path)
    parent, images = data["parent_runtime_sources_sha256"], data["images"]
    from prepare_customer_recovery_image import CHANGED
    if (data["decision"] != "ADR0255" or data["platform"] != {"os": "linux", "architecture": "amd64"}
            or set(images) != {"control", "candidate"} or images["control"] != images["candidate"]
            or len(parent) != 22 or set(images["control"]["runtime_sources_sha256"]) != set(parent)
            or {n for n in parent if parent[n] != images["control"]["runtime_sources_sha256"][n]} != CHANGED
            or data["offline_validation"]["modules_verified"] != 22
            or any(data["offline_validation"].get(n) is not True for n in (
                "all_modules_imported", "payment_operation_in_openapi", "pipeline_default_off"))):
        raise ValueError("Same recovery image and exact source scope required")
    if require_registry:
        image = images["control"]
        if (data.get("registry_published") is not True or data.get("registry_pulled_runtime_verified") is not True
                or not all(re.fullmatch(r"sha256:[0-9a-f]{64}", image.get(n) or "")
                           for n in ("registry_manifest_digest", "registry_configuration_digest"))
                or image["registry_manifest_digest"] == image["registry_configuration_digest"]
                or image["registry_image"] != "swr.ap-southeast-2.myhuaweicloud.com/chawansit/flash-ticketing@" + image["registry_manifest_digest"]):
            raise ValueError("Published and registry-pulled recovery image required; SWR credential pending")
    return data


def active(goal):
    arm = goal.get("comparison_arm")
    expected = {"name": FACTOR, "comparison_arm": arm, "baseline": "0", "candidate": "1",
                "image_pair_receipt_sha256": PROOF_SHA256, "recovery_bundle_sha256": GENERATOR_SHA256,
                "recovery_max_attempts": 3, "database_connections_unchanged": True}
    if (arm not in {"control", "candidate"} or goal.get("decision") != "ADR0228"
            or goal.get("profile") != "cce_paid_comparison" or goal.get("acquisition_budget") != 20
            or goal.get("candidate_factor") != expected):
        raise ValueError("Exact recovery/status-pipeline short comparison required")
    receipt()
    return goal


def plan(goal):
    active(goal)
    from cce_api_adapter import legacy_contract
    from cce_transaction_profile import control_receipt
    if goal["comparison_arm"] == "candidate":
        path, evidence = control_receipt(goal)
    else:
        path, evidence = BASELINE, policy.read(policy.ROOT / BASELINE)
    image = receipt()["images"]["control"]
    return {"decision": "ADR0228", "arms": ["candidate"], "extension_decision": "ADR0255",
            "comparison_arm": goal["comparison_arm"], "kind": "matched_recovery_status_pipeline",
            "single_changed_factor": FACTOR, "common": {"buyer_journeys_per_second": 84, "duration_seconds": 300},
            "diagnostic_connection_decision": "ADR0180", "baseline_evidence": path,
            "baseline_sha256": policy.digest(evidence), "baseline_is_passing_control": goal["comparison_arm"] == "candidate",
            "image_pair_receipt_sha256": PROOF_SHA256, "arm_manifest_digest": image["registry_manifest_digest"],
            "arm_configuration_digest": image["registry_configuration_digest"], "arm_api_source_sha256": image["runtime_sources_sha256"],
            "common_environment_changes": {"DB_FAILURE_DIAGNOSTICS": "1"}, "customer_recovery_max_attempts": 3,
            "recovery_bundle_sha256": GENERATOR_SHA256, "connections_per_api": 4, "payment_connections_per_api": 2,
            "general_connections_per_api": 2, "pod_resources": legacy_contract()["resources"], "replicas": 4,
            "pooler_server_connections": 24, "acquisition_budget": 20, "bridge_cpu_limit": 1,
            "simulator_concurrency": 12, "simulator_database_pool_max": 10,
            "qualification_runs_authorized": 1, "paid_runs_authorized": 1, "safety_tickets_authorized": 2,
            "duration_reason": "Same five-minute workload and budgets; failed recovery-enabled control stops comparison. Mandatory audits and restoration follow."}

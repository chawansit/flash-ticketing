"""ADR0263 one sixteen-slot correction; fixed image, placement and SQL budgets."""
import cce_shared_worker_comparison as shared
import cce_shared_worker_profile as parent
import work_envelope as policy

BASELINE = "docs/capacity/cce/shared-worker-consumer-restored-control-2026-10-10.json"
CONCURRENCY = 16


def factor():
    return {"name": "callback_dispatch_concurrency", "comparison_arm": "correction",
            "baseline": 12, "candidate": CONCURRENCY, "simulator_database_pool_max": 10,
            "shared_image_receipt_sha256": parent.proof_digest(),
            "database_connections_unchanged": True, "gateway_delay_unchanged": True}


def active(goal):
    if (goal.get("extension_decision") != "ADR0263" or goal.get("decision") != "ADR0228"
            or goal.get("profile") != "cce_paid_comparison" or goal.get("comparison_arm") != "correction"
            or goal.get("acquisition_budget") != 20 or goal.get("candidate_factor") != factor()):
        raise ValueError("Exact sixteen-slot single correction required")
    parent.image()
    return goal


def plan(goal):
    active(goal)
    # Reuse the fixed shared-image resource and workload contract, not placement execution.
    legacy = {**goal, "comparison_arm": "control", "candidate_factor": {
        "name": "worker_placement", "comparison_arm": "control",
        "placement_plan_sha256": shared.digest(shared.plan()),
        "shared_image_receipt_sha256": parent.proof_digest(), "database_connections_unchanged": True}}
    result = parent.plan(legacy)
    evidence = policy.read(policy.ROOT / BASELINE)
    return {**result, "extension_decision": "ADR0263", "comparison_arm": "correction",
            "kind": "shared_image_dispatch_correction", "single_changed_factor": "callback_dispatch_concurrency",
            "baseline_evidence": BASELINE, "baseline_sha256": policy.digest(evidence),
            "baseline_is_passing_control": False, "simulator_concurrency": CONCURRENCY,
            "gateway_delay_changes": False,
            "duration_reason": "One five-minute correction on the existing topology; no placement comparison or hourly qualification."}

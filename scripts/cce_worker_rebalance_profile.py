"""ADR0271 fixed-budget event-lane worker placement correction."""
import cce_event_lane_identity as pins
import cce_event_lane_profile as parent
import cce_worker_identity as workers
import work_envelope as policy

BASELINE = "docs/capacity/cce/event-lane-paid-result-2026-10-10.json"


def factor():
    return {"name": "event_lane_worker_placement", "comparison_arm": "correction",
            "worker_counts": workers.specification("ADR0271")["counts"],
            "worker_resources": workers.RESOURCES, "image_receipt_sha256": pins.RECEIPT_SHA256,
            "database_connections_unchanged": True, "gateway_delay_unchanged": True}


def active(goal):
    if (goal.get("extension_decision") != "ADR0271" or goal.get("decision") != "ADR0228"
            or goal.get("profile") != "cce_paid_comparison" or goal.get("comparison_arm") != "correction"
            or goal.get("acquisition_budget") != 20 or goal.get("candidate_factor") != factor()):
        raise ValueError("Exact event-lane worker placement correction required")
    parent.image()
    return goal


def plan(goal):
    active(goal)
    legacy = {**goal, "extension_decision": "ADR0266", "candidate_factor": parent.factor()}
    result = parent.plan(legacy)
    return {**result, "extension_decision": "ADR0271", "kind": "event_lane_worker_placement_correction",
            "single_changed_factor": "event_lane_worker_placement", "baseline_evidence": BASELINE,
            "baseline_sha256": policy.digest(policy.read(policy.ROOT / BASELINE)),
            "baseline_is_passing_control": False, "worker_counts": factor()["worker_counts"],
            "worker_resources": workers.RESOURCES, "maximum_pods": 18,
            "total_requested_vcpu": 7.5, "total_requested_memory_gib": 15,
            "diagnostic_decision": "ADR0272",
            "duration_reason": "One five-minute worker placement correction; actual paid cohort and both lanes must drain."}

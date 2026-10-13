"""ADR0304 observer-only comparison; application flag and budgets stay fixed."""
import re
from pathlib import Path

import work_envelope as policy
from paid_cohort_query import runtime_contract

BASELINE = "docs/capacity/cce/query-attribution-paid-repeat-2026-10-13.json"
KEYS = {"runtime_cohort_decision", "runtime_cohort_arm", "runtime_cohort_contract_sha256",
        "runtime_cohort_control_ledger", "runtime_cohort_control_report_path"}


def selected(goal):
    if goal is None or not KEYS.intersection(goal):
        return False
    arm = goal.get("runtime_cohort_arm")
    if (goal.get("runtime_cohort_decision") != "ADR0304"
            or arm not in {"control", "candidate"}
            or goal.get("runtime_cohort_contract_sha256") != policy.digest(runtime_contract(arm))
            or goal.get("profile") != "cce_paid_comparison"
            or goal.get("extension_decision") != "ADR0271"
            or goal.get("committed_status_decision") != "ADR0298"
            or goal.get("committed_status_arm") != "candidate"
            or goal.get("offered_journeys_per_second") != 84
            or goal.get("offered_seconds") != 300
            or goal.get("maximum_paid_stages") != 1
            or goal.get("maximum_experiment_seconds") != 3600
            or goal.get("acquisition_budget") != 20
            or goal.get("observer_cost_decision") != "ADR0283"
            or any(k.startswith("payment_pipeline_") for k in goal)):
        raise ValueError("Exact short observer-only comparison required")
    controls = {"runtime_cohort_control_ledger", "runtime_cohort_control_report_path"}
    if (arm == "control" and controls.intersection(goal)) or (arm == "candidate" and not controls <= goal.keys()):
        raise ValueError("Fresh observer control prerequisite required")
    return True


def require_control(goal):
    if not selected(goal) or goal["runtime_cohort_arm"] != "candidate":
        raise ValueError("Observer candidate-only control prerequisite")
    ledger = goal["runtime_cohort_control_ledger"]
    relative = goal["runtime_cohort_control_report_path"]
    parts = Path(relative)
    if (not re.fullmatch(r"bounded_cce_paid_comparison__[0-9a-f]{12}", ledger)
            or parts.is_absolute() or ".." in parts.parts or "\\" in relative):
        raise ValueError("Canonical observer control receipt required")
    path = policy.ROOT / parts
    if (path.is_symlink() or not path.resolve().is_relative_to((policy.ROOT / "tmp").resolve())
            or path.name != "cce-comparison.private.json"):
        raise ValueError("Owned observer control report required")
    report = policy.read(path)
    state = policy.read(policy.STATE).get(ledger, {})
    binding = state.get("binding", {})
    rows = [v for v in policy.journal(policy.envelope())["experiments"] if v.get("ledger") == ledger]
    if (len(rows) != 1 or rows[0].get("status") != "PASSED_RESTORED"
            or rows[0].get("result_sha256") != policy.digest(report)
            or state.get("cce_paid_result_sha256") != policy.digest(report)
            or report.get("binding_sha256") != policy.digest(binding)
            or report.get("pass") is not True
            or report.get("restoration_complete") is not True
            or report.get("integrity_verified") is not True
            or report.get("transport_credentials_cleared") is not True
            or report.get("native", {}).get("cleanup_complete") is not True
            or binding.get("cce_runtime_cohort_contract") != runtime_contract("control")
            or binding.get("cce_committed_status_snapshot") != "1"
            or not report.get("native", {}).get("measurement_gates")
            or not all(report["native"]["measurement_gates"].values())
            or report.get("paid_stage", {}).get("customer", {}).get("generator_drops") != 0
            or report.get("paid_stage", {}).get("customer", {}).get("final_customer_failures") != 0
            or report.get("paid_stage", {}).get("observation", {}).get("runtime_cohort_receipts_complete") is not True):
        raise ValueError("Passing restored bound observer control required")
    from cce_metric_admission import contract as metric_contract
    from cce_paid_stage import identity
    if (binding.get("cce_paid_core_sources") != identity()
            or binding.get("cce_metric_admission_contract_sha256") != policy.digest(metric_contract())
            or binding.get("cce_verification_contract_sha256") != goal.get("verification_contract_sha256")
            or binding.get("cce_ingress_route_contract_sha256") != goal.get("ingress_route_contract_sha256")
            or binding.get("cce_acquisition_budget") != 20):
        raise ValueError("Observer control source/configuration drift")
    return relative, report


def extend_plan(plan, goal):
    if not selected(goal):
        return plan
    baseline = BASELINE
    if goal["runtime_cohort_arm"] == "candidate":
        baseline, _ = require_control(goal)
    return {**plan, "kind": "runtime_cohort_query_comparison",
            "single_changed_factor": "observer_runtime_cohort_query",
            "runtime_cohort_contract": runtime_contract(goal["runtime_cohort_arm"]),
            "baseline_evidence": baseline,
            "baseline_sha256": policy.digest(policy.read(policy.ROOT / baseline)),
            "baseline_is_passing_control": goal["runtime_cohort_arm"] == "candidate",
            "duration_reason": "One unchanged 84/s, 300s observer query arm; application snapshot enabled in both."}


def validate_candidate_binding(goal, binding):
    require_control(goal)
    prior = policy.read(policy.STATE)[goal["runtime_cohort_control_ledger"]]["binding"]
    # Fresh experiment namespace, fixture and baseline pointer legitimately differ.
    ignored = {"cce_runtime_cohort_contract", "cce_manifest_sha256", "baseline_sha256"}
    old = {k: v for k, v in prior.items() if k not in ignored}
    new = {k: v for k, v in binding.items() if k not in ignored}
    if old != new or binding.get("cce_runtime_cohort_contract") != runtime_contract("candidate"):
        raise ValueError("Only observer SQL may differ from the passing control")
    return True

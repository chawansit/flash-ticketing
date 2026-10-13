"""ADR0306 revisit the API partition on the fixed committed-status runtime."""
import work_envelope as policy
from cce_customer_outcome_slo import contract as outcome_contract
from cce_customer_outcome_slo import selected as outcome_selected

BASELINE = "docs/capacity/cce/runtime-cohort-control-result-2026-10-13.json"
KEYS = {"payment_slot_decision", "payment_slot_arm"}


def selected(goal):
    if goal is None or not KEYS.intersection(goal):
        return False
    if (goal.get("payment_slot_decision") != "ADR0306"
            or goal.get("payment_slot_arm") not in {"control", "candidate"}
            or goal.get("extension_decision") != "ADR0271"
            or goal.get("profile") != "cce_paid_comparison"
            or goal.get("committed_status_decision") != "ADR0298"
            or goal.get("committed_status_arm") != "candidate"
            or goal.get("offered_journeys_per_second") != 84
            or goal.get("offered_seconds") != 300
            or goal.get("maximum_paid_stages") != 1
            or goal.get("maximum_experiment_seconds") != 3600
            or goal.get("acquisition_budget") != 20
            or goal.get("observer_cost_decision") != "ADR0283"
            or any(k.startswith(("payment_pipeline_", "runtime_cohort_")) for k in goal)
            or not outcome_selected(goal)):
        raise ValueError("Exact fixed-budget payment partition comparison required")
    return True


def connections(goal):
    return 3 if selected(goal) and goal["payment_slot_arm"] == "candidate" else 2


def require_control(goal):
    if not selected(goal) or goal["payment_slot_arm"] != "candidate":
        raise ValueError("Candidate requires a fresh payment-slot control")
    import cce_transaction_profile as transaction
    relative, report = transaction.control_receipt(goal)
    binding = policy.read(policy.STATE)[goal["control_ledger"]]["binding"]
    if (binding.get("cce_payment_slot_decision") != "ADR0306"
            or binding.get("cce_payment_slot_arm") != "control"
            or binding.get("cce_payment_pool_max") != 2
            or binding.get("cce_committed_status_snapshot") != "1"
            or binding.get("cce_customer_outcome_contract") != outcome_contract()):
        raise ValueError("Passing same-target payment-slot control required")
    return relative, report


def extend_plan(plan, goal):
    if not selected(goal):
        return plan
    baseline = BASELINE
    if goal["payment_slot_arm"] == "candidate":
        baseline, _ = require_control(goal)
    payment = connections(goal)
    return {**plan, "kind": "fixed_budget_payment_slot_comparison",
            "single_changed_factor": "API_PAYMENT_POOL_MAX", "payment_slot_decision": "ADR0306",
            "payment_slot_arm": goal["payment_slot_arm"],
            "payment_connections_per_api": payment, "general_connections_per_api": 4 - payment,
            "connections_per_api": 4, "customer_outcome_contract": outcome_contract(),
            "baseline_evidence": baseline, "baseline_sha256": policy.digest(policy.read(policy.ROOT / baseline)),
            "baseline_is_passing_control": goal["payment_slot_arm"] == "candidate"}


def validate_candidate_binding(goal, binding):
    _, report = require_control(goal)
    original = report["comparison_binding"]
    differences = {k for k in original.keys() | binding.keys() if original.get(k) != binding.get(k)}
    allowed = {"cce_payment_slot_arm", "cce_payment_pool_max", "cce_transaction_arm",
               "cce_manifest_sha256", "baseline_sha256"}
    if differences - allowed or binding.get("cce_payment_pool_max") != 3:
        raise ValueError("Candidate changed factors outside the fixed payment partition")

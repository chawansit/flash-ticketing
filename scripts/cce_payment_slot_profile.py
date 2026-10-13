"""ADR0306 revisit the API partition on the fixed committed-status runtime."""
import copy
import re
from pathlib import Path

import work_envelope as policy
from cce_customer_outcome_slo import contract as outcome_contract
from cce_customer_outcome_slo import selected as outcome_selected

BASELINE = "docs/capacity/cce/runtime-cohort-control-result-2026-10-13.json"
KEYS = {"payment_slot_decision", "payment_slot_arm"}
SELF = "scripts/cce_payment_slot_profile.py"
REVIEWED_PRIOR_VALIDATOR = "d86d9f74ea8c26ef0993b0c38304ef57cd076e2519eec9130672b1e2cf8dc1b7"


def compatible_sources(original, current):
    if original == current:
        return True
    if (not isinstance(original, dict) or not isinstance(current, dict)
            or original.keys() != current.keys()
            or original.get(SELF) != REVIEWED_PRIOR_VALIDATOR
            or any(original[k] != current[k] for k in original if k != SELF)):
        raise ValueError("Source drift outside the reviewed admission validator")
    return True


def selected(goal):
    if goal is None or not KEYS.intersection(goal):
        return False
    from cce_committed_hourly_profile import selected as hourly_selected
    hourly = hourly_selected(goal)
    if (goal.get("payment_slot_decision") != "ADR0306"
            or goal.get("payment_slot_arm") not in {"control", "candidate"}
            or goal.get("extension_decision") != "ADR0271"
            or goal.get("profile") != ("cce_hourly_qualification" if hourly else "cce_paid_comparison")
            or goal.get("committed_status_decision") != "ADR0298"
            or goal.get("committed_status_arm") != "candidate"
            or goal.get("offered_journeys_per_second") != 84
            or goal.get("offered_seconds") != (3600 if hourly else 300)
            or goal.get("maximum_paid_stages") != 1
            or goal.get("maximum_experiment_seconds") != (5400 if hourly else 3600)
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
    from customer_gateway_read_bundle import selected as gateway_selected
    if gateway_selected(goal):
        from cce_gateway_read_profile import require_reference
        return require_reference(goal)
    from cce_committed_hourly_profile import selected as hourly_selected
    if hourly_selected(goal):
        from cce_committed_hourly_profile import require_short
        return require_short(goal)
    ledger, relative = goal.get("control_ledger", ""), goal.get("control_report_path", "")
    path = Path(relative)
    if (not re.fullmatch(r"bounded_cce_paid_comparison__[0-9a-f]{12}", ledger)
            or path.is_absolute() or ".." in path.parts or "\\" in relative
            or path.name != "cce-comparison.private.json"):
        raise ValueError("Canonical passing control receipt required")
    path = policy.ROOT / path
    if path.is_symlink() or not path.resolve().is_relative_to((policy.ROOT / "tmp").resolve()):
        raise ValueError("Owned passing control receipt required")
    report = policy.read(path)
    state = policy.read(policy.STATE).get(ledger, {})
    binding = state.get("binding", {})
    entries = [v for v in policy.journal(policy.envelope())["experiments"] if v.get("ledger") == ledger]
    import run_cce_paid_comparison as core
    compatible_sources(binding.get("cce_paid_entry_sources"), core.identity())
    compatible_sources(binding.get("cce_paid_core_sources"), core.paid.identity())
    gates = report.get("native", {}).get("measurement_gates", {})
    if (len(entries) != 1 or entries[0].get("status") != "PASSED_RESTORED"
            or entries[0].get("result_sha256") != policy.digest(report)
            or state.get("cce_paid_result_sha256") != policy.digest(report)
            or report.get("binding_sha256") != policy.digest(binding)
            or any(report.get(k) is not True for k in
                   ("pass", "restoration_complete", "integrity_verified", "transport_credentials_cleared"))
            or report.get("capacity_stages_started") != 1
            or state.get("paid_runs_started") != 1 or state.get("active_run")
            or report.get("native", {}).get("cleanup_complete") is not True
            or not gates or any(v is not True for v in gates.values())
            or binding.get("configuration_sha256") != policy.envelope()["existing_resource_configuration_sha256"]
            or binding.get("cce_acquisition_budget") != 20
            or binding.get("cce_payment_slot_decision") != "ADR0306"
            or binding.get("cce_payment_slot_arm") != "control"
            or binding.get("cce_payment_pool_max") != 2
            or binding.get("cce_committed_status_snapshot") != "1"
            or binding.get("cce_customer_outcome_contract") != outcome_contract()
            or report.get("paid_stage", {}).get("customer_outcome_gate", {}).get("pass") is not True):
        raise ValueError("Passing same-target payment-slot control required")
    return relative, report


def extend_plan(plan, goal):
    if not selected(goal):
        return plan
    baseline = BASELINE
    if goal["payment_slot_arm"] == "candidate":
        baseline, _ = require_control(goal)
    payment = connections(goal)
    from customer_gateway_read_bundle import selected as gateway_selected
    if gateway_selected(goal):
        from cce_gateway_read_profile import extend_plan as gateway_plan
        return gateway_plan({**plan, "payment_connections_per_api": payment,
                             "general_connections_per_api": 4 - payment,
                             "connections_per_api": 4}, goal)
    return {**plan, "kind": "fixed_budget_payment_slot_comparison",
            "single_changed_factor": "API_PAYMENT_POOL_MAX", "payment_slot_decision": "ADR0306",
            "payment_slot_arm": goal["payment_slot_arm"],
            "payment_connections_per_api": payment, "general_connections_per_api": 4 - payment,
            "connections_per_api": 4, "customer_outcome_contract": outcome_contract(),
            "baseline_evidence": baseline, "baseline_sha256": policy.digest(policy.read(policy.ROOT / baseline)),
            "baseline_is_passing_control": goal["payment_slot_arm"] == "candidate"}


def validate_service_semantics(goal, original, binding):
    path = Path(goal.get("payment_slot_snapshot_path", ""))
    if (path.is_absolute() or ".." in path.parts or path.name != "snapshot.private.json"
            or "\\" in goal.get("payment_slot_snapshot_path", "")):
        raise ValueError("Canonical retained control snapshot required")
    path = policy.ROOT / path
    if path.is_symlink() or not path.resolve().is_relative_to((policy.ROOT / "tmp").resolve()):
        raise ValueError("Owned retained control snapshot required")
    import run_cce_paid_comparison as core
    candidate = core.api_semantics(core.service_for(policy.read(path)["saved"]))
    control = copy.deepcopy(candidate)
    if candidate["environment"].get("API_PAYMENT_POOL_MAX") != "3":
        raise ValueError("Exactly three payment connections required")
    control["environment"]["API_PAYMENT_POOL_MAX"] = "2"
    if (policy.digest(control) != original.get("saved_api_service_sha256")
            or policy.digest(candidate) != binding.get("saved_api_service_sha256")):
        raise ValueError("Saved service changed outside the payment partition")


def validate_candidate_binding(goal, binding):
    from customer_gateway_read_bundle import selected as gateway_selected
    if gateway_selected(goal):
        from cce_gateway_read_profile import validate_binding
        return validate_binding(goal, binding)
    require_control(goal)
    original = policy.read(policy.STATE)[goal["control_ledger"]]["binding"]
    reference = copy.deepcopy(original)
    for key in ("cce_paid_entry_sources", "cce_paid_core_sources"):
        compatible_sources(reference[key], binding[key])
        reference[key] = binding[key]
    differences = {k for k in reference.keys() | binding.keys() if reference.get(k) != binding.get(k)}
    allowed = {"cce_payment_slot_arm", "cce_payment_pool_max", "cce_transaction_arm",
               "cce_manifest_sha256", "baseline_sha256", "saved_api_service_sha256"}
    if differences - allowed or binding.get("cce_payment_pool_max") != 3:
        raise ValueError("Candidate changed factors outside the fixed payment partition")
    validate_service_semantics(goal, original, binding)
    if original["cce_paid_core_sources"] != binding["cce_paid_core_sources"]:
        binding["cce_admission_validator_lineage"] = {
            "decision": "ADR0306", "changed_source": SELF,
            "from_sha256": original["cce_paid_core_sources"][SELF],
            "to_sha256": binding["cce_paid_core_sources"][SELF],
            "application_and_other_runner_sources_unchanged": True}

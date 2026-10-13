"""ADR0308 conditional reuse of a passed short runtime for hourly qualification."""
import re
from pathlib import Path

import cce_committed_status_identity as pins
import cce_customer_outcome_slo as outcome
import cce_inventory_renewal as renewal
import work_envelope as policy

KEYS = {"committed_hourly_decision", "qualification_short_ledger", "qualification_short_report_path"}


def selected(goal):
    if goal is None or not KEYS.intersection(goal):
        return False
    if (goal.get("committed_hourly_decision") != "ADR0308"
            or goal.get("hourly_qualification_decision") != "ADR0281"
            or goal.get("extension_decision") != "ADR0271"):
        raise ValueError("Explicit committed-runtime hourly binding required")
    return True


def short_view(goal):
    value = {**goal, "decision": "ADR0228", "profile": "cce_paid_comparison",
             "offered_seconds": 300, "maximum_experiment_seconds": 3600,
             "payment_slot_arm": "control"}
    for key in (*KEYS, "hourly_qualification_decision", "hourly_adapters_sha256",
                "hourly_observer_bootstrap_decision", "ingress_hourly_decision",
                "ingress_hourly_prerequisite_sha256"):
        value.pop(key, None)
    return value


def require_short(goal):
    if not selected(goal):
        raise ValueError("Explicit committed hourly short prerequisite required")
    ledger, relative = goal.get("qualification_short_ledger", ""), goal.get("qualification_short_report_path", "")
    path = Path(relative)
    if (not re.fullmatch(r"bounded_cce_paid_comparison__[0-9a-f]{12}", ledger)
            or path.is_absolute() or ".." in path.parts or "\\" in relative
            or path.name != "cce-comparison.private.json"):
        raise ValueError("Canonical short prerequisite required")
    path = policy.ROOT / path
    if path.is_symlink() or not path.resolve().is_relative_to((policy.ROOT / "tmp").resolve()):
        raise ValueError("Owned short prerequisite required")
    report = policy.read(path)
    state = policy.read(policy.STATE).get(ledger, {})
    binding = state.get("binding", {})
    entries = [v for v in policy.journal(policy.envelope())["experiments"] if v.get("ledger") == ledger]
    customer = report.get("paid_stage", {}).get("customer", {})
    issuance = report.get("paid_stage", {}).get("short_issuance_window", {})
    financial = report.get("paid_stage", {}).get("financial", {})
    gates = report.get("native", {}).get("measurement_gates", {})
    payment = 3 if goal.get("payment_slot_arm") == "candidate" else 2
    if (len(entries) != 1 or entries[0].get("status") != "PASSED_RESTORED"
            or entries[0].get("result_sha256") != policy.digest(report)
            or state.get("cce_paid_result_sha256") != policy.digest(report)
            or state.get("paid_runs_started") != 1 or state.get("active_run")
            or report.get("capacity_stages_started") != 1
            or report.get("binding_sha256") != policy.digest(binding)
            or any(report.get(k) is not True for k in
                   ("pass", "restoration_complete", "integrity_verified", "transport_credentials_cleared"))
            or report.get("native", {}).get("cleanup_complete") is not True
            or not gates or any(v is not True for v in gates.values())
            or binding.get("configuration_sha256") != policy.envelope()["existing_resource_configuration_sha256"]
            or binding.get("cce_payment_slot_decision") != "ADR0306"
            or binding.get("cce_payment_slot_arm") != goal.get("payment_slot_arm")
            or binding.get("cce_payment_pool_max") != payment
            or binding.get("cce_committed_status_receipt_sha256") != pins.RECEIPT_SHA256
            or binding.get("cce_committed_status_snapshot") != "1"
            or binding.get("cce_acquisition_budget") != 20
            or binding.get("cce_customer_outcome_contract") != outcome.contract()
            or binding.get("cce_inventory_renewal_contract") != renewal.contract()
            or report.get("paid_stage", {}).get("customer_outcome_gate", {}).get("pass") is not True
            or customer.get("scheduled") != 25200 or customer.get("dispatched") != 25200
            or customer.get("generator_drops") != 0
            or financial.get("pass") is not True or financial.get("hold_deadlines_elapsed") is not True
            or issuance.get("unique_paid_issued_tickets", 0) < 25000):
        raise ValueError("Fully passed and restored same-runtime short prerequisite required")
    return relative, report


def active(goal):
    if not selected(goal):
        raise ValueError("Explicit committed hourly profile required")
    expected = {"decision": "ADR0232", "profile": "cce_hourly_qualification",
                "offered_journeys_per_second": 84, "offered_seconds": 3600,
                "maximum_experiment_seconds": 5400, "maximum_paid_stages": 1,
                "generator_concurrency": 500, "acquisition_budget": 20,
                "maximum_pods": 18, "pod_vcpu": 1, "pod_memory_gib": 2,
                "worker_resources": {"cpu": "250m", "memory": "512Mi"},
                "committed_status_decision": "ADR0298", "committed_status_arm": "candidate",
                "payment_slot_decision": "ADR0306",
                "hourly_observer_bootstrap_decision": "ADR0294"}
    if any(type(goal.get(k)) is not type(v) or goal.get(k) != v for k, v in expected.items()):
        raise ValueError("Exact fixed-budget committed hourly scope required")
    if goal.get("payment_slot_arm") not in {"control", "candidate"}:
        raise ValueError("Explicit measured API partition required")
    require_short(goal)
    from cce_customer_outcome_slo import selected as outcome_selected
    from cce_inventory_renewal import selected as renewal_selected
    from cce_nginx_ingress import hourly_baseline
    from cce_status_reuse_hourly import adapters
    from cce_verification_contract import selected as verified
    from cce_worker_rebalance_profile import active as placement_active
    if (goal.get("hourly_adapters_sha256") != policy.digest(adapters())
            or not verified(goal) or not outcome_selected(goal) or not renewal_selected(goal)):
        raise ValueError("Exact hourly adapters and independent quality contracts required")
    hourly_baseline(goal)
    placement_active(short_view(goal))
    return goal


def plan(goal):
    active(goal)
    relative, report = require_short(goal)
    from cce_status_reuse_hourly import adapters
    from cce_worker_rebalance_profile import plan as placement_plan
    value = placement_plan(short_view(goal))
    payment = 3 if goal["payment_slot_arm"] == "candidate" else 2
    return {**value, "decision": "ADR0232", "hourly_qualification_decision": "ADR0281",
            "committed_hourly_decision": "ADR0308",
            "kind": "committed_runtime_customer_outcome_hourly_qualification",
            "single_changed_factor": "duration_and_distinct_cohort",
            "common": {"buyer_journeys_per_second": 84, "duration_seconds": 3600},
            "baseline_evidence": relative, "baseline_sha256": policy.digest(report),
            "baseline_is_passing_control": True, "matched_performance_comparison": False,
            "payment_slot_arm": goal["payment_slot_arm"], "payment_connections_per_api": payment,
            "general_connections_per_api": 4 - payment, "connections_per_api": 4,
            "hourly_adapters_sha256": policy.digest(adapters()), "worker_deadline_seconds": 5400,
            "live_admission_maximum_seconds": 4300, "live_admission_maximum_samples": 440,
            "expected_terminal_tickets": 302400, "minimum_issued_inside_hour": 300000,
            "cohort_observer_interval_seconds": 10, "experiment_seconds_limit": 5400,
            "fixture_shows": 1008,
            "duration_reason": "Qualify the passed runtime with unchanged workload rate/resources and strict financial guarantees; 0.01% final customer target. One hour plus mandatory audits/restoration within90minutes."}


def validate_binding(goal, binding):
    relative, report = require_short(goal)
    original = policy.read(policy.STATE)[goal["qualification_short_ledger"]]["binding"]
    keys = ("configuration_sha256", "diagnostic_target_sha256", "image_proof_sha256",
            "saved_api_service_sha256", "cce_acquisition_budget", "cce_payment_pool_max",
            "cce_committed_status_receipt_sha256", "cce_committed_status_snapshot",
            "cce_customer_outcome_contract", "cce_inventory_renewal_contract")
    if any(original.get(key) != binding.get(key) for key in keys):
        raise ValueError("Hourly application/runtime/budget or correctness contract drift")
    return {"decision": "ADR0308", "short_ledger": goal["qualification_short_ledger"],
            "short_result_sha256": policy.digest(report),
            "short_binding_sha256": policy.digest(original),
            "short_evidence": relative, "duration_only_extension": True}

"""ADR0327 exact120/s workload extension; no application or budget change."""
import hashlib

import work_envelope as policy

RATE, SHOWS, EXPECTED, TARGET = 120, 120, 36000, 35400
KEYS = {"throughput_push_decision", "throughput_push_contract_sha256"}
REFERENCE_PATH = "tmp/adr0151-6fe62a507a9a/cce-comparison.private.json"
REFERENCE_LEDGER = "bounded_cce_ticket_target__0c2bceb2cf11"
REFERENCE_SHA256 = "6058404afe336e9851ff0ade5b9c2277b26af72b917d186fb2401f34455a3cd0"
SOURCES = ("cce_throughput_push.py", "cce_recovery_headroom.py", "cce_fulfillment_batch_profile.py",
           "cce_paid_profiles.py", "cce_paid_stage.py", "cce_paid_inputs.py", "fixture_identity_evidence.py", "cce_bounded_financial_audit.py", "cce_api_adapter.py",
           "cce_native_workers.py", "cce_live_admission.py", "cce_dependency_probe.py", "run_cce_paid_comparison.py", "work_envelope.py")


def contract():
    return {"decision": "ADR0327", "offered_rate": RATE, "offered_seconds": 300,
            "expected_journeys": EXPECTED, "fixture_shows": SHOWS,
            "minimum_unique_inside_window": TARGET, "concurrency": 500,
            "experiment_seconds_limit": 3600, "physical_connections": 24,
            "reference_result_sha256": REFERENCE_SHA256,
            "sources": {name: hashlib.sha256((policy.ROOT / "scripts" / name).read_bytes().replace(
                b"\r\n", b"\n")).hexdigest() for name in SOURCES}}


def selected(goal):
    if goal is None or not KEYS.intersection(goal):
        return False
    if (goal.get("throughput_push_decision") != "ADR0327"
            or goal.get("throughput_push_contract_sha256") != policy.digest(contract())
            or goal.get("fulfillment_batch_decision") != "ADR0324"):
        raise ValueError("Exact source-bound120/s fulfillment workload required")
    return True


def bounds(goal=None):
    return (RATE, SHOWS, EXPECTED, TARGET) if selected(goal) else (102, 102, 30600, 30000)


def require_reference():
    report = policy.read(policy.ROOT / REFERENCE_PATH)
    state = policy.read(policy.STATE)[REFERENCE_LEDGER]
    entries = [v for v in policy.journal(policy.envelope())["experiments"] if v["ledger"] == REFERENCE_LEDGER]
    gates = report.get("native", {}).get("measurement_gates", {})
    stage = report.get("paid_stage", {})
    customer = stage.get("customer", {})
    if (policy.digest(report) != REFERENCE_SHA256 or len(entries) != 1
            or entries[0].get("status") != "PASSED_RESTORED"
            or entries[0].get("result_sha256") != REFERENCE_SHA256
            or state.get("cce_paid_result_sha256") != REFERENCE_SHA256
            or state.get("paid_runs_started") != 1 or state.get("active_run")
            or report.get("binding_sha256") != policy.digest(state["binding"])
            or any(report.get(k) is not True for k in
                   ("pass", "restoration_complete", "integrity_verified", "transport_credentials_cleared"))
            or report.get("native", {}).get("cleanup_complete") is not True
            or len(gates) != 12 or any(v is not True for v in gates.values())
            or any(customer.get(k) != 30600 for k in ("scheduled", "dispatched", "fulfilled"))
            or any(customer.get(k) != 0 for k in
                   ("generator_drops", "first_attempt_error_journeys", "final_customer_failures"))
            or stage.get("financial", {}).get("pass") is not True
            or stage.get("financial", {}).get("hold_deadlines_elapsed") is not True
            or stage.get("global_queues", {}).get("pass") is not True
            or stage.get("ticket_target_issuance", {}).get("pass") is not True):
        raise ValueError("Fully passed and restored ADR0326 prerequisite required")
    return REFERENCE_PATH, report


def validate_binding(goal, binding):
    if not selected(goal):
        raise ValueError("Explicit120/s source-bound workload required")
    _, report = require_reference()
    original = policy.read(policy.STATE)[REFERENCE_LEDGER]["binding"]
    changed = {"cce_paid_entry_sources", "cce_paid_core_sources", "cce_manifest_sha256", "baseline_sha256",
               "cce_verification_contract_sha256", "cce_headroom_contract", "cce_fulfillment_batch_contract",
               "cce_throughput_push_contract"}
    if (any(original.get(k) != binding.get(k) for k in original.keys() | binding.keys() if k not in changed)
            or binding.get("cce_throughput_push_contract") != contract()
            or binding.get("baseline_sha256") != policy.digest(report)):
        raise ValueError("Rate push changed application, route, resources, recovery or connection budget")
    for key in ("cce_paid_entry_sources", "cce_paid_core_sources"):
        before, after = original[key], binding[key]
        added = {"scripts/cce_throughput_push.py"}
        allowed = {"scripts/" + name for name in SOURCES}
        if (set(after) != set(before) | added
                or any(before[k] != after[k] for k in before if k not in allowed)):
            raise ValueError("Rate push source drift outside the declared workload adapter")
    return True

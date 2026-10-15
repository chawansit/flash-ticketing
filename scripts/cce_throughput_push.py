"""ADR0327/0328 exact120/140 workloads; no application or budget change."""
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


HIGHER_REFERENCE_PATH = "tmp/adr0151-ac8debc44a23/cce-comparison.private.json"
HIGHER_REFERENCE_LEDGER = "bounded_cce_ticket_target__ecd5af0cff30"
HIGHER_REFERENCE_SHA256 = "d4fc2f65bd725745f836989ed43fe98cc49f3dce9b88f3e05f855ac67935c94e"


def higher(goal):
    return goal is not None and goal.get("throughput_push_decision") == "ADR0328"


def reference(goal=None):
    if higher(goal):
        return HIGHER_REFERENCE_PATH, HIGHER_REFERENCE_LEDGER, HIGHER_REFERENCE_SHA256, 36000
    return REFERENCE_PATH, REFERENCE_LEDGER, REFERENCE_SHA256, 30600


def contract(goal=None):
    rate, shows, expected, target = (140, 140, 42000, 41400) if higher(goal) else (RATE, SHOWS, EXPECTED, TARGET)
    return {"decision": "ADR0328" if higher(goal) else "ADR0327", "offered_rate": rate, "offered_seconds": 300,
            "expected_journeys": expected, "fixture_shows": shows,
            "minimum_unique_inside_window": target, "concurrency": 500,
            "experiment_seconds_limit": 3600, "physical_connections": 24,
            "reference_result_sha256": reference(goal)[2],
            "sources": {name: hashlib.sha256((policy.ROOT / "scripts" / name).read_bytes().replace(
                b"\r\n", b"\n")).hexdigest() for name in SOURCES}}


def selected(goal):
    if goal is None or not KEYS.intersection(goal):
        return False
    if (goal.get("throughput_push_decision") not in ("ADR0327", "ADR0328")
            or goal.get("throughput_push_contract_sha256") != policy.digest(contract(goal))
            or goal.get("fulfillment_batch_decision") != "ADR0324"):
        raise ValueError("Exact source-bound120/140/s fulfillment workload required")
    return True


def bounds(goal=None):
    if not selected(goal):
        return 102, 102, 30600, 30000
    return (140, 140, 42000, 41400) if higher(goal) else (RATE, SHOWS, EXPECTED, TARGET)


def require_reference(goal=None):
    path, ledger, digest, expected = reference(goal)
    report = policy.read(policy.ROOT / path)
    state = policy.read(policy.STATE)[ledger]
    entries = [v for v in policy.journal(policy.envelope())["experiments"] if v["ledger"] == ledger]
    gates = report.get("native", {}).get("measurement_gates", {})
    stage = report.get("paid_stage", {})
    customer = stage.get("customer", {})
    if (policy.digest(report) != digest or len(entries) != 1
            or entries[0].get("status") != "PASSED_RESTORED"
            or entries[0].get("result_sha256") != digest
            or state.get("cce_paid_result_sha256") != digest
            or state.get("paid_runs_started") != 1 or state.get("active_run")
            or report.get("binding_sha256") != policy.digest(state["binding"])
            or any(report.get(k) is not True for k in
                   ("pass", "restoration_complete", "integrity_verified", "transport_credentials_cleared"))
            or report.get("native", {}).get("cleanup_complete") is not True
            or len(gates) != 12 or any(v is not True for v in gates.values())
            or any(customer.get(k) != expected for k in ("scheduled", "dispatched", "fulfilled"))
            or any(customer.get(k) != 0 for k in
                   ("generator_drops", "first_attempt_error_journeys", "final_customer_failures"))
            or stage.get("financial", {}).get("pass") is not True
            or stage.get("financial", {}).get("hold_deadlines_elapsed") is not True
            or stage.get("global_queues", {}).get("pass") is not True
            or stage.get("ticket_target_issuance", {}).get("pass") is not True):
        raise ValueError("Fully passed and restored lower-rate prerequisite required")
    return path, report


def validate_binding(goal, binding):
    if not selected(goal):
        raise ValueError("Explicit120/140/s source-bound workload required")
    _, report = require_reference(goal)
    original = policy.read(policy.STATE)[reference(goal)[1]]["binding"]
    changed = {"cce_paid_entry_sources", "cce_paid_core_sources", "cce_manifest_sha256", "baseline_sha256",
               "cce_verification_contract_sha256", "cce_headroom_contract", "cce_fulfillment_batch_contract",
               "cce_throughput_push_contract"}
    if (any(original.get(k) != binding.get(k) for k in original.keys() | binding.keys() if k not in changed)
            or binding.get("cce_throughput_push_contract") != contract(goal)
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

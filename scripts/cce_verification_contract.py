"""ADR0285 explicit verification opt-in and byte-bound implementation identity."""
import hashlib
import json
from pathlib import Path


def digest(value):
    return hashlib.sha256(json.dumps(value, sort_keys=True, separators=(",", ":")).encode()).hexdigest()


def contract():
    root = Path(__file__).parent
    names = ("cce_bounded_financial_audit.py", "independent_database_sampling.py",
             "database_wait_evidence.py", "audit_checkout_smoke.py", "cce_transaction_recovery.py",
             "cce_verification_contract.py", "observe_two_host_pipeline.py", "cce_paid_observers.py",
             "cce_paid_stage.py", "cce_slot_trace_transport.py", "observe_cce_workers.py",
             "cce_metric_admission.py", "cce_paid_lifecycle.py")
    return {"decision": "ADR0285", "sources": {name: hashlib.sha256(
        (root / name).read_bytes().replace(b"\r\n", b"\n")).hexdigest() for name in names},
        "sampling_seconds": 1, "statistics_gap_seconds": 2, "cohort_age_seconds": 10,
        "data_connections": 1, "audit_seconds": 600, "audit_transport_seconds": 780,
        "statistics_schedule_decision": "ADR0288", "statistics_order": "before_cohort",
        "fixture_preparation_decision": "ADR0289",
        "fixture_preparation_order": "before_transition_admission",
        "short_fixture_preparation_decision": "ADR0292",
        "short_fixture_preparation_order": "after_safety"}


def selected(goal):
    if goal is None or "verification_decision" not in goal:
        return False
    if (goal.get("verification_decision") != "ADR0285"
            or goal.get("verification_contract_sha256") != digest(contract())):
        raise ValueError("Exact unchanged verification contract required")
    return True


def validate_inventory(inventory, approved_sha256):
    marker = inventory.get("verification_contract")
    if marker is None:
        return False
    # Runtime only needs the sampler/stats sources; full local contract is sealed before deployment.
    if (digest(inventory) != approved_sha256 or marker.get("decision") != "ADR0285"
            or marker.get("data_connections") != 1 or marker.get("sampling_seconds") != 1
            or marker.get("statistics_gap_seconds") != 2 or marker.get("cohort_age_seconds") != 10
            or marker.get("statistics_schedule_decision") != "ADR0288"
            or marker.get("statistics_order") != "before_cohort"
            or marker.get("fixture_preparation_decision") != "ADR0289"
            or marker.get("fixture_preparation_order") != "before_transition_admission"
            or marker.get("short_fixture_preparation_decision") != "ADR0292"
            or marker.get("short_fixture_preparation_order") != "after_safety"):
        raise ValueError("Exact bounded diagnostic inventory binding required")
    for name in ("independent_database_sampling.py", "database_wait_evidence.py"):
        actual = hashlib.sha256((Path(__file__).parent / name).read_bytes().replace(b"\r\n", b"\n")).hexdigest()
        if marker.get("sources", {}).get(name) != actual:
            raise ValueError("Transferred independent diagnostic helper changed")
    return True

"""ADR0232 separately registered one-hour qualification; no short-scope replay."""

import run_cce_paid_comparison as core
from cce_paid_profiles import HOURLY

BASELINE = "artifacts/cce-historical-reports/cce-acquisition-headroom-comparison-2026-10-09.json"
PUBLISHED_BASELINE = "docs/capacity/flash-sale-opening/cce-acquisition-headroom-comparison-2026-10-09.json"
BASELINE_REPORT = "8d53302985cc0cd072edb62eb1f6caf4cda4507d65171728c235a839a16f2def"


def identity():
    names = ("cce_hourly_paid_generator.py", "cce_hourly_paid_leaf.py", "cce_hourly_window.py",
             "cce_paid_profiles.py", "run_cce_hourly_qualification.py")
    return {**core.identity(), **{"scripts/" + name: core.paid.sha((core.policy.ROOT / "scripts" / name).read_bytes()) for name in names}}


def plan():
    import cce_transaction_profile as transaction
    goal = transaction.active()
    if goal is not None:
        if goal.get("extension_decision") != "ADR0251":
            raise ValueError("Separate exact hourly runtime binding required")
        from cce_simulator_dispatch_profile import hourly_plan
        return hourly_plan(goal)
    evidence = core.policy.read(core.policy.ROOT / BASELINE)
    if (evidence.get("original_report_sha256") != BASELINE_REPORT
            or evidence.get("pass") is not True
            or evidence.get("offered_journeys_per_second") != 84
            or evidence.get("offered_seconds") != 300
            or evidence.get("configuration", {}).get("api_shared_acquisition_budget") != 20
            or evidence.get("financial", {}).get("pass") is not True
            or evidence.get("financial", {}).get("hold_deadlines_elapsed") is not True
            or evidence.get("restoration", {}).get("restoration_complete") is not True
            or evidence.get("restoration", {}).get("integrity_verified") is not True
            or evidence.get("restoration", {}).get("namespace_removed") is not True
            or evidence.get("restoration", {}).get("cleanup_failures") != []
            or not evidence.get("measurement_gates")
            or any(value is not True for value in evidence["measurement_gates"].values())):
        raise ValueError("Exact fully passing short control required before hourly load")
    return {
        "decision": "ADR0232", "arms": ["candidate"],
        "common": {"buyer_journeys_per_second": 84, "duration_seconds": 3600},
        "kind": "fixed_native_hourly_qualification", "diagnostic_connection_decision": "ADR0180",
        "baseline_evidence": BASELINE, "baseline_sha256": core.policy.digest(evidence),
        "published_baseline_evidence": PUBLISHED_BASELINE,
        "acquisition_budget": 20, "replicas": 4,
        "pod_resources": core.native.contract()["resources"], "bridge_cpu_limit": 1,
        "pooler_server_connections": 24, "qualification_runs_authorized": 1,
        "paid_runs_authorized": 1, "safety_tickets_authorized": 2,
        "expected_terminal_tickets": 302400, "minimum_issued_inside_hour": 300000,
        "cohort_observer_interval_seconds": 10, "experiment_seconds_limit": 5400,
        "duration_reason": "One continuous measured hour is necessary to qualify 300000 tickets/hour; mandatory TTL, durability, queues and restoration remain outside the offered window.",
    }


def execute(*args):
    return core.execute(*args, profile=HOURLY)


def outcome(report, scope, binding):
    restored, integrity, passed = core.outcome(report, scope, binding, profile=HOURLY)
    gates = report.get("native", {}).get("measurement_gates", {})
    return restored, integrity, passed and gates.get("hourly_issuance") is True

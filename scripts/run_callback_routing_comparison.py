"""ADR0216 routing-only comparison using the existing protected paid runner."""
import json
from datetime import datetime

import callback_routing_contract as policy
from observe_two_host_pipeline import summarize_distribution
from run_diagnostic_placement_comparison import create_runner as protected_runner

LEDGER = "bounded_callback_routing"
AUTHORIZATION = "adr0216-callback-routing-pair-2026-10-08"
PREFIX = "business_http:/v1/webhooks/payments:POST:"


def summarize_callbacks(rows, inventory, *, offered_start_utc, offered_end_utc, expected_decision="ADR0216", routes=None):
    """Strict offered-window HTTP evidence; not counts of distinct paid tickets."""
    verified = summarize_distribution(rows, inventory, offered_start_utc=offered_start_utc,
                                      offered_end_utc=offered_end_utc)
    start, end = (datetime.fromisoformat(verified[k]) for k in ("observed_start_utc", "observed_end_utc"))
    selected = [r for r in rows if start <= datetime.fromisoformat(r["utc"]) <= end]
    import math

    previous = {}
    first = {}
    for row in selected:
        current = {}
        for label, metrics in row["api_replicas"].items():
            counters = {k: v for k, v in metrics.items() if k.startswith(PREFIX)}
            if any(type(v) not in {int, float} or not math.isfinite(v) or v < 0 for v in counters.values()):
                raise ValueError("Invalid callback counter")
            if any(counters.get(k, 0) < v for k, v in previous.get(label, {}).items()):
                raise ValueError("Callback counter reset or disappeared")
            current[label] = counters
        if not first:
            first = current
        previous = current
    successful = {label: counters.get(PREFIX + "200", 0) - first[label].get(PREFIX + "200", 0)
                  for label, counters in previous.items()}
    attempts = {label: sum(v - first[label].get(k, 0) for k, v in counters.items())
                for label, counters in previous.items()}
    marker = inventory.get("status_refresh_contract", {})
    arm = inventory.get("arm")
    from observe_two_host_pipeline import verify_admission_factor_evidence
    verify_admission_factor_evidence(inventory)
    if marker.get("decision") != expected_decision or expected_decision not in {"ADR0216", "ADR0217", "ADR0219", "ADR0222", "ADR0224"}:
        raise ValueError("Callback evidence requires the exact routing contract")
    secondary = {k: v for k, v in successful.items() if k.startswith("secondary:")}
    primary = {k: v for k, v in successful.items() if k.startswith("primary:")}
    routes = policy.ROUTES if routes is None else routes
    shared = routes[arm] == "http://load-balancer:8000"
    passed = (all(v > 0 for v in successful.values()) if shared
              else all(v == 0 for v in secondary.values()) and sum(primary.values()) > 0)
    return {"callback_routing_verified": passed, "successful_callback_request_deltas": successful,
            "callback_request_deltas": attempts, "callback_url": routes[arm],
            "observed_start_utc": verified["observed_start_utc"],
            "observed_end_utc": verified["observed_end_utc"],
            "scope": "Bracketing callback HTTP counters; duplicates and replays may be included, not unique paid tickets."}


def create_runner(*, policy_module=policy, ledger=LEDGER, authorization=AUTHORIZATION,
                  decision="ADR0216", profile_name="callback_routing",
                  runner_filename="run_callback_routing_comparison.py", extra_identity=(), arms=("control", "candidate")):
    engine = protected_runner(policy_module=policy_module, ledger=ledger, authorization=authorization,
                              decision=decision, profile_name=profile_name,
                              runner_filename=runner_filename, arms=arms,
                              extra_identity=("run_diagnostic_placement_comparison.py", *extra_identity))
    original_measurements, original_arm = engine.measurements, engine.run_arm

    def measurements(record, trace_path, inventory):
        result = original_measurements(record, trace_path, inventory)
        try:
            rows = [json.loads(line) for line in trace_path.read_text().splitlines() if line.strip()]
            result["callback_routing"] = summarize_callbacks(rows, inventory,
                offered_start_utc=record["offered_start_utc"], offered_end_utc=record["offered_end_utc"],
                expected_decision=decision, routes=policy_module.ROUTES)
        except (OSError, ValueError, KeyError, TypeError) as exc:
            result["callback_routing"] = {"callback_routing_verified": False, "failure_type": type(exc).__name__}
        return result

    def run_arm(*args, **kwargs):
        result = original_arm(*args, **kwargs)
        if kwargs.get("execute") and result.get("measurements", {}).get("callback_routing", {}).get("callback_routing_verified") is not True:
            result["pass"] = False
            result["gates"]["all_required_gates_pass"] = False
            result["gates"]["failed_gates"].append("callback_routing_verified")
        return result

    engine.measurements, engine.run_arm = measurements, run_arm
    return engine

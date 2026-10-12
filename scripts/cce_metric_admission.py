"""ADR0287 explicit, source-bound completeness from the existing metrics trace."""
import hashlib
import math
from pathlib import Path

import observe_cce_paid_pipeline as native
import observe_two_host_pipeline as pipeline
import slot_failure_evidence as slots
from cce_verification_contract import digest

DECISION = "ADR0287"
SOURCES = ("cce_metric_admission.py", "slot_failure_evidence.py",
           "observe_cce_paid_pipeline.py", "observe_two_host_pipeline.py",
           "cce_paid_observers.py", "cce_paid_lifecycle.py")


class CaptureError(ValueError):
    def __init__(self, reason_code):
        self.reason_code = reason_code
        super().__init__("Metric admission capture: " + reason_code)


def contract():
    root = Path(__file__).parent
    return {"decision": DECISION, "source": "existing_one_second_api_metrics",
            "maximum_gap_seconds": 2, "additional_database_connections": 0,
            "additional_kubernetes_log_requests": 0,
            "sources": {name: hashlib.sha256(
                (root / name).read_bytes().replace(b"\r\n", b"\n")).hexdigest() for name in SOURCES}}


def selected(goal):
    keys = {"metric_admission_decision", "metric_admission_contract_sha256"}
    if goal is None or not keys.intersection(goal):
        return False
    from cce_verification_contract import selected as verification_selected
    if (goal.get("metric_admission_decision") != DECISION
            or goal.get("metric_admission_contract_sha256") != digest(contract())
            or goal.get("extension_decision") not in {"ADR0271", "ADR0277"}
            or not verification_selected(goal)):
        raise ValueError("Exact source-bound metric admission scope required")
    return True


def validate_binding(binding):
    if (binding.get("cce_live_admission_decision") != DECISION
            or binding.get("cce_metric_admission_contract_sha256") != digest(contract())):
        raise ValueError("Exact metric admission capture binding required")


def summarize(rows, receipts, start, end, *, hourly=False):
    try:
        starts = native.validate_receipts(receipts)
        coverage = pipeline.summarize_endpoint_distribution(
            rows, set(starts), offered_start_utc=start, offered_end_utc=end, hourly=hourly)
    except (ValueError, KeyError, TypeError) as error:
        raise CaptureError("window_or_receipt_invalid") from error
    if len(rows) > (4400 if hourly else 600):
        raise CaptureError("sample_budget_exhausted")
    # Validate process identity across the full retained history, including drain,
    # not just the CPU/offered interval. Rollover reconciliation uses all samples.
    previous = {}
    for row in rows:
        apis = row.get("api_replicas", {})
        if row.get("api_metrics_error") or set(apis) != set(starts):
            raise CaptureError("endpoint_missing")
        for address, metrics in apis.items():
            count = metrics.get("business_http_requests_total")
            if (metrics.get("process_start_time_seconds") != starts[address]
                    or type(count) not in (int, float) or not math.isfinite(count)
                    or count < previous.get(address, 0)):
                raise CaptureError("process_or_counter_changed")
            previous[address] = count
    try:
        result = slots.summarize(rows, starts)
    except (ValueError, KeyError, TypeError) as error:
        raise CaptureError("slot_history_incomplete") from error
    return {**result, "decision": DECISION, "source": contract()["source"],
            "contract_sha256": digest(contract()), "receipt_sha256": digest(receipts["receipts"]),
            "samples": len(rows), "coverage": coverage,
            "additional_kubernetes_log_requests": 0}


def validate_summary(value, binding, receipts):
    validate_binding(binding)
    if (not isinstance(value, dict) or value.get("complete") is not True
            or value.get("decision") != DECISION or value.get("source") != contract()["source"]
            or value.get("contract_sha256") != binding["cce_metric_admission_contract_sha256"]
            or value.get("receipt_sha256") != digest(receipts)
            or value.get("coverage", {}).get("all_four_replicas_observed") is not True
            or value.get("coverage", {}).get("per_replica_traffic_distribution") is not True):
        raise ValueError("Complete bound metric admission history required")
    return True


def extend_plan(plan, goal):
    if not selected(goal):
        return plan
    result = {**plan, "diagnostic_decision": DECISION, "metric_admission_capture": contract(),
              "single_changed_factor": "admission_diagnostic_transport",
              "kind": "admission_capture_validation", "matched_performance_comparison": False}
    for field in ("live_admission_maximum_seconds", "live_admission_maximum_samples"):
        result.pop(field, None)
    return result

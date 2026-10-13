"""ADR0305 explicit customer-outcome allowance; sealed raw reports stay visible."""
import copy
import hashlib
import math
from pathlib import Path

import work_envelope as policy

DENOMINATOR = 10000
KEYS = {"customer_outcome_decision", "customer_outcome_contract_sha256"}
TRANSIENT_OUTCOMES = {
    "hold_http_503", "order_http_503", "payment_http_503",
    "ticket_timeout", "command_timeout",
}


def contract():
    return {"decision": "ADR0305", "maximum_final_failure_percent": 0.01,
            "failure_denominator": DENOMINATOR, "generator_drops": 0,
            "recovery_max_attempts": 3, "latency_gates": "unchanged",
            "financial_correctness": "zero_tolerance",
            "helper_sha256": hashlib.sha256(Path(__file__).read_bytes().replace(b"\r\n", b"\n")).hexdigest()}


def selected(goal):
    if goal is None or not KEYS.intersection(goal):
        return False
    approved = policy.envelope().get("approved_customer_outcome_target", {})
    if (goal.get("customer_outcome_decision") != "ADR0305"
            or goal.get("customer_outcome_contract_sha256") != policy.digest(contract())
            or approved.get("maximum_final_customer_failure_percent") != 0.01
            or approved.get("maximum_final_customer_failure_fraction") != 0.0001
            or goal.get("profile") not in {"cce_paid_comparison", "cce_hourly_qualification"}):
        raise ValueError("Explicit approved and source-bound customer outcome target required")
    return True


def count(value):
    if type(value) is not int or value < 0:
        raise ValueError("Complete nonnegative integer customer accounting required")
    return value


def evaluate(customer, expected):
    names = ("scheduled", "dispatched", "completed", "fulfilled", "fulfilled_by_deadline",
             "distinct_orders", "distinct_tickets", "generator_drops", "final_customer_failures",
             "first_attempt_error_journeys", "recovered_journeys", "retry_attempts")
    values = {k: count(customer.get(k)) for k in names}
    outcomes = customer.get("outcomes", {})
    if not isinstance(outcomes, dict) or any(type(k) is not str for k in outcomes):
        raise ValueError("Complete customer outcomes required")
    for value in outcomes.values():
        count(value)
    shards = customer.get("shards", [])
    if not isinstance(shards, list) or len(shards) != 2:
        raise ValueError("Two complete generator shards required")
    shard_counts = {k: 0 for k in ("scheduled", "dispatched", "generator_drops", "fulfilled_by_deadline")}
    combined = {}
    shard_checks = []
    for shard in shards:
        for key in shard_counts:
            shard_counts[key] += count(shard.get(key))
        results = shard.get("outcomes", {})
        if not isinstance(results, dict):
            raise TypeError("Complete shard outcomes required")
        for key, value in results.items():
            combined[key] = combined.get(key, 0) + count(value)
        success = results.get("fulfilled", 0)
        shard_checks.append(
            count(shard.get("scheduled")) == count(shard.get("dispatched")) == sum(results.values())
            and count(shard.get("generator_drops")) == 0
            and count(shard.get("fulfilled_by_deadline")) == success
        )
    lag = [customer.get(k) for k in ("dispatch_lag_p95_ms", "dispatch_lag_max_ms", "total_journey_p95_ms")]
    if any(type(v) not in (int, float) or not math.isfinite(v) or v < 0 for v in lag):
        raise ValueError("Complete finite latency evidence required")
    failures = values["final_customer_failures"]
    dispatched = values["dispatched"]
    checks = {
        "complete_accounting": expected > 0 and values["scheduled"] == dispatched == values["completed"] == expected
            and sum(outcomes.values()) == dispatched
            and values["fulfilled"] + failures == dispatched
            and outcomes.get("fulfilled", 0) == values["fulfilled"],
        "unique_confirmed_tickets": values["fulfilled"] == values["distinct_orders"] == values["distinct_tickets"],
        "completion_deadline": values["fulfilled_by_deadline"] == values["fulfilled"],
        "zero_generator_drops": values["generator_drops"] == 0,
        "complete_shards": all(shard_checks) and combined == outcomes
            and all(shard_counts[k] == values[k] for k in shard_counts)
            and customer.get("generator_shards") == 2
            and customer.get("shard_exit_codes") in ([0, 0], [0, 1], [1, 0], [1, 1]),
        "known_customer_failures": all(k == "fulfilled" or k in TRANSIENT_OUTCOMES or v == 0
                                     for k, v in outcomes.items()),
        "bounded_final_failures": failures * DENOMINATOR <= dispatched,
        "dispatch_latency": lag[0] <= 100 and lag[1] <= 500,
        "bounded_recovery": customer.get("recovery_max_attempts") == 3
            and values["recovered_journeys"] <= values["first_attempt_error_journeys"] <= dispatched,
        "unchanged_strict_success": failures != 0 or customer.get("strict_generator_pass", customer.get("pass")) is True,
    }
    return {"decision": "ADR0305", "contract_sha256": policy.digest(contract()),
            "denominator_dispatched_journeys": dispatched, "final_customer_failures": failures,
            "maximum_final_customer_failures": dispatched // DENOMINATOR,
            "final_customer_failure_percent": failures / dispatched * 100 if dispatched else None,
            "first_attempt_error_journeys": values["first_attempt_error_journeys"],
            "recovered_journeys": values["recovered_journeys"], "retry_attempts": values["retry_attempts"],
            "total_journey_p95_ms": lag[2], "checks": checks, "pass": all(checks.values())}


def financial(result, customer, expected):
    """All original predicates ran; only unpaid/customer-confirmation counts may differ."""
    outcome = evaluate(customer, expected)
    counts = result.get("counts", {})
    relationships = result.get("relationships", {})
    from worker_separation_audits import RELATIONSHIP_NAMES
    if result.get("failure_type") or not isinstance(counts, dict) or set(relationships) != set(RELATIONSHIP_NAMES):
        return {**result, "customer_outcome_financial_pass": False, "pass": False}
    names = ("orders", "fulfilled_orders", "expired_orders", "pending_orders", "payment_attempts",
             "pending_payment_attempts", "succeeded_payments", "bookings", "tickets", "payment_callbacks",
             "incomplete_callback_deliveries", "callback_delivery_attempts", "callback_delivery_target",
             "duplicate_booked_seats", "multi_booking_orders", "unpublished_outbox", "dead_letters")
    for key in names:
        count(counts.get(key))
    for value in relationships.values():
        count(value)
    paid = counts["succeeded_payments"]
    failed = customer["final_customer_failures"]
    checks = {
        "bounded_unpaid_orders": expected - failed <= counts["orders"] <= expected
            and counts["orders"] == counts["fulfilled_orders"] + counts["expired_orders"]
            and counts["expired_orders"] <= failed,
        "every_payment_issued": customer["fulfilled"] <= paid <= expected
            and all(counts[k] == paid for k in ("fulfilled_orders", "payment_attempts", "bookings", "tickets")),
        "callbacks_complete": counts["payment_callbacks"] >= paid
            and counts["callback_delivery_attempts"] == counts["callback_delivery_target"] == paid,
        "zero_pending_or_duplicate": all(counts[k] == 0 for k in (
            "pending_orders", "pending_payment_attempts", "incomplete_callback_deliveries",
            "duplicate_booked_seats", "multi_booking_orders", "unpublished_outbox", "dead_letters")),
        "all_relationships_valid": relationships["unique_issued_tickets"] == paid
            and all(relationships[k] == 0 for k in RELATIONSHIP_NAMES if k != "unique_issued_tickets"),
        "complete_customer_evidence": outcome["checks"]["complete_accounting"]
            and outcome["checks"]["unique_confirmed_tickets"] and outcome["checks"]["complete_shards"],
    }
    value = copy.deepcopy(result)
    value["strict_audit_pass"] = result.get("pass")
    value["strict_audit_checks"] = copy.deepcopy(result.get("checks", {}))
    value["checks"] = {
        "post_ttl_complete": relationships["holds_not_past_ttl"] == 0,
        "payments_durable": checks["every_payment_issued"] and checks["callbacks_complete"]
            and checks["zero_pending_or_duplicate"] and checks["bounded_unpaid_orders"],
        "ticket_relationships_valid": checks["all_relationships_valid"],
    }
    value["customer_outcome_financial_checks"] = checks
    value["customer_outcome_financial_pass"] = all(checks.values())
    value["pass"] = all(checks.values())
    return value

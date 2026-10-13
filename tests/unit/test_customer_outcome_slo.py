"""ADR0305/0306 regression cases for outcome accounting and financial safety."""
import copy
import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[2] / "scripts"))
import cce_customer_outcome_slo as slo
import cce_payment_slot_profile as slots
import work_envelope as policy
from worker_separation_audits import RELATIONSHIP_NAMES


def customer(failures=0, expected=25200):
    success = expected - failures
    shards = []
    for index in range(2):
        n = expected // 2
        bad = failures if index else 0
        outcomes = {"fulfilled": n - bad}
        if bad:
            outcomes["payment_http_503"] = bad
        shards.append({"scheduled": n, "dispatched": n, "generator_drops": 0,
                       "fulfilled_by_deadline": n - bad, "outcomes": outcomes})
    return {"scheduled": expected, "dispatched": expected, "completed": expected,
            "fulfilled": success, "fulfilled_by_deadline": success,
            "distinct_orders": success, "distinct_tickets": success, "generator_drops": 0,
            "final_customer_failures": failures, "first_attempt_error_journeys": failures,
            "recovered_journeys": 0, "retry_attempts": failures * 2,
            "outcomes": {"fulfilled": success, **({"payment_http_503": failures} if failures else {})},
            "shards": shards, "generator_shards": 2,
            "shard_exit_codes": [0, 1 if failures else 0], "recovery_max_attempts": 3,
            "dispatch_lag_p95_ms": 2.0, "dispatch_lag_max_ms": 3.0,
            "total_journey_p95_ms": 2500.0, "pass": failures == 0}


def financial(paid=25200, orders=25200):
    counts = {k: 0 for k in ("orders", "fulfilled_orders", "expired_orders", "pending_orders",
              "payment_attempts", "pending_payment_attempts", "succeeded_payments", "bookings", "tickets",
              "payment_callbacks", "incomplete_callback_deliveries", "callback_delivery_attempts",
              "callback_delivery_target", "duplicate_booked_seats", "multi_booking_orders",
              "unpublished_outbox", "dead_letters")}
    counts.update(orders=orders, expired_orders=orders-paid)
    for key in ("fulfilled_orders", "payment_attempts", "succeeded_payments", "bookings", "tickets",
                "payment_callbacks", "callback_delivery_attempts", "callback_delivery_target"):
        counts[key] = paid
    relationships = dict.fromkeys(RELATIONSHIP_NAMES, 0)
    relationships["unique_issued_tickets"] = paid
    return {"pass": paid == 25200, "counts": counts, "relationships": relationships,
            "hold_deadlines_elapsed": True}


@pytest.mark.parametrize("failures,passed", [(0, True), (1, True), (2, True), (3, False)])
def test_exact_short_failure_budget(failures, passed):
    row = customer(failures)
    unchanged = copy.deepcopy(row)
    outcome = slo.evaluate(row, 25200)
    assert outcome["pass"] is passed
    assert outcome["maximum_final_customer_failures"] == 2
    assert row == unchanged


@pytest.mark.parametrize("failures,passed", [(30, True), (31, False)])
def test_hourly_failure_budget(failures, passed):
    assert slo.evaluate(customer(failures, 302400), 302400)["pass"] is passed


@pytest.mark.parametrize("key,value", [
    ("generator_drops", 1), ("completed", 25199), ("dispatched", 25199),
    ("fulfilled_by_deadline", 25199), ("distinct_orders", 25199),
    ("distinct_tickets", 25199), ("dispatch_lag_max_ms", 501), ("dispatch_lag_p95_ms", 101),
    ("recovery_max_attempts", 4), ("generator_shards", 1),
])
def test_other_gates_not_relaxed(key, value):
    assert not slo.evaluate({**customer(), key: value}, 25200)["pass"]


@pytest.mark.parametrize("value", [None, -1, True, 1.5])
def test_missing_or_corrupt_counts_fail_closed(value):
    with pytest.raises(ValueError):
        slo.evaluate({**customer(), "final_customer_failures": value}, 25200)


def test_coordinator_failure_not_customer_allowance():
    row = customer(1)
    row["outcomes"]["generator_exception"] = row["outcomes"].pop("payment_http_503")
    row["shards"][1]["outcomes"]["generator_exception"] = row["shards"][1]["outcomes"].pop("payment_http_503")
    assert not slo.evaluate(row, 25200)["pass"]


@pytest.mark.parametrize("paid,orders", [(25200, 25200), (25198, 25200), (25198, 25198)])
def test_confirmation_and_unpaid_failure_preserve_financial_audit(paid, orders):
    row = financial(paid, orders)
    before = copy.deepcopy(row)
    result = slo.financial(row, customer(2), 25200)
    assert result["pass"]
    assert result["strict_audit_pass"] is row["pass"]
    assert row == before


@pytest.mark.parametrize("key", ["tickets", "bookings", "payment_attempts"])
def test_lost_or_extra_financial_objects_fail(key):
    row = financial()
    row["counts"][key] -= 1
    assert not slo.financial(row, customer(2), 25200)["pass"]


@pytest.mark.parametrize("key", ["duplicate_booked_seats", "pending_payment_attempts", "dead_letters",
                                "unpublished_outbox", "multi_booking_orders"])
def test_zero_tolerance_counts(key):
    row = financial()
    row["counts"][key] = 1
    assert not slo.financial(row, customer(2), 25200)["pass"]


def test_relationship_failure_and_missing_predicates_fail():
    row = financial()
    row["relationships"][RELATIONSHIP_NAMES[0]] = 1
    assert not slo.financial(row, customer(2), 25200)["pass"]
    row["relationships"].pop(RELATIONSHIP_NAMES[1])
    assert not slo.financial(row, customer(2), 25200)["pass"]


def test_approved_binding_required(monkeypatch):
    monkeypatch.setattr(policy, "envelope", lambda: {"approved_customer_outcome_target": {
        "maximum_final_customer_failure_percent": 0.01, "maximum_final_customer_failure_fraction": 0.0001}})
    goal = {"profile": "cce_paid_comparison", "customer_outcome_decision": "ADR0305",
            "customer_outcome_contract_sha256": policy.digest(slo.contract())}
    assert slo.selected(goal)
    assert not slo.selected({})
    with pytest.raises(ValueError):
        slo.selected({**goal, "customer_outcome_contract_sha256": "0"*64})


def goal(arm="control"):
    return {"payment_slot_decision": "ADR0306", "payment_slot_arm": arm,
            "extension_decision": "ADR0271", "profile": "cce_paid_comparison",
            "committed_status_decision": "ADR0298", "committed_status_arm": "candidate",
            "offered_journeys_per_second": 84, "offered_seconds": 300,
            "maximum_paid_stages": 1, "maximum_experiment_seconds": 3600,
            "acquisition_budget": 20, "observer_cost_decision": "ADR0283",
            "customer_outcome_decision": "ADR0305",
            "customer_outcome_contract_sha256": policy.digest(slo.contract())}


def test_partition_same_total_and_reject_combined_factors(monkeypatch):
    monkeypatch.setattr(slots, "outcome_selected", lambda _: True)
    assert slots.connections(goal()) == 2
    assert slots.connections(goal("candidate")) == 3
    for changes in ({"runtime_cohort_arm": "candidate"}, {"acquisition_budget": 21},
                    {"offered_seconds": 3600}, {"payment_pipeline_arm": "candidate"}):
        with pytest.raises(ValueError):
            slots.selected({**goal(), **changes})


def test_candidate_binding_rejects_other_changes(monkeypatch):
    ledger = "bounded_cce_paid_comparison__123456abcdef"
    value = {**goal("candidate"), "control_ledger": ledger}
    original = {"cce_payment_pool_max": 2, "configuration_sha256": "same", "cce_acquisition_budget": 20,
                "cce_paid_core_sources": {}, "cce_paid_entry_sources": {}}
    monkeypatch.setattr(slots, "require_control", lambda _: ("path", {}))
    monkeypatch.setattr(policy, "read", lambda _: {ledger: {"binding": original}})
    monkeypatch.setattr(slots, "validate_service_semantics", lambda *_: None)
    current = {**original, "cce_payment_pool_max": 3}
    slots.validate_candidate_binding(value, current)
    with pytest.raises(ValueError):
        slots.validate_candidate_binding(value, {**current, "configuration_sha256": "different"})


def test_only_reviewed_validator_source_lineage_allowed():
    old = {slots.SELF: slots.REVIEWED_PRIOR_VALIDATOR, "application.py": "same"}
    assert slots.compatible_sources(old, {**old, slots.SELF: "new"})
    with pytest.raises(ValueError):
        slots.compatible_sources(old, {**old, "application.py": "different"})
    with pytest.raises(ValueError):
        slots.compatible_sources({**old, slots.SELF: "unreviewed"}, {**old, slots.SELF: "new"})


def test_saved_service_allows_only_exact_partition(tmp_path, monkeypatch):
    sys.path.insert(0, str(Path(__file__).resolve().parents[2] / "scripts"))
    import run_cce_paid_comparison as core
    p = tmp_path / "tmp" / "scope" / "snapshot.private.json"
    p.parent.mkdir(parents=True)
    p.write_text('{"saved":{}}')
    monkeypatch.setattr(policy, "ROOT", tmp_path)
    monkeypatch.setattr(core, "service_for", lambda _: {})
    candidate = {"environment": {"API_PAYMENT_POOL_MAX": "3", "database": "same"}, "image": "same"}
    monkeypatch.setattr(core, "api_semantics", lambda _: copy.deepcopy(candidate))
    control = copy.deepcopy(candidate)
    control["environment"]["API_PAYMENT_POOL_MAX"] = "2"
    value = {"payment_slot_snapshot_path": "tmp/scope/snapshot.private.json"}
    slots.validate_service_semantics(value,
        {"saved_api_service_sha256": policy.digest(control)},
        {"saved_api_service_sha256": policy.digest(candidate)})
    candidate["environment"]["database"] = "changed"
    with pytest.raises(ValueError):
        slots.validate_service_semantics(value,
            {"saved_api_service_sha256": policy.digest(control)},
            {"saved_api_service_sha256": policy.digest(candidate)})

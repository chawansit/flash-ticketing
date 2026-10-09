"""Fault injection for ADR0229 terminal recovery; no cloud calls."""

import copy
import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[2] / "scripts"))
import cce_paid_terminal_recovery as recovery
import work_envelope as policy


@pytest.fixture
def case(monkeypatch):
    fixture = {"show_ids": [str(i) for i in range(84)]}
    binding = {"fixed": True}
    customer = {
        "scheduled": 25200,
        "dispatched": 25200,
        "completed": 25200,
        "generator_drops": 0,
        "retry_attempts": 0,
        "outcomes": {
            "fulfilled": 92,
            "payment_http_503": 13312,
            "order_http_503": 11639,
            "ticket_timeout": 157,
        },
    }
    report = {
        "run": recovery.RUN,
        "pass": False,
        "capacity_stages_started": 1,
        "restoration_complete": True,
        "paid_stage": {
            "customer": customer,
            "fixture_identity": {"fixture_identity_sha256": policy.digest(fixture)},
        },
    }
    result = policy.digest(report)
    monkeypatch.setattr(recovery, "RESULT", result)
    entry = {
        "ledger": recovery.LEDGER,
        "profile": "cce_paid_comparison",
        "status": "RECOVERY_REQUIRED",
        "result_sha256": result,
        "binding_sha256": policy.digest(binding),
    }
    scope = {"binding": binding, "cce_paid_result_sha256": result, "paid_runs_started": 1, "active_run": None}
    terminal = {
        "pass": True,
        "hold_deadlines_elapsed": True,
        "order_states": {"EXPIRED": 13312, "FULFILLED": 2250, "REFUNDED": 9638},
        "refund_states": {"REFUNDED": 9638},
        "unaccounted_succeeded_payments": 0,
        "invalid_fulfilled_relationships": 0,
        "invalid_refunded_relationships": 0,
        "counts": copy.deepcopy(recovery.COUNTS),
    }
    financial = {
        "pass": True,
        "hold_deadlines_elapsed": True,
        **dict.fromkeys(("orders", "fulfilled_orders", "succeeded_payments", "bookings", "tickets"), 1),
        "duplicate_booked_seats": 0,
        "multi_booking_orders": 0,
        "callback_delivery_attempts": 3,
        "callback_delivery_target": 3,
    }
    evidence = {
        "ledger": recovery.LEDGER,
        "run": recovery.RUN,
        "namespace": recovery.aborted.namespace_for(recovery.RUN),
        "original_result_sha256": result,
        "binding_sha256": entry["binding_sha256"],
        "pass": True,
        "original_paid_result_preserved": True,
        "paid_scope_consumed": True,
        "owned_fixture_sale_windows_closed": True,
        "verified_owned_events": 86,
        "gates": dict.fromkeys(recovery.aborted.GATES, True),
        "queue_counts": {"empty": True},
        "actual_elapsed_seconds": 40,
        "paid_terminal_financial": terminal,
        "financial": financial,
        "native_safety_financial": copy.deepcopy(financial),
    }
    monkeypatch.setattr(
        recovery.aborted, "queue_checks", lambda counts, members: counts == {"empty": True} and members == 1
    )
    return report, entry, scope, evidence, fixture


def test_recovery_keeps_original_capacity_failure_and_consumed_stage(case):
    assert recovery.validate(*case)
    assert case[0]["pass"] is False and case[2]["paid_runs_started"] == 1


@pytest.mark.parametrize("gate", recovery.aborted.GATES)
def test_each_recovery_gate_required(case, gate):
    case[3]["gates"][gate] = False
    with pytest.raises(ValueError):
        recovery.validate(*case)


@pytest.mark.parametrize("field", list(recovery.COUNTS))
def test_every_financial_count_and_pending_state_required(case, field):
    case[3]["paid_terminal_financial"]["counts"][field] += 1
    with pytest.raises(ValueError):
        recovery.validate(*case)


@pytest.mark.parametrize(
    "fault",
    [
        "unknown_run",
        "result",
        "binding",
        "paid_counter",
        "active",
        "refund",
        "unaccounted",
        "ttl",
        "queue",
        "fixture",
        "customer",
        "callback",
        "elapsed",
    ],
)
def test_foreign_unresolved_or_modified_evidence_rejected(case, fault):
    report, entry, scope, evidence, fixture = case
    if fault == "unknown_run":
        entry["ledger"] = "foreign"
    elif fault == "result":
        entry["result_sha256"] = "x"
    elif fault == "binding":
        scope["binding"] = {"changed": True}
    elif fault == "paid_counter":
        scope["paid_runs_started"] = 0
    elif fault == "active":
        scope["active_run"] = "active"
    elif fault == "refund":
        evidence["paid_terminal_financial"]["refund_states"]["REFUNDED"] -= 1
    elif fault == "unaccounted":
        evidence["paid_terminal_financial"]["unaccounted_succeeded_payments"] = 1
    elif fault == "ttl":
        evidence["paid_terminal_financial"]["hold_deadlines_elapsed"] = False
    elif fault == "queue":
        evidence["queue_counts"] = {"pending": 1}
    elif fault == "fixture":
        fixture["show_ids"][0] = fixture["show_ids"][1]
    elif fault == "customer":
        report["paid_stage"]["customer"]["outcomes"]["fulfilled"] = 2250
    elif fault == "callback":
        evidence["native_safety_financial"]["callback_delivery_attempts"] = 2
    elif fault == "elapsed":
        evidence["actual_elapsed_seconds"] = float("nan")
    with pytest.raises(ValueError):
        recovery.validate(*case)


def test_exact_stopped_marker_can_be_recovered_with_independent_proof(case):
    case[2]["active_run"] = recovery.RUN
    assert recovery.validate(*case)
    case[3]["gates"]["generator_idle"] = False
    with pytest.raises(ValueError):
        recovery.validate(*case)


def test_owned_fixture_windows_must_all_be_closed(case):
    case[3]["owned_fixture_sale_windows_closed"] = False
    with pytest.raises(ValueError):
        recovery.validate(*case)


@pytest.fixture
def bridge_case(case, monkeypatch):
    report, entry, scope, evidence, fixture = copy.deepcopy(case)
    report.update(
        run=recovery.BRIDGE_RUN,
        integrity_verified=True,
        native={
            "measurement_gates": dict.fromkeys(recovery.MEASUREMENT_GATES, True),
            "cleanup_failures": [{"operation": "candidate_restore", "type": "ValueError"}],
        },
    )
    customer = report["paid_stage"]["customer"]
    customer.update(outcomes={"fulfilled": 25200}, pass_=True, distinct_orders=25200, distinct_tickets=25200)
    customer["pass"] = customer.pop("pass_")
    result = policy.digest(report)
    monkeypatch.setattr(recovery, "BRIDGE_RESULT", result)
    entry.update(ledger=recovery.BRIDGE_LEDGER, result_sha256=result, actual_elapsed_seconds=100)
    scope["cce_paid_result_sha256"] = result
    evidence.update(
        ledger=recovery.BRIDGE_LEDGER,
        run=recovery.BRIDGE_RUN,
        namespace=recovery.aborted.namespace_for(recovery.BRIDGE_RUN),
        original_result_sha256=result,
    )
    terminal = evidence["paid_terminal_financial"]
    terminal.update(
        counts=recovery.recovery_case(recovery.BRIDGE_RUN)[3],
        order_states={"FULFILLED": 25200},
        refund_states={},
    )
    return report, entry, scope, evidence, fixture


def test_bridge_measurement_recovery_preserves_original_failure(bridge_case):
    assert recovery.validate(*bridge_case)
    assert bridge_case[0]["pass"] is False
    assert bridge_case[2]["paid_runs_started"] == 1


@pytest.mark.parametrize("gate", recovery.MEASUREMENT_GATES)
def test_bridge_recovery_requires_every_original_measurement_gate(bridge_case, monkeypatch, gate):
    bridge_case[0]["native"]["measurement_gates"][gate] = False
    result = policy.digest(bridge_case[0])
    monkeypatch.setattr(recovery, "BRIDGE_RESULT", result)
    bridge_case[1]["result_sha256"] = result
    bridge_case[2]["cce_paid_result_sha256"] = result
    bridge_case[3]["original_result_sha256"] = result
    with pytest.raises(ValueError, match="Exact measured bridge case"):
        recovery.validate(*bridge_case)


@pytest.mark.parametrize("field", list(recovery.COUNTS))
def test_bridge_recovery_requires_every_financial_count(bridge_case, field):
    bridge_case[3]["paid_terminal_financial"]["counts"][field] += 1
    with pytest.raises(ValueError):
        recovery.validate(*bridge_case)


@pytest.mark.parametrize("gate", recovery.aborted.GATES)
def test_bridge_recovery_requires_all_independent_gates(bridge_case, gate):
    bridge_case[3]["gates"][gate] = False
    with pytest.raises(ValueError):
        recovery.validate(*bridge_case)


def test_bridge_close_preserves_original_result_and_cannot_repeat(bridge_case, monkeypatch, tmp_path):
    report, entry, scope, evidence, _fixture = bridge_case
    root = tmp_path / "repo"
    directory = root / "tmp" / recovery.BRIDGE_RUN
    directory.mkdir(parents=True)
    path = directory / "independent-paid-recovery.private.json"
    policy.write(path, evidence)
    state = {"current_run": None, recovery.BRIDGE_LEDGER: scope}
    records = {"experiments": [entry]}
    monkeypatch.setattr(policy, "ROOT", root)
    monkeypatch.setattr(policy, "STATE", root / "state.json")
    monkeypatch.setattr(policy, "JOURNAL", root / "journal.json")
    monkeypatch.setattr(policy, "LOCK", root / "no-lock")
    policy.write(policy.STATE, state)
    policy.write(directory / "cce-comparison.private.json", report)
    fixture_file = directory / "candidate" / "fixture-identity.json"
    fixture_file.parent.mkdir()
    policy.write(fixture_file, {"fixture_identity": _fixture})
    monkeypatch.setattr(policy, "envelope", dict)
    monkeypatch.setattr(policy, "journal", lambda envelope: records)
    before = (directory / "cce-comparison.private.json").read_bytes()
    closed = recovery.close(path)
    assert closed["status"] == "FAILED_RESTORED"
    assert closed["capacity_qualified"] is False
    assert scope["paid_runs_started"] == 1
    assert (directory / "cce-comparison.private.json").read_bytes() == before
    assert entry["initial_status"] == "RECOVERY_REQUIRED"
    with pytest.raises(ValueError):
        recovery.close(path)


@pytest.fixture
def control_case(bridge_case, monkeypatch):
    report, entry, scope, evidence, fixture = copy.deepcopy(bridge_case)
    report.update(run=recovery.CONTROL_RUN, integrity_verified=False)
    report["native"] = {
        "measurement_gates": {
            key: key
            not in {"customer_load", "post_ttl_financial", "zero_double_booking", "payment_durability"}
            for key in recovery.MEASUREMENT_GATES
        },
        "cleanup_failures": [],
    }
    counts, outcomes, states, refunds = recovery.recovery_case(recovery.CONTROL_RUN)[3:]
    report["paid_stage"]["customer"].update(outcomes=outcomes, distinct_orders=25198, distinct_tickets=25198)
    report["paid_stage"]["customer"]["pass"] = False
    result = policy.digest(report)
    monkeypatch.setattr(recovery, "CONTROL_RESULT", result)
    entry.update(ledger=recovery.CONTROL_LEDGER, result_sha256=result)
    scope["cce_paid_result_sha256"] = result
    evidence.update(
        ledger=recovery.CONTROL_LEDGER,
        run=recovery.CONTROL_RUN,
        namespace=recovery.aborted.namespace_for(recovery.CONTROL_RUN),
        original_result_sha256=result,
    )
    evidence["paid_terminal_financial"].update(counts=counts, order_states=states, refund_states=refunds)
    return report, entry, scope, evidence, fixture


def test_failed_control_recovers_without_qualifying_capacity(control_case):
    assert recovery.validate(*control_case)
    assert control_case[0]["pass"] is False
    assert control_case[0]["integrity_verified"] is False
    assert control_case[2]["paid_runs_started"] == 1


@pytest.mark.parametrize("field", list(recovery.COUNTS))
def test_failed_control_requires_every_financial_count(control_case, field):
    control_case[3]["paid_terminal_financial"]["counts"][field] += 1
    with pytest.raises(ValueError):
        recovery.validate(*control_case)


@pytest.mark.parametrize("gate", recovery.aborted.GATES)
def test_failed_control_requires_every_recovery_gate(control_case, gate):
    control_case[3]["gates"][gate] = False
    with pytest.raises(ValueError):
        recovery.validate(*control_case)


@pytest.mark.parametrize("fault", ["promote", "gate", "cleanup", "customer", "outcome"])
def test_failed_control_rejects_rewritten_result(control_case, monkeypatch, fault):
    report, entry, scope, evidence, _fixture = control_case
    if fault == "promote":
        report["integrity_verified"] = True
    elif fault == "gate":
        report["native"]["measurement_gates"]["customer_load"] = True
    elif fault == "cleanup":
        report["native"]["cleanup_failures"] = [{"operation": "candidate_restore", "type": "ValueError"}]
    elif fault == "customer":
        report["paid_stage"]["customer"]["distinct_tickets"] = 25199
    else:
        report["paid_stage"]["customer"]["outcomes"]["fulfilled"] = 25199
    result = policy.digest(report)
    monkeypatch.setattr(recovery, "CONTROL_RESULT", result)
    entry["result_sha256"] = scope["cce_paid_result_sha256"] = evidence["original_result_sha256"] = result
    with pytest.raises(ValueError):
        recovery.validate(*control_case)


@pytest.fixture
def diagnostic_case(control_case, monkeypatch):
    report, entry, scope, evidence, fixture = copy.deepcopy(control_case)
    report["run"] = recovery.DIAGNOSTIC_RUN
    counts, outcomes, states, refunds = recovery.recovery_case(recovery.DIAGNOSTIC_RUN)[3:]
    report["paid_stage"]["customer"].update(outcomes=outcomes, distinct_orders=25178, distinct_tickets=25178)
    result = policy.digest(report)
    monkeypatch.setattr(recovery, "DIAGNOSTIC_RESULT", result)
    entry.update(ledger=recovery.DIAGNOSTIC_LEDGER, result_sha256=result)
    scope["cce_paid_result_sha256"] = result
    evidence.update(
        ledger=recovery.DIAGNOSTIC_LEDGER,
        run=recovery.DIAGNOSTIC_RUN,
        namespace=recovery.aborted.namespace_for(recovery.DIAGNOSTIC_RUN),
        original_result_sha256=result,
    )
    evidence["paid_terminal_financial"].update(counts=counts, order_states=states, refund_states=refunds)
    return report, entry, scope, evidence, fixture


def test_diagnostic_recovery_keeps_original_customer_failures(diagnostic_case):
    assert recovery.validate(*diagnostic_case)
    assert diagnostic_case[0]["paid_stage"]["customer"]["outcomes"]["order_http_503"] == 20
    assert diagnostic_case[0]["integrity_verified"] is False


@pytest.mark.parametrize("field", list(recovery.COUNTS))
def test_diagnostic_recovery_requires_every_financial_count(diagnostic_case, field):
    diagnostic_case[3]["paid_terminal_financial"]["counts"][field] += 1
    with pytest.raises(ValueError):
        recovery.validate(*diagnostic_case)


@pytest.mark.parametrize("gate", recovery.aborted.GATES)
def test_diagnostic_recovery_requires_every_independent_gate(diagnostic_case, gate):
    diagnostic_case[3]["gates"][gate] = False
    with pytest.raises(ValueError):
        recovery.validate(*diagnostic_case)

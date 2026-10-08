"""Exact stopped-run recovery verifier faults; no cloud calls."""

import copy
import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[2] / "scripts"))
import cce_paid_abort_recovery as recovery
import work_envelope as policy


@pytest.fixture
def case(monkeypatch):
    report = {
        "run": recovery.RUN,
        "pass": False,
        "capacity_stages_started": 0,
        "restoration_complete": True,
        "integrity_verified": True,
    }
    result = policy.digest(report)
    monkeypatch.setattr(recovery, "RESULT", result)
    binding = {"source": "fixed"}
    entry = {
        "ledger": recovery.LEDGER,
        "profile": "cce_paid_comparison",
        "status": "RECOVERY_REQUIRED",
        "result_sha256": result,
        "binding_sha256": policy.digest(binding),
    }
    scope = {"cce_paid_result_sha256": result, "paid_runs_started": 0, "active_run": None, "binding": binding}
    financial = dict.fromkeys(("orders", "fulfilled_orders", "succeeded_payments", "bookings", "tickets"), 1)
    financial.update(
        dict.fromkeys(
            (
                "pending_orders",
                "expired_orders",
                "pending_payment_attempts",
                "duplicate_booked_seats",
                "multi_booking_orders",
                "unpublished_outbox",
                "dead_letters",
                "incomplete_callback_deliveries",
            ),
            0,
        )
    )
    financial.update(
        pass_=True, hold_deadlines_elapsed=True, callback_delivery_attempts=3, callback_delivery_target=3
    )
    financial["pass"] = financial.pop("pass_")
    evidence = {
        "decision": "ADR0228",
        "ledger": recovery.LEDGER,
        "run": recovery.RUN,
        "namespace": recovery.namespace_for(recovery.RUN),
        "original_result_sha256": result,
        "binding_sha256": entry["binding_sha256"],
        "pass": True,
        "gates": dict.fromkeys(recovery.GATES, True),
        "financial": financial,
        "queue_counts": {"test": True},
        "actual_elapsed_seconds": 1.0,
    }
    monkeypatch.setattr(
        recovery, "queue_checks", lambda value, members: value == {"test": True} and members == 1
    )
    return report, entry, scope, evidence


def test_exact_failed_attempt_can_only_close_after_independent_proof(case):
    assert recovery.validate(*case) is True
    assert case[0]["pass"] is False and case[2]["paid_runs_started"] == 0


@pytest.mark.parametrize("gate", recovery.GATES)
def test_every_independent_recovery_gate_is_required(case, gate):
    case[3]["gates"][gate] = False
    with pytest.raises(ValueError):
        recovery.validate(*case)


@pytest.mark.parametrize(
    "fault", ["result", "paid", "binding", "callback_attempts", "callback_target", "duplicate", "queue"]
)
def test_unknown_or_paid_case_cannot_use_pre_dispatch_closure(case, fault):
    report, entry, scope, evidence = copy.deepcopy(case)
    if fault == "result":
        entry["result_sha256"] = "foreign"
    if fault == "paid":
        scope["paid_runs_started"] = 1
    if fault == "binding":
        scope["binding"] = {}
    if fault == "callback_attempts":
        evidence["financial"]["callback_delivery_attempts"] = 2
    if fault == "callback_target":
        evidence["financial"]["callback_delivery_target"] = 4
    if fault == "duplicate":
        evidence["financial"]["duplicate_booked_seats"] = 1
    if fault == "queue":
        evidence["queue_counts"] = {}
    with pytest.raises(ValueError):
        recovery.validate(report, entry, scope, evidence)


def test_new_exact_pre_dispatch_case_requires_its_own_digest(case, monkeypatch):
    report, entry, scope, evidence = copy.deepcopy(case)
    ledger = "bounded_cce_paid_comparison__b7a481e968a9"
    run = "adr0151-6b8f809a403c"
    report["run"] = evidence["run"] = run
    result = policy.digest(report)
    monkeypatch.setitem(recovery.CASES, ledger, (run, result))
    entry["ledger"] = evidence["ledger"] = ledger
    entry["result_sha256"] = scope["cce_paid_result_sha256"] = evidence["original_result_sha256"] = result
    evidence["namespace"] = recovery.namespace_for(run)
    assert recovery.validate(report, entry, scope, evidence)
    evidence["original_result_sha256"] = "foreign"
    with pytest.raises(ValueError):
        recovery.validate(report, entry, scope, evidence)


def test_unknown_abort_identity_cannot_close():
    with pytest.raises(ValueError, match="Unknown"):
        recovery.case_identity("bounded_cce_paid_comparison__unknown")

import sys
from pathlib import Path
from uuid import UUID

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[2] / "scripts"))
import cce_partition_control_recovery as recovery
from run_two_host_paid_comparison import financial_audit_program

SHOWS = [str(UUID(int=i + 1)) for i in range(84)]


def test_matched_audit_accepts_exact_distinct_cohort():
    compile(financial_audit_program(SHOWS, 25200), "matched", "exec")
    compile(recovery.financial_program(SHOWS, 23377), "recovery", "exec")


@pytest.mark.parametrize("shows,paid", [(SHOWS[:-1], 23377),
    ([SHOWS[0]] * 84, 23377), (SHOWS, 25197), (SHOWS, 25201), (SHOWS, True)])
def test_recovery_rejects_cohort_or_unaccounted_outcomes(shows, paid):
    with pytest.raises(ValueError):
        recovery.financial_program(shows, paid)


@pytest.mark.parametrize("count", [23377, 25201, True])
def test_normal_audit_keeps_exact_expected_count_boundary(count):
    with pytest.raises(ValueError):
        financial_audit_program(SHOWS, count)


def test_incomplete_cohort_cannot_claim_matched_recovery():
    with pytest.raises(ValueError):
        recovery.financial_program(SHOWS[:60], 23377)


def test_recovery_refuses_to_close_successful_or_different_run():
    with pytest.raises(ValueError):
        recovery.validate({"pass": True, "run": "other"}, {}, {}, {}, {})



def proof(monkeypatch):
    import work_envelope as policy
    fixture = {"show_ids": SHOWS}
    report = {"run": recovery.RUN, "pass": False, "restoration_complete": True,
              "capacity_stages_started": 1, "paid_stage": {
              "fixture_identity": {"fixture_identity_sha256": policy.digest(fixture)},
              "customer": {"pass": False, "scheduled": 25200, "dispatched": 23390,
              "completed": 23390, "generator_drops": 1810, "retry_attempts": 0,
              "distinct_orders": 23295, "distinct_tickets": 23295,
              "outcomes": {"fulfilled": 23295, "payment_http_503": 13, "order_http_503": 82}}}}
    result = policy.digest(report)
    monkeypatch.setattr(recovery, "RESULT", result)
    binding = {"run_id": recovery.RUN, "cce_partition_decision": "ADR0245",
               "cce_payment_pool_max": 2, "cce_transaction_arm": "control"}
    entry = {"ledger": recovery.LEDGER, "profile": "cce_paid_comparison",
             "status": "RECOVERY_REQUIRED", "result_sha256": result,
             "binding_sha256": policy.digest(binding)}
    scope = {"binding": binding, "cce_paid_result_sha256": result, "paid_runs_started": 1}
    from worker_separation_audits import QUEUE_ZERO, RELATIONSHIP_NAMES
    counts = dict.fromkeys(["fulfilled_orders", "payment_attempts", "succeeded_payments",
                          "bookings", "tickets", "payment_callbacks", "callback_delivery_attempts",
                          "callback_delivery_target"], 23377)
    counts.update(dict.fromkeys(["pending_orders", "pending_payment_attempts", "incomplete_callback_deliveries",
                                "duplicate_booked_seats", "multi_booking_orders", "unpublished_outbox", "dead_letters"], 0))
    counts.update(pass_=True, orders=23390, expired_orders=13, expected=23390,
                  expected_paid=23377, expected_callback_deliveries_per_payment=1)
    counts["pass"] = counts.pop("pass_")
    relations = dict.fromkeys(RELATIONSHIP_NAMES, 0)
    relations["unique_issued_tickets"] = 23377
    financial = {"pass": True, "counts": counts, "relationships": relations,
                 "checks": dict.fromkeys(["post_ttl_complete", "payments_durable", "ticket_relationships_valid"], True)}
    safety = dict.fromkeys(["orders", "fulfilled_orders", "succeeded_payments", "bookings", "tickets"], 1)
    safety.update({"pass": True, "hold_deadlines_elapsed": True, "duplicate_booked_seats": 0,
                   "multi_booking_orders": 0, "callback_delivery_attempts": 3, "callback_delivery_target": 3})
    queues = {**dict.fromkeys(QUEUE_ZERO, 0), "kafka_members": 1, "pass": True}
    evidence = {"decision": "ADR0246", "run": recovery.RUN, "ledger": recovery.LEDGER,
                "original_result_sha256": result, "binding_sha256": policy.digest(binding),
                "pass": True, "capacity_qualified": False, "actual_elapsed_seconds": 30,
                "gates": dict.fromkeys(recovery.GATES, True), "financial": financial,
                "cleanup_namespace": "flash-cce-" + recovery.RUN.removeprefix("adr0151-"),
                "restored_consumer_replicas": 1, "queues": queues, "safety_financial": safety, "native_safety_financial": safety}
    return report, entry, scope, evidence, fixture


def test_complete_recovery_preserves_failed_customer_gate(monkeypatch):
    args = proof(monkeypatch)
    assert recovery.validate(*args)
    assert args[0]["pass"] is False


@pytest.mark.parametrize("fault", ["lost_payment", "duplicate", "relationship", "pending_queue",
                                   "helper_present", "wrong_binding", "paid_replay", "claim_capacity", "wrong_namespace"])
def test_recovery_closure_rejects_incomplete_or_forged_proof(monkeypatch, fault):
    report, entry, scope, evidence, fixture = proof(monkeypatch)
    if fault == "lost_payment": evidence["financial"]["counts"]["tickets"] -= 1
    elif fault == "duplicate": evidence["financial"]["counts"]["duplicate_booked_seats"] = 1
    elif fault == "relationship": evidence["financial"]["relationships"]["booking_relationship_errors"] = 1
    elif fault == "pending_queue": evidence["queues"]["reservation_stream_pending"] = 1
    elif fault == "helper_present": evidence["gates"]["helpers_absent"] = False
    elif fault == "wrong_binding": evidence["binding_sha256"] = "0" * 64
    elif fault == "paid_replay": scope["paid_runs_started"] = 2
    elif fault == "claim_capacity": evidence["capacity_qualified"] = True
    elif fault == "wrong_namespace": evidence["cleanup_namespace"] = "flash-cce-0f735e978cba"
    with pytest.raises(ValueError):
        recovery.validate(report, entry, scope, evidence, fixture)


def test_recovery_requires_restored_consumer_membership(monkeypatch):
    report, entry, scope, evidence, fixture = proof(monkeypatch)
    evidence["queues"]["kafka_members"] = 6
    with pytest.raises(ValueError):
        recovery.validate(report, entry, scope, evidence, fixture)


def test_safety_audit_reuses_scoped_relationships_and_keeps_read_only_limits():
    body = recovery.safety_program(SHOWS[0])
    compile(body, "safety-recovery", "exec")
    assert "SET TRANSACTION ISOLATION LEVEL REPEATABLE READ READ ONLY" in body
    assert "statement_timeout='20s'" in body
    assert "lock_timeout='2s'" in body
    assert "UNION SELECT" in body
    assert "expected_orders=1" not in body  # Parameters stay explicit in the pinned payload.
    with pytest.raises(ValueError):
        recovery.safety_program("not-a-show")


def test_wrong_partition_cannot_close_control(monkeypatch):
    import work_envelope as policy
    report, entry, scope, evidence, fixture = proof(monkeypatch)
    scope["binding"]["cce_payment_pool_max"] = 3
    entry["binding_sha256"] = evidence["binding_sha256"] = policy.digest(scope["binding"])
    with pytest.raises(ValueError):
        recovery.validate(report, entry, scope, evidence, fixture)

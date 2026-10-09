import sys
from pathlib import Path
from uuid import UUID

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[2] / "scripts"))
import cce_simulator_hourly_recovery as recovery
from run_two_host_paid_comparison import financial_audit_program

SHOWS = [str(UUID(int=i + 1)) for i in range(1008)]


def test_matched_audit_accepts_exact_distinct_cohort():
    compile(financial_audit_program(SHOWS[:84], 25200), "matched", "exec")
    compile(recovery.financial_program(SHOWS, 301961), "recovery", "exec")


@pytest.mark.parametrize("shows,paid", [(SHOWS[:-1], 301961),
    ([SHOWS[0]] * 1008, 301961), (SHOWS, 25197), (SHOWS, 25201), (SHOWS, True)])
def test_recovery_rejects_cohort_or_unaccounted_outcomes(shows, paid):
    with pytest.raises(ValueError):
        recovery.financial_program(shows, paid)


@pytest.mark.parametrize("count", [301961, 25201, True])
def test_normal_audit_keeps_exact_expected_count_boundary(count):
    with pytest.raises(ValueError):
        financial_audit_program(SHOWS, count)


def test_incomplete_cohort_cannot_claim_matched_recovery():
    with pytest.raises(ValueError):
        recovery.financial_program(SHOWS[:60], 301961)


def test_recovery_refuses_to_close_successful_or_different_run():
    with pytest.raises(ValueError):
        recovery.validate({"pass": True, "run": "other"}, {}, {}, {}, {})



def proof(monkeypatch):
    import work_envelope as policy
    fixture = {"show_ids": SHOWS}
    report = {"run": recovery.RUN, "pass": False, "restoration_complete": True,
              "capacity_stages_started": 1, "paid_stage": {
              "fixture_identity": {"fixture_identity_sha256": policy.digest(fixture)},
              "customer": {"pass": False, "scheduled": 302400, "dispatched": 302028,
              "completed": 302028, "generator_drops": 372, "retry_attempts": 0,
              "distinct_orders": 301924, "distinct_tickets": 301924,
              "outcomes": {"fulfilled": 301924, "payment_http_503": 67, "order_http_503": 37}}}}
    result = policy.digest(report)
    monkeypatch.setattr(recovery, "RESULT", result)
    binding = {"run_id": recovery.RUN, "cce_simulator_decision": "ADR0251", "cce_simulator_concurrency": 12,
               "cce_simulator_database_pool_max": 10, "cce_simulator_image_id": "sha256:a6cdfd34312cabf61c276233bb53f741a70c5567a36c8bc3ec11844c67a06107", "cce_transaction_arm": "correction",
               "cce_transaction_pair_sha256": "aede238a4a8f56a565fac1003633d6df8947f42d9a9aa8704240ac5f1857bc4b"}
    entry = {"ledger": recovery.LEDGER, "profile": "cce_hourly_qualification",
             "status": "RECOVERY_REQUIRED", "result_sha256": result,
             "binding_sha256": policy.digest(binding)}
    scope = {"binding": binding, "cce_paid_result_sha256": result, "paid_runs_started": 1}
    from worker_separation_audits import QUEUE_ZERO, RELATIONSHIP_NAMES
    counts = dict.fromkeys(["fulfilled_orders", "payment_attempts", "succeeded_payments",
                          "bookings", "tickets", "payment_callbacks", "callback_delivery_attempts",
                          "callback_delivery_target"], 301961)
    counts.update(dict.fromkeys(["pending_orders", "pending_payment_attempts", "incomplete_callback_deliveries",
                                "duplicate_booked_seats", "multi_booking_orders", "unpublished_outbox", "dead_letters"], 0))
    counts.update(pass_=True, orders=302028, expired_orders=67, expected=302028,
                  expected_paid=301961, expected_callback_deliveries_per_payment=1)
    counts["pass"] = counts.pop("pass_")
    relations = dict.fromkeys(RELATIONSHIP_NAMES, 0)
    relations["unique_issued_tickets"] = 301961
    financial = {"pass": True, "counts": counts, "relationships": relations,
                 "checks": dict.fromkeys(["post_ttl_complete", "payments_durable", "ticket_relationships_valid"], True)}
    safety = dict.fromkeys(["orders", "fulfilled_orders", "succeeded_payments", "bookings", "tickets"], 1)
    safety.update({"pass": True, "hold_deadlines_elapsed": True, "duplicate_booked_seats": 0,
                   "multi_booking_orders": 0, "callback_delivery_attempts": 3, "callback_delivery_target": 3})
    queues = {**dict.fromkeys(QUEUE_ZERO, 0), "kafka_members": 1, "pass": True}
    evidence = {"decision": "ADR0253", "run": recovery.RUN, "ledger": recovery.LEDGER,
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
    scope["binding"]["cce_simulator_database_pool_max"] = 12
    entry["binding_sha256"] = evidence["binding_sha256"] = policy.digest(scope["binding"])
    with pytest.raises(ValueError):
        recovery.validate(report, entry, scope, evidence, fixture)


@pytest.mark.parametrize("field,value", [
    ("cce_transaction_pair_sha256", "0" * 64),
    ("cce_simulator_decision", "ADR0245"),
    ("cce_partition_decision", "ADR0245"),
])
def test_other_image_pair_or_decision_cannot_close_control(monkeypatch, field, value):
    import work_envelope as policy
    report, entry, scope, evidence, fixture = proof(monkeypatch)
    scope["binding"][field] = value
    entry["binding_sha256"] = evidence["binding_sha256"] = policy.digest(scope["binding"])
    with pytest.raises(ValueError):
        recovery.validate(report, entry, scope, evidence, fixture)


def test_relationship_audit_batches_in_one_read_only_snapshot_without_timeout_relaxation():
    body = recovery.financial_program(SHOWS, 301961)
    assert "SET TRANSACTION ISOLATION LEVEL REPEATABLE READ READ ONLY" in body
    assert "range(0, len(show_ids), 84)" in body
    assert "SET LOCAL statement_timeout='20s'" in body
    assert "statement_timeout='60s'" not in body
    assert "zip(totals, values, strict=True)" in body
    assert "UNION SELECT" in body
    compile(body, "same-snapshot-hourly-recovery", "exec")


@pytest.mark.parametrize("corrupt_last_batch", [False, True])
def test_executed_batched_snapshot_covers_every_show_and_preserves_late_corruption(corrupt_last_batch):
    from contextlib import nullcontext
    from types import SimpleNamespace
    namespace = {}
    body = recovery.financial_program(SHOWS, 301961)
    exec(body.split("\ncohort=")[0], namespace)
    namespace["audit"] = lambda *args: {"pass": True}
    calls = []
    class Conn:
        def transaction(self): return nullcontext()
        def execute(self, sql, args=None):
            if args is None: return None
            batch = args[0]
            assert len(batch) <= 84 and all(v == batch for v in args)
            calls.append(batch)
            unique = 25200 - (439 if len(calls) == 12 else 0)
            row = [0, int(corrupt_last_batch and len(calls) == 12), 0, 0, 0, unique, 0]
            return SimpleNamespace(fetchone=lambda: row)
    result = namespace["financial_snapshot"](Conn(), SHOWS, 302028, 301961, 1)
    assert [value for batch in calls for value in batch] == SHOWS
    assert result["relationships"]["unique_issued_tickets"] == 301961
    assert result["pass"] is (not corrupt_last_batch)

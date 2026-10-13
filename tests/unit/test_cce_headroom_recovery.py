"""A failed target can restore only after every actual payment reconciles."""
import copy
import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[2] / "scripts"))
import cce_headroom_recovery as r
import work_envelope as p
from worker_separation_audits import QUEUE_ZERO, RELATIONSHIP_NAMES


@pytest.fixture
def proof(monkeypatch):
    f = {"show_ids": [str(i) for i in range(102)]}
    binding = {"runtime": "same"}
    raw = {**dict.fromkeys(r.PAID_COUNTS, r.PAID), **dict.fromkeys(r.ZERO_COUNTS, 0),
           "orders": r.ORDERS, "expired_orders": r.EXPIRED, "expected": 30600,
           "expected_paid": 30600, "pass": False}
    fresh_counts = {**raw, "expected": r.ORDERS, "expected_paid": r.PAID,
                    "expected_callback_deliveries_per_payment": 1, "pass": True}
    relations = {k: r.PAID if k == "unique_issued_tickets" else 0 for k in RELATIONSHIP_NAMES}
    fresh = {"counts": fresh_counts, "relationships": relations, "pass": True,
             "hold_deadlines_elapsed": True, "checks": {"post_ttl_complete": True,
                    "payments_durable": True, "ticket_relationships_valid": True}}
    normal = {**dict.fromkeys(QUEUE_ZERO, 0), "kafka_members": 1, "pass": True, "event_lane_mode": "normal"}
    split = {**normal, "kafka_members": 6, "event_lane_mode": "split",
             "projection_kafka_total_lag": 0, "projection_kafka_members": 1}
    stage = {"financial": {"counts": raw, "relationships": relations},
        "customer": {"scheduled": 30600, "dispatched": r.ORDERS, "completed": r.ORDERS,
          "fulfilled": 26636, "generator_drops": 2583, "final_customer_failures": 1381,
          "outcomes": {"fulfilled": 26636, "order_http_503": 1305, "payment_http_503": 76}},
        "private_cleanup_pass": True, "private_manifests_removed": True,
        "generator_idle_after_stop": True, "job_cleanup_errors": [], "retired_shows": 102,
        "fixture_identity": {"fixture_identity_sha256": p.digest(f)},
        "customer_outcome_gate": {"pass": False}, "ticket_target_issuance": {"pass": False},
        "global_queues": split}
    report = {"run": r.RUN, "pass": False, "integrity_verified": False,
      "restoration_complete": True, "transport_credentials_cleared": True,
      "capacity_stages_started": 1, "binding_sha256": p.digest(binding), "paid_stage": stage,
      "native": {"cleanup_complete": True, "cleanup_failures": [],
        "namespace_cleanup": {"namespace_removed": True},
        "resource_cleanup": {"owned_resources_removed": True}}}
    sha = p.digest(report)
    monkeypatch.setattr(r, "RESULT", sha)
    entry = {"ledger": r.LEDGER, "profile": "cce_ticket_target", "status": "RECOVERY_REQUIRED",
             "result_sha256": sha, "binding_sha256": p.digest(binding)}
    scope = {"cce_paid_result_sha256": sha, "binding": binding, "active_run": r.RUN, "paid_runs_started": 1}
    safety = {"pass": True, "hold_deadlines_elapsed": True, "orders": 1, "fulfilled_orders": 1,
      "succeeded_payments": 1, "bookings": 1, "tickets": 1, "duplicate_booked_seats": 0,
      "multi_booking_orders": 0, "callback_delivery_attempts": 3, "callback_delivery_target": 3}
    evidence = {"decision": "ADR0313", "run": r.RUN, "ledger": r.LEDGER,
      "original_result_sha256": sha, "binding_sha256": p.digest(binding),
      "pass": True, "capacity_qualified": False, "gates": dict.fromkeys(r.GATES, True),
      "fresh_financial": fresh, "queues": normal, "actual_elapsed_seconds": 20,
      "safety_financial": safety, "native_safety_financial": copy.deepcopy(safety)}
    return report, entry, scope, evidence, f


def test_actual_paid_cohort_can_restore_without_qualifying_failed_load(proof):
    original = copy.deepcopy(proof)
    assert r.validate(*proof)
    assert proof == original and proof[0]["pass"] is False


@pytest.mark.parametrize("kind", ["raw", "lost_payment", "duplicate", "relations", "cleanup",
    "queue", "qualification", "safety", "active_scope", "expiry", "fixture"])
def test_missing_integrity_or_ownership_never_restores(proof, kind):
    report, entry, scope, evidence, fixture = copy.deepcopy(proof)
    if kind == "raw":
        report["pass"] = True
    elif kind == "lost_payment":
        evidence["fresh_financial"]["counts"]["tickets"] -= 1
    elif kind == "duplicate":
        evidence["fresh_financial"]["counts"]["duplicate_booked_seats"] = 1
    elif kind == "relations":
        evidence["fresh_financial"]["relationships"]["booking_relationship_errors"] = 1
    elif kind == "cleanup":
        evidence["gates"]["namespace_absent"] = False
    elif kind == "queue":
        evidence["queues"]["pending_callback_deliveries"] = 1
    elif kind == "qualification":
        evidence["capacity_qualified"] = True
    elif kind == "safety":
        evidence["native_safety_financial"]["pass"] = False
    elif kind == "active_scope":
        scope["active_run"] = "different"
    elif kind == "expiry":
        evidence["fresh_financial"]["counts"]["expired_orders"] -= 1
    else:
        fixture["show_ids"].pop()
    with pytest.raises(ValueError):
        r.validate(report, entry, scope, evidence, fixture)


def test_declared_second_case_closes_only_its_actual_paid_cohort(proof):
    report, entry, scope, evidence, fixture = copy.deepcopy(proof)
    report["run"] = r.SLOT_RUN
    customer = report["paid_stage"]["customer"]
    customer.update(scheduled=30600, dispatched=27276, completed=27276,
                    fulfilled=27276, generator_drops=3324, final_customer_failures=0,
                    outcomes={"fulfilled": 27276})
    counts = report["paid_stage"]["financial"]["counts"]
    counts.update({k: 27276 for k in r.PAID_COUNTS})
    counts.update(orders=27276, expired_orders=0)
    relations = report["paid_stage"]["financial"]["relationships"]
    relations["unique_issued_tickets"] = 27276
    fresh = evidence["fresh_financial"]["counts"]
    fresh.update({k: 27276 for k in r.PAID_COUNTS})
    fresh.update(orders=27276, expected=27276, expected_paid=27276, expired_orders=0)
    evidence["fresh_financial"]["relationships"]["unique_issued_tickets"] = 27276
    entry.update(ledger=r.SLOT_LEDGER, result_sha256=r.SLOT_RESULT)
    scope.update(active_run=r.SLOT_RUN, cce_paid_result_sha256=r.SLOT_RESULT)
    evidence.update(run=r.SLOT_RUN, ledger=r.SLOT_LEDGER, original_result_sha256=r.SLOT_RESULT)
    # The fixture is synthetic; authentication itself remains required.
    with pytest.raises(ValueError):
        r.validate(report, entry, scope, evidence, fixture)
    assert r.case(r.SLOT_RUN)[3:6] == (27276, 27276, 0)
    with pytest.raises(ValueError):
        r.case("adr0151-unknown")


def test_four_writer_throughput_pass_does_not_qualify_failed_quality(proof, monkeypatch):
    report, entry, scope, evidence, fixture = copy.deepcopy(proof)
    report["run"] = r.WRITER_RUN
    stage = report["paid_stage"]
    stage["customer"].update(dispatched=30529, completed=30529, fulfilled=30529,
        generator_drops=71, final_customer_failures=0, outcomes={"fulfilled": 30529})
    stage["ticket_target_issuance"]["pass"] = True
    stage["financial"]["counts"].update({k: 30529 for k in r.PAID_COUNTS})
    stage["financial"]["counts"].update(orders=30529, expired_orders=0)
    stage["financial"]["relationships"]["unique_issued_tickets"] = 30529
    fresh = evidence["fresh_financial"]
    fresh["counts"].update({k: 30529 for k in r.PAID_COUNTS})
    fresh["counts"].update(orders=30529, expected=30529, expected_paid=30529, expired_orders=0)
    fresh["relationships"]["unique_issued_tickets"] = 30529
    sha = p.digest(report)
    monkeypatch.setattr(r, "WRITER_RESULT", sha)
    entry.update(ledger=r.WRITER_LEDGER, result_sha256=sha)
    scope.update(active_run=r.WRITER_RUN, cce_paid_result_sha256=sha)
    evidence.update(run=r.WRITER_RUN, ledger=r.WRITER_LEDGER, original_result_sha256=sha)
    assert r.validate(report, entry, scope, evidence, fixture)
    assert report["pass"] is False and evidence["capacity_qualified"] is False
    fresh["counts"]["tickets"] -= 1
    with pytest.raises(ValueError):
        r.validate(report, entry, scope, evidence, fixture)


def test_batch_candidate_lossless_closure_preserves_failed_customer(proof, monkeypatch):
    report, entry, scope, evidence, fixture = copy.deepcopy(proof)
    report["run"] = r.BATCH_RUN
    stage = report["paid_stage"]
    stage["customer"].update(dispatched=27975, completed=27975, fulfilled=27974,
        generator_drops=2625, final_customer_failures=1,
        outcomes={"fulfilled":27974, "command_http_502":1})
    stage["financial"]["counts"].update({k:27974 for k in r.PAID_COUNTS})
    stage["financial"]["counts"].update(orders=27975, expired_orders=1)
    stage["financial"]["relationships"]["unique_issued_tickets"] = 27974
    fresh = evidence["fresh_financial"]
    fresh["counts"].update({k:27974 for k in r.PAID_COUNTS})
    fresh["counts"].update(orders=27975, expected=27975, expected_paid=27974, expired_orders=1)
    fresh["relationships"]["unique_issued_tickets"] = 27974
    sha = p.digest(report)
    monkeypatch.setattr(r,"BATCH_RESULT",sha)
    entry.update(ledger=r.BATCH_LEDGER,result_sha256=sha)
    scope.update(active_run=r.BATCH_RUN,cce_paid_result_sha256=sha)
    evidence.update(run=r.BATCH_RUN,ledger=r.BATCH_LEDGER,original_result_sha256=sha)
    assert r.validate(report,entry,scope,evidence,fixture)
    assert not report["pass"] and not evidence["capacity_qualified"]
    evidence["fresh_financial"]["counts"]["expired_orders"] = 0
    with pytest.raises(ValueError):
        r.validate(report,entry,scope,evidence,fixture)

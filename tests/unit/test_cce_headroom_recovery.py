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


@pytest.mark.parametrize("fault",[None,"missing_ticket","aliases","missing_shard","invented_order"])
def test_partition_closure_requires_actual_payment_and_pooler_proof(proof,monkeypatch,fault):
    from cce_backend_partition import ORIGINAL_CONFIG, POOLER_IMAGE
    report,entry,scope,evidence,fixture=copy.deepcopy(proof)
    report["run"]=r.PARTITION_RUN
    stage=report["paid_stage"]
    stage["customer"].update(dispatched=15399,completed=15399,fulfilled=15398,
        generator_drops=15201,final_customer_failures=1,outcomes={"fulfilled":15398,"hold_http_502":1},
        measurement_complete=True,accounting_complete=True,shard_evidence_states=["final","final"])
    stage["financial"]["counts"].update({k:15398 for k in r.PAID_COUNTS})
    stage["financial"]["counts"].update(orders=15398,expired_orders=0)
    stage["financial"]["relationships"]["unique_issued_tickets"]=15398
    fresh=evidence["fresh_financial"]
    fresh["counts"].update({k:15398 for k in r.PAID_COUNTS})
    fresh["counts"].update(orders=15398,expected=15398,expected_paid=15398,expired_orders=0)
    fresh["relationships"]["unique_issued_tickets"]=15398
    evidence["pooler_rollback"]={"configuration_sha256":ORIGINAL_CONFIG,"image":POOLER_IMAGE,
        "aliases_removed":True,"native_rollback_verified":True,"fresh_live_verified":True}
    if fault=="invented_order":stage["customer"]["dispatched"]=15398
    if fault=="missing_shard":stage["customer"]["shard_evidence_states"][1]="missing"
    sha=p.digest(report);monkeypatch.setattr(r,"PARTITION_RESULT",sha)
    entry.update(ledger=r.PARTITION_LEDGER,result_sha256=sha)
    scope.update(active_run=r.PARTITION_RUN,cce_paid_result_sha256=sha)
    evidence.update(run=r.PARTITION_RUN,ledger=r.PARTITION_LEDGER,original_result_sha256=sha)
    if fault=="aliases":evidence["pooler_rollback"]["aliases_removed"]=False
    if fault=="missing_ticket":fresh["counts"]["tickets"]-=1
    if fault is None:
        assert r.validate(report,entry,scope,evidence,fixture)
        assert report["pass"] is False and evidence["capacity_qualified"] is False
    else:
        with pytest.raises(ValueError):r.validate(report,entry,scope,evidence,fixture)


@pytest.mark.parametrize("fault",[None,"paid_but_unconfirmed","expired","pooler"])
def test_rebalance_closure_preserves_paid_customer_failure(proof,monkeypatch,fault):
    from cce_backend_partition import ORIGINAL_CONFIG, POOLER_IMAGE
    report,entry,scope,evidence,fixture=copy.deepcopy(proof)
    report["run"]=r.REBALANCE_RUN
    stage=report["paid_stage"]
    stage["customer"].update(dispatched=25423,completed=25423,fulfilled=20710,
        generator_drops=5177,final_customer_failures=4713,outcomes=r.case(r.REBALANCE_RUN)[-1],
        measurement_complete=True,accounting_complete=True,shard_evidence_states=["final","final"])
    stage["financial"]["counts"].update({k:22869 for k in r.PAID_COUNTS})
    stage["financial"]["counts"].update(orders=25064,expired_orders=2195)
    stage["financial"]["relationships"]["unique_issued_tickets"]=22869
    fresh=evidence["fresh_financial"]
    fresh["counts"].update({k:22869 for k in r.PAID_COUNTS})
    fresh["counts"].update(orders=25064,expected=25064,expected_paid=22869,expired_orders=2195)
    fresh["relationships"]["unique_issued_tickets"]=22869
    evidence["pooler_rollback"]={"configuration_sha256":ORIGINAL_CONFIG,"image":POOLER_IMAGE,
        "aliases_removed":True,"native_rollback_verified":True,"fresh_live_verified":True}
    if fault=="paid_but_unconfirmed":stage["customer"]["fulfilled"]=22869
    sha=p.digest(report);monkeypatch.setattr(r,"REBALANCE_RESULT",sha)
    entry.update(ledger=r.REBALANCE_LEDGER,result_sha256=sha)
    scope.update(active_run=r.REBALANCE_RUN,cce_paid_result_sha256=sha)
    evidence.update(run=r.REBALANCE_RUN,ledger=r.REBALANCE_LEDGER,original_result_sha256=sha)
    if fault=="expired":fresh["counts"]["expired_orders"]-=1
    if fault=="pooler":evidence["pooler_rollback"]["aliases_removed"]=False
    if fault is None:
        assert r.validate(report,entry,scope,evidence,fixture)
        assert stage["customer"]["final_customer_failures"]==4713
    else:
        with pytest.raises(ValueError):r.validate(report,entry,scope,evidence,fixture)


@pytest.mark.parametrize("fault", [None, "recovery_counts", "retry_count", "missing_shard", "wrong_contract", "ticket_loss"])
def test_payment_context_failure_closes_exact_actual_cohort_without_changing_quality(proof, monkeypatch, fault):
    from cce_measured_payment_context_profile import contract
    report, entry, scope, evidence, fixture = copy.deepcopy(proof)
    binding = {"cce_measured_payment_context_contract": contract()}
    report.update(run=r.CONTEXT_RUN, binding_sha256=p.digest(binding))
    stage = report["paid_stage"]
    stage["customer"].update(scheduled=30600, dispatched=30280, completed=30280, fulfilled=30280,
        generator_drops=320, final_customer_failures=0, outcomes={"fulfilled": 30280},
        first_attempt_error_journeys=114, recovered_journeys=114, retry_attempts=119,
        measurement_complete=True, accounting_complete=True, shard_evidence_states=["final", "final"])
    stage["financial"]["counts"].update({k: 30280 for k in r.PAID_COUNTS})
    stage["financial"]["counts"].update(orders=30280, expired_orders=0)
    stage["financial"]["relationships"]["unique_issued_tickets"] = 30280
    evidence["fresh_financial"]["counts"].update({k: 30280 for k in r.PAID_COUNTS})
    evidence["fresh_financial"]["counts"].update(orders=30280, expected=30280, expected_paid=30280, expired_orders=0)
    evidence["fresh_financial"]["relationships"]["unique_issued_tickets"] = 30280
    sha = p.digest(report)
    monkeypatch.setattr(r, "CONTEXT_RESULT", sha)
    entry.update(ledger=r.CONTEXT_LEDGER, result_sha256=sha, binding_sha256=p.digest(binding))
    scope.update(active_run=r.CONTEXT_RUN, cce_paid_result_sha256=sha, binding=binding)
    evidence.update(run=r.CONTEXT_RUN, ledger=r.CONTEXT_LEDGER, original_result_sha256=sha,
                    binding_sha256=p.digest(binding))
    if fault == "recovery_counts":
        stage["customer"]["recovered_journeys"] -= 1
    elif fault == "retry_count":
        stage["customer"]["retry_attempts"] -= 1
    elif fault == "missing_shard":
        stage["customer"]["shard_evidence_states"] = ["final", "running"]
    elif fault == "wrong_contract":
        scope["binding"]["cce_measured_payment_context_contract"]["backend_connections"] = 25
    elif fault == "ticket_loss":
        evidence["fresh_financial"]["counts"]["tickets"] -= 1
    original = copy.deepcopy((report, entry, scope, evidence, fixture))
    if fault:
        with pytest.raises(ValueError):
            r.validate(report, entry, scope, evidence, fixture)
    else:
        assert r.validate(report, entry, scope, evidence, fixture)
    assert (report, entry, scope, evidence, fixture) == original
    assert report["pass"] is False and report["paid_stage"]["customer"]["generator_drops"] == 320


@pytest.mark.parametrize("fault", [None, "payment_loss", "missing_restore", "changed_snapshot", "namespace", "capacity_claim", "hidden_drop"])
def test_notification_failed_cleanup_requires_fresh_financial_and_original_runtime(proof, monkeypatch, fault):
    import cce_notification_dedup_profile as profile
    report, entry, scope, evidence, fixture = copy.deepcopy(proof)
    monkeypatch.setattr(profile, "contract", lambda: {"decision": "ADR0322"})
    saved = {"owned": "original_snapshot"}
    monkeypatch.setattr(p, "read", lambda path: {"saved": saved})
    scope["binding"]["cce_notification_dedup_contract"] = profile.contract()
    binding_sha = p.digest(scope["binding"])
    report.update(run=r.NOTIFICATION_RUN, restoration_complete=False, binding_sha256=binding_sha)
    stage = report["paid_stage"]
    stage.update(private_cleanup_pass=False, ticket_target_issuance={"pass": True})
    stage["customer"].update(dispatched=30581, completed=30581, fulfilled=30581, generator_drops=19,
        final_customer_failures=0, outcomes={"fulfilled": 30581}, first_attempt_error_journeys=20,
        recovered_journeys=20, retry_attempts=20, measurement_complete=True,
        accounting_complete=True, shard_evidence_states=["final", "final"])
    for counts in (stage["financial"]["counts"], evidence["fresh_financial"]["counts"]):
        counts.update({k: 30581 for k in r.PAID_COUNTS})
        counts.update(orders=30581, expired_orders=0)
    evidence["fresh_financial"]["counts"].update(expected=30581, expected_paid=30581)
    for relationships in (stage["financial"]["relationships"], evidence["fresh_financial"]["relationships"]):
        relationships["unique_issued_tickets"] = 30581
    report["native"].update(cleanup_complete=False, cleanup_failures=[
        {"operation": "candidate_restore", "type": "ValueError"},
        {"operation": "stage_cleanup", "type": "ValueError"},
        {"operation": "global_drain", "type": "RuntimeError"}])
    sha = p.digest(report)
    monkeypatch.setattr(r, "NOTIFICATION_RESULT", sha)
    entry.update(ledger=r.NOTIFICATION_LEDGER, result_sha256=sha, binding_sha256=binding_sha)
    scope.update(active_run=r.NOTIFICATION_RUN, cce_paid_result_sha256=sha)
    evidence.update(run=r.NOTIFICATION_RUN, ledger=r.NOTIFICATION_LEDGER, original_result_sha256=sha,
        binding_sha256=binding_sha, restoration_repair={"saved_snapshot_sha256": p.digest(saved),
        **dict.fromkeys(("saved_semantics_match", "owned_restore_model_match", "all_fixture_shows_retired",
        "extra_workers_removed", "credential_snapshots_removed"), True)})
    if fault == "payment_loss": evidence["fresh_financial"]["counts"]["tickets"] -= 1
    elif fault == "missing_restore": evidence["restoration_repair"]["extra_workers_removed"] = False
    elif fault == "changed_snapshot": evidence["restoration_repair"]["saved_snapshot_sha256"] = "0" * 64
    elif fault == "namespace": evidence["gates"]["namespace_absent"] = False
    elif fault == "capacity_claim": evidence["capacity_qualified"] = True
    elif fault == "hidden_drop": report["paid_stage"]["customer"]["generator_drops"] = 0
    original = copy.deepcopy(report)
    if fault is None:
        assert r.validate(report, entry, scope, evidence, fixture)
        assert report == original and report["pass"] is False
    else:
        with pytest.raises(ValueError): r.validate(report, entry, scope, evidence, fixture)

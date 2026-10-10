import copy
import sys
from pathlib import Path
from uuid import UUID

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[2] / "scripts"))
import cce_paid_observers as observations
import cce_shared_control_recovery as recovery
import work_envelope as policy
from worker_separation_audits import QUEUE_ZERO, RELATIONSHIP_NAMES


def proof(monkeypatch):
    fixture = {"show_ids": [str(UUID(int=i + 1)) for i in range(84)]}
    customer = dict.fromkeys(("dispatched", "completed", "fulfilled", "distinct_orders", "distinct_tickets"), 20040)
    customer.update(scheduled=25200, generator_drops=5160, outcomes={"fulfilled": 20040},
                    final_customer_failures=0, first_attempt_error_journeys=594, recovered_journeys=594)
    customer["pass"] = False
    report = {"run": recovery.RUN, "pass": False, "capacity_stages_started": 1,
              "restoration_complete": True, "transport_credentials_cleared": True,
              "native": {"cleanup_complete": True}, "paid_stage": {"customer": customer,
              "fixture_identity": {"fixture_identity_sha256": policy.digest(fixture)}}}
    result = policy.digest(report)
    monkeypatch.setattr(recovery, "RESULT", result)
    binding = {"cce_worker_placement_decision": "ADR0259", "cce_transaction_arm": "control"}
    entry = {"ledger": recovery.LEDGER, "status": "RECOVERY_REQUIRED", "profile": "cce_paid_comparison",
             "result_sha256": result, "binding_sha256": policy.digest(binding)}
    scope = {"binding": binding, "cce_paid_result_sha256": result, "paid_runs_started": 1}
    counts = dict.fromkeys(("orders", "fulfilled_orders", "payment_attempts", "succeeded_payments", "bookings",
                           "tickets", "payment_callbacks", "callback_delivery_attempts", "callback_delivery_target",
                           "expected", "expected_paid"), 20040)
    counts.update(dict.fromkeys(("expired_orders", "pending_orders", "pending_payment_attempts", "incomplete_callback_deliveries",
                                "duplicate_booked_seats", "multi_booking_orders", "unpublished_outbox", "dead_letters"), 0))
    counts.update({"pass": True, "expected_callback_deliveries_per_payment": 1})
    relationships = dict.fromkeys(RELATIONSHIP_NAMES, 0)
    relationships["unique_issued_tickets"] = 20040
    financial = {"pass": True, "counts": counts, "relationships": relationships,
                 "checks": dict.fromkeys(("post_ttl_complete", "payments_durable", "ticket_relationships_valid"), True)}
    safety = copy.deepcopy(financial)
    for key in ("orders", "fulfilled_orders", "payment_attempts", "succeeded_payments", "bookings", "tickets", "payment_callbacks", "expected", "expected_paid"):
        safety["counts"][key] = 1
    safety["counts"].update(callback_delivery_attempts=3, callback_delivery_target=3)
    safety["relationships"]["unique_issued_tickets"] = 1
    evidence = {"decision": "ADR0260", "run": recovery.RUN, "ledger": recovery.LEDGER,
                "original_result_sha256": result, "binding_sha256": policy.digest(binding),
                "pass": True, "capacity_qualified": False, "gates": dict.fromkeys(recovery.GATES, True),
                "cleanup_namespace": "flash-cce-ba78fda091fd", "verified_owned_events": 86,
                "actual_elapsed_seconds": 30, "queues": {**dict.fromkeys(QUEUE_ZERO, 0), "kafka_members": 1, "pass": True},
                "financial": financial, "safety_relationships": safety, "native_safety_relationships": copy.deepcopy(safety)}
    return report, entry, scope, evidence, fixture


def test_dispatched_recovery_preserves_failed_capacity(monkeypatch):
    args = proof(monkeypatch)
    assert recovery.validate(*args)
    assert args[0]["pass"] is False and args[0]["paid_stage"]["customer"]["generator_drops"] == 5160
    body = recovery.financial_program(args[-1]["show_ids"], 20040)
    compile(body, "recovery", "exec")
    assert "REPEATABLE READ READ ONLY" in body and "UNION SELECT" in body


@pytest.mark.parametrize("fault", ["original", "lost_payment", "duplicate", "relationship", "ttl", "safety", "pending_queue", "helper", "replay", "capacity", "candidate"])
def test_closure_rejects_incomplete_or_unrelated_proof(monkeypatch, fault):
    report, entry, scope, evidence, fixture = proof(monkeypatch)
    if fault == "original": report["pass"] = True
    elif fault == "lost_payment": evidence["financial"]["counts"]["tickets"] -= 1
    elif fault == "duplicate": evidence["financial"]["counts"]["duplicate_booked_seats"] = 1
    elif fault == "relationship": evidence["financial"]["relationships"]["booking_relationship_errors"] = 1
    elif fault == "ttl": evidence["financial"]["relationships"]["holds_not_past_ttl"] = 1
    elif fault == "safety": evidence["native_safety_relationships"]["counts"]["callback_delivery_attempts"] = 2
    elif fault == "pending_queue": evidence["queues"]["reservation_stream_pending"] = 1
    elif fault == "helper": evidence["gates"]["helpers_absent"] = False
    elif fault == "replay": scope["paid_runs_started"] = 2
    elif fault == "capacity": evidence["capacity_qualified"] = True
    else:
        scope["binding"]["cce_transaction_arm"] = "candidate"
        entry["binding_sha256"] = evidence["binding_sha256"] = policy.digest(scope["binding"])
    with pytest.raises(ValueError): recovery.validate(report, entry, scope, evidence, fixture)


@pytest.mark.parametrize("failed", ["slot", "pipeline", "both"])
def test_slot_rollover_keeps_independent_cpu_and_database_evidence(monkeypatch, failed):
    monkeypatch.setattr(observations.native, "validate_receipts", lambda _: {"pod": 1})
    monkeypatch.setattr(observations, "api_cpu", lambda *a, **kw: {"pass": True, "cores": 2})
    def missing(*args, **kwargs): raise ValueError("Missing slot history")
    record = {}
    failures = observations.summarize_independent_diagnostics(
        record, [], Path("private"), {}, "start", "end", hourly=False,
        summarize_waits=lambda *a, **kw: {"complete": True},
        summarize_slots=missing if failed in {"slot", "both"} else lambda *a: {"complete": True},
        summarize_pipeline=missing if failed in {"pipeline", "both"} else lambda *a: {"samples": 3})
    assert record["native_api_cpu"]["pass"] and record["database_wait_capture"]["complete"]
    assert len(failures) == (2 if failed == "both" else 1)
    assert all(f["type"] == "ValueError" for f in failures)
    # Available summaries never erase completeness failures.
    assert failures


@pytest.mark.parametrize("fault", [None, "lost_ticket", "wrong_drops", "original_image_result", "wrong_scope"])
def test_exact_corrected_control_recovery(monkeypatch, fault):
    from dataclasses import replace

    report, entry, scope, evidence, fixture = proof(monkeypatch)
    target = recovery.CORRECTED
    report["run"] = evidence["run"] = target.run
    entry["ledger"] = evidence["ledger"] = target.ledger
    evidence["cleanup_namespace"] = target.namespace
    customer = report["paid_stage"]["customer"]
    for key in ("dispatched", "completed", "fulfilled", "distinct_orders", "distinct_tickets"):
        customer[key] = target.expected
    customer.update(generator_drops=target.drops, outcomes={"fulfilled": target.expected}, first_attempt_error_journeys=target.recovered, recovered_journeys=target.recovered)
    counts = evidence["financial"]["counts"]
    for key, value in list(counts.items()):
        if value == 20040:
            counts[key] = target.expected
    evidence["financial"]["relationships"]["unique_issued_tickets"] = target.expected
    result = policy.digest(report)
    target = replace(target, result=result)
    monkeypatch.setattr(recovery, "CORRECTED", target)
    entry["result_sha256"] = scope["cce_paid_result_sha256"] = evidence["original_result_sha256"] = result
    if fault == "lost_ticket": counts["tickets"] -= 1
    elif fault == "wrong_drops": customer["generator_drops"] += 1
    elif fault == "original_image_result": evidence["original_result_sha256"] = recovery.RESULT
    elif fault == "wrong_scope": target = replace(target, expected=target.expected-1)
    if fault:
        with pytest.raises(ValueError): recovery.validate(report, entry, scope, evidence, fixture, target=target)
    else:
        assert recovery.validate(report, entry, scope, evidence, fixture, target=target)
        compile(recovery.financial_program(fixture["show_ids"],24410,target=target),"corrected-cohort","exec")


@pytest.mark.parametrize("fault", [None, "slot_budget", "sql_budget", "runtime", "observer", "lost_payment", "drop_count"])
def test_exact_dispatch_correction_recovery(monkeypatch, fault):
    from dataclasses import replace
    report, entry, scope, evidence, fixture = proof(monkeypatch)
    target = recovery.DISPATCH
    report["run"] = evidence["run"] = target.run
    entry["ledger"] = evidence["ledger"] = target.ledger
    evidence["cleanup_namespace"] = target.namespace
    c = report["paid_stage"]["customer"]
    for k in ("dispatched", "completed", "fulfilled", "distinct_orders", "distinct_tickets"):
        c[k] = target.expected
    c.update(generator_drops=target.drops, outcomes={"fulfilled": target.expected},
             first_attempt_error_journeys=target.recovered, recovered_journeys=target.recovered)
    for k, v in list(evidence["financial"]["counts"].items()):
        if v == 20040:evidence["financial"]["counts"][k] = target.expected
    evidence["financial"]["relationships"]["unique_issued_tickets"] = target.expected
    scope["binding"] = {"cce_dispatch_decision": "ADR0263", "cce_simulator_concurrency": 16,
                        "cce_simulator_database_pool_max": 10, "cce_transaction_arm": "correction"}
    report["native"]["measurement_gates"] = {"unchanged_native_pods": True, "native_observer": True}
    result = policy.digest(report)
    target = replace(target, result=result)
    monkeypatch.setattr(recovery, "DISPATCH", target)
    entry["result_sha256"] = scope["cce_paid_result_sha256"] = evidence["original_result_sha256"] = result
    entry["binding_sha256"] = evidence["binding_sha256"] = policy.digest(scope["binding"])
    if fault == "slot_budget":scope["binding"]["cce_simulator_concurrency"] = 12
    elif fault == "sql_budget":scope["binding"]["cce_simulator_database_pool_max"] = 16
    elif fault == "runtime":report["native"]["measurement_gates"]["unchanged_native_pods"] = False
    elif fault == "observer":report["native"]["measurement_gates"]["native_observer"] = False
    elif fault == "lost_payment":evidence["financial"]["counts"]["tickets"] -= 1
    elif fault == "drop_count":c["generator_drops"] += 1
    if fault:
        with pytest.raises(ValueError):recovery.validate(report, entry, scope, evidence, fixture, target=target)
    else:
        assert recovery.validate(report, entry, scope, evidence, fixture, target=target)

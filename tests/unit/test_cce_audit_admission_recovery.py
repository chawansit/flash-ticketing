"""Recovered ownership must never rewrite a failed benchmark or financial loss."""
import copy
import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[2] / "scripts"))
import cce_audit_admission_recovery as recovery
import work_envelope as policy
from worker_separation_audits import QUEUE_ZERO, RELATIONSHIP_NAMES


@pytest.fixture
def proof(monkeypatch):
    fixture = {"show_ids": [str(i) for i in range(1008)]}
    binding = {"immutable": True}
    counts = {**dict.fromkeys(recovery.PAID_COUNTS, 302400),
              **dict.fromkeys(recovery.ZERO_COUNTS, 0)}
    financial = {"pass": True, "hold_deadlines_elapsed": True, "counts": counts,
                 "relationships": {k: 302400 if k == "unique_issued_tickets" else 0
                                   for k in RELATIONSHIP_NAMES}}
    extra = dict.fromkeys(("pending_confirmation_receipts", "review_confirmation_receipts",
                           "confirmation_capacity_outstanding", "confirmation_capacity_mismatches"), 0)
    normal = {**dict.fromkeys(QUEUE_ZERO, 0), **extra, "kafka_members": 1,
              "pass": True, "event_lane_mode": "normal"}
    split = {**normal, "kafka_members": 6, "event_lane_mode": "split",
             "projection_kafka_total_lag": 0, "projection_kafka_members": 1}
    customer = {**dict.fromkeys(("scheduled", "dispatched", "completed", "fulfilled",
                                "fulfilled_by_deadline", "distinct_orders", "distinct_tickets"), 302400),
                **dict.fromkeys(("generator_drops", "first_attempt_error_journeys",
                                "recovered_journeys", "retry_attempts", "final_customer_failures"), 0),
                "pass": True}
    paid = {"financial": financial, "financial_failure_type": "TimeoutError",
            "customer": customer, "customer_outcome_gate": {"pass": True},
            "hourly_issuance": {"pass": True}, "observation": {"observer_pass": True},
            "global_queues": split, "fixture_identity": {"fixture_identity_sha256": policy.digest(fixture)},
            "private_cleanup_pass": True, "retired_shows": 1008, "private_manifests_removed": True,
            "generator_idle_after_stop": True, "job_cleanup_errors": []}
    report = {"run": recovery.RUN, "pass": False, "capacity_stages_started": 1,
              "restoration_complete": True, "transport_credentials_cleared": True,
              "paid_stage": paid, "native": {"cleanup_complete": True, "cleanup_failures": [],
              "namespace_cleanup": {"namespace_removed": True},
              "resource_cleanup": {"owned_resources_removed": True}}}
    result = policy.digest(report)
    monkeypatch.setattr(recovery, "RESULT", result)
    entry = {"ledger": recovery.LEDGER, "status": "RECOVERY_REQUIRED",
             "result_sha256": result, "binding_sha256": policy.digest(binding)}
    scope = {"cce_paid_result_sha256": result, "binding": binding,
             "paid_runs_started": 1, "active_run": recovery.RUN}
    evidence = {"decision": "ADR0309", "run": recovery.RUN, "ledger": recovery.LEDGER,
                "original_result_sha256": result, "binding_sha256": entry["binding_sha256"],
                "gates": dict.fromkeys(recovery.GATES, True), "pass": True,
                "capacity_qualified": False, "queues": normal,
                "fresh_financial_matches_raw_cleanup": True, "fresh_financial": financial,
                "actual_elapsed_seconds": 15}
    return report, entry, scope, evidence, fixture


def test_exact_complete_recovered_proof_is_valid(proof):
    assert recovery.validate(*proof)


@pytest.mark.parametrize("kind", ["lost_ticket", "duplicate", "unexpired", "queue",
                                "projection_lag", "confirmation_review", "cleanup",
                                "extra_dispatch", "qualification", "fresh_drift", "raw_tamper"])
def test_unsafe_or_qualified_recovery_is_rejected(proof, monkeypatch, kind):
    report, entry, scope, evidence, fixture = copy.deepcopy(proof)
    if kind == "lost_ticket":
        report["paid_stage"]["financial"]["counts"]["tickets"] -= 1
    elif kind == "duplicate":
        report["paid_stage"]["financial"]["counts"]["duplicate_booked_seats"] = 1
    elif kind == "unexpired":
        report["paid_stage"]["financial"]["relationships"]["holds_not_past_ttl"] = 1
    elif kind == "queue":
        evidence["queues"]["unpublished_outbox"] = 1
    elif kind == "projection_lag":
        report["paid_stage"]["global_queues"]["projection_kafka_total_lag"] = 1
    elif kind == "confirmation_review":
        report["paid_stage"]["global_queues"]["review_confirmation_receipts"] = 1
    elif kind == "cleanup":
        report["native"]["cleanup_failures"] = [{"operation": "unknown"}]
    elif kind == "extra_dispatch":
        scope["paid_runs_started"] = 2
    elif kind == "qualification":
        evidence["capacity_qualified"] = True
    elif kind == "fresh_drift":
        evidence["fresh_financial"]["pass"] = False
    else:
        report["unexpected"] = "changed"
    if kind != "raw_tamper":
        digest = policy.digest(report)
        monkeypatch.setattr(recovery, "RESULT", digest)
        entry["result_sha256"] = scope["cce_paid_result_sha256"] = evidence["original_result_sha256"] = digest
    with pytest.raises(ValueError):
        recovery.validate(report, entry, scope, evidence, fixture)

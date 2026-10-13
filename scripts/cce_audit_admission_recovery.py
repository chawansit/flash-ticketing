"""ADR0309 exact failed hourly ownership closure; raw benchmark stays failed."""
import copy
import math
from pathlib import Path

import work_envelope as policy

RUN = "adr0151-148c89e0e082"
LEDGER = "bounded_cce_hourly_qualification__3d62bb238b9e"
RESULT = "86a21c323c283318b37382b448ac28fb5ad9a31b1e32eccc1fcc8d8907df2e68"
GATES = ("runtime_restored", "runtime_stable", "namespace_absent", "helpers_absent",
         "secondary_empty", "generator_idle", "queues_zero")
ZERO_COUNTS = ("expired_orders", "pending_orders", "pending_payment_attempts",
               "incomplete_callback_deliveries", "duplicate_booked_seats",
               "multi_booking_orders", "unpublished_outbox", "dead_letters")
PAID_COUNTS = ("orders", "fulfilled_orders", "payment_attempts", "succeeded_payments",
               "bookings", "tickets", "payment_callbacks", "callback_delivery_attempts",
               "callback_delivery_target")


def queues_valid(value, members, split):
    from worker_separation_audits import QUEUE_ZERO, queue_checks
    base = {k: value.get(k) for k in (*QUEUE_ZERO, "kafka_members", "pass")}
    extra = ("pending_confirmation_receipts", "review_confirmation_receipts",
             "confirmation_capacity_outstanding", "confirmation_capacity_mismatches")
    return (queue_checks(base, members)
            and all(value.get(k) == 0 for k in extra)
            and value.get("event_lane_mode") == ("split" if split else "normal")
            and (not split or (value.get("projection_kafka_total_lag") == 0
                               and value.get("projection_kafka_members") == 1)))


def validate(report, entry, scope, evidence, fixture):
    from worker_separation_audits import RELATIONSHIP_NAMES
    paid = report.get("paid_stage", {})
    counts = paid.get("financial", {}).get("counts", {})
    relationships = paid.get("financial", {}).get("relationships", {})
    c = paid.get("customer", {})
    n = report.get("native", {})
    elapsed = evidence.get("actual_elapsed_seconds")
    required = (
        policy.digest(report) == RESULT, report.get("run") == RUN, report.get("pass") is False,
        entry.get("ledger") == LEDGER, entry.get("status") == "RECOVERY_REQUIRED",
        entry.get("result_sha256") == RESULT, scope.get("cce_paid_result_sha256") == RESULT,
        entry.get("binding_sha256") == policy.digest(scope.get("binding", {})),
        scope.get("paid_runs_started") == report.get("capacity_stages_started") == 1,
        scope.get("active_run") == RUN, report.get("restoration_complete") is True,
        report.get("transport_credentials_cleared") is True,
        n.get("cleanup_complete") is True, n.get("cleanup_failures") == [],
        n.get("namespace_cleanup", {}).get("namespace_removed") is True,
        n.get("resource_cleanup", {}).get("owned_resources_removed") is True,
        paid.get("private_cleanup_pass") is True, paid.get("retired_shows") == 1008,
        paid.get("private_manifests_removed") is True,
        paid.get("generator_idle_after_stop") is True, paid.get("job_cleanup_errors") == [],
        paid.get("financial_failure_type") == "TimeoutError",
        paid.get("financial", {}).get("pass") is True,
        paid.get("financial", {}).get("hold_deadlines_elapsed") is True,
        all(counts.get(k) == 302400 for k in PAID_COUNTS),
        all(counts.get(k) == 0 for k in ZERO_COUNTS),
        set(relationships) == set(RELATIONSHIP_NAMES),
        relationships.get("unique_issued_tickets") == 302400,
        all(relationships.get(k) == 0 for k in RELATIONSHIP_NAMES if k != "unique_issued_tickets"),
        all(c.get(k) == 302400 for k in ("scheduled", "dispatched", "completed", "fulfilled",
                                      "fulfilled_by_deadline", "distinct_orders", "distinct_tickets")),
        all(c.get(k) == 0 for k in ("generator_drops", "first_attempt_error_journeys",
                                  "recovered_journeys", "retry_attempts", "final_customer_failures")),
        c.get("pass") is True, paid.get("customer_outcome_gate", {}).get("pass") is True,
        paid.get("hourly_issuance", {}).get("pass") is True,
        paid.get("observation", {}).get("observer_pass") is True,
        queues_valid(paid.get("global_queues", {}), 6, True),
        len(set(fixture.get("show_ids", []))) == 1008,
        policy.digest(fixture) == paid.get("fixture_identity", {}).get("fixture_identity_sha256"),
        evidence.get("decision") == "ADR0309", evidence.get("run") == RUN,
        evidence.get("ledger") == LEDGER, evidence.get("original_result_sha256") == RESULT,
        evidence.get("binding_sha256") == entry.get("binding_sha256"),
        evidence.get("gates") == dict.fromkeys(GATES, True), evidence.get("pass") is True,
        evidence.get("capacity_qualified") is False,
        evidence.get("fresh_financial_matches_raw_cleanup") is True,
        evidence.get("fresh_financial", {}).get("pass") is True,
        evidence.get("fresh_financial", {}).get("hold_deadlines_elapsed") is True,
        evidence.get("fresh_financial", {}).get("counts") == counts,
        evidence.get("fresh_financial", {}).get("relationships") == relationships,
        queues_valid(evidence.get("queues", {}), 1, False),
    )
    if (not all(required) or type(elapsed) not in (int, float)
            or not math.isfinite(elapsed) or not 0 <= elapsed <= 600):
        raise ValueError("Exact consumed failed hourly proof and fresh restoration required")
    return True


def close(path):
    path = Path(path)
    if (path.is_symlink() or path.stat().st_nlink != 1
            or path.resolve().parent != (policy.ROOT / "tmp" / RUN).resolve()):
        raise ValueError("Exact owned regular terminal recovery receipt required")
    state, records = policy.read(policy.STATE), policy.journal(policy.envelope())
    matches = [v for v in records["experiments"] if v.get("ledger") == LEDGER]
    if len(matches) != 1 or state.get("current_run") != RUN or policy.LOCK.exists():
        raise ValueError("One stopped consumed recovery scope required")
    report = policy.read(policy.ROOT / "tmp" / RUN / "cce-comparison.private.json")
    fixture = policy.read(policy.ROOT / "tmp" / RUN / "candidate/fixture-identity.json")["fixture_identity"]
    entry, scope, evidence = matches[0], state[LEDGER], policy.read(path)
    validate(report, entry, scope, evidence, fixture)
    entry.update(initial_status=entry["status"],
                 initial_actual_elapsed_seconds=entry["actual_elapsed_seconds"],
                 status="FAILED_RESTORED", recovery={
                     "decision": "ADR0309", "evidence": path.relative_to(policy.ROOT).as_posix(),
                     "sha256": policy.digest(evidence),
                     "actual_elapsed_seconds": evidence["actual_elapsed_seconds"],
                     "capacity_qualified": False})
    entry["actual_elapsed_seconds"] += evidence["actual_elapsed_seconds"]
    scope["terminal_audit_admission_recovery"] = copy.deepcopy(entry["recovery"])
    scope["active_run"] = None
    state["current_run"] = None
    policy.write(policy.JOURNAL, records)
    policy.write(policy.STATE, state)
    return {"status": entry["status"], "original_result_preserved": True, "capacity_qualified": False}

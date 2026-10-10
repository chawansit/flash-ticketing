"""ADR0260 exact dispatched-cohort closure; no paid load or capacity approval."""
import copy
import math
from pathlib import Path

import work_envelope as policy
from cce_transaction_recovery import RELATIONSHIP_SQL
from cce_transaction_recovery import safety_program as build_safety_program
from worker_separation_audits import financial_body, queue_checks

RUN = "adr0151-ba78fda091fd"
LEDGER = "bounded_cce_paid_comparison__1dfe7edba664"
RESULT = "98d5fa9409ae86e2c918109b7aa398fbcf49225f6924346352668941497fb037"
EXPECTED = 20040
GATES = ("runtime_restored", "runtime_stable", "namespace_absent", "helpers_absent",
         "secondary_empty", "generator_idle", "private_inputs_removed", "owned_shows_retired")


def safety_program(show_id):
    return build_safety_program(show_id)

def financial_program(show_ids, expected_paid):
    if len(show_ids) != 84 or expected_paid != EXPECTED:
        raise ValueError("Exact dispatched shared-image cohort required")
    from worker_separation_audits import RELATIONSHIP_SQL as old
    body = financial_body({"show_ids": show_ids, "expected_orders": EXPECTED,
                           "expected_paid": EXPECTED, "callbacks": 1})
    marker = "RELATIONSHIP_SQL=" + repr(old)
    if body.count(marker) != 1:
        raise ValueError("Canonical financial builder changed")
    return body.replace(marker, "RELATIONSHIP_SQL=" + repr(RELATIONSHIP_SQL), 1)


def validate(report, entry, scope, evidence, fixture):
    customer = report.get("paid_stage", {}).get("customer", {})
    elapsed = evidence.get("actual_elapsed_seconds")
    checks = (
        report.get("run") == RUN, policy.digest(report) == RESULT, report.get("pass") is False,
        entry.get("ledger") == LEDGER, entry.get("status") == "RECOVERY_REQUIRED",
        entry.get("profile") == "cce_paid_comparison", entry.get("result_sha256") == RESULT,
        scope.get("cce_paid_result_sha256") == RESULT,
        entry.get("binding_sha256") == policy.digest(scope.get("binding")),
        scope.get("binding", {}).get("cce_worker_placement_decision") == "ADR0259",
        scope.get("binding", {}).get("cce_transaction_arm") == "control",
        scope.get("active_run") in (None, RUN), scope.get("paid_runs_started") == 1,
        report.get("capacity_stages_started") == 1, report.get("restoration_complete") is True,
        report.get("native", {}).get("cleanup_complete") is True,
        report.get("transport_credentials_cleared") is True,
        customer.get("scheduled") == 25200, customer.get("generator_drops") == 5160,
        all(customer.get(k) == EXPECTED for k in ("dispatched", "completed", "fulfilled", "distinct_orders", "distinct_tickets")),
        customer.get("outcomes") == {"fulfilled": EXPECTED}, customer.get("final_customer_failures") == 0,
        customer.get("first_attempt_error_journeys") == customer.get("recovered_journeys") == 594,
        customer.get("pass") is False,
        policy.digest(fixture) == report["paid_stage"]["fixture_identity"]["fixture_identity_sha256"],
        len(fixture.get("show_ids", [])) == 84,
        evidence.get("decision") == "ADR0260", evidence.get("run") == RUN,
        evidence.get("ledger") == LEDGER, evidence.get("original_result_sha256") == RESULT,
        evidence.get("binding_sha256") == entry.get("binding_sha256"), evidence.get("pass") is True,
        evidence.get("capacity_qualified") is False, evidence.get("gates") == dict.fromkeys(GATES, True),
        evidence.get("cleanup_namespace") == "flash-cce-ba78fda091fd",
        evidence.get("verified_owned_events") == 86,
        queue_checks(evidence.get("queues", {}), 1),
    )
    if not all(checks) or type(elapsed) not in (int, float) or not math.isfinite(elapsed) or not 0 <= elapsed <= 600:
        raise ValueError("Exact stopped failed control and independent recovery required")
    financial = evidence.get("financial", {})
    counts = financial.get("counts", {})
    relationships = financial.get("relationships", {})
    if (financial.get("pass") is not True or counts.get("pass") is not True
            or any(counts.get(k) != EXPECTED for k in ("orders", "fulfilled_orders", "payment_attempts",
                "succeeded_payments", "bookings", "tickets", "payment_callbacks", "callback_delivery_attempts",
                "callback_delivery_target", "expected", "expected_paid"))
            or any(counts.get(k) != 0 for k in ("expired_orders", "pending_orders", "pending_payment_attempts",
                "incomplete_callback_deliveries", "duplicate_booked_seats", "multi_booking_orders", "unpublished_outbox", "dead_letters"))
            or counts.get("expected_callback_deliveries_per_payment") != 1
            or relationships.get("unique_issued_tickets") != EXPECTED
            or any(relationships.get(k) != 0 for k in ("hold_relationship_errors", "booking_relationship_errors",
                "order_item_relationship_errors", "inventory_relationship_errors", "fulfilled_ticket_relationship_errors", "holds_not_past_ttl"))
            or financial.get("checks") != {"post_ttl_complete": True, "payments_durable": True, "ticket_relationships_valid": True}):
        raise ValueError("Every dispatched payment and ticket must reconcile")
    for name in ("safety_relationships", "native_safety_relationships"):
        safety = evidence.get(name, {})
        counts, relationships = safety.get("counts", {}), safety.get("relationships", {})
        if (safety.get("pass") is not True or counts.get("pass") is not True
                or any(counts.get(k) != 1 for k in ("orders", "fulfilled_orders", "succeeded_payments", "bookings", "tickets"))
                or counts.get("callback_delivery_attempts") != 3 or counts.get("callback_delivery_target") != 3
                or counts.get("duplicate_booked_seats") != 0 or counts.get("multi_booking_orders") != 0
                or relationships.get("unique_issued_tickets") != 1
                or any(relationships.get(k) != 0 for k in ("hold_relationship_errors", "booking_relationship_errors",
                    "order_item_relationship_errors", "inventory_relationship_errors", "fulfilled_ticket_relationship_errors", "holds_not_past_ttl"))):
            raise ValueError("Both safety payment relationships required")
    return True


def close(path):
    path = Path(path)
    if path.is_symlink() or path.stat().st_nlink != 1 or path.resolve().parent != (policy.ROOT / "tmp" / RUN).resolve():
        raise ValueError("Owned regular recovery receipt required")
    state, journal = policy.read(policy.STATE), policy.journal(policy.envelope())
    matches = [r for r in journal["experiments"] if r.get("ledger") == LEDGER]
    if len(matches) != 1 or policy.LOCK.exists() or state.get("current_run") not in (None, RUN):
        raise ValueError("Stopped exact consumed scope required")
    root = policy.ROOT / "tmp" / RUN
    report = policy.read(root / "cce-comparison.private.json")
    fixture = policy.read(root / "candidate/fixture-identity.json")["fixture_identity"]
    entry, scope, evidence = matches[0], state[LEDGER], policy.read(path)
    validate(report, entry, scope, evidence, fixture)
    entry.update(initial_status=entry["status"], initial_actual_elapsed_seconds=entry["actual_elapsed_seconds"],
                 status="FAILED_RESTORED", recovery={"decision": "ADR0260",
                 "evidence": path.resolve().relative_to(policy.ROOT).as_posix(), "sha256": policy.digest(evidence),
                 "actual_elapsed_seconds": evidence["actual_elapsed_seconds"], "capacity_qualified": False})
    entry["actual_elapsed_seconds"] += evidence["actual_elapsed_seconds"]
    scope["cce_shared_control_terminal_recovery"] = copy.deepcopy(entry["recovery"])
    scope["active_run"] = None
    if state.get("current_run") == RUN:
        state["current_run"] = None
    policy.write(policy.JOURNAL, journal)
    policy.write(policy.STATE, state)
    return {"status": "FAILED_RESTORED", "original_result_preserved": True, "capacity_qualified": False}

"""ADR0313 exact failed-headroom ownership closure; raw quality remains failed."""
import copy
import math
from pathlib import Path

import work_envelope as policy
from cce_audit_admission_recovery import queues_valid
from worker_separation_audits import RELATIONSHIP_NAMES

RUN = "adr0151-211188fa6615"
LEDGER = "bounded_cce_ticket_target__37fc44dbf69e"
RESULT = "463e92253a3613ea9bc5e591823596681b890611290d224bc8ad9a83618b1424"
ORDERS, PAID, EXPIRED = 28017, 27941, 76
SLOT_RUN = "adr0151-4a166cdbdda6"
SLOT_LEDGER = "bounded_cce_ticket_target__476c1bdb0c87"
SLOT_RESULT = "d5a00a534026bce91b131a968f036e38c816e076f95e056e842a011127f4ae01"

WRITER_RUN = "adr0151-6fd6d0275799"
WRITER_LEDGER = "bounded_cce_ticket_target__cf9e12d21c2f"
WRITER_RESULT = "250aa2541308119f8da10a7ba3ebdf8024d99fb8a1fffbb796e8eead55fbfa2a"

BATCH_RUN = "adr0151-3f899bb32f0e"
BATCH_LEDGER = "bounded_cce_ticket_target__debd1beb6d6b"
BATCH_RESULT = "aa6875dbd7ab0669159a60a28c21e97e81f143792f347acc71249f7daeb58c92"


def case(run):
    if run == RUN:
        return RUN, LEDGER, RESULT, ORDERS, PAID, EXPIRED, 26636, 2583, 1381, {"fulfilled": 26636, "order_http_503": 1305, "payment_http_503": 76}
    if run == SLOT_RUN:
        return SLOT_RUN, SLOT_LEDGER, SLOT_RESULT, 27276, 27276, 0, 27276, 3324, 0, {"fulfilled": 27276}
    if run == WRITER_RUN:
        return WRITER_RUN, WRITER_LEDGER, WRITER_RESULT, 30529, 30529, 0, 30529, 71, 0, {"fulfilled": 30529}
    if run == BATCH_RUN:
        return BATCH_RUN, BATCH_LEDGER, BATCH_RESULT, 27975, 27974, 1, 27974, 2625, 1, {"fulfilled": 27974, "command_http_502": 1}
    raise ValueError("Unknown failed headroom recovery case")
GATES = ("runtime_restored", "runtime_stable", "namespace_absent", "helpers_absent",
         "secondary_empty", "generator_idle", "queues_zero")
PAID_COUNTS = ("fulfilled_orders", "payment_attempts", "succeeded_payments", "bookings",
               "tickets", "payment_callbacks", "callback_delivery_attempts", "callback_delivery_target")
ZERO_COUNTS = ("pending_orders", "pending_payment_attempts", "incomplete_callback_deliveries",
               "duplicate_booked_seats", "multi_booking_orders", "unpublished_outbox", "dead_letters")


def validate(report, entry, scope, evidence, fixture):
    run, ledger, result, orders, paid, expired, fulfilled, drops, failures, outcomes = case(report.get("run"))
    stage, native = report.get("paid_stage", {}), report.get("native", {})
    c = stage.get("customer", {})
    raw_counts = stage.get("financial", {}).get("counts", {})
    fresh = evidence.get("fresh_financial", {})
    counts, relations = fresh.get("counts", {}), fresh.get("relationships", {})
    elapsed = evidence.get("actual_elapsed_seconds")
    required = (
        policy.digest(report) == result, report.get("run") == run, report.get("pass") is False,
        report.get("integrity_verified") is False, report.get("restoration_complete") is True,
        report.get("transport_credentials_cleared") is True,
        entry.get("ledger") == ledger, entry.get("profile") == "cce_ticket_target",
        entry.get("status") == "RECOVERY_REQUIRED", entry.get("result_sha256") == result,
        scope.get("cce_paid_result_sha256") == result,
        entry.get("binding_sha256") == report.get("binding_sha256") == policy.digest(scope.get("binding", {})),
        scope.get("active_run") == run, scope.get("paid_runs_started") == report.get("capacity_stages_started") == 1,
        native.get("cleanup_complete") is True, native.get("cleanup_failures") == [],
        native.get("namespace_cleanup", {}).get("namespace_removed") is True,
        native.get("resource_cleanup", {}).get("owned_resources_removed") is True,
        stage.get("private_cleanup_pass") is True, stage.get("private_manifests_removed") is True,
        stage.get("generator_idle_after_stop") is True, stage.get("job_cleanup_errors") == [],
        stage.get("retired_shows") == 102, len(set(fixture.get("show_ids", []))) == 102,
        policy.digest(fixture) == stage.get("fixture_identity", {}).get("fixture_identity_sha256"),
        c.get("scheduled") == 30600, c.get("dispatched") == c.get("completed") == orders,
        c.get("fulfilled") == fulfilled, c.get("generator_drops") == drops,
        c.get("final_customer_failures") == failures,
        c.get("outcomes") == outcomes,
        stage.get("customer_outcome_gate", {}).get("pass") is False,
        stage.get("ticket_target_issuance", {}).get("pass") is (run == WRITER_RUN),
        raw_counts.get("orders") == orders, raw_counts.get("expired_orders") == expired,
        all(raw_counts.get(k) == paid for k in PAID_COUNTS),
        all(raw_counts.get(k) == 0 for k in ZERO_COUNTS),
        evidence.get("decision") == "ADR0313", evidence.get("run") == run,
        evidence.get("ledger") == ledger, evidence.get("original_result_sha256") == result,
        evidence.get("binding_sha256") == entry.get("binding_sha256"),
        evidence.get("pass") is True, evidence.get("capacity_qualified") is False,
        evidence.get("gates") == dict.fromkeys(GATES, True),
        fresh.get("pass") is True, fresh.get("hold_deadlines_elapsed") is True,
        counts.get("expected") == counts.get("orders") == orders,
        counts.get("expected_paid") == paid, counts.get("expired_orders") == expired,
        counts.get("expected_callback_deliveries_per_payment") == 1, counts.get("pass") is True,
        all(counts.get(k) == paid for k in PAID_COUNTS),
        all(counts.get(k) == 0 for k in ZERO_COUNTS),
        set(relations) == set(RELATIONSHIP_NAMES), relations.get("unique_issued_tickets") == paid,
        all(relations.get(k) == 0 for k in RELATIONSHIP_NAMES if k != "unique_issued_tickets"),
        relations == stage.get("financial", {}).get("relationships"),
        fresh.get("checks") == {"post_ttl_complete": True, "payments_durable": True, "ticket_relationships_valid": True},
        queues_valid(stage.get("global_queues", {}), 6, True),
        queues_valid(evidence.get("queues", {}), 1, False),
    )
    if (not all(required) or type(elapsed) not in (float, int)
            or not math.isfinite(elapsed) or not 0 <= elapsed <= 900):
        raise ValueError("Exact failed headroom and independently reconciled retained cohort required")
    for key in ("safety_financial", "native_safety_financial"):
        safety = evidence.get(key, {})
        if (safety.get("pass") is not True or safety.get("hold_deadlines_elapsed") is not True
                or any(safety.get(k) != 1 for k in ("orders", "fulfilled_orders", "succeeded_payments", "bookings", "tickets"))
                or safety.get("duplicate_booked_seats") != 0 or safety.get("multi_booking_orders") != 0
                or safety.get("callback_delivery_attempts") != 3 or safety.get("callback_delivery_target") != 3):
            raise ValueError("Both safety payments must reconcile independently")
    return True


def close(path):
    path = Path(path)
    run, ledger, _, *_ = case(path.parent.name)
    if (path.is_symlink() or path.stat().st_nlink != 1
            or path.resolve().parent != (policy.ROOT / "tmp" / run).resolve()):
        raise ValueError("Exact owned regular headroom recovery receipt required")
    state, records = policy.read(policy.STATE), policy.journal(policy.envelope())
    entries = [e for e in records["experiments"] if e.get("ledger") == ledger]
    if len(entries) != 1 or state.get("current_run") != run or policy.LOCK.exists():
        raise ValueError("One stopped consumed headroom recovery scope required")
    entry, scope, evidence = entries[0], state[ledger], policy.read(path)
    report = policy.read(policy.ROOT / "tmp" / run / "cce-comparison.private.json")
    fixture = policy.read(policy.ROOT / "tmp" / run / "candidate/fixture-identity.json")["fixture_identity"]
    validate(report, entry, scope, evidence, fixture)
    recovery = {"decision": "ADR0313", "evidence": path.relative_to(policy.ROOT).as_posix(),
                "sha256": policy.digest(evidence), "actual_elapsed_seconds": evidence["actual_elapsed_seconds"],
                "capacity_qualified": False}
    entry.update(initial_status=entry["status"], initial_actual_elapsed_seconds=entry["actual_elapsed_seconds"],
                 status="FAILED_RESTORED", recovery=recovery)
    entry["actual_elapsed_seconds"] += evidence["actual_elapsed_seconds"]
    scope["headroom_terminal_recovery"] = copy.deepcopy(recovery)
    scope["active_run"] = None
    state["current_run"] = None
    policy.write(policy.JOURNAL, records)
    policy.write(policy.STATE, state)
    return {"status": "FAILED_RESTORED", "original_result_preserved": True, "capacity_qualified": False}

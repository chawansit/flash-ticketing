"""ADR0235 exact hourly financial recovery; never changes the failed result."""
import copy
import inspect
import math
from pathlib import Path
from uuid import UUID

import work_envelope as policy
from worker_separation_audits import financial_body, queue_checks

RUN = "adr0151-0f735e978cba"
LEDGER = "bounded_cce_hourly_qualification__1e443fffc382"
RESULT = "f39a347987c433906b785abcbb6615a58bd720b36e413d857299a5b8968a73cb"
GATES = ("runtime_restored", "runtime_stable", "namespace_absent", "helpers_absent",
         "secondary_empty", "generator_idle", "private_inputs_removed", "owned_shows_retired")


def expectations(show_ids, expected_orders, expected_paid, callbacks):
    if (not isinstance(show_ids, list) or len(show_ids) != 1008
            or len(set(show_ids)) != 1008
            or any(not isinstance(v, str) or str(UUID(v)) != v for v in show_ids)
            or type(expected_orders) is not int or expected_orders != 302400
            or type(expected_paid) is not int or expected_paid not in {302399, 302400}
            or type(callbacks) is not int or callbacks != 1):
        raise ValueError("Exact consumed hourly terminal expectations required")


RELATIONSHIP_SQL = "\nWITH cohort AS (SELECT * FROM orders WHERE event_id=ANY(%s::uuid[])),\n scoped_bookings AS (SELECT b.* FROM bookings b WHERE b.event_id=ANY(%s::uuid[])\n                    UNION SELECT b.* FROM bookings b JOIN orders o ON o.id=b.order_id\n                    WHERE o.event_id=ANY(%s::uuid[])),\n items AS (SELECT i.order_id,count(*) AS n,sum(i.price) AS total,\n                  count(*) FILTER (WHERE i.event_id<>o.event_id) AS wrong_event\n           FROM order_items i JOIN cohort o ON o.id=i.order_id GROUP BY i.order_id),\n issued AS (SELECT b.order_id,count(t.id) AS n FROM scoped_bookings b\n            LEFT JOIN tickets t ON t.booking_id=b.id GROUP BY b.order_id)\n SELECT\n (SELECT count(*) FROM cohort o LEFT JOIN holds h ON h.id=o.hold_id\n  WHERE h.id IS NULL OR h.event_id<>o.event_id OR h.actor<>o.actor\n    OR (o.status='FULFILLED' AND h.status<>'CONSUMED')),\n (SELECT count(*) FROM scoped_bookings b LEFT JOIN cohort o ON o.id=b.order_id\n  LEFT JOIN order_items i ON i.order_id=b.order_id AND i.event_id=b.event_id AND i.seat_id=b.seat_id\n  LEFT JOIN event_seats s ON s.event_id=b.event_id AND s.seat_id=b.seat_id\n  LEFT JOIN payment_attempts p ON p.order_id=b.order_id LEFT JOIN tickets t ON t.booking_id=b.id\n  WHERE o.id IS NULL OR o.status<>'FULFILLED' OR o.event_id<>b.event_id OR i.order_id IS NULL\n     OR s.booked_order_id IS DISTINCT FROM b.order_id OR s.hold_id IS NOT NULL\n     OR p.id IS NULL OR p.status<>'SUCCEEDED' OR p.outcome<>'SUCCEEDED' OR t.id IS NULL),\n (SELECT count(*) FROM cohort o LEFT JOIN items i ON i.order_id=o.id\n  LEFT JOIN events e ON e.id=o.event_id LEFT JOIN issued t ON t.order_id=o.id\n  WHERE (o.status='FULFILLED' AND coalesce(i.n,0)<>1) OR o.total<>coalesce(i.total,0)\n    OR o.currency<>e.currency OR coalesce(i.wrong_event,0)>0\n    OR (o.status='EXPIRED' AND t.order_id IS NOT NULL)),\n (SELECT count(*) FROM (SELECT s.* FROM event_seats s WHERE s.event_id=ANY(%s::uuid[])\n                       UNION SELECT s.* FROM event_seats s JOIN orders o ON o.id=s.booked_order_id\n                       WHERE o.event_id=ANY(%s::uuid[])) s\n  WHERE s.booked_order_id IS NOT NULL AND NOT EXISTS(SELECT 1 FROM scoped_bookings b\n                   WHERE b.order_id=s.booked_order_id AND b.event_id=s.event_id AND b.seat_id=s.seat_id)),\n (SELECT count(*) FROM cohort o LEFT JOIN issued t ON t.order_id=o.id\n  WHERE o.status='FULFILLED' AND coalesce(t.n,0)<>1),\n (SELECT count(DISTINCT t.id) FROM tickets t JOIN scoped_bookings b ON b.id=t.booking_id),\n (SELECT count(*) FROM holds WHERE event_id=ANY(%s::uuid[]) AND expires_at>=clock_timestamp())\n"

def financial_program(show_ids, expected_paid):
    expectations(show_ids, 302400, expected_paid, 1)
    from worker_separation_audits import expectations as short_expectations
    body = financial_body({"show_ids": show_ids, "expected_orders": 302400,
                           "expected_paid": expected_paid, "callbacks": 1})
    original = inspect.getsource(short_expectations)
    if body.count(original) != 1:
        raise ValueError("Canonical read-only financial builder changed")
    from worker_separation_audits import RELATIONSHIP_SQL as old_sql
    marker = "RELATIONSHIP_SQL=" + repr(old_sql)
    if body.count(marker) != 1:
        raise ValueError("Canonical relationship SQL builder changed")
    statement = "        row = conn.execute(RELATIONSHIP_SQL, (show_ids,) * 6).fetchone()"
    if body.count(statement) != 1:
        raise ValueError("Canonical financial snapshot changed")
    return body.replace(original, inspect.getsource(expectations), 1).replace(
        marker, "RELATIONSHIP_SQL=" + repr(RELATIONSHIP_SQL), 1).replace(
        statement, "        conn.execute(\"SET LOCAL statement_timeout='60s'\")\n" + statement, 1)


def validate(report, entry, scope, evidence, fixture):
    elapsed = evidence.get("actual_elapsed_seconds")
    customer = report.get("paid_stage", {}).get("customer", {})
    required = (
        report.get("run") == RUN, report.get("pass") is False,
        policy.digest(report) == RESULT, entry.get("ledger") == LEDGER,
        entry.get("profile") == "cce_hourly_qualification",
        entry.get("status") == "RECOVERY_REQUIRED", entry.get("result_sha256") == RESULT,
        scope.get("cce_paid_result_sha256") == RESULT,
        report.get("capacity_stages_started") == scope.get("paid_runs_started") == 1,
        scope.get("active_run") in (None, RUN), report.get("restoration_complete") is True,
        entry.get("binding_sha256") == policy.digest(scope.get("binding")),
        evidence.get("decision") == "ADR0235", evidence.get("run") == RUN,
        evidence.get("ledger") == LEDGER, evidence.get("original_result_sha256") == RESULT,
        evidence.get("binding_sha256") == entry.get("binding_sha256"),
        evidence.get("pass") is True, evidence.get("capacity_qualified") is False,
        evidence.get("gates") == dict.fromkeys(GATES, True),
        policy.digest(fixture) == report.get("paid_stage", {}).get("fixture_identity", {}).get("fixture_identity_sha256"),
        customer.get("outcomes") == {"fulfilled": 302399, "payment_http_503": 1},
        customer.get("pass") is False, customer.get("generator_drops") == 0,
        customer.get("retry_attempts") == 0,
        all(customer.get(k) == 302400 for k in ("scheduled", "dispatched", "completed")),
        customer.get("distinct_orders") == customer.get("distinct_tickets") == 302399,
        evidence.get("restored_consumer_replicas") == 1,
        queue_checks(evidence.get("queues", {}), 1),
    )
    if (not all(required) or type(elapsed) not in (int, float)
            or not math.isfinite(elapsed) or not 0 <= elapsed <= 600):
        raise ValueError("Exact failed hourly scope and independent recovery proof required")
    financial = evidence.get("financial", {})
    counts, relationships = financial.get("counts", {}), financial.get("relationships", {})
    paid = counts.get("expected_paid")
    expectations(fixture.get("show_ids"), 302400, paid, 1)
    if (financial.get("pass") is not True or counts.get("pass") is not True
            or counts.get("orders") != 302400 or counts.get("expired_orders") != 302400 - paid
            or any(counts.get(k) != paid for k in ("fulfilled_orders", "payment_attempts", "succeeded_payments", "bookings", "tickets", "payment_callbacks"))
            or counts.get("expected") != 302400 or counts.get("expected_callback_deliveries_per_payment") != 1
            or financial.get("checks") != {"post_ttl_complete": True, "payments_durable": True, "ticket_relationships_valid": True}
            or any(counts.get(k) != 0 for k in ("pending_orders", "pending_payment_attempts",
                "incomplete_callback_deliveries", "duplicate_booked_seats", "multi_booking_orders",
                "unpublished_outbox", "dead_letters"))
            or counts.get("callback_delivery_attempts") != paid or counts.get("callback_delivery_target") != paid
            or relationships.get("unique_issued_tickets") != paid
            or any(relationships.get(k) != 0 for k in ("hold_relationship_errors",
                "booking_relationship_errors", "order_item_relationship_errors",
                "inventory_relationship_errors", "fulfilled_ticket_relationship_errors", "holds_not_past_ttl"))):
        raise ValueError("Every successful payment and expired unpaid order must reconcile")
    for key in ("safety_financial", "native_safety_financial"):
        safety = evidence.get(key, {})
        if (safety.get("pass") is not True or safety.get("hold_deadlines_elapsed") is not True
                or any(safety.get(k) != 1 for k in ("orders", "fulfilled_orders", "succeeded_payments", "bookings", "tickets"))
                or safety.get("duplicate_booked_seats") != 0 or safety.get("multi_booking_orders") != 0
                or safety.get("callback_delivery_attempts") != 3 or safety.get("callback_delivery_target") != 3):
            raise ValueError("Both original safety payments must remain durable")
    return True


def close(path):
    path = Path(path)
    if (path.is_symlink() or path.stat().st_nlink != 1
            or path.resolve().parent != (policy.ROOT / "tmp" / RUN).resolve()):
        raise ValueError("Exact owned regular recovery receipt required")
    state, records = policy.read(policy.STATE), policy.journal(policy.envelope())
    matches = [r for r in records["experiments"] if r.get("ledger") == LEDGER]
    if len(matches) != 1 or state.get("current_run") not in (None, RUN) or policy.LOCK.exists():
        raise ValueError("One stopped consumed hourly reservation required")
    report = policy.read(policy.ROOT / "tmp" / RUN / "cce-comparison.private.json")
    fixture = policy.read(policy.ROOT / "tmp" / RUN / "candidate/fixture-identity.json")["fixture_identity"]
    entry, scope, evidence = matches[0], state[LEDGER], policy.read(path)
    validate(report, entry, scope, evidence, fixture)
    entry.update(initial_status=entry["status"],
                 initial_actual_elapsed_seconds=entry["actual_elapsed_seconds"],
                 status="FAILED_RESTORED", recovery={"decision": "ADR0235",
                 "evidence": path.resolve().relative_to(policy.ROOT).as_posix(),
                 "sha256": policy.digest(evidence), "actual_elapsed_seconds": evidence["actual_elapsed_seconds"],
                 "capacity_qualified": False})
    entry["actual_elapsed_seconds"] += evidence["actual_elapsed_seconds"]
    scope["cce_hourly_terminal_recovery"] = copy.deepcopy(entry["recovery"])
    scope["active_run"] = None
    if state.get("current_run") == RUN:
        state["current_run"] = None
    policy.write(policy.JOURNAL, records)
    policy.write(policy.STATE, state)
    return {"status": entry["status"], "original_result_preserved": True,
            "paid_runs_started": 1, "capacity_qualified": False}

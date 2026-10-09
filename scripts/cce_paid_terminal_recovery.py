"""ADR0229 exact paid/refunded recovery; never qualifies capacity."""

import copy
import math
from pathlib import Path

import cce_paid_abort_recovery as aborted
import work_envelope as policy

LEDGER = "bounded_cce_paid_comparison__cfd706a87a52"
RUN = "adr0151-5db81bfa8ec2"
RESULT = "77d9869de0de950e0b427e0f67ca5dc874c335148bffe75e2073e80747fcb539"
COUNTS = {
    "orders": 25200,
    "fulfilled_orders": 2250,
    "bookings": 2250,
    "tickets": 2250,
    "expired_orders": 13312,
    "payment_attempts": 11888,
    "succeeded_payments": 11888,
    "payment_callbacks": 11888,
    "callback_delivery_attempts": 11888,
    "callback_delivery_target": 11888,
    **dict.fromkeys(
        (
            "pending_orders",
            "pending_payment_attempts",
            "incomplete_callback_deliveries",
            "duplicate_booked_seats",
            "multi_booking_orders",
            "unpublished_outbox",
            "dead_letters",
        ),
        0,
    ),
}


BRIDGE_RUN = "adr0151-73e82b8d5210"
BRIDGE_LEDGER = "bounded_cce_paid_comparison__33bd91855dcc"
BRIDGE_RESULT = "d99b0754805fe5a3df109d1fbfddde367bcf62a4e66891052773e8a835810a02"
CONTROL_RUN = "adr0151-c5edbd09ed73"
CONTROL_LEDGER = "bounded_cce_paid_comparison__6c2d3e4257b9"
CONTROL_RESULT = "323979cf0e2cfb4f40e241fd35a8e3c16fe9eb969303d312d0f20e61a7b76c0e"
DIAGNOSTIC_RUN = "adr0151-11edb3936dcc"
DIAGNOSTIC_LEDGER = "bounded_cce_paid_comparison__d17725c2400a"
DIAGNOSTIC_RESULT = "26fd430d7d3facb8b2f0294691cb9bba540626bf591d61782be8e630d9503279"
MEASUREMENT_GATES = (
    "customer_load",
    "post_ttl_financial",
    "zero_double_booking",
    "payment_durability",
    "full_queue_drain",
    "native_observer",
    "native_cpu",
    "unchanged_background",
    "unchanged_native_pods",
)


def recovery_case(run):
    if run == DIAGNOSTIC_RUN:
        counts = recovery_case(BRIDGE_RUN)[3]
        for key, value in counts.items():
            if value == 25200:
                counts[key] = 25198
        counts.update(orders=25200, expired_orders=2)
        return (
            DIAGNOSTIC_RUN,
            DIAGNOSTIC_LEDGER,
            DIAGNOSTIC_RESULT,
            counts,
            {"fulfilled": 25178, "payment_http_503": 2, "order_http_503": 20},
            {"FULFILLED": 25198, "EXPIRED": 2},
            {},
        )
    if run == CONTROL_RUN:
        counts = recovery_case(BRIDGE_RUN)[3]
        for key, value in counts.items():
            if value == 25200:
                counts[key] = 25199
        counts.update(orders=25200, expired_orders=1)
        return (
            CONTROL_RUN,
            CONTROL_LEDGER,
            CONTROL_RESULT,
            counts,
            {"fulfilled": 25198, "payment_http_503": 1, "order_http_503": 1},
            {"FULFILLED": 25199, "EXPIRED": 1},
            {},
        )
    if run == BRIDGE_RUN:
        counts = {
            key: (
                25200
                if key
                in {
                    "orders",
                    "fulfilled_orders",
                    "bookings",
                    "tickets",
                    "payment_attempts",
                    "succeeded_payments",
                    "payment_callbacks",
                    "callback_delivery_attempts",
                    "callback_delivery_target",
                }
                else 0
            )
            for key in COUNTS
        }
        return (
            BRIDGE_RUN,
            BRIDGE_LEDGER,
            BRIDGE_RESULT,
            counts,
            {"fulfilled": 25200},
            {"FULFILLED": 25200},
            {},
        )
    return (
        RUN,
        LEDGER,
        RESULT,
        COUNTS,
        {"fulfilled": 92, "payment_http_503": 13312, "order_http_503": 11639, "ticket_timeout": 157},
        {"EXPIRED": 13312, "FULFILLED": 2250, "REFUNDED": 9638},
        {"REFUNDED": 9638},
    )


def validate(report, entry, scope, evidence, fixture):
    run, ledger, result, expected_counts, outcomes, order_states, refund_states = recovery_case(
        report.get("run")
    )
    required = (
        entry.get("ledger") == ledger,
        entry.get("profile") == "cce_paid_comparison",
        entry.get("status") == "RECOVERY_REQUIRED",
        entry.get("result_sha256") == result,
        policy.digest(report) == result,
        scope.get("cce_paid_result_sha256") == result,
        report.get("run") == run,
        report.get("pass") is False,
        report.get("capacity_stages_started") == scope.get("paid_runs_started") == 1,
        scope.get("active_run") in (None, run),
        report.get("restoration_complete") is True,
        entry.get("binding_sha256") == policy.digest(scope.get("binding")),
        evidence.get("original_result_sha256") == result,
        evidence.get("binding_sha256") == entry.get("binding_sha256"),
        evidence.get("ledger") == ledger,
        evidence.get("run") == run,
        evidence.get("namespace") == aborted.namespace_for(run),
        evidence.get("pass") is True,
        evidence.get("original_paid_result_preserved") is True,
        evidence.get("paid_scope_consumed") is True,
        evidence.get("gates") == dict.fromkeys(aborted.GATES, True),
        evidence.get("owned_fixture_sale_windows_closed") is True,
        evidence.get("verified_owned_events") == 86,
        aborted.queue_checks(evidence.get("queue_counts", {}), 1),
        len(set(fixture.get("show_ids", []))) == 84,
        policy.digest(fixture)
        == report.get("paid_stage", {}).get("fixture_identity", {}).get("fixture_identity_sha256"),
    )
    elapsed = evidence.get("actual_elapsed_seconds")
    if (
        not all(required)
        or type(elapsed) not in (int, float)
        or not math.isfinite(elapsed)
        or not 0 <= elapsed <= 600
    ):
        raise ValueError("Exact consumed failed paid scope and independent recovery required")
    c = report["paid_stage"]["customer"]
    if (
        any(c.get(k) != 25200 for k in ("scheduled", "dispatched", "completed"))
        or c.get("generator_drops") != 0
        or c.get("retry_attempts") != 0
        or c.get("outcomes") != outcomes
    ):
        raise ValueError("Original failed customer outcomes required")
    if run == BRIDGE_RUN and (
        report.get("integrity_verified") is not True
        or report.get("native", {}).get("measurement_gates") != dict.fromkeys(MEASUREMENT_GATES, True)
        or report.get("native", {}).get("cleanup_failures")
        != [{"operation": "candidate_restore", "type": "ValueError"}]
        or c.get("pass") is not True
        or c.get("distinct_orders") != 25200
        or c.get("distinct_tickets") != 25200
    ):
        raise ValueError("Exact measured bridge case and isolated intermediate recovery failure required")
    if run in {CONTROL_RUN, DIAGNOSTIC_RUN} and (
        report.get("integrity_verified") is not False
        or report.get("native", {}).get("measurement_gates")
        != {
            key: key
            not in {"customer_load", "post_ttl_financial", "zero_double_booking", "payment_durability"}
            for key in MEASUREMENT_GATES
        }
        or report.get("native", {}).get("cleanup_failures") != []
        or c.get("pass") is not False
        or c.get("distinct_orders") != outcomes["fulfilled"]
        or c.get("distinct_tickets") != outcomes["fulfilled"]
    ):
        raise ValueError("Exact failed control and successful restoration required")
    t = evidence.get("paid_terminal_financial", {})
    counts = t.get("counts", {})
    if (
        t.get("pass") is not True
        or t.get("hold_deadlines_elapsed") is not True
        or t.get("order_states") != order_states
        or t.get("refund_states") != refund_states
        or any(
            t.get(k) != 0
            for k in (
                "unaccounted_succeeded_payments",
                "invalid_fulfilled_relationships",
                "invalid_refunded_relationships",
            )
        )
        or any(counts.get(k) != v for k, v in expected_counts.items())
    ):
        raise ValueError("Every payment must reconcile to one ticket or durable simulated refund")
    for key in ("financial", "native_safety_financial"):
        f = evidence.get(key, {})
        if (
            f.get("pass") is not True
            or f.get("hold_deadlines_elapsed") is not True
            or any(
                f.get(k) != 1
                for k in ("orders", "fulfilled_orders", "succeeded_payments", "bookings", "tickets")
            )
            or any(f.get(k) != 0 for k in ("duplicate_booked_seats", "multi_booking_orders"))
            or not f.get("callback_delivery_attempts") == f.get("callback_delivery_target") == 3
        ):
            raise ValueError("Both safety payments must remain durable")
    return True


def close(path):
    path = Path(path)
    run, ledger, _result, *_ = recovery_case(path.parent.name)
    if (
        path.is_symlink()
        or path.stat().st_nlink != 1
        or path.resolve().parent != (policy.ROOT / "tmp" / run).resolve()
    ):
        raise ValueError("Exact owned regular evidence required")
    state, records = policy.read(policy.STATE), policy.journal(policy.envelope())
    matches = [r for r in records["experiments"] if r.get("ledger") == ledger]
    if len(matches) != 1 or state.get("current_run") not in (None, run) or policy.LOCK.exists():
        raise ValueError("One stopped reservation required")
    entry, scope = matches[0], state[ledger]
    report = policy.read(policy.ROOT / "tmp" / run / "cce-comparison.private.json")
    evidence = policy.read(path)
    fixture = policy.read(policy.ROOT / "tmp" / run / "candidate/fixture-identity.json")["fixture_identity"]
    validate(report, entry, scope, evidence, fixture)
    entry.update(
        initial_status=entry["status"],
        initial_actual_elapsed_seconds=entry["actual_elapsed_seconds"],
        status="FAILED_RESTORED",
        recovery={
            "decision": "ADR0229",
            "evidence": path.resolve().relative_to(policy.ROOT).as_posix(),
            "sha256": policy.digest(evidence),
            "actual_elapsed_seconds": evidence["actual_elapsed_seconds"],
            "capacity_qualified": False,
        },
    )
    entry["actual_elapsed_seconds"] += evidence["actual_elapsed_seconds"]
    scope["cce_paid_terminal_recovery"] = copy.deepcopy(entry["recovery"])
    scope["active_run"] = None
    if state.get("current_run") == run:
        state["current_run"] = None
    policy.write(policy.JOURNAL, records)
    policy.write(policy.STATE, state)
    return {
        "status": entry["status"],
        "paid_runs_started": 1,
        "original_result_preserved": True,
        "capacity_qualified": False,
    }

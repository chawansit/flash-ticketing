"""ADR0228 exact pre-dispatch abort closure; does not replay or pass the experiment."""

import copy
import math

import work_envelope as policy
from cce_dependency_probe import namespace_for
from worker_separation_audits import queue_checks

LEDGER = "bounded_cce_paid_comparison__5f2031ff5a64"
RUN = "adr0151-98586075f718"
RESULT = "51e7d5a5fb5006a57cfa3aa1248c4024c014ff552399407f697c9732c7515e43"
CASES = {
    "bounded_cce_paid_comparison__90a4d6b3c702": (
        "adr0151-5ef264b39180",
        "d9dba3b58aa724f68e7f752b0fcb3821b33e299af3db4bfd7e3a39c450b44c6b",
    ),
    "bounded_cce_paid_comparison__0343d38b322a": (
        "adr0151-687c7f581371",
        "1c6fdad2b38f6d05ef994d26088281c19757a5c914c5e830dd943a81aa79dd4b",
    ),
    "bounded_cce_paid_comparison__3dcaff0ecfd1": (
        "adr0151-75ccf7395822",
        "38ec032c5bb429c83d039b9a66982c53bcfee2a9ea338f57d8df7d5e26cee16d",
    ),
    "bounded_cce_paid_comparison__6000678b67e2": (
        "adr0151-ff50e8d4005b",
        "784fc1a017b4a6f14d022a7c03bb3dcaa1f258bd5e58d389093080f9a853f2f5",
    ),
    "bounded_cce_paid_comparison__4852f25a20b6": (
        "adr0151-7f92bec3c3d5",
        "fd000b3036dfef2a3efad7f14687a5f692a4a063612cfb44623593c40463ebd1",
    ),
    "bounded_cce_paid_comparison__98227cf43a6b": (
        "adr0151-c95a1c214e51",
        "db1f27fe417164be4798d4ebcf7faacce3b0d0a2f994b9efab01ddb703fefc9c",
    ),
    "bounded_cce_paid_comparison__b7a481e968a9": (
        "adr0151-6b8f809a403c",
        "ca8a98883a40a7ad356d10bc559a72dc168775d57717d44727b8c3e966cf8208",
    ),
}


def case_identity(ledger):
    if ledger == LEDGER:
        return RUN, RESULT
    if ledger not in CASES:
        raise ValueError("Unknown stopped CCE attempt")
    return CASES[ledger]


GATES = (
    "saved_runtime_semantics_restored",
    "runtime_ids_starts_stable",
    "namespace_absent",
    "owned_helpers_absent",
    "secondary_empty",
    "generator_idle",
    "safety_payment_durable",
    "zero_double_booking",
    "all_queues_zero",
    "temporary_transport_credentials_cleared",
)


def validate(report, entry, scope, evidence):
    ledger = entry.get("ledger")
    run, result = case_identity(ledger)
    financial = evidence.get("financial", {})
    if ledger in {"bounded_cce_paid_comparison__98227cf43a6b", "bounded_cce_paid_comparison__4852f25a20b6", "bounded_cce_paid_comparison__6000678b67e2", "bounded_cce_paid_comparison__3dcaff0ecfd1", "bounded_cce_paid_comparison__0343d38b322a", "bounded_cce_paid_comparison__90a4d6b3c702"}:
        native = evidence.get("native_safety_financial", {})
        cleanup = evidence.get("native_fixture_cleanup", {})
        if (
            native.get("pass") is not True
            or native.get("hold_deadlines_elapsed") is not True
            or any(native.get(k) != 1 for k in ("orders", "fulfilled_orders", "succeeded_payments", "bookings", "tickets"))
            or native.get("duplicate_booked_seats") != 0
            or native.get("multi_booking_orders") != 0
            or not (native.get("callback_delivery_attempts") == native.get("callback_delivery_target") == 3)
            or cleanup.get("safety_retired") is not True
            or cleanup.get("paid_retired") is not True
            or cleanup.get("cleanup_errors") != []
            or cleanup.get("unknown_fixture_identity")
        ):
            raise ValueError("Native safety payment and both fixture retirements required")
    if (
        entry.get("ledger") != ledger
        or entry.get("profile") != "cce_paid_comparison"
        or entry.get("status") != "RECOVERY_REQUIRED"
        or entry.get("result_sha256") != result
        or policy.digest(report) != result
        or scope.get("cce_paid_result_sha256") != result
        or report.get("run") != run
        or report.get("pass") is not False
        or report.get("capacity_stages_started") != 0
        or scope.get("paid_runs_started") != 0
        or scope.get("active_run")
        or report.get("restoration_complete") is not True
        or report.get("integrity_verified") is not True
        or entry.get("binding_sha256") != policy.digest(scope.get("binding"))
        or evidence.get("decision") != "ADR0228"
        or evidence.get("ledger") != ledger
        or evidence.get("run") != run
        or evidence.get("namespace") != namespace_for(run)
        or evidence.get("original_result_sha256") != result
        or evidence.get("binding_sha256") != entry["binding_sha256"]
        or evidence.get("pass") is not True
        or set(evidence.get("gates", {})) != set(GATES)
        or any(evidence["gates"][k] is not True for k in GATES)
        or not queue_checks(evidence.get("queue_counts", {}), 1)
        or financial.get("pass") is not True
        or financial.get("hold_deadlines_elapsed") is not True
        or any(
            financial.get(k) != 1
            for k in ("orders", "fulfilled_orders", "succeeded_payments", "bookings", "tickets")
        )
        or not (financial.get("callback_delivery_attempts") == financial.get("callback_delivery_target") == 3)
        or any(
            financial.get(k) != 0
            for k in (
                "pending_orders",
                "expired_orders",
                "pending_payment_attempts",
                "duplicate_booked_seats",
                "multi_booking_orders",
                "unpublished_outbox",
                "dead_letters",
                "incomplete_callback_deliveries",
            )
        )
        or type(evidence.get("actual_elapsed_seconds")) not in (float, int)
        or not math.isfinite(evidence["actual_elapsed_seconds"])
        or evidence["actual_elapsed_seconds"] < 0
    ):
        raise ValueError("Exact original abort and complete independent recovery proof required")
    return True


def close(path):
    path = path.resolve()
    evidence = policy.read(path)
    ledger = evidence.get("ledger")
    run, _ = case_identity(ledger)
    if not path.is_relative_to((policy.ROOT / "tmp" / run).resolve()) or path.is_symlink():
        raise ValueError("Owned recovery proof required")
    state = policy.read(policy.STATE)
    records = policy.journal(policy.envelope())
    matches = [r for r in records["experiments"] if r["ledger"] == ledger]
    if len(matches) != 1 or state.get("current_run"):
        raise ValueError("One stopped exact reservation required")
    entry = matches[0]
    scope = state[ledger]
    report = policy.read(policy.ROOT / "tmp" / run / "cce-comparison.private.json")
    evidence = policy.read(path)
    validate(report, entry, scope, evidence)
    entry.update(
        initial_status=entry["status"],
        initial_actual_elapsed_seconds=entry["actual_elapsed_seconds"],
        status="FAILED_RESTORED",
        recovery={
            "decision": "ADR0228",
            "evidence": path.relative_to(policy.ROOT).as_posix(),
            "sha256": policy.digest(evidence),
            "actual_elapsed_seconds": evidence["actual_elapsed_seconds"],
        },
    )
    entry["actual_elapsed_seconds"] += evidence["actual_elapsed_seconds"]
    scope["cce_paid_abort_recovery"] = copy.deepcopy(entry["recovery"])
    policy.write(policy.JOURNAL, records)
    policy.write(policy.STATE, state)
    return {"status": entry["status"], "original_result_preserved": True, "paid_runs_started": 0}

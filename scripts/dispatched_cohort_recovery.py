"""ADR0221 append-only recovery of immutable, explicitly allowlisted paid failures."""
import hashlib
import math
import os
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from types import MappingProxyType

from safety_diagnostic_abort_recovery import queue_pass, read, regular
from work_envelope import digest

KEY = "bounded_shared_callback_rate_probe__aea04bbd86e2"
RUN = "adr0151-ee1f1f1716f5"
ARM = "adr0151-arm-a8dd48533823"
EXPECTED = 24331


@dataclass(frozen=True)
class RecoveryCase:
    key: str
    run: str
    arm: str
    expected: int
    drops: int
    profile: str
    decision: str


CASES = MappingProxyType({
    KEY: RecoveryCase(KEY, RUN, ARM, EXPECTED, 869, "shared_callback_rate_probe", "ADR0219"),
    "bounded_interleaved_refresh_probe__380556ea4230": RecoveryCase(
        "bounded_interleaved_refresh_probe__380556ea4230", "adr0151-cac068bf517e",
        "adr0151-arm-1189895a697f", 25168, 32, "interleaved_refresh_probe", "ADR0222"),
    "bounded_orders_event_index_probe__c9a9ead76625": RecoveryCase(
        "bounded_orders_event_index_probe__c9a9ead76625", "adr0151-bb1154096e30",
        "adr0151-arm-a977076bad22", 24423, 777, "orders_event_index_probe", "ADR0224"),
    "bounded_writer_write_pipeline_probe__eb7ff590f16c": RecoveryCase(
        "bounded_writer_write_pipeline_probe__eb7ff590f16c", "adr0151-4988723f04a7",
        "adr0151-arm-3000e8c3419a", 25196, 4, "writer_write_pipeline_probe", "ADR0225"),
})


def case_for(entry):
    case = CASES.get(entry.get("ledger"))
    if case is None:
        raise ValueError("Only an immutable allowlisted paid failure may be classified")
    return case
GATES = ("dispatched_cohort_durable", "post_ttl_relationships", "zero_double_booking", "full_queue_drain",
         "exact_runtime_restored", "generator_idle", "owned_cleanup", "original_failure_preserved")


def retained(root, entry, state):
    case = case_for(entry)
    root = Path(root).resolve()
    scope = state.get(case.key, {})
    if (entry.get("ledger") != case.key or entry.get("profile") != case.profile
            or entry.get("status") != "RECOVERY_REQUIRED" or state.get("current_run") or scope.get("active_run")
            or scope.get("paid_runs_started") != 1 or scope.get("paid_protocols_started") != 1
            or digest(scope.get("binding")) != entry.get("binding_sha256")
            or len(entry.get("reports", [])) != 2 or entry["reports"][0].get("pass") is not True
            or entry["reports"][1].get("run") != case.run or entry["reports"][1].get("pass") is not False):
        raise ValueError("Exact consumed failed paid scope required")
    owned = root / "tmp" / case.run
    directory = owned / case.arm
    paths = [owned / "comparison-summary.json", directory / "state.json",
             directory / "candidate/stage.private.json", directory / "candidate/fixture-identity.json"]
    report, saved, stage, fixture = [read(owned, p) for p in paths]
    arm = report.get("arms", {}).get("candidate", {})
    customer, financial = stage.get("customer", {}), stage.get("financial", {})
    if (report.get("pass") is not False or report.get("experiment_decision") != case.decision
            or report.get("run") != case.run or set(report.get("arms", {})) != {"candidate"}
            or digest(report.get("binding")) != entry["binding_sha256"]
            or arm.get("restoration_complete") is not True or arm.get("capacity_stages_started") != 1
            or stage.get("customers_dispatched") is not True or stage.get("retired_shows") != 84
            or customer.get("scheduled") != 25200 or customer.get("generator_drops") != case.drops
            or any(customer.get(k) != case.expected for k in ("dispatched", "completed", "fulfilled", "distinct_orders", "distinct_tickets"))
            or customer.get("retry_attempts") != 0 or customer.get("outcomes") != {"fulfilled": case.expected}
            or financial.get("pass") is not False or financial.get("expected") != 25200
            or financial.get("expected_paid") != 25200 or financial.get("hold_deadlines_elapsed") is not True
            or any(financial.get(k) != case.expected for k in ("orders", "fulfilled_orders", "succeeded_payments", "bookings", "tickets"))
            or any(financial.get(k) != 0 for k in ("pending_orders", "expired_orders", "pending_payment_attempts",
                "incomplete_callback_deliveries", "duplicate_booked_seats", "multi_booking_orders", "unpublished_outbox", "dead_letters"))
            or any(saved.get(k) is not True for k in ("restore_pass", "primary_runtime_semantics_restored",
                "secondary_resources_removed", "generator_idle_after", "credential_snapshots_removed"))
            or saved.get("primary_api_count") != 4 or not queue_pass(saved.get("restored_global_queues", {}))
            or stage.get("private_cleanup_pass") is not True or stage.get("diagnostic_credentials_removed") is not True
            or stage.get("cleanup_errors") != [] or fixture != stage.get("fixture_identity")
            or fixture.get("fixture_identity_sha256") != digest(fixture.get("fixture_identity"))
            or fixture.get("fixture_identity", {}).get("shows") != 84
            or len(set(fixture.get("fixture_identity", {}).get("show_ids", []))) != 84):
        raise ValueError("Retained paid failure, exact fixture and restoration proof required")
    if case.decision in {"ADR0224", "ADR0225"}:
        from types import SimpleNamespace

        from orders_event_index_probe_contract import OrdersEventIndexProbeContract

        proof = SimpleNamespace(index_before=saved.get("orders_event_index_before"),
                                index_after=saved.get("orders_event_index_after"))
        if not OrdersEventIndexProbeContract.index_verified(proof):
            raise ValueError("Exact persistent index proof required for index correction recovery")
    return paths, report, fixture


def verify(root, entry, state, fresh_path, *, now=None):
    case = case_for(entry)
    paths, report, fixture = retained(root, entry, state)
    fresh_path = regular(Path(root) / "tmp", fresh_path)
    fresh = read(Path(root) / "tmp", fresh_path)
    at = datetime.fromisoformat(fresh.get("verified_at_utc", ""))
    now = now or datetime.now(UTC)
    financial = fresh.get("fixture_financial_audit", {})
    counts, relationships = financial.get("counts", {}), financial.get("relationships", {})
    if (at.tzinfo is None or not 0 <= (now - at).total_seconds() <= 120
            or fresh.get("ledger") != case.key or fresh.get("binding_sha256") != entry["binding_sha256"]
            or fresh.get("original_result_sha256") != digest(report)
            or fresh.get("fixture_identity_sha256") != fixture["fixture_identity_sha256"]
            or fresh.get("new_paid_runs_started") != 0
            or type(fresh.get("actual_elapsed_seconds")) not in {int, float}
            or not math.isfinite(fresh["actual_elapsed_seconds"]) or not 0 <= fresh["actual_elapsed_seconds"] <= 600
            or any(fresh.get(k) is not True for k in ("pass", "runtime_unchanged", "original_runtime_restored",
                "zero_double_booking", "all_queues_zero", "kafka_drained", "generator_idle", "original_failed_gate_preserved"))
            or not queue_pass(fresh.get("queue_counts", {}))
            or financial.get("pass") is not True or not financial.get("checks") or not all(financial["checks"].values())
            or counts.get("pass") is not True or counts.get("expected") != case.expected or counts.get("expected_paid") != case.expected
            or any(counts.get(k) != case.expected for k in ("orders", "fulfilled_orders", "succeeded_payments", "bookings", "tickets"))
            or any(counts.get(k) != 0 for k in ("duplicate_booked_seats", "multi_booking_orders", "pending_orders",
                "pending_payment_attempts", "incomplete_callback_deliveries", "unpublished_outbox", "dead_letters"))
            or financial.get("fixture_hold_rows") != case.expected
            or relationships.get("unique_issued_tickets") != case.expected or relationships.get("holds_not_past_ttl") != 0
            or any(relationships.get(k) != 0 for k in ("hold_relationship_errors", "booking_relationship_errors",
                "order_item_relationship_errors", "inventory_relationship_errors", "fulfilled_ticket_relationship_errors"))):
        raise ValueError("Fresh independent financial, relationship and runtime verification required")
    if case.decision in {"ADR0224", "ADR0225"}:
        from types import SimpleNamespace

        from orders_event_index_probe_contract import OrdersEventIndexProbeContract

        index = fresh.get("orders_event_index")
        proof = SimpleNamespace(index_before=index, index_after=index)
        saved = read(Path(root) / "tmp" / case.run, Path(root) / "tmp" / case.run / case.arm / "state.json")
        if (not OrdersEventIndexProbeContract.index_verified(proof)
                or index["index"] != saved["orders_event_index_after"]["index"]):
            raise ValueError("Fresh identical persistent index proof required")
    paths.append(fresh_path)
    return {"decision": "ADR0221", "ledger": case.key, "binding_sha256": entry["binding_sha256"],
            "verified_at_utc": at.isoformat(), "gates": dict.fromkeys(GATES, True),
            "scheduled": 25200, "dispatched_paid_tickets": case.expected, "undispatched": case.drops,
            "original_experiment_pass": False, "capacity_qualified": False,
            "verification_elapsed_seconds": fresh["actual_elapsed_seconds"],
            "retained_artifact_sha256": {p.relative_to(Path(root)).as_posix(): hashlib.sha256(p.read_bytes()).hexdigest() for p in paths},
            "scope": "Only recovery cleared; original throughput, latency and diagnostic gates remain failed. Fresh experiment identity required."}


def resolved(records, entry, root):
    item = records.get("verified_paid_recoveries", {}).get(entry.get("ledger"))
    try:
        case = case_for(entry)
        if entry.get("ledger") != case.key or not isinstance(item, dict) or item.get("entry_sha256") != digest(entry): return False
        path = regular(Path(root) / "docs/capacity/flash-sale-opening", Path(root) / item["receipt_path"])
        receipt = read(path.parent, path)
        if (hashlib.sha256(path.read_bytes()).hexdigest() != item["receipt_sha256"]
                or receipt.get("decision") != "ADR0221" or receipt.get("ledger") != case.key
                or receipt.get("binding_sha256") != entry["binding_sha256"] or receipt.get("gates") != dict.fromkeys(GATES, True)
                or receipt.get("original_experiment_pass") is not False or receipt.get("capacity_qualified") is not False
                or receipt.get("dispatched_paid_tickets") != case.expected or receipt.get("undispatched") != case.drops
                or len(receipt.get("retained_artifact_sha256", {})) != 5): return False
        return all(hashlib.sha256(regular(Path(root) / "tmp", Path(root) / name).read_bytes()).hexdigest() == value
                   for name, value in receipt["retained_artifact_sha256"].items())
    except (OSError, ValueError, TypeError, KeyError):
        return False


def append_receipt(fresh_path, receipt_path, *, ledger=KEY):
    import work_envelope as policy
    if not policy.LOCK.exists() or policy.read(policy.LOCK).get("pid") != os.getpid() or policy.PAUSE.exists():
        raise ValueError("Owned recovery lock required")
    state, records = policy.read(policy.STATE), policy.journal(policy.envelope())
    if records["human_pause"] or state.get("human_pause") or state.get("current_run"):
        raise ValueError("Pause or active run blocks classification")
    entry = next(e for e in records["experiments"] if e["ledger"] == ledger)
    case = case_for(entry)
    if case.key in records.get("verified_paid_recoveries", {}): raise ValueError("Recovery already recorded")
    receipt = verify(policy.ROOT, entry, state, fresh_path)
    path = Path(receipt_path).absolute()
    if path.parent != policy.ROOT / "docs/capacity/flash-sale-opening" or path.exists():
        raise ValueError("Fresh public recovery receipt required")
    policy.write(path, receipt)
    records.setdefault("verified_paid_recoveries", {})[case.key] = {"entry_sha256": digest(entry),
        "receipt_path": path.relative_to(policy.ROOT).as_posix(), "receipt_sha256": hashlib.sha256(path.read_bytes()).hexdigest()}
    records.setdefault("recovery_verifications", []).append({"ledger": case.key, "evidence": path.relative_to(policy.ROOT).as_posix(),
        "actual_elapsed_seconds": receipt["verification_elapsed_seconds"]})
    policy.write(policy.JOURNAL, records)
    return receipt

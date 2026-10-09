"""ADR0218 verified safety-only cleanup; preserves failed experiments and gates."""
import hashlib
import json
import os
import re
from datetime import UTC, datetime
from pathlib import Path

from work_envelope import digest

GATES = ("zero_paid_dispatch", "safety_financial_durable", "zero_double_booking",
         "full_queue_drain", "exact_runtime_restored", "owned_resources_removed", "credentials_removed")
WRITER_ABORT = ("bounded_writer_write_pipeline_probe__bd6a3461abd9", "adr0151-3a1e553acc1c",
                "adr0151-arm-4c250bd06e90", "a13e8bc95695726e6193238edc0acc921c1a75ee9b6d317b34660895827bf58b")
QUEUE_KEYS = ("unpublished_outbox", "pending_refresh", "dead_letters", "pending_callback_deliveries",
              "pending_refunds", "reservation_stream_entries", "reservation_stream_pending", "kafka_total_lag",
              "pending_confirmation_receipts", "review_confirmation_receipts", "confirmation_capacity_outstanding",
              "confirmation_capacity_mismatches")


def regular(root, path, maximum=2 * 1024 * 1024):
    root, path = Path(root).resolve(), Path(path).absolute()
    if not path.is_relative_to(root) or path != path.resolve() or not path.is_file() or path.stat().st_size > maximum:
        raise ValueError("Exact bounded regular recovery artifact required")
    return path


def read(root, path):
    return json.loads(regular(root, path).read_text(encoding="utf-8"))


def queue_pass(value):
    return (value.get("pass") is True and all(type(value.get(k)) is int and value[k] == 0 for k in QUEUE_KEYS)
            and type(value.get("kafka_members")) is int and value["kafka_members"] == 1)


def verify(root, entry, state):
    root = Path(root).resolve()
    key = entry.get("ledger", "")
    scope = state.get(key, {})
    writer_case = key == WRITER_ABORT[0]
    profile = "writer_write_pipeline_probe" if writer_case else "shared_callback_placement"
    decision = "ADR0225" if writer_case else "ADR0217"
    expected_arms = {"candidate"} if writer_case else {"control", "candidate"}
    if (entry.get("profile") != profile or entry.get("status") != "RECOVERY_REQUIRED"
            or (not writer_case and not re.fullmatch(r"bounded_shared_callback_placement__[0-9a-f]{12}", key))
            or len(entry.get("reports", [])) != 1 or entry["reports"][0].get("pass") is not False
            or state.get("current_run") or scope.get("active_run")
            or scope.get("paid_runs_started") != 0 or scope.get("paid_protocols_started") != 0
            or digest(scope.get("binding")) != entry.get("binding_sha256")):
        raise ValueError("Exact consumed safety-only scope required")
    run = entry["reports"][0]["run"]
    if not re.fullmatch(r"adr0151-[0-9a-f]{12}", run): raise ValueError("Owned report identity required")
    if writer_case and (run != WRITER_ABORT[1] or entry["binding_sha256"] != WRITER_ABORT[3]):
        raise ValueError("Exact known writer safety abort identity required")
    owned = root / "tmp" / run
    path = regular(owned, owned / "comparison-summary.json")
    report = read(owned, path)
    if (report.get("run") != run or report.get("experiment_decision") != decision
            or report.get("pass") is not False or report.get("capacity_stages_started") != 0
            or set(report.get("arms", {})) != expected_arms
            or digest(report.get("binding")) != entry["binding_sha256"]):
        raise ValueError("Exact retained failed safety report required")
    artifacts = [path]
    for arm, outcome in report["arms"].items():
        directory = Path(outcome["evidence_directory"])
        if not directory.is_absolute(): directory = root / directory
        directory = directory.absolute()
        if (directory.parent != owned or not re.fullmatch(r"adr0151-arm-[0-9a-f]{12}", directory.name)
                or outcome.get("restoration_complete") is not True or outcome.get("pre_safety_source_pass") is not True
                or outcome.get("capacity_stages_started") != 0):
            raise ValueError("Owned restored safety arm required")
        sp, tp = directory / "state.json", directory / arm / "stage.private.json"
        saved, stage = read(owned, sp), read(owned, tp)
        artifacts.extend((sp, tp))
        if writer_case:
            from orders_event_index_probe_contract import OrdersEventIndexProbeContract
            proof = object.__new__(OrdersEventIndexProbeContract)
            proof.index_before = saved.get("orders_event_index_before")
            proof.index_after = saved.get("orders_event_index_after")
            if (directory.name != WRITER_ABORT[2]
                    or stage.get("admission_failure_capture") != {"complete": False, "failure_type": "UnboundLocalError"}
                    or not proof.index_verified() or proof.index_before.get("verification_only") is not True):
                raise ValueError("Exact missing diagnostic selector and unchanged read-only index required")

        flags = ("pass", "restore_pass", "qualification_checks_pass", "secondary_resources_removed",
                 "confirmation_worker_removed_after_drain", "primary_runtime_semantics_restored",
                 "generator_idle_after", "credential_snapshots_removed", "candidate_pre_safety_sources_verified", "frozen_audit_harness_match")
        safety, financial = saved.get("safety", {}), saved.get("post_ttl_financial", {})
        if (any(saved.get(k) is not True for k in flags) or saved.get("primary_api_count") != 4
                or saved.get("probe_execution", {}).get("returncode") != 0
                or safety.get("pass") is not True or safety.get("accepted_holds") != 1
                or safety.get("wave_statuses") != {"202": 1, "409": 99}
                or any(safety.get(k) is not True for k in ("customer_ticket_confirmed", "other_actor_denied",
                                                         "cross_host_hold_replay", "cross_host_payment_replay"))
                or financial.get("pass") is not True or financial.get("hold_deadlines_elapsed") is not True
                or any(type(financial.get(k)) is not int or financial[k] != 1 for k in ("orders", "fulfilled_orders", "succeeded_payments", "bookings", "tickets"))
                or any(type(financial.get(k)) is not int or financial[k] != 0 for k in ("duplicate_booked_seats", "multi_booking_orders", "pending_orders", "pending_payment_attempts", "incomplete_callback_deliveries", "dead_letters", "unpublished_outbox"))
                or not queue_pass(saved.get("restored_global_queues", {}))
                or stage.get("customers_dispatched") is not False or stage.get("pre_dispatch_qualified") is not True
                or stage.get("private_cleanup_pass") is not True or stage.get("diagnostic_credentials_removed") is not True
                or stage.get("cleanup_errors") != [] or stage.get("failure_type") is not None):
            raise ValueError("Financial, no-dispatch, cleanup and restoration proof required")
    return {"decision": "ADR0218", "ledger": key, "binding_sha256": entry["binding_sha256"],
            "verified_at_utc": datetime.now(UTC).isoformat(), "gates": dict.fromkeys(GATES, True),
            "retained_artifact_sha256": {p.relative_to(root).as_posix(): hashlib.sha256(regular(owned, p).read_bytes()).hexdigest() for p in artifacts},
            "scope": "Safety-only diagnostic abort restored; original failed qualification retained. Fresh full safety qualification required; no capacity claim."}


def resolved(records, entry, root):
    item = records.get("verified_safety_aborts", {}).get(entry.get("ledger"))
    try:
        if not isinstance(item, dict) or set(item) != {"entry_sha256", "receipt_path", "receipt_sha256"} or item["entry_sha256"] != digest(entry): return False
        public_root = Path(root).resolve() / "docs/capacity/flash-sale-opening"
        path = regular(public_root, Path(root) / item["receipt_path"])
        receipt = read(public_root, path)
        if (hashlib.sha256(path.read_bytes()).hexdigest() != item["receipt_sha256"]
                or receipt.get("decision") != "ADR0218" or receipt.get("ledger") != entry["ledger"]
                or receipt.get("binding_sha256") != entry["binding_sha256"] or receipt.get("gates") != dict.fromkeys(GATES, True)
                or len(receipt.get("retained_artifact_sha256", {})) != (3 if entry["ledger"] == WRITER_ABORT[0] else 5)): return False
        owned = Path(root).resolve() / "tmp" / entry["reports"][0]["run"]
        return all(hashlib.sha256(regular(owned, Path(root) / name).read_bytes()).hexdigest() == fingerprint
                   for name, fingerprint in receipt["retained_artifact_sha256"].items())
    except (OSError, ValueError, TypeError, KeyError):
        return False


def append_receipt(key, receipt_path):
    import work_envelope as policy
    if (not policy.LOCK.exists() or policy.read(policy.LOCK).get("pid") != os.getpid() or policy.PAUSE.exists()):
        raise ValueError("Owned recovery lock required")
    data, state = policy.envelope(), policy.read(policy.STATE)
    records = policy.journal(data)
    if records["human_pause"] or state.get("human_pause") or state.get("current_run"):
        raise ValueError("Pause or active run blocks recovery classification")
    entry = next(e for e in records["experiments"] if e["ledger"] == key)
    if key in records.get("verified_safety_aborts", {}): raise ValueError("Recovery receipt already recorded")
    receipt = verify(policy.ROOT, entry, state)
    path = Path(receipt_path).absolute()
    allowed = policy.ROOT / "docs/capacity/flash-sale-opening"
    if path.parent != allowed or path.exists(): raise ValueError("Fresh approved public receipt required")
    policy.write(path, receipt)
    records.setdefault("verified_safety_aborts", {})[key] = {"entry_sha256": digest(entry),
        "receipt_path": path.relative_to(policy.ROOT).as_posix(), "receipt_sha256": hashlib.sha256(path.read_bytes()).hexdigest()}
    policy.write(policy.JOURNAL, records)
    return receipt

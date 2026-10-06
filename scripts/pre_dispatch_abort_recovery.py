"""ADR0182 narrow append-only recovery for a verified ADR0181 receipt abort."""
import hashlib
import json
import re
from datetime import UTC, datetime
from pathlib import Path

GATES = ("zero_paid_dispatch", "retained_safety_financial_pass", "retained_safety_authorization_pass",
         "retained_restoration_pass", "retained_credential_cleanup_pass", "fresh_runtime_unchanged",
         "fresh_primary_four", "fresh_secondary_empty", "fresh_generator_idle", "fresh_queues_zero",
         "fresh_credentials_absent")
QUEUE_KEYS = ("unpublished_outbox", "pending_refresh", "dead_letters", "pending_callback_deliveries",
              "pending_refunds", "reservation_stream_entries", "reservation_stream_pending", "kafka_total_lag",
              "pending_confirmation_receipts", "review_confirmation_receipts", "confirmation_capacity_outstanding",
              "confirmation_capacity_mismatches")


def queue_pass(value):
    return isinstance(value, dict) and value.get("pass") is True and all(
        type(value.get(k)) is int and value[k] == 0 for k in QUEUE_KEYS)


def retained_pass(report, state, stage, failure):
    if (report.get("pass") is not False or (type(report.get("capacity_stages_started")) is not int or report["capacity_stages_started"] != 0)
            or set(report.get("arms", {})) != {"control"}
            or report["arms"]["control"].get("restoration_complete") is not True
            or (type(state.get("capacity_stages_started")) is not int or state["capacity_stages_started"] != 0) or stage.get("customers_dispatched") is not False
            or any(k in stage for k in ("customer", "customer_job", "offered_start_utc", "offered_end_utc"))
            or failure.splitlines()[-1] != "ValueError: Exact qualified inventory receipt required"):
        raise ValueError("Exact known zero-dispatch receipt abort required")
    safety, financial = state.get("safety", {}), state.get("post_ttl_financial", {})
    if (any(safety.get(k) is not True for k in ("pass", "one_durable_owner_before_payment", "cross_host_hold_replay",
                "cross_host_payment_replay", "other_actor_denied", "customer_ticket_confirmed"))
            or safety.get("wave_requests") != 100 or safety.get("accepted_holds") != 1
            or financial.get("pass") is not True or financial.get("hold_deadlines_elapsed") is not True
            or any(type(financial.get(k)) is not int or financial[k] != 1 for k in ("expected", "expected_paid", "fulfilled_orders", "succeeded_payments", "bookings", "tickets"))
            or any(type(financial.get(k)) is not int or financial[k] != 0 for k in ("pending_orders", "pending_payment_attempts", "incomplete_callback_deliveries", "duplicate_booked_seats", "multi_booking_orders", "unpublished_outbox", "dead_letters"))
            or state.get("safety_confirmation_receipts", {}).get("pass") is not True):
        raise ValueError("Complete retained safety financial/authorization proof required")
    if (any(state.get(k) is not True for k in ("restore_pass", "primary_runtime_semantics_restored",
            "secondary_resources_removed", "generator_idle_after", "credential_snapshots_removed"))
            or state.get("primary_api_count") != 4 or not queue_pass(state.get("restored_global_queues"))
            or stage.get("diagnostic_credentials_removed") is not True or stage.get("private_cleanup_pass") is not True
            or stage.get("cleanup_errors") != []):
        raise ValueError("Complete retained restoration and credential proof required")
    return True


def bounded_json(path):
    path = Path(path)
    if path.is_symlink() or not path.is_file() or path.stat().st_size > 2 * 1024 * 1024:
        raise ValueError("Exact bounded regular recovery evidence required")
    return json.loads(path.read_text(encoding="utf-8"))


def verify_artifacts(root, entry, fresh, *, now=None):
    now = now or datetime.now(UTC)
    if (entry.get("profile") != "diagnostic_placement" or entry.get("status") != "RECOVERY_REQUIRED"
            or not re.fullmatch(r"bounded_diagnostic_placement__[0-9a-f]{12}", entry.get("ledger", ""))
            or len(entry.get("reports", [])) != 2 or entry["reports"][0].get("pass") is not True
            or entry["reports"][1].get("pass") is not False):
        raise ValueError("Exact consumed diagnostic placement scope required")
    run = entry["reports"][1]["run"]
    if not re.fullmatch(r"adr0151-[0-9a-f]{12}", run):
        raise ValueError("Owned original report identity required")
    report_path = root / "tmp" / run / "comparison-summary.json"
    report = bounded_json(report_path)
    dry_path = root / "tmp" / entry["reports"][0]["run"] / "comparison-summary.json"
    dry = bounded_json(dry_path)
    from work_envelope import digest
    if (dry.get("pass") is not True or set(dry.get("arms", {})) != {"control", "candidate"}
            or any(arm.get("restoration_complete") is not True for arm in dry["arms"].values())
            or digest(dry.get("binding")) != entry["binding_sha256"]
            or digest(report.get("binding")) != entry["binding_sha256"]):
        raise ValueError("Exact retained qualification and binding required")
    directory = root / report["arms"]["control"]["evidence_directory"]
    if (not directory.resolve().is_relative_to((root / "tmp" / run).resolve())
            or directory.is_symlink() or not re.fullmatch(r"adr0151-arm-[0-9a-f]{12}", directory.name)
            or report.get("run") != run):
        raise ValueError("Owned original arm evidence required")
    state_path, stage_path = directory / "state.json", directory / "control/stage.private.json"
    failure_path = directory / "control/failure.private.log"
    state, stage = bounded_json(state_path), bounded_json(stage_path)
    if failure_path.is_symlink() or failure_path.stat().st_size > 65536:
        raise ValueError("Bounded original failure required")
    retained_pass(report, state, stage, failure_path.read_text())
    stamp = datetime.fromisoformat(fresh.get("captured_at_utc", ""))
    if (stamp.tzinfo is None or not 0 <= (now - stamp).total_seconds() <= 120
            or fresh.get("ledger") != entry["ledger"] or fresh.get("binding_sha256") != entry["binding_sha256"]
            or any(fresh.get(k) is not True for k in ("runtime_unchanged", "primary_four", "secondary_empty",
                    "generator_idle", "credentials_absent")) or not queue_pass(fresh.get("queues"))):
        raise ValueError("Fresh exact recovery observation required")
    return {"decision": "ADR0182", "ledger": entry["ledger"], "binding_sha256": entry["binding_sha256"],
            "verified_at_utc": now.isoformat(), "gates": dict.fromkeys(GATES, True),
            "retained_artifact_sha256": {p.relative_to(root).as_posix(): hashlib.sha256(p.read_bytes()).hexdigest()
                                        for p in (dry_path, report_path, state_path, stage_path, failure_path)},
            "fresh_probe_sha256": hashlib.sha256(json.dumps(fresh, sort_keys=True).encode()).hexdigest(),
            "scope": "Verified pre-dispatch abort only; original failed gates and reservation remain unchanged. No load permission or capacity claim."}


def resolved(records, entry, root):
    item = records.get("verified_aborts", {}).get(entry["ledger"])
    if not isinstance(item, dict) or set(item) != {"entry_sha256", "receipt_path", "receipt_sha256"}:
        return False
    from work_envelope import digest
    if item["entry_sha256"] != digest(entry):
        return False
    try:
        path = root / item["receipt_path"]
        if not path.resolve().is_relative_to((root / "docs/capacity/flash-sale-opening").resolve()):
            return False
        receipt = bounded_json(path)
        return (hashlib.sha256(path.read_bytes()).hexdigest() == item["receipt_sha256"]
                and receipt.get("decision") == "ADR0182" and receipt.get("ledger") == entry["ledger"]
                and receipt.get("binding_sha256") == entry["binding_sha256"]
                and receipt.get("gates") == dict.fromkeys(GATES, True))
    except (OSError, ValueError, TypeError, KeyError):
        return False


def append_receipt(key, fresh, receipt_path):
    import os

    import work_envelope as policy
    if (not policy.LOCK.exists() or policy.read(policy.LOCK).get("pid") != os.getpid()
            or policy.PAUSE.exists()):
        raise ValueError("Owned recovery lock and no human pause required")
    data, state = policy.envelope(), policy.read(policy.STATE)
    records = policy.journal(data)
    if state.get("current_run") or state.get("human_pause") or records["human_pause"]:
        raise ValueError("Active run or human pause blocks recovery classification")
    entry = next(item for item in records["experiments"] if item["ledger"] == key)
    if state.get(key, {}).get("active_run") or state.get(key, {}).get("paid_runs_started") != 0:
        raise ValueError("Ambiguous or reserved paid dispatch blocks abort recovery")
    receipt_path = Path(receipt_path)
    if (not receipt_path.resolve().is_relative_to((policy.ROOT / "docs/capacity/flash-sale-opening").resolve())
            or receipt_path.exists() or key in records.get("verified_aborts", {})):
        raise ValueError("Fresh owned recovery receipt required")
    receipt = verify_artifacts(policy.ROOT, entry, fresh)
    policy.write(receipt_path, receipt)
    records.setdefault("verified_aborts", {})[key] = {
        "entry_sha256": policy.digest(entry), "receipt_path": receipt_path.relative_to(policy.ROOT).as_posix(),
        "receipt_sha256": hashlib.sha256(receipt_path.read_bytes()).hexdigest()}
    policy.write(policy.JOURNAL, records)
    return receipt


def missing_container_observation(status, error, container_id):
    if type(status) is not int or status != 1 or not isinstance(error, str):
        return False
    if not isinstance(container_id, str) or not re.fullmatch(r"[0-9a-f]{64}", container_id):
        return False
    pattern = r"(?:error:|error response from daemon:)\s*no such (?:object|container):\s*" + re.escape(container_id)
    return re.fullmatch(pattern, error.strip().lower()) is not None

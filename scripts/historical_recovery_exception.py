"""ADR0190 human-approved exception for one unreconciled historical paid run."""
import hashlib
import os
from datetime import UTC, datetime

from pre_dispatch_abort_recovery import bounded_json, queue_pass

LEDGER = "bounded_diagnostic_placement__9647f50ed259"
ENTRY_SHA256 = "a55723b9869fb7e8f0fb968a3b32b3e28d0ec0ff61cfd0297c1ab80d49dc9bc0"
BINDING_SHA256 = "49dea775d29ddfb86f863db5de68d0e5e30850acfef2028e6e3ace160946ad01"
CONFIGURATION_SHA256 = "eaddb6cfae8a9442820b13f73be6df87290fabda96507deffdc8158b43c48b4b"
RECEIPT_PATH = "docs/capacity/flash-sale-opening/historical-paid-fixture-exception-2026-10-06.json"
APPROVAL = {"source": "direct_human_message", "date_bangkok": "2026-10-06",
            "instruction": "yes but if there is not information. You can ignore it and proceed test again.",
            "unreconciled_historical_information_accepted": True}
GATES = ("retained_runtime_restoration", "retained_credential_cleanup", "fresh_runtime_unchanged",
         "fresh_primary_four", "fresh_secondary_empty", "fresh_generator_idle",
         "fresh_credentials_absent", "fresh_global_queues_zero")
REPORT_PATH = "tmp/adr0151-647e2505c995/comparison-summary.json"
ARM_PATHS = {"control": "tmp/adr0151-647e2505c995/adr0151-arm-d5ef03846c60",
             "candidate": "tmp/adr0151-647e2505c995/adr0151-arm-b0dcedbec185"}


def artifact(root, name):
    path = root / name
    if path.absolute() != path.resolve() or not path.resolve().is_relative_to(root.resolve()):
        raise ValueError("Exact regular historical artifact required")
    return path, bounded_json(path)


def known_entry(entry):
    from work_envelope import digest
    return (entry.get("ledger") == LEDGER and entry.get("status") == "RECOVERY_REQUIRED"
            and entry.get("binding_sha256") == BINDING_SHA256 and digest(entry) == ENTRY_SHA256)


def retained(root, entry):
    from work_envelope import digest
    if not known_entry(entry):
        raise ValueError("Only the exact human-approved consumed historical entry is covered")
    path, report = artifact(root, REPORT_PATH)
    if (report.get("run") != "adr0151-647e2505c995" or report.get("pass") is not False
            or report.get("capacity_stages_started") != 2 or set(report.get("arms", {})) != set(ARM_PATHS)
            or digest(report.get("binding")) != BINDING_SHA256):
        raise ValueError("Original failed paid result and binding required")
    paths = [path]
    for arm, directory in ARM_PATHS.items():
        if (report["arms"][arm].get("restoration_complete") is not True
                or report["arms"][arm].get("evidence_directory") != directory):
            raise ValueError("Original exact arm restoration required")
        state_path, state = artifact(root, directory + "/state.json")
        stage_path, stage = artifact(root, directory + "/" + arm + "/stage.private.json")
        if (any(state.get(k) is not True for k in ("restore_pass", "primary_runtime_semantics_restored",
                "secondary_resources_removed", "generator_idle_after", "credential_snapshots_removed"))
                or state.get("primary_api_count") != 4 or not queue_pass(state.get("restored_global_queues"))
                or stage.get("customers_dispatched") is not True
                or stage.get("diagnostic_credentials_removed") is not True
                or stage.get("private_cleanup_pass") is not True or stage.get("cleanup_errors") != []):
            raise ValueError("Retained paid dispatch, restoration and credential cleanup required")
        paths.extend((state_path, stage_path))
    return {p.relative_to(root).as_posix(): hashlib.sha256(p.read_bytes()).hexdigest() for p in paths}


def verify(root, entry, fresh, approval, *, now=None):
    now = now or datetime.now(UTC)
    if approval != APPROVAL:
        raise ValueError("Explicit historical uncertainty approval required")
    hashes = retained(root, entry)
    stamp = datetime.fromisoformat(fresh.get("captured_at_utc", ""))
    if (stamp.tzinfo is None or not 0 <= (now - stamp).total_seconds() <= 120
            or fresh.get("ledger") != LEDGER or fresh.get("binding_sha256") != BINDING_SHA256
            or fresh.get("configuration_sha256") != CONFIGURATION_SHA256
            or any(fresh.get(k) is not True for k in ("runtime_unchanged", "primary_four", "secondary_empty",
                "generator_idle", "credentials_absent")) or not queue_pass(fresh.get("queues"))):
        raise ValueError("Fresh original runtime, idle generator and empty queues required")
    return {"decision": "ADR0190", "kind": "human_accepted_historical_uncertainty",
            "ledger": LEDGER, "binding_sha256": BINDING_SHA256,
            "configuration_sha256": CONFIGURATION_SHA256, "approval": approval,
            "recorded_at_utc": now.isoformat(), "historical_financial_reconciliation_verified": False,
            "original_result_pass": False, "old_scope_replay_allowed": False,
            "future_correctness_gates_unchanged": True, "gates": dict.fromkeys(GATES, True),
            "retained_artifact_sha256": hashes, "fresh_observation": fresh}


def accepted(records, entry, root):
    from work_envelope import digest
    item = records.get("historical_exceptions", {}).get(LEDGER)
    if (not known_entry(entry) or not isinstance(item, dict)
            or set(item) != {"entry_sha256", "receipt_path", "receipt_sha256"}
            or item.get("entry_sha256") != ENTRY_SHA256 or item.get("receipt_path") != RECEIPT_PATH):
        return False
    try:
        path, receipt = artifact(root, RECEIPT_PATH)
        # Reverify the observation at admission-record time, not at a later experiment time.
        # Every subsequent experiment retains its own live preflight and correctness gates.
        expected = verify(root, entry, receipt["fresh_observation"], receipt["approval"],
                          now=datetime.fromisoformat(receipt["recorded_at_utc"]))
        return (digest(receipt) == digest(expected)
                and hashlib.sha256(path.read_bytes()).hexdigest() == item["receipt_sha256"])
    except (OSError, ValueError, TypeError, KeyError):
        return False


def append_exception(fresh, approval):
    import work_envelope as policy
    if (policy.LOCK.is_symlink() or not policy.LOCK.exists()
            or policy.read(policy.LOCK).get("pid") != os.getpid() or policy.PAUSE.exists()):
        raise ValueError("Owned exception lock and no pause required")
    data, state = policy.envelope(), policy.read(policy.STATE)
    records = policy.journal(data)
    if (data["existing_resource_configuration_sha256"] != CONFIGURATION_SHA256
            or state.get("current_run") or state.get("human_pause") or records["human_pause"]
            or state.get(LEDGER, {}).get("active_run")
            or any(e["status"] == "ACTIVE" for e in records["experiments"])):
        raise ValueError("Original infrastructure, no active run and no pause required")
    entry = next(e for e in records["experiments"] if e["ledger"] == LEDGER)
    receipt_path = policy.ROOT / RECEIPT_PATH
    if receipt_path.exists() or receipt_path.is_symlink() or LEDGER in records.get("historical_exceptions", {}):
        raise ValueError("Historical exception cannot be overwritten or replayed")
    receipt = verify(policy.ROOT, entry, fresh, approval)
    policy.write(receipt_path, receipt)
    records.setdefault("historical_exceptions", {})[LEDGER] = {
        "entry_sha256": policy.digest(entry), "receipt_path": RECEIPT_PATH,
        "receipt_sha256": hashlib.sha256(receipt_path.read_bytes()).hexdigest()}
    policy.write(policy.JOURNAL, records)
    return receipt

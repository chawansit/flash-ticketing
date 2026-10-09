"""ADR0218: reject uncertain cleanup and preserve original failed evidence."""
import json
import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[2] / "scripts"))
import safety_diagnostic_abort_recovery as recovery
import work_envelope as envelope
from test_work_envelope import area  # noqa: F401


def write(path, value):
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(value))


def artifacts(root):
    key, run = "bounded_shared_callback_placement__" + "a" * 12, "adr0151-" + "b" * 12
    binding = {"test": "immutable"}
    entry = {"ledger": key, "profile": "shared_callback_placement", "status": "RECOVERY_REQUIRED",
             "binding_sha256": envelope.digest(binding), "reports": [{"run": run, "pass": False}]}
    state = {key: {"binding": binding, "active_run": None, "paid_runs_started": 0, "paid_protocols_started": 0}}
    report = {"run": run, "experiment_decision": "ADR0217", "binding": binding, "pass": False,
              "capacity_stages_started": 0, "arms": {}}
    for i, arm in enumerate(("control", "candidate")):
        directory = root / "tmp" / run / ("adr0151-arm-" + str(i + 1) * 12)
        report["arms"][arm] = {"evidence_directory": directory.relative_to(root).as_posix(),
             "restoration_complete": True, "capacity_stages_started": 0, "pre_safety_source_pass": True}
        saved = dict.fromkeys(("pass", "restore_pass", "qualification_checks_pass", "secondary_resources_removed",
             "confirmation_worker_removed_after_drain", "primary_runtime_semantics_restored", "generator_idle_after",
             "credential_snapshots_removed", "candidate_pre_safety_sources_verified", "frozen_audit_harness_match"), True)
        saved.update(primary_api_count=4, probe_execution={"returncode": 0},
          safety={**dict.fromkeys(("pass", "customer_ticket_confirmed", "other_actor_denied", "cross_host_hold_replay", "cross_host_payment_replay"), True),
                  "accepted_holds": 1, "wave_statuses": {"202": 1, "409": 99}},
          post_ttl_financial={"pass": True, "hold_deadlines_elapsed": True,
             **dict.fromkeys(("orders", "fulfilled_orders", "succeeded_payments", "bookings", "tickets"), 1),
             **dict.fromkeys(("duplicate_booked_seats", "multi_booking_orders", "pending_orders", "pending_payment_attempts", "incomplete_callback_deliveries", "dead_letters", "unpublished_outbox"), 0)},
          restored_global_queues={"pass": True, "kafka_members": 1, **dict.fromkeys(recovery.QUEUE_KEYS, 0)})
        stage = {"customers_dispatched": False, "pre_dispatch_qualified": True, "private_cleanup_pass": True,
                 "diagnostic_credentials_removed": True, "cleanup_errors": [], "failure_type": None}
        write(directory / "state.json", saved); write(directory / arm / "stage.private.json", stage)
    write(root / "tmp" / run / "comparison-summary.json", report)
    return entry, state, report


def test_preserves_failure_and_hashes_originals(tmp_path):
    entry, state, report = artifacts(tmp_path)
    proof = recovery.verify(tmp_path, entry, state)
    assert entry["status"] == "RECOVERY_REQUIRED" and report["pass"] is False
    assert proof["gates"] == dict.fromkeys(recovery.GATES, True)
    assert len(proof["retained_artifact_sha256"]) == 5


@pytest.mark.parametrize("drift", ["paid", "dispatch", "active", "binding", "financial", "duplicate", "ttl", "queue", "restore", "credentials", "source", "missing", "path"])
def test_uncertainty_blocks_recovery(tmp_path, drift):
    entry, state, report = artifacts(tmp_path)
    directory = tmp_path / report["arms"]["candidate"]["evidence_directory"]
    sp, tp = directory / "state.json", directory / "candidate/stage.private.json"
    saved, stage = json.loads(sp.read_text()), json.loads(tp.read_text())
    if drift == "paid": state[entry["ledger"]]["paid_runs_started"] = 1
    elif drift == "dispatch": stage["customers_dispatched"] = True
    elif drift == "active": state["current_run"] = "active"
    elif drift == "binding": entry["binding_sha256"] = "f" * 64
    elif drift == "financial": saved["post_ttl_financial"]["tickets"] = True
    elif drift == "duplicate": saved["post_ttl_financial"]["duplicate_booked_seats"] = 1
    elif drift == "ttl": saved["post_ttl_financial"]["hold_deadlines_elapsed"] = False
    elif drift == "queue": saved["restored_global_queues"]["pending_callback_deliveries"] = 1
    elif drift == "restore": saved["primary_runtime_semantics_restored"] = False
    elif drift == "credentials": stage["diagnostic_credentials_removed"] = False
    elif drift == "source": saved["candidate_pre_safety_sources_verified"] = False
    elif drift == "missing": saved.pop("generator_idle_after")
    else: report["arms"]["candidate"]["evidence_directory"] = "tmp/unowned/adr0151-arm-" + "e" * 12
    write(sp, saved); write(tp, stage)
    write(tmp_path / "tmp" / entry["reports"][0]["run"] / "comparison-summary.json", report)
    with pytest.raises((ValueError, OSError)): recovery.verify(tmp_path, entry, state)


def test_append_and_tamper_recheck(area, monkeypatch):  # noqa: F811
    _binding, _, _ = area
    root = envelope.STATE.parent; monkeypatch.setattr(envelope, "ROOT", root)
    entry, state, report = artifacts(root)
    entry.update(reserved_seconds=3600, actual_elapsed_seconds=756, envelope_sha256=envelope.digest(envelope.envelope()))
    records = envelope.read(envelope.JOURNAL); records["experiments"].append(entry); envelope.write(envelope.JOURNAL, records)
    prior = envelope.read(envelope.STATE); prior.update(state); envelope.write(envelope.STATE, prior)
    path = root / "docs/capacity/flash-sale-opening/safety-abort.json"; path.parent.mkdir(parents=True)
    recovery.append_receipt(entry["ledger"], path)
    records = envelope.read(envelope.JOURNAL)
    assert records["experiments"][-1] == entry and recovery.resolved(records, entry, root)
    envelope.check_available(envelope.envelope(), records, prior)
    with pytest.raises(ValueError, match="already"): recovery.append_receipt(entry["ledger"], path)
    damaged = root / report["arms"]["candidate"]["evidence_directory"] / "state.json"
    damaged.write_text(damaged.read_text() + " ")
    assert recovery.resolved(records, entry, root) is False
    with pytest.raises(ValueError, match="recovery"): envelope.check_available(envelope.envelope(), records, prior)


@pytest.mark.parametrize("drift", [None, "unknown_scope", "diagnostic_cause", "index", "dispatch", "paid", "financial", "restore"])
def test_exact_writer_safety_case_preserves_all_recovery_gates(tmp_path, monkeypatch, drift):
    entry, state, report = artifacts(tmp_path)
    old_key = entry["ledger"]
    saved_dir = tmp_path / report["arms"]["candidate"]["evidence_directory"]
    saved = json.loads((saved_dir / "state.json").read_text())
    stage = json.loads((saved_dir / "candidate/stage.private.json").read_text())
    key, run, arm, _ = recovery.WRITER_ABORT
    monkeypatch.setattr(recovery, "WRITER_ABORT", (key, run, arm, entry["binding_sha256"]))
    entry.update(ledger=key, profile="writer_write_pipeline_probe", reports=[{"run": run, "pass": False}])
    state = {key: state[old_key]}
    report.update(run=run, experiment_decision="ADR0225")
    directory = tmp_path / "tmp" / run / arm
    report["arms"] = {"candidate": {**report["arms"]["candidate"], "evidence_directory": directory.relative_to(tmp_path).as_posix()}}
    index = {"pass": True, "verification_only": True, "index": {"oid": 123, "valid": True, "ready": True, "unique": False, "method": "btree", "key": "event_id", "predicate": None, "columns": 1, "expected_table": True, "pass": True}}
    saved.update(orders_event_index_before=index, orders_event_index_after=json.loads(json.dumps(index)))
    stage["admission_failure_capture"] = {"complete": False, "failure_type": "UnboundLocalError"}
    if drift == "unknown_scope":
        entry["ledger"] = key[:-1] + "f"
        state = {entry["ledger"]: state[key]}
    elif drift == "diagnostic_cause": stage["admission_failure_capture"]["failure_type"] = "TimeoutError"
    elif drift == "index": saved["orders_event_index_after"]["index"]["oid"] += 1
    elif drift == "dispatch": stage["customers_dispatched"] = True
    elif drift == "paid": state[key]["paid_runs_started"] = 1
    elif drift == "financial": saved["post_ttl_financial"]["tickets"] = 0
    elif drift == "restore": saved["primary_runtime_semantics_restored"] = False
    write(directory / "state.json", saved)
    write(directory / "candidate/stage.private.json", stage)
    write(tmp_path / "tmp" / run / "comparison-summary.json", report)
    if drift:
        with pytest.raises(ValueError): recovery.verify(tmp_path, entry, state)
    else:
        proof = recovery.verify(tmp_path, entry, state)
        assert len(proof["retained_artifact_sha256"]) == 3
        assert proof["gates"] == dict.fromkeys(recovery.GATES, True)
        assert entry["status"] == "RECOVERY_REQUIRED" and report["pass"] is False

"""ADR0190: one explicit exception cannot relax future tests or reopen history."""
import copy
import json
import os
import sys
from datetime import UTC, datetime, timedelta
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT / "scripts"))
import historical_recovery_exception as exception
import work_envelope as policy
from pre_dispatch_abort_recovery import QUEUE_KEYS


@pytest.fixture
def area(tmp_path, monkeypatch):
    original = policy.envelope()
    journal = policy.read(policy.JOURNAL)
    entry = copy.deepcopy(next(e for e in journal["experiments"] if e["ledger"] == exception.LEDGER))
    binding = {"configuration_sha256": exception.CONFIGURATION_SHA256}
    entry["binding_sha256"] = policy.digest(binding)
    monkeypatch.setattr(exception, "BINDING_SHA256", entry["binding_sha256"])
    monkeypatch.setattr(exception, "ENTRY_SHA256", policy.digest(entry))
    queues = {**dict.fromkeys(QUEUE_KEYS, 0), "pass": True}
    report = {"run": "adr0151-647e2505c995", "pass": False, "capacity_stages_started": 2,
              "binding": binding, "arms": {}}
    for arm, directory in exception.ARM_PATHS.items():
        report["arms"][arm] = {"restoration_complete": True, "evidence_directory": directory}
        state = {**dict.fromkeys(["restore_pass", "primary_runtime_semantics_restored",
                 "secondary_resources_removed", "generator_idle_after", "credential_snapshots_removed"], True),
                 "primary_api_count": 4, "restored_global_queues": queues}
        stage = {"customers_dispatched": True, "diagnostic_credentials_removed": True,
                 "private_cleanup_pass": True, "cleanup_errors": []}
        for name, value in [(directory + "/state.json", state), (directory + "/" + arm + "/stage.private.json", stage)]:
            destination = tmp_path / name
            destination.parent.mkdir(parents=True, exist_ok=True)
            destination.write_text(json.dumps(value))
    (tmp_path / exception.REPORT_PATH).write_text(json.dumps(report))
    (tmp_path / exception.RECEIPT_PATH).parent.mkdir(parents=True)
    for key, value in [("ROOT", tmp_path), ("ENVELOPE", tmp_path / "envelope.json"),
                       ("STATE", tmp_path / "state.json"), ("JOURNAL", tmp_path / "journal.json"),
                       ("LOCK", tmp_path / "lock"), ("PAUSE", tmp_path / "pause")]:
        monkeypatch.setattr(policy, key, value)
    policy.write(policy.ENVELOPE, original)
    records = {"schema_version": 1, "envelope_id": original["envelope_id"], "human_pause": False,
               "experiments": [entry]}
    state = {"current_run": None, "human_pause": False, "cloud_load_requires_resume": False,
             exception.LEDGER: {"active_run": None}}
    policy.write(policy.JOURNAL, records)
    policy.write(policy.STATE, state)
    policy.write(policy.LOCK, {"pid": os.getpid(), "run": "adr0151-" + "a" * 12})
    now = datetime.now(UTC)
    fresh = {"ledger": exception.LEDGER, "binding_sha256": exception.BINDING_SHA256,
             "configuration_sha256": exception.CONFIGURATION_SHA256, "captured_at_utc": now.isoformat(),
             **dict.fromkeys(["runtime_unchanged", "primary_four", "secondary_empty", "generator_idle", "credentials_absent"], True),
             "queues": {**dict.fromkeys(QUEUE_KEYS, 0), "pass": True}}
    return tmp_path, entry, fresh, records, state


def install(area):
    return exception.append_exception(area[2], copy.deepcopy(exception.APPROVAL))


def test_known_exception_preserves_failed_history_and_requires_new_scope(area):
    root, entry, _fresh, records, state = area
    before = copy.deepcopy(entry)
    receipt = install(area)
    records = policy.read(policy.JOURNAL)
    assert records["experiments"] == [before]
    assert exception.accepted(records, entry, root)
    assert receipt["historical_financial_reconciliation_verified"] is False
    assert receipt["original_result_pass"] is False
    assert receipt["old_scope_replay_allowed"] is False
    assert receipt["future_correctness_gates_unchanged"] is True
    assert policy.check_available(policy.envelope(), records, state) == 3600
    binding = {"configuration_sha256": exception.CONFIGURATION_SHA256}
    plan = {"decision": "ADR0171", "arms": ["control"],
            "common": {"buyer_journeys_per_second": 60, "duration_seconds": 300}}
    new = policy.reserve(binding, plan)
    assert new["ledger"] != entry["ledger"]
    assert policy.read(policy.JOURNAL)["experiments"][0] == before
    with pytest.raises(ValueError):
        policy.scope_authorized(policy.read(policy.STATE), entry["ledger"], binding)
    closed = policy.ActionGuard(new["ledger"], binding).finish([], 1)
    assert closed["status"] == "RECOVERY_REQUIRED"
    with pytest.raises(ValueError):
        policy.reserve(binding, plan)


@pytest.mark.parametrize("field", ["runtime_unchanged", "primary_four", "secondary_empty", "generator_idle", "credentials_absent"])
def test_bad_live_restoration_prevents_install(area, field):
    area[2][field] = False
    with pytest.raises(ValueError):
        install(area)
    assert "historical_exceptions" not in policy.read(policy.JOURNAL)


@pytest.mark.parametrize("case", ["stale", "future", "config", "binding", "ledger", "queues", "missing_consent"])
def test_receipt_requires_fresh_exact_authority_and_zero_queues(area, case):
    _root, _entry, fresh, _records, _state = area
    if case in {"stale", "future"}:
        fresh["captured_at_utc"] = (datetime.now(UTC) + timedelta(seconds=-121 if case == "stale" else 60)).isoformat()
    elif case in {"config", "binding", "ledger"}:
        fresh[{"config": "configuration_sha256", "binding": "binding_sha256", "ledger": "ledger"}[case]] = "wrong"
    elif case == "queues":
        fresh["queues"]["pending_callback_deliveries"] = 1
    approval = {} if case == "missing_consent" else exception.APPROVAL
    with pytest.raises(ValueError):
        exception.append_exception(fresh, approval)


@pytest.mark.parametrize("case", ["receipt", "entry", "report", "path", "missing_receipt"])
def test_modified_proof_keeps_forward_admission_blocked(area, case):
    root, _entry, _fresh, records, state = area
    install(area)
    records = policy.read(policy.JOURNAL)
    if case == "receipt":
        p = root / exception.RECEIPT_PATH
        r = json.loads(p.read_text()); r["original_result_pass"] = True; p.write_text(json.dumps(r))
    elif case == "entry":
        records["experiments"][0]["actual_elapsed_seconds"] += 1
    elif case == "report":
        p = root / exception.REPORT_PATH
        r = json.loads(p.read_text()); r["pass"] = True; p.write_text(json.dumps(r))
    elif case == "path":
        records["historical_exceptions"][exception.LEDGER]["receipt_path"] = "../outside.json"
    else:
        (root / exception.RECEIPT_PATH).unlink()
    assert not exception.accepted(records, records["experiments"][0], root)
    with pytest.raises(ValueError):
        policy.check_available(policy.envelope(), records, state)


@pytest.mark.parametrize("case", ["pause", "active", "other_recovery", "legacy_recovery"])
def test_exception_never_ignores_other_blockers(area, case):
    _root, entry, _fresh, records, state = area
    install(area); records = policy.read(policy.JOURNAL)
    if case == "pause":
        state["human_pause"] = True
    elif case in {"active", "other_recovery"}:
        other = copy.deepcopy(entry); other["ledger"] = "bounded_diagnostic_placement__" + "b" * 12
        other["status"] = "ACTIVE" if case == "active" else "RECOVERY_REQUIRED"
        records["experiments"].append(other)
    else:
        state["bounded_unrelated"] = {"recovery_required": True}
    with pytest.raises(ValueError):
        policy.check_available(policy.envelope(), records, state)


def test_duplicate_install_cannot_rewrite_or_replay_exception(area):
    install(area)
    before = policy.JOURNAL.read_bytes()
    with pytest.raises(ValueError):
        install(area)
    assert policy.JOURNAL.read_bytes() == before


def test_failed_append_retains_orphan_receipt_but_does_not_admit(area, monkeypatch):
    original = policy.write
    def fail_journal(path, value):
        if path == policy.JOURNAL:
            raise OSError("synthetic journal failure")
        original(path, value)
    monkeypatch.setattr(policy, "write", fail_journal)
    with pytest.raises(OSError):
        install(area)
    records = policy.read(policy.JOURNAL)
    assert not exception.accepted(records, area[1], area[0])
    assert (area[0] / exception.RECEIPT_PATH).exists()
    with pytest.raises(ValueError):
        policy.check_available(policy.envelope(), records, area[4])


def test_registered_exception_identity_matches_original_committed_entry():
    records = json.loads((ROOT / "docs/capacity/work-envelope-ledger.json").read_text())
    original = next(e for e in records["experiments"] if e["ledger"] == exception.LEDGER)
    assert exception.known_entry(original)

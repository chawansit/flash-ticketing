"""Strict zero-customer CCE recovery; all cloud checks are mocked."""
import copy
import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[2] / "scripts"))
import cce_dependency_recovery as recovery
import work_envelope as policy


@pytest.fixture
def case(tmp_path, monkeypatch):
    envelope = policy.envelope()
    monkeypatch.setattr(policy, "ROOT", tmp_path)
    for name in ("ENVELOPE", "JOURNAL", "STATE"):
        monkeypatch.setattr(policy, name, tmp_path / name)
    policy.write(policy.ENVELOPE, envelope)
    binding = {"configuration_sha256": "a"*64}
    report = {"run": recovery.RUN, "customer_writes": 0, "capacity_stages_started": 0,
              "generator_idle": True, "namespace_removed": True, "bridge_removed": True,
              "temporary_credentials_removed": True, "existing_runtime_unchanged": False}
    monkeypatch.setattr(recovery, "RESULT_SHA", policy.digest(report))
    entry = {"ledger": recovery.LEDGER, "profile": recovery.PROFILE, "status": "RECOVERY_REQUIRED",
             "reserved_seconds": 3600, "actual_elapsed_seconds": 25,
             "result_sha256": recovery.RESULT_SHA, "binding_sha256": policy.digest(binding)}
    policy.write(policy.JOURNAL, {"schema_version": 1, "envelope_id": envelope["envelope_id"],
                                 "human_pause": False, "experiments": [entry]})
    scope = {"binding": binding, "cce_result_sha256": recovery.RESULT_SHA,
             "paid_runs_started": 0, "safety_protocols_started": 0, "paid_protocols_started": 0}
    policy.write(policy.STATE, {"current_run": None, recovery.LEDGER: scope})
    receipt = {"decision": "ADR0228", "ledger": recovery.LEDGER,
               "original_result_sha256": recovery.RESULT_SHA, "binding_sha256": policy.digest(binding),
               "namespace": recovery.namespace_for(recovery.RUN), "actual_elapsed_seconds": 12,
               "queue_counts": {**{k: 0 for k in recovery.queue_checks.__globals__["QUEUE_ZERO"]},
                                "kafka_members": 1, "pass": True}}
    for name in ("pass", "runtime_unchanged", "audit_ids_starts_stable", "namespace_absent",
                 "bridge_absent", "generator_idle", "zero_double_booking", "all_queues_zero",
                 "kafka_drained", "temporary_credentials_removed"):
        receipt[name] = True
    path = tmp_path / "tmp" / "recovery.json"
    path.parent.mkdir()
    policy.write(path, receipt)
    return report, path, receipt, copy.deepcopy(entry)


def test_closure_keeps_failed_result_and_counts(case):
    report, path, _, original = case
    result = recovery.close(report, path)
    assert result["status"] == "FAILED_RESTORED"
    entry = policy.read(policy.JOURNAL)["experiments"][0]
    assert entry["initial_status"] == original["status"]
    assert entry["result_sha256"] == original["result_sha256"]
    assert entry["initial_actual_elapsed_seconds"] == 25
    assert entry["actual_elapsed_seconds"] == 37
    assert policy.read(policy.STATE)[recovery.LEDGER]["paid_runs_started"] == 0
    with pytest.raises(ValueError):
        recovery.close(report, path)


@pytest.mark.parametrize("field", ["pass", "runtime_unchanged", "audit_ids_starts_stable",
                                  "namespace_absent", "bridge_absent", "generator_idle",
                                  "zero_double_booking", "all_queues_zero", "kafka_drained",
                                  "temporary_credentials_removed", "original_result_sha256",
                                  "binding_sha256", "namespace", "queue_counts", "actual_elapsed_seconds"])
def test_every_recovery_gate_required(case, field):
    report, path, receipt, original = case
    receipt[field] = None
    policy.write(path, receipt)
    with pytest.raises((ValueError, TypeError)):
        recovery.close(report, path)
    assert policy.read(policy.JOURNAL)["experiments"][0] == original


def test_paid_or_different_failure_cannot_use_exception(case):
    report, path, _, original = case
    report["customer_writes"] = 1
    with pytest.raises(ValueError):
        recovery.close(report, path)
    assert policy.read(policy.JOURNAL)["experiments"][0] == original

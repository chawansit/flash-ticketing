"""ADR0228 append-only proof of cleanup after the exact zero-customer CCE probe."""
import copy
import math
from pathlib import Path

import work_envelope as policy
from cce_dependency_probe import PROFILE, namespace_for
from worker_separation_audits import queue_checks

LEDGER = "bounded_cce_dependency_probe__2a7b5311601a"
RUN = "adr0151-ab5fe14d1c72"
RESULT_SHA = "d030ca037671ddd16a92f9b168cb23e0e719b0d1a756a7239200b196cd685127"


RETRY_CASE = ("bounded_cce_dependency_probe__b14e8117a3ba", "adr0151-752abd03b92f",
              "e133ac47b2f78536a2b7e6f75a59912c1bbb3748e3294c2947a9f833557ec1ef")


def case_identity(report):
    cases = ((LEDGER, RUN, RESULT_SHA), RETRY_CASE)
    matches = [case for case in cases if report.get("run") == case[1] and policy.digest(report) == case[2]]
    if len(matches) != 1:
        raise ValueError("One exact allowlisted zero-customer CCE case required")
    return matches[0]


def close(report, evidence):
    ledger, run, result_sha = case_identity(report)
    idle_field = "generator_idle" if ledger == LEDGER else "generator_idle_after"
    state = policy.read(policy.STATE)
    records = policy.journal(policy.envelope())
    matches = [r for r in records["experiments"] if r["ledger"] == ledger]
    if len(matches) != 1:
        raise ValueError("Exact original CCE reservation required")
    entry = matches[0]
    scope = state[ledger]
    if (entry["profile"] != PROFILE or entry["status"] != "RECOVERY_REQUIRED"
            or entry.get("result_sha256") != result_sha or policy.digest(report) != result_sha
            or report.get("run") != run or report.get("customer_writes") != 0
            or report.get("capacity_stages_started") != 0 or report.get(idle_field) is not True
            or report.get("namespace_removed") is not True or report.get("bridge_removed") is not True
            or report.get("temporary_credentials_removed") is not True
            or report.get("existing_runtime_unchanged") is not False
            or scope.get("cce_result_sha256") != result_sha
            or entry["binding_sha256"] != policy.digest(scope["binding"])
            or any(scope.get(k) != 0 for k in ("paid_runs_started", "safety_protocols_started",
                                              "paid_protocols_started"))
            or state.get("current_run") or scope.get("active_run")):
        raise ValueError("Exact zero-customer failed CCE case required")
    path = Path(evidence).resolve()
    if not path.is_relative_to((policy.ROOT / "tmp").resolve()) or Path(evidence).is_symlink():
        raise ValueError("Owned recovery evidence required")
    receipt = policy.read(path)
    required = ("pass", "runtime_unchanged", "audit_ids_starts_stable", "namespace_absent",
                "bridge_absent", "generator_idle", "zero_double_booking", "all_queues_zero",
                "kafka_drained", "temporary_credentials_removed")
    if (receipt.get("decision") != "ADR0228" or receipt.get("ledger") != ledger
            or receipt.get("original_result_sha256") != result_sha
            or receipt.get("binding_sha256") != entry["binding_sha256"]
            or receipt.get("namespace") != namespace_for(run)
            or any(receipt.get(k) is not True for k in required)
            or not queue_checks(receipt.get("queue_counts"), 1)
            or type(receipt.get("actual_elapsed_seconds")) not in (int, float)
            or not math.isfinite(receipt["actual_elapsed_seconds"])
            or receipt["actual_elapsed_seconds"] < 0):
        raise ValueError("Complete independently verified CCE cleanup evidence required")
    entry["initial_status"] = entry["status"]
    entry["initial_actual_elapsed_seconds"] = entry["actual_elapsed_seconds"]
    entry["recovery"] = {"decision": "ADR0228", "evidence": str(path.relative_to(policy.ROOT)),
                         "sha256": policy.digest(receipt),
                         "actual_elapsed_seconds": receipt["actual_elapsed_seconds"]}
    entry["actual_elapsed_seconds"] += receipt["actual_elapsed_seconds"]
    entry["status"] = "FAILED_RESTORED"
    policy.write(policy.JOURNAL, records)
    scope["cce_recovery"] = copy.deepcopy(entry["recovery"])
    policy.write(policy.STATE, state)
    return {"status": "FAILED_RESTORED", "original_result_preserved": True,
            "paid_runs_started": 0, "recovery_seconds": receipt["actual_elapsed_seconds"]}

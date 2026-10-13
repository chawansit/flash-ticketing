"""ADR0309 terminal read-only work follows confirmed paid-job shutdown."""
import hashlib
import time
from pathlib import Path

import work_envelope as policy


def contract():
    return {"decision": "ADR0309", "transport_seconds": 780, "query_seconds": 600,
            "ttl_wait_seconds": 150, "additional_paid_stages": 0,
            "source_sha256": hashlib.sha256(Path(__file__).read_bytes().replace(b"\\r\\n", b"\\n")).hexdigest()}


def selected(goal):
    keys = {"terminal_audit_admission_decision", "terminal_audit_admission_contract_sha256"}
    if goal is None or not keys.intersection(goal):
        return False
    if (goal.get("terminal_audit_admission_decision") != "ADR0309"
            or goal.get("terminal_audit_admission_contract_sha256") != policy.digest(contract())
            or goal.get("committed_hourly_decision") != "ADR0308"
            or goal.get("profile") != "cce_hourly_qualification"):
        raise ValueError("Explicit source-bound terminal audit admission required")
    return True


def admit(stage):
    if stage.guard.binding.get("cce_terminal_audit_admission_contract") != contract():
        raise ValueError("Reserved terminal audit admission required")
    state = policy.read(policy.STATE)
    scope = state.get(stage.guard.key, {})
    if (scope.get("binding") != stage.guard.binding
            or state.get("current_run") != stage.run or scope.get("active_run") != stage.run
            or scope.get("paid_runs_started") != 1
            or scope.get("attempted_paid_arms") != ["candidate"]
            or stage.record.get("customers_dispatched") is not True
            or stage.record.get("generator_idle_after_stop") is not True
            or stage.record.get("job_cleanup_errors")):
        raise ValueError("Consumed paid allowance and stopped owned generator jobs required")
    already_cleanup = getattr(stage.session, "cleanup_mode", False)
    if not already_cleanup:
        # Check the unchanged source/binding with no unused780-second reservation.
        stage.guard.check(0)
    remaining = stage.guard.deadline - time.monotonic()
    stage.record["terminal_financial_admission"] = {
        "decision": "ADR0309", "requested_transport_seconds": 780,
        "remaining_experiment_seconds": remaining,
        "generator_jobs_confirmed_stopped": True, "binding_rechecked": not already_cleanup,
        "mandatory_cleanup_mode": True, "additional_paid_stages": 0}
    stage.checkpoint()
    stage.session.begin_cleanup()
    return stage.record["terminal_financial_admission"]

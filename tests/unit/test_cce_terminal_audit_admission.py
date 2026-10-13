"""Terminal audits cannot enable more paid work or accept unknown job ownership."""
import copy
import sys
import time
from pathlib import Path
from types import SimpleNamespace

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[2] / "scripts"))
import cce_terminal_audit_admission as terminal
import work_envelope as policy


@pytest.fixture
def stage(monkeypatch):
    calls = []
    binding = {"cce_terminal_audit_admission_contract": terminal.contract()}
    guard = SimpleNamespace(key="scope", binding=binding, deadline=time.monotonic() + 60,
                            check=lambda timeout: calls.append(timeout))
    session = SimpleNamespace(cleanup_mode=False)
    session.begin_cleanup = lambda: setattr(session, "cleanup_mode", True)
    value = SimpleNamespace(run="run", guard=guard, session=session, checkpoint=lambda: None,
                            record={"customers_dispatched": True, "generator_idle_after_stop": True,
                                    "job_cleanup_errors": []})
    state = {"current_run": "run", "scope": {"binding": binding, "active_run": "run",
                                            "paid_runs_started": 1, "attempted_paid_arms": ["candidate"]}}
    monkeypatch.setattr(policy, "read", lambda _: state)
    return value, state, calls


def test_stopped_paid_work_admits_full_audit_without_reserving_unused_maximum(stage):
    value, _, calls = stage
    result = terminal.admit(value)
    assert calls == [0]
    assert value.session.cleanup_mode
    assert 0 < result["remaining_experiment_seconds"] < 780
    assert result["requested_transport_seconds"] == 780
    assert result["additional_paid_stages"] == 0


@pytest.mark.parametrize("key,value", [("generator_idle_after_stop", False),
                                     ("job_cleanup_errors", ["unknown"]),
                                     ("customers_dispatched", False)])
def test_unstopped_or_undispatched_job_cannot_enter_terminal_mode(stage, key, value):
    original, _, _ = stage
    original.record[key] = value
    with pytest.raises(ValueError):
        terminal.admit(original)
    assert not original.session.cleanup_mode


@pytest.mark.parametrize("key,value", [("paid_runs_started", 0), ("paid_runs_started", 2),
                                     ("active_run", "foreign"),
                                     ("attempted_paid_arms", ["candidate", "candidate"])])
def test_unconsumed_or_wrong_scope_cannot_bypass_action_clock(stage, key, value):
    original, state, _ = stage
    state["scope"][key] = value
    with pytest.raises(ValueError):
        terminal.admit(original)
    assert not original.session.cleanup_mode


def test_wrong_binding_is_rejected_before_cleanup(stage):
    original, state, _ = stage
    state["scope"]["binding"] = {"wrong": True}
    with pytest.raises(ValueError):
        terminal.admit(original)


def test_source_guard_failure_does_not_activate_cleanup(stage):
    original, _, _ = stage
    def changed(_):
        raise ValueError("source drift")
    original.guard.check = changed
    with pytest.raises(ValueError):
        terminal.admit(original)
    assert not original.session.cleanup_mode


def test_existing_mandatory_cleanup_can_reaudit_without_expired_paid_clock(stage):
    original, _, calls = stage
    original.session.cleanup_mode = True
    original.guard.deadline = time.monotonic() - 1
    assert terminal.admit(original)["mandatory_cleanup_mode"]
    assert calls == []


def test_terminal_opt_in_keeps_exact_query_transport_and_ttl_bounds():
    c = terminal.contract()
    assert (c["query_seconds"], c["transport_seconds"], c["ttl_wait_seconds"]) == (600, 780, 150)
    goal = {"terminal_audit_admission_decision": "ADR0309",
            "terminal_audit_admission_contract_sha256": policy.digest(c),
            "committed_hourly_decision": "ADR0308", "profile": "cce_hourly_qualification"}
    assert terminal.selected(goal)
    assert not terminal.selected({})
    with pytest.raises(ValueError):
        terminal.selected({**goal, "profile": "cce_paid_comparison"})
    wrong = copy.deepcopy(goal)
    wrong["terminal_audit_admission_contract_sha256"] = "wrong"
    with pytest.raises(ValueError):
        terminal.selected(wrong)

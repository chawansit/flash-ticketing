"""ADR0172 safety regressions; no network or cloud mutation."""
import copy
import os
import sys
from datetime import UTC, datetime, timedelta
from pathlib import Path
from types import SimpleNamespace

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[2] / "scripts"))
import run_status_refresh_comparison as engine
import run_work_envelope as runner
import work_envelope as policy
from qualify_two_host_deployment import Session


@pytest.fixture
def area(tmp_path, monkeypatch):
    original = policy.envelope()
    for key, name in (("ENVELOPE", "envelope.json"), ("JOURNAL", "journal.json"),
                      ("STATE", "state.json"), ("LOCK", "lock"), ("PAUSE", "pause")):
        monkeypatch.setattr(policy, key, tmp_path / name)
    policy.write(policy.ENVELOPE, original)
    policy.write(policy.JOURNAL, {"schema_version": 1, "envelope_id": original["envelope_id"],
                                 "human_pause": False, "experiments": []})
    state = {"current_run": None, "human_pause": False, "cloud_load_requires_resume": True,
             "cloud_load_stop_kind": "scope_exhausted",
             policy.BASE_LEDGER: {"status": "CONSUMED_PASSED_RESTORED", "active_run": None,
                                  "paid_runs_started": 1}}
    policy.write(policy.STATE, state)
    policy.write(policy.LOCK, {"pid": os.getpid(), "run": "adr0151-" + "a" * 12})
    binding = {"configuration_sha256": original["existing_resource_configuration_sha256"],
               "adapter_identity": {"example": "a" * 64}, "artifact_sha256": "b" * 64}
    plan = {"decision": "ADR0171", "arms": ["control"],
            "common": {"buyer_journeys_per_second": 60, "duration_seconds": 300}}
    return binding, plan, state


def reports(paid_pass=True):
    gates = {g: True for g in ("post_ttl_financial", "zero_double_booking",
                              "full_keyspace_queue_drain", "kafka_drain")}
    return [
        {"run": "adr0151-" + "1" * 12, "pass": True, "capacity_stages_started": 0,
         "arms": {"control": {"pass": True, "restoration_complete": True}}},
        {"run": "adr0151-" + "2" * 12, "pass": paid_pass, "capacity_stages_started": 1,
         "arms": {"control": {"pass": paid_pass, "restoration_complete": True,
                              "gates": {"paid": gates}}}},
    ]


def test_fresh_scope_preserves_consumed_history_and_exact_binding(area):
    binding, plan, old = area
    entry = policy.reserve(binding, plan)
    state = policy.read(policy.STATE)
    assert state[policy.BASE_LEDGER] == old[policy.BASE_LEDGER]
    assert state[entry["ledger"]]["paid_runs_started"] == 0
    assert state[entry["ledger"]]["binding"] == binding
    assert policy.scope_authorized(state, entry["ledger"], binding) == entry
    assert policy.base_ledger(entry["ledger"]) == policy.BASE_LEDGER
    assert runner.status()["reserved_seconds"] == 3600


def test_failed_restored_control_can_get_new_identity_without_human_approval(area):
    binding, plan, _ = area
    entry = policy.reserve(binding, plan)
    closed = policy.ActionGuard(entry["ledger"], binding).finish(reports(False), 900)
    assert closed["status"] == "FAILED_RESTORED"
    fresh = policy.reserve(binding, plan)
    assert fresh["ledger"] != entry["ledger"]
    assert runner.status()["reserved_seconds"] == 7200
    assert runner.status()["actual_elapsed_seconds"] == 900


@pytest.mark.parametrize("failure", ["restoration", "financial", "double_booking", "queue", "active", "no_report"])
def test_uncertain_integrity_or_ownership_blocks_following_load(area, failure):
    binding, plan, _ = area
    entry = policy.reserve(binding, plan)
    items = reports()
    if failure == "restoration":
        items[1]["arms"]["control"]["restoration_complete"] = False
    elif failure in {"financial", "double_booking", "queue"}:
        field = {"financial": "post_ttl_financial", "double_booking": "zero_double_booking",
                 "queue": "full_keyspace_queue_drain"}[failure]
        items[1]["arms"]["control"]["gates"]["paid"][field] = False
    elif failure == "active":
        state = policy.read(policy.STATE)
        state["current_run"] = "adr0151-" + "3" * 12
        policy.write(policy.STATE, state)
    else:
        items = []
    assert policy.ActionGuard(entry["ledger"], binding).finish(items, 100)["status"] == "RECOVERY_REQUIRED"
    with pytest.raises(ValueError):
        policy.reserve(binding, plan)


def test_consumed_reservation_cannot_be_replayed_or_refunded(area):
    binding, plan, _ = area
    entry = policy.reserve(binding, plan)
    guard = policy.ActionGuard(entry["ledger"], binding)
    guard.finish(reports(), 12)
    with pytest.raises(ValueError):
        policy.scope_authorized(policy.read(policy.STATE), entry["ledger"], binding)
    with pytest.raises(ValueError):
        guard.finish(reports(), 0)
    assert runner.status()["reserved_seconds"] == 3600


@pytest.mark.parametrize("limit", [3599, 3600, 7199])
def test_finite_cumulative_budget_is_conservative_and_not_refunded(area, limit):
    binding, plan, _ = area
    data = policy.read(policy.ENVELOPE)
    data["time"]["cumulative_experiment_seconds_limit"] = limit
    policy.write(policy.ENVELOPE, data)
    if limit < 3600:
        with pytest.raises(ValueError, match="budget"):
            policy.reserve(binding, plan)
    else:
        entry = policy.reserve(binding, plan)
        policy.ActionGuard(entry["ledger"], binding).finish(reports(False), 1)
        with pytest.raises(ValueError, match="budget"):
            policy.reserve(binding, plan)


@pytest.mark.parametrize("case", ["unclassified_pause", "human_state", "human_journal", "stop_file", "legacy_recovery"])
def test_pause_is_not_confused_with_scope_exhaustion(area, case):
    binding, plan, _ = area
    state = policy.read(policy.STATE)
    if case == "unclassified_pause":
        state.pop("cloud_load_stop_kind")
    elif case == "human_state":
        state["human_pause"] = True
    elif case == "human_journal":
        data = policy.read(policy.JOURNAL)
        data["human_pause"] = True
        policy.write(policy.JOURNAL, data)
    elif case == "stop_file":
        runner.set_pause(True)
    else:
        state[policy.BASE_LEDGER]["recovery_required"] = True
    policy.write(policy.STATE, state)
    with pytest.raises(ValueError):
        policy.reserve(binding, plan)


def test_live_pause_blocks_actions_but_not_owned_cleanup(area):
    binding, plan, _ = area
    entry = policy.reserve(binding, plan)
    guard = policy.ActionGuard(entry["ledger"], binding)
    session = Session.__new__(Session)
    session.action_guard, session.cleanup_mode = guard, False
    before = policy.read(policy.STATE)
    runner.set_pause(True)  # Works even with an active lock; touches only the stop marker.
    assert policy.read(policy.STATE) == before
    with pytest.raises(ValueError):
        session.check_action(45)
    session.begin_cleanup()
    session.check_action(180)
    runner.set_pause(False)
    guard.check()


def test_deadline_expiry_blocks_experiment_and_counts_cleanup_overrun(area):
    binding, plan, _ = area
    entry = policy.reserve(binding, plan)
    records = policy.read(policy.JOURNAL)
    records["experiments"][0]["started_at_utc"] = (datetime.now(UTC) - timedelta(seconds=3601)).isoformat()
    policy.write(policy.JOURNAL, records)
    with pytest.raises(ValueError, match="deadline"):
        policy.ActionGuard(entry["ledger"], binding).check()
    closed = policy.ActionGuard(entry["ledger"], binding).finish(reports(), 3650)
    assert closed["status"] == "FAILED_RESTORED"
    assert runner.status()["actual_elapsed_seconds"] == 3650


@pytest.mark.parametrize("field", ["configuration_sha256", "artifact_sha256", "adapter_identity"])
def test_configuration_artifact_or_harness_drift_rejects_scope(area, field):
    binding, plan, _ = area
    entry = policy.reserve(binding, plan)
    bad = copy.deepcopy(binding)
    bad[field] = "drift"
    with pytest.raises(ValueError):
        policy.scope_authorized(policy.read(policy.STATE), entry["ledger"], bad)


@pytest.mark.parametrize("change", ["cost", "resize", "gate", "profile", "schedule", "main", "unlimited_unapproved"])
def test_boundary_changes_require_explicit_qualification(area, change):
    data = policy.read(policy.ENVELOPE)
    if change == "cost":
        data["spending"]["maximum_new_infrastructure_spend"] = 1
    elif change == "resize":
        data["spending"]["allow_new_resources_or_resize"] = True
    elif change == "gate":
        data["customer_gates"]["zero_payment_loss"] = False
    elif change == "profile":
        data["qualified_profiles"].append("higher_load")
    elif change == "schedule":
        data["permissions"]["unattended_schedule"] = True
    elif change == "main":
        data["permissions"]["merge_main"] = True
    else:
        data["time"]["unlimited_cumulative_time_explicitly_authorized"] = False
    policy.write(policy.ENVELOPE, data)
    with pytest.raises(ValueError):
        policy.envelope()


def test_resource_change_rejected_before_reservation(area):
    binding, plan, _ = area
    binding["configuration_sha256"] = "9" * 64
    with pytest.raises(ValueError, match="resource"):
        policy.reserve(binding, plan)
    assert policy.read(policy.JOURNAL)["experiments"] == []


def test_crash_between_journal_and_scope_write_blocks_future_experiment(area, monkeypatch):
    binding, plan, _ = area
    original = policy.write
    def fail_state(path, value):
        if path == policy.STATE:
            raise OSError("synthetic crash")
        original(path, value)
    monkeypatch.setattr(policy, "write", fail_state)
    with pytest.raises(OSError):
        policy.reserve(binding, plan)
    assert policy.read(policy.JOURNAL)["experiments"][0]["status"] == "ACTIVE"
    monkeypatch.setattr(policy, "write", original)
    with pytest.raises(ValueError):
        policy.reserve(binding, plan)


def test_standing_validation_only_bypasses_exhausted_legacy_scope(area, monkeypatch):
    binding, plan, _ = area
    entry = policy.reserve(binding, plan)
    monkeypatch.setattr(engine, "LEDGER", entry["ledger"])
    monkeypatch.setattr(engine, "AUTHORIZATION", entry["authorization_id"])
    monkeypatch.setattr(engine, "ARMS", ("control",))
    engine.validate_release(policy.read(policy.STATE), binding, execute=False)
    state = policy.read(policy.STATE)
    state[entry["ledger"]]["standing_envelope"]["envelope_sha256"] = "forged"
    with pytest.raises(ValueError):
        engine.validate_release(state, binding, execute=False)


@pytest.mark.parametrize("branch", ["main", "master", "feature/unreviewed", "refs/heads/main", "codex/../main", "codex/"])
def test_publication_permissions_reject_other_destinations(area, branch):
    assert not policy.publication_allowed(branch, reviewed=True, sanitized=True)


def test_publication_requires_review_and_sanitization(area):
    assert policy.publication_allowed("codex/standing-work-envelope", reviewed=True, sanitized=True)
    assert not policy.publication_allowed("codex/standing-work-envelope", reviewed=False, sanitized=True)
    assert not policy.publication_allowed("codex/standing-work-envelope", reviewed=True, sanitized=False)


def test_fresh_stage_names_preserve_contract_and_diagnostic_collector(area):
    import run_slow_database_diagnostics
    import slow_database_contract

    plan = slow_database_contract.plan()
    isolated = run_slow_database_diagnostics.create_runner()
    contract = isolated.StatusRefreshContract(plan["artifact_receipt"], "control",
                                              plan["expected_runtime_source_sha256"])
    isolated.LEDGER = policy.BASE_LEDGER + "__" + "a" * 12
    stage = isolated.RefreshStages(False, {}, contract, "adr0151-" + "a" * 12)
    assert stage.ledger_key == isolated.LEDGER
    assert policy.base_ledger(stage.ledger_key) == policy.BASE_LEDGER
    assert "slow_database_capture" in Path("scripts/run_two_host_paid_comparison.py").read_text()


@pytest.mark.parametrize("qualification_pass,measured_pass,expected_calls", [
    (False, True, [False]), (True, False, [False, True]), (True, True, [False, True])])
def test_single_command_qualifies_then_executes_only_on_pass(area, tmp_path, monkeypatch,
                                                           qualification_pass, measured_pass, expected_calls):
    import run_slow_database_diagnostics
    import slow_database_contract

    binding, plan, old = area
    policy.LOCK.unlink()  # The orchestration owns its allocation lock.
    calls = []
    outcomes = reports(measured_pass)
    outcomes[0]["pass"] = qualification_pass
    fake = SimpleNamespace(
        source_contract=dict,
        StatusRefreshContract=lambda *_a: object(),
        RefreshStages=lambda *_a: None,
        binding_for=lambda *_a: binding,
        comparison=SimpleNamespace(
            validate_config=lambda _c: None,
            frozen_bundle=dict,
            subprocess=SimpleNamespace(run=lambda *_a, **_k: None, DEVNULL=-3)))
    def protocol(*_a, execute, qualification=None):
        fake.ENVELOPE_GUARD.check()
        calls.append(execute)
        if execute:
            assert qualification is outcomes[0] and qualification["pass"] is True
        return outcomes[1 if execute else 0]
    fake.protocol = protocol
    monkeypatch.setattr(run_slow_database_diagnostics, "create_runner", lambda: fake)
    monkeypatch.setattr(slow_database_contract, "plan", lambda: plan)
    # Use the real shared RunLock against an isolated test path.
    monkeypatch.setattr(engine, "LOCK", policy.LOCK)
    for name in ("config", "artifact"):
        policy.write(tmp_path / name, {})
    result = runner.execute(tmp_path / "config", tmp_path / "artifact", tmp_path)
    assert calls == expected_calls
    assert result["status"] == ("PASSED_RESTORED" if qualification_pass and measured_pass else
                                "FAILED_RESTORED" if qualification_pass else "RECOVERY_REQUIRED")
    assert not policy.LOCK.exists()
    assert policy.read(policy.STATE)[policy.BASE_LEDGER] == old[policy.BASE_LEDGER]
    assert len(policy.read(policy.JOURNAL)["experiments"]) == 1


def test_guard_rejection_before_ssh_constructor_opens_no_connections(area, monkeypatch, tmp_path):
    calls = []
    monkeypatch.setattr(Session, "_open", lambda *_a: calls.append("ssh"))
    guard = SimpleNamespace(check=lambda *_a: (_ for _ in ()).throw(TimeoutError("expired")))
    with pytest.raises(TimeoutError):
        Session({}, tmp_path, "synthetic-test-password", action_guard=guard)
    assert calls == []


def test_monotonic_timeout_reserves_time_for_whole_remote_action(area, monkeypatch):
    binding, plan, _ = area
    entry = policy.reserve(binding, plan)
    guard = policy.ActionGuard(entry["ledger"], binding)
    monkeypatch.setattr(policy.time, "monotonic", lambda: guard.deadline - 30)
    with pytest.raises(TimeoutError):
        guard.check(45)



def test_legacy_release_cannot_bypass_enabled_standing_envelope(area):
    binding, _, _ = area
    state = {"standing_work_envelope": {"decision": "ADR0172"},
             "cloud_load_requires_resume": False, "current_run": None}
    with pytest.raises(ValueError, match="standing-envelope"):
        engine.validate_release(state, binding, execute=False)


def test_foreign_lock_cannot_reserve_a_cloud_experiment(area):
    binding, plan, _ = area
    policy.write(policy.LOCK, {"pid": os.getpid() + 1000000, "run": "adr0151-" + "a" * 12})
    with pytest.raises(ValueError, match="owned"):
        policy.reserve(binding, plan)
    assert policy.read(policy.JOURNAL)["experiments"] == []


@pytest.mark.parametrize("remote_ref,oid", [
    ("refs/heads/main", "a" * 40), ("refs/tags/v1", "a" * 40),
    ("refs/heads/codex/example", "0" * 40)])
def test_actual_publication_hook_rejects_main_tags_and_deletion(remote_ref, oid):
    import shutil
    import subprocess

    shell = shutil.which("sh")
    if shell is None and shutil.which("git"):
        bundled = Path(shutil.which("git")).resolve().parent.parent / "bin/sh.exe"
        if bundled.is_file():
            shell = str(bundled)
    if shell is None:
        pytest.skip("POSIX shell unavailable; hook enforcement is checked on actual publication")
    result = subprocess.run([shell, ".githooks/pre-push"],
                            input=f"refs/heads/codex/example {oid} {remote_ref} {'b' * 40}\n",
                            text=True, capture_output=True, check=False)
    assert result.returncode != 0
    assert "requires approval" in result.stderr or "deletion" in result.stderr



@pytest.mark.parametrize("lost", ["current_run", "active_run"])
def test_runtime_ownership_drift_blocks_further_actions(area, lost):
    binding, plan, _ = area
    entry = policy.reserve(binding, plan)
    state = policy.read(policy.STATE)
    state["current_run"] = state[entry["ledger"]]["active_run"] = "adr0151-" + "a" * 12
    if lost == "current_run":
        state["current_run"] = None
    else:
        state[entry["ledger"]]["active_run"] = None
    policy.write(policy.STATE, state)
    with pytest.raises(ValueError, match="ownership"):
        policy.ActionGuard(entry["ledger"], binding).check()

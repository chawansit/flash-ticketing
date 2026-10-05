import copy
import json
import subprocess
import sys
from pathlib import Path
from types import SimpleNamespace

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[2] / "scripts"))
import run_two_host_paid_comparison as comparison
import run_two_host_rate_probe as probe
from qualify_two_host_deployment import IMAGE


@pytest.fixture
def baseline():
    path = comparison.ROOT / "docs/capacity/flash-sale-opening/two-host-paid-replacement-comparison-2026-10-05.json"
    return json.loads(path.read_text())


def authorized():
    return {"cloud_load_requires_resume": False, "current_run": None,
            probe.LEDGER: {"authorization_id": probe.AUTHORIZATION_ID, "qualification_runs_authorized": 1,
                          "paid_runs_authorized": 1, "paid_runs_started": 0}}


def test_original_defaults_and_distinct_probe_expectations():
    original = comparison.Stages(True, {})
    candidate = probe.ProbeStages(True, {})
    assert (original.rate, original.expected_tickets, original.ledger_key, original.stage_limit) == (60, 18000, "bounded_control", 2)
    assert (candidate.rate, candidate.expected_tickets, candidate.ledger_key, candidate.stage_limit) == (84, 25200, "bounded_rate_probe", 1)


@pytest.mark.parametrize("kwargs", [{"rate": 100}, {"rate": True}, {"stage_limit": True},
                                     {"rate": 84}, {"ledger_key": "arbitrary"},
                                     {"rate": 84, "ledger_key": "bounded_rate_probe", "stage_limit": 2}])
def test_unapproved_contracts_rejected(kwargs):
    with pytest.raises(ValueError, match="bounded stage"):
        comparison.Stages(True, {}, **kwargs)


@pytest.mark.parametrize("field,value", [("paid_runs_started", 1), ("paid_runs_started", None),
                                         ("paid_runs_started", False), ("paid_runs_authorized", 0),
                                         ("paid_runs_authorized", True), ("qualification_runs_authorized", 0),
                                         ("qualification_runs_authorized", True), ("authorization_id", "old-decision")])
def test_consumed_or_unapproved_ledger_rejected_before_execution(baseline, field, value):
    state = authorized()
    state[probe.LEDGER][field] = value
    with pytest.raises(ValueError):
        probe.validate_release(state, baseline, execute=True)


@pytest.mark.parametrize("field,value", [("cloud_load_requires_resume", True), ("current_run", "active")])
def test_pause_and_active_run_rejected(baseline, field, value):
    state = authorized()
    state[field] = value
    with pytest.raises(ValueError):
        probe.validate_release(state, baseline, execute=False)


@pytest.mark.parametrize("case", ["failed", "restore", "missing", "false", "foreign-name"])
def test_baseline_requires_all_known_gates_and_restoration(baseline, case):
    changed = copy.deepcopy(baseline)
    if case == "failed":
        changed["pass"] = False
    elif case == "restore":
        changed["restore_pass"] = False
    else:
        gates = changed["gates"]["candidate"]["paid"]
        if case == "missing":
            gates.pop("zero_double_booking")
        elif case == "false":
            gates["zero_double_booking"] = False
        else:
            gates["foreign"] = gates.pop("zero_double_booking")
    with pytest.raises(ValueError):
        probe.validate_release(authorized(), changed, execute=True)


def test_release_does_not_reset_consumed_comparison_ledger(baseline):
    state = authorized()
    state["bounded_control"] = {"paid_runs_started": 2, "attempted_paid_arms": ["control", "candidate"]}
    before = copy.deepcopy(state)
    probe.validate_release(state, baseline, execute=True)
    assert state == before


@pytest.mark.parametrize("expected", [18000, 25200])
def test_financial_audit_executes_matching_paid_expectation(monkeypatch, capsys, expected):
    calls = []
    class Connection:
        def __enter__(self):
            return self
        def __exit__(self, *_):
            return False
        def execute(self, sql, args=None):
            self.sql = sql
            return self
        def fetchone(self):
            return (True,) if "max(expires_at)<" in self.sql else (0,)
    def audit(conn, events, orders, paid, duplicates):
        calls.append((events, orders, paid, duplicates))
        return {"pass": True}
    monkeypatch.setitem(sys.modules, "psycopg", SimpleNamespace(connect=lambda *_args, **_kwargs: Connection()))
    monkeypatch.setitem(sys.modules, "audit_checkout_smoke", SimpleNamespace(audit=audit))
    monkeypatch.setenv("DATABASE_URL", "synthetic")
    monkeypatch.setattr(comparison.time, "sleep", lambda _: None)
    exec(compile(comparison.financial_audit_program(["event"], expected), "audit", "exec"), {})  # noqa: S102
    assert calls == [(["event"], expected, expected, 1)]
    assert json.loads(capsys.readouterr().out)["hold_deadlines_elapsed"] is True


@pytest.mark.parametrize("value", [0, 25199, 30000, True])
def test_invalid_financial_expectation_rejected(value):
    with pytest.raises(ValueError):
        comparison.financial_audit_program([], value)


@pytest.mark.parametrize("case", ["changed-image", "changed-service", "missing-consumer", "command-failure", "invalid-id", "changed-source"])
def test_worker_log_collection_rejects_changed_identity_or_missing_coverage(tmp_path, monkeypatch, case):
    rows = [{"id": f"{i:064x}", "role": "consumer" if i < 6 else "simulator"} for i in range(7)]
    if case == "missing-consumer":
        rows.pop(0)
    if case == "invalid-id":
        rows[0]["id"] = "not-a-container"
    spec = tmp_path / "spec.json"
    spec.write_text(json.dumps({"containers": rows}))
    def inspect(args, **kwargs):
        if args[1] == "exec":
            return json.dumps({"source_match": case != "changed-source"})
        cid = args[-1]
        role = next(r["role"] for r in rows if r["id"] == cid)
        return json.dumps([{"Id": cid, "Image": "changed" if case == "changed-image" else IMAGE,
                            "Config": {"Labels": {"com.docker.compose.service": "api" if case == "changed-service" else role}}}])
    monkeypatch.setattr(subprocess, "check_output", inspect)
    monkeypatch.setattr(subprocess, "run", lambda *_args, **_kwargs: SimpleNamespace(returncode=int(case == "command-failure"), stdout=b"", stderr=b""))
    with pytest.raises(ValueError):
        exec(compile(probe.worker_logs_program(str(spec), "2026-10-05T00:00:00+00:00", {"consumer": IMAGE, "simulator": IMAGE}), "logs", "exec"), {})  # noqa: S102


@pytest.mark.parametrize("distinct_images", [False, True])
def test_worker_log_collection_is_time_scoped_bounded_and_read_only(tmp_path, monkeypatch, capsys, distinct_images):
    images = {"consumer": "sha256:" + "1" * 64, "simulator": "sha256:" + "2" * 64} if distinct_images else {"consumer": IMAGE, "simulator": IMAGE}
    rows = [{"id": f"{i:064x}", "role": "consumer" if i < 6 else "simulator"} for i in range(7)]
    spec = tmp_path / "spec.json"
    spec.write_text(json.dumps({"containers": rows}))
    calls = []
    def inspect(args, **kwargs):
        if args[1] == "exec":
            return json.dumps({"source_match": True})
        cid = args[-1]
        role = next(r["role"] for r in rows if r["id"] == cid)
        return json.dumps([{"Id": cid, "Image": images[role], "Config": {"Labels": {"com.docker.compose.service": role}}}])
    def logs(args, **kwargs):
        calls.append(args)
        assert args[0:2] == ["docker", "logs"] and "--since" in args and args[args.index("--tail") + 1] == "200"
        return SimpleNamespace(returncode=0, stdout=b"x" * 20000, stderr=b"HTTP Error 503")
    monkeypatch.setattr(subprocess, "check_output", inspect)
    monkeypatch.setattr(subprocess, "run", logs)
    exec(compile(probe.worker_logs_program(str(spec), "2026-10-05T00:00:00+00:00", images), "logs", "exec"), {})  # noqa: S102
    result = json.loads(capsys.readouterr().out)
    assert len(calls) == len(result["logs"]) == 7
    assert all(len(row["text"]) == 16384 and row["tail_truncated"] for row in result["logs"])
    assert probe.diagnostic_summary(result["logs"])["http_status_literal_occurrences"] == {"503": 7}


def test_control_hook_never_launches_a_capacity_arm():
    stage = probe.ProbeStages(True, {})
    stage(None, "control", None, None, None, None)
    assert stage.results == {}
    with pytest.raises(ValueError, match="Candidate-only"):
        stage(None, "unexpected", None, None, None, None)


@pytest.mark.parametrize("field,value", [("pass", False), ("restore_pass", False),
                                         ("worker_error_evidence_pass", False),
                                         ("capacity_stages_started", 1), ("capacity_stages_started", False),
                                         ("adapter_identity", {"changed": "hash"}), ("kind", "old")])
def test_stale_or_incomplete_dry_qualification_rejected(field, value):
    report = {"pass": True, "kind": "two_host_84_buyer_probe_dry", "restore_pass": True,
              "capacity_stages_started": 0, "worker_error_evidence_pass": True, "adapter_identity": {"pinned": "hash"}}
    assert probe.qualification_matches(report, {"pinned": "hash"})
    report[field] = value
    assert not probe.qualification_matches(report, {"pinned": "hash"})


@pytest.mark.parametrize("dry_pass", [True, False])
def test_one_command_qualifies_then_conditionally_dispatches(monkeypatch, tmp_path, baseline, dry_pass):
    state = authorized()
    state["bounded_control"] = {"comparison_report": "baseline.json", "paid_runs_started": 2}
    state_path = tmp_path / "docs/capacity/CURRENT_STATE.json"
    state_path.parent.mkdir(parents=True)
    state_path.write_text(json.dumps(state))
    (tmp_path / "baseline.json").write_text(json.dumps(baseline))
    config_path = tmp_path / "config.json"
    config_path.write_text(json.dumps({
        "primary": {"repo": "/root/primary", "private_ipv4": "10.0.0.1"},
        "secondary": {"prepared_directory": "/root/secondary", "private_ipv4": "10.0.0.2"},
        "generator": {"repo": "/root/generator", "private_ipv4": "10.0.0.3"}}))
    monkeypatch.setattr(probe, "ROOT", tmp_path)
    monkeypatch.setattr(probe, "identity", lambda: {"pinned": "hash"})
    monkeypatch.setattr(comparison, "frozen_bundle", dict)
    monkeypatch.setattr(sys, "argv", ["probe", "--config", str(config_path), "--ssh-runtime", str(tmp_path), "--execute"])
    calls = []
    def protocol(config, bundle, *, execute):
        calls.append(execute)
        return {"pass": dry_pass, "kind": "two_host_84_buyer_probe_dry", "restore_pass": True,
                "capacity_stages_started": 0, "worker_error_evidence_pass": True,
                "adapter_identity": {"pinned": "hash"}}, tmp_path / "report.json"
    monkeypatch.setattr(probe, "protocol", protocol)
    if dry_pass:
        probe.main()
        assert calls == [False, True]
    else:
        with pytest.raises(SystemExit):
            probe.main()
        assert calls == [False]
    assert json.loads(state_path.read_text())["bounded_control"]["paid_runs_started"] == 2


def test_missing_logs_after_success_fail_stage_before_restoration(monkeypatch, tmp_path):
    record = {"pass": True}
    def passed(self, session, arm, routes, saved, owner, output):
        (output / arm).mkdir()
        self.results[arm] = record
    monkeypatch.setattr(comparison.Stages, "__call__", passed)
    def unavailable(*_args):
        raise RuntimeError("log unavailable")
    session = SimpleNamespace(call=unavailable, state={}, checkpoint=lambda: None)
    stage = probe.ProbeStages(False, {})
    with pytest.raises(ValueError, match="evidence missing"):
        stage(session, "candidate", None, {"model": {"services": {role: {"image": IMAGE} for role in ("consumer", "simulator")}}}, "/owned", tmp_path)
    assert record["pass"] is False and record["worker_error_evidence"]["pass"] is False
    assert json.loads((tmp_path / "candidate/stage.private.json").read_text())["pass"] is False


@pytest.fixture(scope="module")
def frozen_coordinator(tmp_path_factory):
    import run_synchronized_paid_generator as clock
    path = tmp_path_factory.mktemp("frozen-rate-probe") / "paid_ticket_sharded_generator.py"
    path.write_bytes(comparison.frozen_bundle()["scripts/paid_ticket_sharded_generator.py"])
    return clock.load(path, 1030, now=1000)


def test_frozen_coordinator_rejects_odd_rate_before_launch(frozen_coordinator):
    import asyncio
    with pytest.raises(ValueError, match="even positive rate"):
        asyncio.run(frozen_coordinator.run(SimpleNamespace(rate=85, concurrency=500), {}))


def test_prepared84_rate_has_disjoint_sufficient_frozen_shards(frozen_coordinator):
    manifest = {"schema_version": 1, "environment": "development",
                "show_ids": [f"show-{i}" for i in range(84)],
                "viewer_tokens": [f"synthetic-{i}" for i in range(25200)],
                "seats_per_show": 300, "seat_offset": 0}
    shards = frozen_coordinator.split_manifest(manifest, 2, 12600)
    assert all(len(row["show_ids"]) == 42 and len(row["viewer_tokens"]) == 12600 for row in shards)
    assert set(shards[0]["show_ids"]).isdisjoint(shards[1]["show_ids"])
    assert set(shards[0]["viewer_tokens"]).isdisjoint(shards[1]["viewer_tokens"])
    insufficient = {**manifest, "show_ids": manifest["show_ids"][:-1]}
    with pytest.raises(ValueError, match="insufficient distinct seats"):
        frozen_coordinator.split_manifest(insufficient, 2, 12600)


@pytest.mark.parametrize("field", ["restore_pass", "primary_runtime_semantics_restored",
                                    "secondary_resources_removed", "generator_idle_after",
                                    "credential_snapshots_removed", "restored_global_queues"])
def test_dry_release_requires_actual_full_restore_and_secret_cleanup(field):
    result = {"restore_pass": True, "primary_runtime_semantics_restored": True,
              "secondary_resources_removed": True, "generator_idle_after": True,
              "credential_snapshots_removed": True, "restored_global_queues": {"pass": True}}
    assert probe.restoration_complete(result)
    result.pop(field)
    assert not probe.restoration_complete(result)


@pytest.mark.parametrize("images", [{}, {"consumer": IMAGE},
                                     {"consumer": "floating:latest", "simulator": IMAGE}])
def test_worker_diagnostics_require_saved_immutable_role_images(images):
    with pytest.raises(ValueError, match="Immutable"):
        probe.worker_logs_program("/owned/spec", "2026-10-05T00:00:00Z", images)

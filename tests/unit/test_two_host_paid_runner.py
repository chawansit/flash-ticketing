import json
import sys
from pathlib import Path
from types import SimpleNamespace

import pytest
from test_two_host_scaling_preparation import inventory

sys.path.insert(0, str(Path(__file__).resolve().parents[2] / "scripts"))
import collect_two_host_inventory as collector
import run_synchronized_paid_generator as clock
import run_two_host_paid_comparison as runner


def env():
    return {
        "DATABASE_URL": "postgresql://user:synthetic-secret@pgbouncer:5432/ticketing?sslmode=disable",
        "REDIS_URL": "redis://:synthetic-redis@10.0.0.5:6379/0",
        "JWT_SECRET": "synthetic-jwt",
        "PAYMENT_CALLBACK_SECRET": "synthetic-callback",
        "ENVIRONMENT": "development",
        "DB_POOL_MAX": "3",
    }


def test_authority_preserves_credentials_name_options_but_allows_private_host_only():
    baseline = collector.authorities(env())
    candidate = env()
    candidate["DATABASE_URL"] = candidate["DATABASE_URL"].replace("pgbouncer", "10.0.0.1")
    candidate["DB_POOL_MAX"] = "4"
    assert collector.authorities(candidate) == baseline
    assert "synthetic" not in json.dumps(baseline)


@pytest.mark.parametrize(
    "field,value",
    [
        ("DATABASE_URL", "postgresql://user:changed@pgbouncer:5432/ticketing?sslmode=disable"),
        ("DATABASE_URL", "postgresql://user:synthetic-secret@pgbouncer:5432/another?sslmode=disable"),
        ("DATABASE_URL", "postgresql://user:synthetic-secret@pgbouncer:5432/ticketing?sslmode=verify-full"),
        ("REDIS_URL", "redis://:changed@10.0.0.5:6379/0"),
        ("JWT_SECRET", "changed"),
        ("ENVIRONMENT", "production"),
    ],
)
def test_authority_cannot_hide_shared_state_auth_or_tls_changes(field, value):
    changed = env()
    changed[field] = value
    assert collector.authorities(changed) != collector.authorities(env())


@pytest.fixture(scope="module")
def bundle():
    return runner.frozen_bundle()


def test_frozen78_files_and_common_clock_keep_original_children(bundle, tmp_path):
    path = tmp_path / "paid_ticket_sharded_generator.py"
    path.write_bytes(bundle["scripts/paid_ticket_sharded_generator.py"])
    frozen = clock.load(path, 1030, now=1000)
    assert frozen.time() + 8 == 1030
    assert "start_at = time() + 8" in path.read_text()
    assert len(bundle) == 78


@pytest.mark.parametrize("start", [999, 1002, 1061, float("nan"), float("inf")])
def test_common_clock_rejects_missed_far_or_invalid_deadline(bundle, tmp_path, start):
    path = tmp_path / "paid_ticket_sharded_generator.py"
    path.write_bytes(bundle["scripts/paid_ticket_sharded_generator.py"])
    with pytest.raises(ValueError, match="Common start"):
        clock.load(path, start, now=1000)


def test_frozen_coordinator_rejects_changed_source(bundle, tmp_path):
    path = tmp_path / "paid_ticket_sharded_generator.py"
    path.write_bytes(bundle["scripts/paid_ticket_sharded_generator.py"] + b"\n# changed\n")
    with pytest.raises(ValueError, match="source"):
        clock.load(path, 1030, now=1000)


def gate_evidence():
    record = {
        "inventory_contract": {"inventory_contract_pass": True},
        "customer": {"pass": True},
        "financial": {"pass": True, "duplicate_booked_seats": 0, "hold_deadlines_elapsed": True},
        "global_queues": {"pass": True, "kafka_total_lag": 0},
        "cpu_window": {"both_hosts_cpu_observed": True, "same_offered_measurement_window": True},
        "distribution": {"all_four_replicas_observed": True, "per_replica_traffic_distribution": True},
        "frozen_helpers_verified": True,
        "transferred_generator_identity": True,
        "pinned_harness_files": 78,
        "private_cleanup_pass": True,
        "pipeline_summary": {
            "api": {"observed_replicas": 4},
            "consumer": {"max_replicas": 6},
            "api_metrics_errors": 0,
            "database_errors": 0,
            "resources": {"pass": True},
            "role_database": {"writer": {"observed_replicas": 3, "counter_reset_detected": False}},
            "writer_phases": {"counter_reset_detected": False},
        },
        "kafka_summary": {
            "samples": 10,
            "sample_errors": 0,
            "members_min": 6,
            "members_max": 6,
            "ownership_observed": True,
            "max_partitions_per_member": 1,
            "last_member_partition_groups": [[i] for i in range(6)],
        },
    }
    restoration = {
        "restore_pass": True,
        "primary_runtime_semantics_restored": True,
        "generator_idle_after": True,
        "credential_snapshots_removed": True,
        "secondary_resources_removed": True,
        "safety": {
            "one_durable_owner_before_payment": True,
            "accepted_holds": 1,
            "cross_host_hold_replay": True,
            "cross_host_payment_replay": True,
        },
    }
    return record, inventory(), restoration


def test_all22_and10_gates_preserved_and_customer_failure_not_erased():
    record, inv, restore = gate_evidence()
    view = runner.gates_for_stage(record, inv, restore)
    assert len(view["paid"]) == 22 and len(view["additional"]) == 10 and view["all_required_gates_pass"]
    record["customer"]["pass"] = False
    view = runner.gates_for_stage(record, inv, restore)
    assert view["failed_gates"] == ["customer_load"] and not view["all_required_gates_pass"]


@pytest.mark.parametrize(
    "section,field",
    [
        ("financial", "hold_deadlines_elapsed"),
        ("financial", "pass"),
        ("global_queues", "pass"),
        ("cpu_window", "same_offered_measurement_window"),
        ("distribution", "all_four_replicas_observed"),
    ],
)
def test_missing_or_failed_correctness_and_observer_evidence_never_passes(section, field):
    record, inv, restore = gate_evidence()
    record[section].pop(field)
    assert not runner.gates_for_stage(record, inv, restore)["all_required_gates_pass"]


@pytest.mark.parametrize(
    "field",
    ["restore_pass", "generator_idle_after", "credential_snapshots_removed", "secondary_resources_removed"],
)
def test_failed_restore_cleanup_or_generator_gate_is_not_hidden(field):
    record, inv, restore = gate_evidence()
    restore[field] = False
    assert not runner.gates_for_stage(record, inv, restore)["all_required_gates_pass"]


@pytest.mark.parametrize("transient", [False, True])
def test_job_environment_keeps_database_variable_names_and_separate_logs(monkeypatch, tmp_path, capsys, transient):
    import pathlib
    import subprocess

    observed = []
    real_path = pathlib.Path

    class Stat:
        def __truediv__(self, value):
            return self

        def read_bytes(self):
            self.reads = getattr(self, "reads", 0) + 1
            if transient and self.reads == 1:
                return b""
            return b"\0".join(a.encode() for a in observed[-1][0]) + b"\0"

        def read_text(self):
            return "42 (owned) " + " ".join(["S"] + ["0"] * 18 + ["99"])

    def paths(value):
        return Stat() if str(value).startswith("/proc/") else real_path(value)

    def spawn(args, **kwargs):
        observed.append((args, kwargs))
        return SimpleNamespace(pid=42)

    monkeypatch.setenv("DATABASE_URL", "private-test-dsn")
    monkeypatch.setattr(pathlib, "Path", paths)
    monkeypatch.setattr(subprocess, "Popen", spawn)
    for name in ("pipeline", "kafka"):
        code = runner.job_program(["python", str(tmp_path / (name + ".py"))], str(tmp_path), database=True)
        exec(compile(code, "owned_job", "exec"), {})  # noqa: S102 - trusted embedded code, mocked process
    assert all(
        v[1]["env"]["TEST_DATABASE_URL"] == "private-test-dsn" and v[1]["start_new_session"] for v in observed
    )
    assert {p.name for p in tmp_path.iterdir()} == {
        "job-pipeline.log", "job-kafka.log", "job-pipeline-identity.json", "job-kafka-identity.json"
    }
    for name in ("pipeline", "kafka"):
        job = json.loads((tmp_path / ("job-" + name + "-identity.json")).read_text())
        assert job["start_ticks"] == 99 and len(job["command_sha256"]) == 64
    assert "private-test-dsn" not in capsys.readouterr().out


@pytest.mark.parametrize("reuse,wrong_command", [(True, False), (False, True)])
def test_owned_stop_refuses_reused_pid_or_different_command(monkeypatch, reuse, wrong_command):
    import os
    import pathlib

    signals = []

    class FakePath:
        def __init__(self, *parts):
            self.parts = parts

        def __truediv__(self, v):
            return FakePath(*self.parts, v)

        def read_text(self):
            return "42 (owned) " + " ".join(["S"] + ["0"] * 18 + [str(100 if reuse else 99)])

        def read_bytes(self):
            return b"unrelated" if wrong_command else b"/tmp/owned/script.py"

    monkeypatch.setattr(pathlib, "Path", FakePath)
    monkeypatch.setattr(os, "killpg", lambda *v: signals.append(v), raising=False)
    code = runner.process_program({"pid": 42, "start_ticks": 99, "identity_path": "/tmp/owned"}, stop=True)
    with pytest.raises(ValueError):
        exec(compile(code, "owned_stop", "exec"), {})  # noqa: S102 - trusted embedded code with mocked filesystem/process
    assert not signals


def test_missing_generator_workspace_is_rejected_before_cloud_mutation():
    config = {
        "primary": {"repo": "/root/app", "private_ipv4": "10.0.0.1"},
        "secondary": {"prepared_directory": "/root/secondary", "private_ipv4": "10.0.0.2"},
        "generator": {"private_ipv4": "10.0.0.3"},
    }
    with pytest.raises(ValueError, match="generator"):
        runner.validate_config(config)
    config["generator"]["repo"] = "/root/generator"
    runner.validate_config(config)


@pytest.mark.parametrize("returncode", [1, 137])
def test_completed_job_nonzero_receipt_cannot_be_treated_as_success(monkeypatch, returncode):
    class Session:
        def call(self, *args):
            return {"running": False}

    monkeypatch.setattr(runner, "fetch", lambda *args: json.dumps({"returncode": returncode}).encode())
    with pytest.raises(ValueError, match="nonzero"):
        runner.Stages(False, {}).wait(Session(), {"status_path": "/tmp/owned/exit.json"}, "secondary", 1)


def test_completed_job_without_receipt_cannot_be_treated_as_success(monkeypatch):
    class Session:
        def call(self, *args):
            return {"running": False}

    def absent(*args):
        raise FileNotFoundError("receipt absent")

    monkeypatch.setattr(runner, "fetch", absent)
    with pytest.raises(FileNotFoundError):
        runner.Stages(False, {}).wait(Session(), {"status_path": "/tmp/owned/exit.json"}, "secondary", 1)


@pytest.mark.parametrize("empty_before_signal", [False, True])
def test_empty_command_is_only_tolerated_after_validated_signal(monkeypatch, capsys, empty_before_signal):
    import os
    import pathlib

    signals = []
    terminating_reads = []

    class FakePath:
        def __init__(self, *parts):
            self.parts = parts

        def __truediv__(self, value):
            return FakePath(*self.parts, value)

        def read_text(self):
            state = "S"
            if signals:
                terminating_reads.append(1)
                if len(terminating_reads) > 1:
                    state = "Z"
            return "42 (owned) " + " ".join([state] + ["0"] * 18 + ["99"])

        def read_bytes(self):
            return b"" if signals or empty_before_signal else b"/tmp/owned/script.py"

    monkeypatch.setattr(pathlib, "Path", FakePath)
    monkeypatch.setattr(os, "getpgid", lambda pid: pid, raising=False)
    monkeypatch.setattr(os, "killpg", lambda *args: signals.append(args), raising=False)
    code = runner.process_program({"pid": 42, "start_ticks": 99, "identity_path": "/tmp/owned"}, stop=True)
    if empty_before_signal:
        with pytest.raises(ValueError, match="command"):
            exec(compile(code, "owned_stop", "exec"), {})  # noqa: S102
        assert not signals
    else:
        exec(compile(code, "owned_stop", "exec"), {})  # noqa: S102
        assert len(signals) == 1 and json.loads(capsys.readouterr().out)["running"] is False



def test_report_separates_customer_failure_drops_and_durable_tickets():
    import summarize_two_host_paid_comparison as summary

    stage = {
        "customer": {"dispatched": 100, "generator_drops": 3, "fulfilled_by_deadline": 98,
                     "outcomes": {"fulfilled": 99, "order_http_503": 1},
                     "physical_http_attempts": {"orders": 800, "payments": 99}},
        "financial": {"tickets": 100, "pass": True},
    }
    report = summary.compact(stage)
    assert report["failed_dispatched_journeys"] == 1
    assert report["errors_per_dispatched_journey_percent"] == 1
    assert report["generator_drops"] == 3
    assert report["customer_confirmations_by_deadline"] == 98
    assert report["financial"]["tickets"] == 100
    assert report["customer_http_attempts_total"] == 899



def test_lost_launch_reply_recovers_owned_identity_without_reporting_success():
    owned = {"pid": 42, "start_ticks": 99, "identity_path": "/tmp/owned"}

    class Session:
        calls = 0

        def api(self, *args):
            self.calls += 1
            if self.calls == 1:
                raise RuntimeError("Lost launch response")
            return owned

    jobs, record = [], {}
    with pytest.raises(RuntimeError, match="Lost"):
        runner.Stages(False, {}).launch(Session(), "container", "cid", ["python", "/tmp/owned/observer.py"], "/tmp/owned", jobs, record)
    assert jobs == [("container", owned)]
    assert record["recovered_launch_roles"] == ["container"]



def test_every_pinned_ssh_client_gets_keepalive_without_replaying_actions(monkeypatch, tmp_path):
    import qualify_two_host_deployment as deployment

    clients = []

    class Client:
        def __init__(self):
            self.intervals, self.connections, self.closed = [], [], False
            clients.append(self)

        def load_host_keys(self, path):
            assert path == "pinned-hosts"

        def set_missing_host_key_policy(self, policy):
            assert policy == "reject-changed-host"

        def connect(self, host, **kwargs):
            self.connections.append(host)
            assert kwargs["username"] == "root" and not kwargs["look_for_keys"] and not kwargs["allow_agent"]

        def get_transport(self):
            return self

        def set_keepalive(self, seconds):
            self.intervals.append(seconds)

        def close(self):
            self.closed = True

    monkeypatch.setitem(sys.modules, "paramiko", SimpleNamespace(SSHClient=Client, RejectPolicy=lambda: "reject-changed-host"))
    config = {"known_hosts": "pinned-hosts", **{role: {"host": role} for role in ("primary", "secondary", "generator")}}
    session = deployment.Session(config, tmp_path, "synthetic-secret")
    assert all(client.intervals == [30] and len(client.connections) == 1 for client in clients)
    session.close()
    assert all(client.closed for client in clients)

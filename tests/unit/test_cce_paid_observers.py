"""Native CPU, coverage and background identities; offline only."""

import copy
import json
import sys
from pathlib import Path
from types import SimpleNamespace

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[2] / "scripts"))
import cce_paid_observers as observations
import observe_two_host_cpu as cpu
from cce_paid_profiles import SHORT
from test_observe_cce_paid_pipeline import bundle


def trace():
    starts = observations.native.validate_receipts(bundle())
    return [
        {
            "utc": f"2026-10-08T12:00:{i:02d}+00:00",
            "api_replicas": {
                address: {
                    "process_start_time_seconds": start,
                    "business_http_requests_total": i,
                    "process_cpu_seconds_total": i * 0.25,
                }
                for address, start in starts.items()
            },
        }
        for i in range(4)
    ]


def snapshots():
    counts = {
        "consumer": 6,
        "reservation-writer": 3,
        "maintenance": 1,
        "publisher": 1,
        "reconciler": 1,
        "simulator": 1,
        "confirmation": 1,
        "kafka": 1,
        "pgbouncer": 1,
        "load-balancer": 1,
        "cce-audit": 1,
        "cce-pooler": 1,
    }
    rows = []
    for role, count in counts.items():
        for _ in range(count):
            labels = (
                {"codex-purpose": role, "codex-owner": bundle()["run"],
                 "com.docker.compose.project": "cce-" + bundle()["run"],
                 "com.docker.compose.service": "audit-helper" if role == "cce-audit" else "pooler-bridge"}
                if role.startswith("cce-")
                else {"com.docker.compose.service": role, "com.docker.compose.project": "flash-ticketing"}
            )
            rows.append(
                {
                    "Id": f"{len(rows) + 1:064x}",
                    "Image": "sha256:" + "f" * 64,
                    "Config": {"Labels": labels},
                    "HostConfig": {},
                    "Mounts": [],
                    "State": {"Running": True, "StartedAt": "2026-10-08T11:00:00Z"},
                }
            )
    return rows


def test_cpu_separates_native_processes_and_background_hosts():
    rows = trace()
    result = observations.api_cpu(rows, bundle(), rows[0]["utc"], rows[-1]["utc"])
    assert result["aggregate_api_cpu_cores"] == 1
    assert result["counter_interval_seconds"] == 3
    assert result["distribution"]["all_four_replicas_observed"] is True
    assert "node CPU is not measured" in result["scope"]
    spec = observations.background_spec("primary", snapshots(), "a" * 64, "b" * 64)
    assert len(spec["containers"]) == 19
    assert not any(r["role"] == "api" for r in spec["containers"])
    assert observations.background_spec("secondary", [], "a" * 64, "b" * 64)["containers"] == []


@pytest.mark.parametrize("fault", ["missing", "no_traffic", "cpu_reset", "nonfinite", "gap", "restart"])
def test_bad_native_cpu_evidence_fails(fault):
    rows = trace()
    address = next(iter(rows[0]["api_replicas"]))
    if fault == "missing":
        rows[2]["api_replicas"].pop(address)
    elif fault == "no_traffic":
        for row in rows:
            row["api_replicas"][address]["business_http_requests_total"] = 0
    elif fault == "cpu_reset":
        rows[-1]["api_replicas"][address]["process_cpu_seconds_total"] = -1
    elif fault == "nonfinite":
        rows[-1]["api_replicas"][address]["process_cpu_seconds_total"] = float("nan")
    elif fault == "gap":
        rows[2]["utc"] = "2026-10-08T12:00:20+00:00"
    else:
        rows[2]["api_replicas"][address]["process_start_time_seconds"] += 1
    with pytest.raises(ValueError):
        observations.api_cpu(rows, bundle(), rows[0]["utc"], rows[-1]["utc"])


@pytest.mark.parametrize(
    "fault", ["api", "missing_worker", "extra_worker", "foreign", "unknown", "helper_owner", "unbound"]
)
def test_native_host_spec_rejects_drift(fault):
    rows = snapshots()
    receipt_sha = "b" * 64
    if fault == "api":
        rows[0]["Config"]["Labels"]["com.docker.compose.service"] = "api"
    elif fault == "missing_worker":
        rows.pop(0)
    elif fault == "extra_worker":
        extra = copy.deepcopy(rows[0])
        extra["Id"] = "e" * 64
        rows.append(extra)
    elif fault == "foreign":
        rows[0]["Config"]["Labels"]["com.docker.compose.project"] = "foreign"
    elif fault == "unknown":
        rows[0]["Config"]["Labels"] = {}
    elif fault == "helper_owner":
        rows[-1]["Config"]["Labels"]["codex-owner"] = "foreign"
    else:
        receipt_sha = ""
    with pytest.raises(ValueError):
        observations.background_spec("primary", rows, "a" * 64, receipt_sha)


@pytest.mark.parametrize("fault", ["replace", "restart", "image", "config", "stop"])
def test_background_runtime_change_blocks_comparison(fault):
    before = snapshots()
    after = copy.deepcopy(before)
    if fault == "replace":
        after[0]["Id"] = "d" * 64
    elif fault == "restart":
        after[0]["State"]["StartedAt"] = "2026-10-08T12:00:00Z"
    elif fault == "image":
        after[0]["Image"] = "sha256:" + "a" * 64
    elif fault == "config":
        after[0]["HostConfig"] = {"Memory": 1}
    else:
        after[0]["State"]["Running"] = False
    assert observations.background_unchanged(before, copy.deepcopy(before)) is True
    with pytest.raises(ValueError):
        observations.background_unchanged(before, after)


def test_cpu_parser_accepts_the_actual_generated_arguments(monkeypatch, tmp_path):
    spec = observations.background_spec("secondary", [], "a" * 64, "b" * 64)
    path = tmp_path / "spec.json"
    path.write_text(json.dumps(spec))
    received = []
    monkeypatch.setattr(
        cpu,
        "collect",
        lambda value, start, **kwargs: (
            received.append((value, start, kwargs)) or {"host_role": "secondary", "samples": []}
        ),
    )
    monkeypatch.setattr(cpu, "summarize", lambda value: {"pass": True})
    cpu.main(
        [
            "--spec",
            str(path),
            "--output",
            str(tmp_path / "cpu.json"),
            "--start-at",
            "2026-10-08T12:00:00Z",
            "--seconds",
            "300",
        ]
    )
    assert received[0][2] == {"seconds": 300, "hourly": False}


def test_helper_identity_is_observed_without_compose_emulation(monkeypatch):
    rows = snapshots()[-2:]
    spec = observations.background_spec("primary", snapshots(), "a" * 64, "b" * 64)
    entries = spec["containers"][-2:]
    for index, row in enumerate(rows):
        row["State"]["Pid"] = index + 1
    monkeypatch.setattr(cpu.subprocess, "check_output", lambda *args, **kwargs: json.dumps(rows))
    monkeypatch.setattr(cpu, "proc_identity", lambda pid: pid + 10)
    monkeypatch.setattr(cpu.Path, "read_text", lambda *args, **kwargs: "0::/docker/native\n")
    monkeypatch.setattr(cpu.Path, "is_file", lambda *args: True)
    result = cpu.inspect_containers(entries)
    assert len(result) == 2
    assert all(result[e["id"]]["owner"] == e["owner"] for e in entries)
    serialized = [{"id": cid, **{k: v for k, v in data.items() if k != "path"}} for cid, data in result.items()]
    assert {e["id"]: e["owner"] for e in serialized} == {e["id"]: e["owner"] for e in entries}
    rows[0]["Config"]["Labels"]["codex-owner"] = "foreign"
    with pytest.raises(ValueError):
        cpu.inspect_containers(entries)


def test_cleanup_retains_inputs_when_job_stop_is_unknown():
    calls = []
    stage = SimpleNamespace(
        profile=SHORT,
        session=SimpleNamespace(begin_cleanup=lambda: calls.append("begin")),
        run=bundle()["run"],
        cid="c" * 64,
        stop_jobs=lambda: (_ for _ in ()).throw(ValueError("unknown stop")),
    )
    observer = observations.Observers(stage, {}, None, None, "/owned")
    observer.diagnostic_attempted = True
    with pytest.raises(ValueError):
        observer.cleanup()
    assert calls == ["begin"]


@pytest.mark.parametrize("fault", [None, "helper_drift", "visibility", "pipeline", "kafka", "contract"])
def test_observer_start_transfers_all_helpers_once_and_blocks_bad_startup(monkeypatch, tmp_path, fault):
    import cce_paid_stage as stage_module
    import diagnostic_runner_connection as diagnostics
    import run_two_host_paid_comparison as paid

    uploaded, launched, programs = [], [], []
    inventory = {}

    def upload(session, cid, owner, directory, name, content):
        assert name not in uploaded
        uploaded.append(name)
        return directory + "/" + name

    monkeypatch.setattr(observations, "api_upload", upload)

    def diagnostic_prepare(session, cid, owner, directory, record, callback, context):
        for name in diagnostics.TRANSFER_FILES:
            content = (observations.policy.ROOT / "scripts" / name).read_text()
            if fault == "helper_drift" and name == diagnostics.TRANSFER_FILES[0]:
                content += "# drift"
            callback(session, cid, owner, directory, name, content)
        if fault == "visibility":
            raise ValueError("masked diagnostics")
        record["diagnostic_connection_binding"] = {"decision": "ADR0180"}
        return {"diagnostic_connection_preflight": {"pass": True}}

    monkeypatch.setattr(diagnostics, "prepare", diagnostic_prepare)
    monkeypatch.setattr(diagnostics, "qualify_bound_inventory", lambda *args: None)
    monkeypatch.setattr(paid, "pipeline_startup_view", lambda *args: {"pass": fault != "pipeline"})
    monkeypatch.setattr(paid, "kafka_startup_view", lambda *args: {"pass": fault != "kafka"})

    def api(cid, code, timeout):
        compile(code, "native-observer-program", "exec")
        programs.append(code)
        if "native_helpers_verified" in code:
            return {"native_helpers_verified": True}
        if "for n in ('pipeline','kafka')" in code:
            return {"pipeline": {"database_wait_diagnostics": {"complete": True}}, "kafka": {"ready": True}}
        return {"fresh": True}

    def verify(*args):
        if fault == "contract":
            raise ValueError("contract mismatch")

    session = SimpleNamespace(api=api, call=lambda *args: {"fresh": True})
    stage = SimpleNamespace(
        profile=SHORT,
        session=session,
        run=bundle()["run"],
        cid="c" * 64,
        manifest={"environment": "development", "show_ids": ["owned"], "viewer_tokens": ["test-secret"]},
        output=tmp_path,
        record={},
        job_list=[],
        check=lambda *args: None,
        checkpoint=lambda: None,
    )

    def launch(session, role, cid, args, directory, jobs, record, **kwargs):
        launched.append(args)
        assert stage_module.sha(
            (observations.policy.ROOT / "scripts/observe_cce_paid_pipeline.py").read_bytes()
        )
        return {"job": len(launched)}

    stage.jobs = SimpleNamespace(launch=launch)
    observer = observations.Observers(
        stage, inventory, SimpleNamespace(verify_pipeline_startup=verify), None, "/owned"
    )
    if fault is None:
        observer.start(bundle()["receipts"])
        assert observer.started is True
        assert len(launched) == 2
        assert "--backend" in launched[1] and "--expected-members" not in launched[1]
        from observe_paid_pipeline import paid_observer_seconds

        for command in launched:
            assert command[command.index("--seconds") + 1] == str(paid_observer_seconds(300)) == "480"
        assert observer.record["diagnostic_connection_preflight"]["pass"] is True
    else:
        with pytest.raises(ValueError):
            observer.start(bundle()["receipts"])
        assert observer.started is False
        if fault in {"helper_drift", "visibility"}:
            assert not launched
    assert all(name in uploaded for name in diagnostics.TRANSFER_FILES)
    assert uploaded.count("observe_cce_paid_pipeline.py") == 1
    assert "native-receipts.json" in uploaded if launched else True


def test_failed_pipeline_trace_does_not_skip_kafka_evidence(monkeypatch, tmp_path):
    import summarize_paid_kafka_lag

    calls = []
    stage = SimpleNamespace(
        profile=SHORT,
        session=SimpleNamespace(),
        run=bundle()["run"],
        cid="c" * 64,
        output=tmp_path,
        record={},
        stop_jobs=lambda: calls.append("stop"),
        checkpoint=lambda: None,
    )
    observer = observations.Observers(stage, {}, None, None, "/owned")
    observer.start_utc, observer.end_utc = trace()[0]["utc"], trace()[-1]["utc"]

    def copy_out(session, cid, source, target):
        assert isinstance(target, str) and target.startswith("/owned/cce-observers/")
        calls.append(source)
        if source.endswith("pipeline.jsonl"):
            raise TimeoutError("missing pipeline")
        return b'{"sample":true}\n'

    monkeypatch.setattr(observations, "copy_out", copy_out)
    monkeypatch.setattr(summarize_paid_kafka_lag, "summarize", lambda rows: {"retained": True})
    with pytest.raises(ValueError, match="observation incomplete"):
        observer.collect()
    assert any(name.endswith("kafka.jsonl") for name in calls)
    assert observer.record["kafka_summary"] == {"retained": True}
    assert observer.record["observer_pass"] is False
    assert (tmp_path / "kafka.jsonl").exists()


@pytest.mark.parametrize("fault", [False, True])
def test_cpu_preflight_must_pass_before_any_sampler_launch(fault):
    calls, programs = [], []

    def call(role, code, timeout):
        compile(code, "native-cpu-preflight", "exec")
        programs.append(code)
        if "cpu_preflight_verified" in code:
            return {"cpu_preflight_verified": not fault}
        return {"fresh": True}

    session = SimpleNamespace(
        config={
            "primary": {"repo": "/root/flash-ticketing"},
            "secondary": {"prepared_directory": "/root/prepared-secondary"},
        },
        call=call,
        put=lambda *args: None,
    )
    stage = SimpleNamespace(
        profile=SHORT,
        session=session,
        run=bundle()["run"],
        cid="c" * 64,
        record={},
        job_list=[],
        check=lambda *args: None,
        checkpoint=lambda: None,
        jobs=SimpleNamespace(launch=lambda *args, **kwargs: calls.append("launch") or {"job": 1}),
    )
    observer = observations.Observers(stage, {}, None, None, "/owned")
    observer.started, observer.native = True, bundle()
    if fault:
        with pytest.raises(ValueError, match="CPU source"):
            observer.launch_cpu(
                1791451200,
                {"primary": snapshots(), "secondary": []},
                dict.fromkeys(("primary", "secondary"), "a" * 64),
            )
        assert not calls
    else:
        observer.launch_cpu(
            1791451200,
            {"primary": snapshots(), "secondary": []},
            dict.fromkeys(("primary", "secondary"), "a" * 64),
        )
        assert calls == ["launch", "launch"]
        assert any("/root/flash-ticketing/tmp/" + stage.run + "-cce-cpu" in code for code in programs)
        assert any("/root/prepared-secondary/" + stage.run + "-cce-cpu" in code for code in programs)
        assert observer.record["primary_cpu_preflight_verified"] is True
        assert observer.record["secondary_cpu_preflight_verified"] is True
    assert any("module.inspect_containers(module.validate_spec(value))" in code for code in programs)


def test_cpu_host_receipt_mismatch_is_rejected():
    common = {
        "arm": "candidate",
        "placement": "cce-api-isolation",
        "decision": "ADR0228",
        "start_utc": "2026-10-08T12:00:00Z",
        "end_utc": "2026-10-08T12:05:00Z",
        "pass": True,
        "host_cpu_percent": 1,
        "cpu_cores_by_role": {},
    }
    primary = {
        **common,
        "host_role": "primary",
        "inventory_sha256": "a" * 64,
        "instance_uuid_sha256": "c" * 64,
    }
    secondary = {
        **common,
        "host_role": "secondary",
        "inventory_sha256": "b" * 64,
        "instance_uuid_sha256": "d" * 64,
    }
    with pytest.raises(ValueError):
        cpu.compare_windows(
            primary, secondary, offered_start_utc=common["start_utc"], offered_end_utc=common["end_utc"]
        )


@pytest.mark.parametrize("ack", [{"uploaded": True}, {"uploaded": False}, {}])
def test_runtime_transfer_keeps_payload_out_of_command_arguments(ack):
    from run_two_host_paid_comparison import api_upload_runtime_owned
    put, calls = [], []
    content = "private-cohort-data-" * 300000
    session = SimpleNamespace(
        put=lambda *args: put.append(args),
        call=lambda *args: calls.append(args) or ack,
    )
    if ack == {"uploaded": True}:
        assert api_upload_runtime_owned(session, "cid", "/owned", "/runtime", "manifest.private.json", content) == "/runtime/manifest.private.json"
    else:
        with pytest.raises(ValueError, match="upload unverified"):
            api_upload_runtime_owned(session, "cid", "/owned", "/runtime", "manifest.private.json", content)
    assert put == [("primary", "/owned/manifest.private.json", content, True)]
    assert len(calls[0][1]) < 2000 and "private-cohort-data" not in calls[0][1]
    assert "chown" not in calls[0][1] and "--user" not in calls[0][1]
    assert "stdin=source" in calls[0][1] and "'xb'" in calls[0][1]


@pytest.mark.parametrize("fault", [None, "first_capture", "foreign", "symlink", "hardlink"])
def test_startup_failure_retains_bounded_owned_logs_independently(tmp_path, fault):
    import base64
    import contextlib
    import io
    import os

    directory = tmp_path / "remote"
    directory.mkdir()
    output = tmp_path / "private"
    output.mkdir()
    stage = SimpleNamespace(
        profile=SHORT,
        session=None, run=bundle()["run"], cid="c" * 64,
        output=output, record={}, checkpoint=lambda: None,
    )
    observer = observations.Observers(stage, {}, None, None, "/owned")
    observer.directory = str(directory)
    for label, name in (("pipeline", "observe_cce_paid_pipeline"), ("kafka", "kafka_lag_observe")):
        job = {"identity_path": str(directory), "name": name, "pid": 123}
        observer.record[label + "_job"] = job
        (directory / ("job-" + name + "-identity.json")).write_text(json.dumps(job))
        (directory / ("job-" + name + ".log")).write_bytes(b"x" * 70000 + b"startup failure")
        (directory / ("job-" + name + "-exit.json")).write_text('{"returncode":1}')
    log = directory / "job-observe_cce_paid_pipeline.log"
    if fault == "foreign":
        observer.record["pipeline_job"]["identity_path"] = "/foreign"
    elif fault == "symlink":
        target = directory / "foreign.log"
        log.rename(target)
        try:
            log.symlink_to(target)
        except OSError:
            pytest.skip("Symlink creation unavailable on this host")
    elif fault == "hardlink":
        os.link(log, directory / "linked.log")

    def api(cid, code, timeout):
        assert cid == stage.cid and timeout == 45
        if fault == "first_capture" and "observe_cce_paid_pipeline" in code:
            raise RuntimeError("capture unavailable")
        text = io.StringIO()
        with contextlib.redirect_stdout(text):
            exec(compile(code, "startup-retention", "exec"), {})  # noqa: S102 - Execute the generated owned-file collector regression.
        return json.loads(text.getvalue())

    observer.session = SimpleNamespace(api=api)
    result = observer.retain_startup_failure()
    assert result["kafka"]["retained"] is True
    retained = json.loads((output / "kafka-startup.private.json").read_text())
    assert retained["truncated"] is True
    assert len(base64.b64decode(retained["log_tail_base64"])) == 65536
    assert retained["exit_receipt"] == {"returncode": 1}
    assert result["pipeline"]["retained"] is (fault is None)
    assert stage.record["observation"]["startup_failure_capture"] == result
    assert (output / "pipeline-startup.private.json").exists() is (fault is None)


def test_cleanup_captures_failure_before_removing_credentials(monkeypatch, tmp_path):
    import diagnostic_runner_connection as diagnostics

    order = []
    stage = SimpleNamespace(
        profile=SHORT,
        session=SimpleNamespace(begin_cleanup=lambda: order.append("begin")),
        run=bundle()["run"], cid="c" * 64,
        stop_jobs=lambda: order.append("stop"), record={}, checkpoint=lambda: None,
    )
    observer = observations.Observers(stage, {}, None, None, "/owned")
    observer.diagnostic_attempted = True
    observer.retain_startup_failure = lambda: order.append("capture")
    monkeypatch.setattr(diagnostics, "cleanup", lambda *args: order.append("secrets") or {})
    observer.cleanup()
    assert order == ["begin", "stop", "capture", "secrets"]


@pytest.mark.parametrize("role", ["cce-audit", "cce-pooler"])
@pytest.mark.parametrize("fault", ["project_missing", "project_api", "project_foreign", "service_api", "service_foreign", "owner_missing", "purpose_foreign"])
def test_real_generated_helper_labels_reject_identity_drift(monkeypatch, role, fault):
    rows = snapshots()
    spec = observations.background_spec("primary", rows, "a" * 64, "b" * 64)
    helper = next(row for row in rows if row["Config"]["Labels"].get("codex-purpose") == role)
    labels = helper["Config"]["Labels"]
    if fault == "project_missing":
        labels.pop("com.docker.compose.project")
    elif fault == "project_api":
        labels["com.docker.compose.project"] = "flash-ticketing"
    elif fault == "project_foreign":
        labels["com.docker.compose.project"] = "cce-foreign"
    elif fault == "service_api":
        labels["com.docker.compose.service"] = "api"
    elif fault == "service_foreign":
        labels["com.docker.compose.service"] = "other"
    elif fault == "owner_missing":
        labels.pop("codex-owner")
    else:
        labels["codex-purpose"] = "foreign"
    entries = [entry for entry in spec["containers"] if entry["role"] == role]
    monkeypatch.setattr(cpu.subprocess, "check_output", lambda *args, **kwargs: json.dumps([helper]))
    with pytest.raises(ValueError):
        cpu.inspect_containers(entries)

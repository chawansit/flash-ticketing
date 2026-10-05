import copy
import sys
from pathlib import Path

import pytest

SCRIPTS = Path(__file__).resolve().parents[2] / "scripts"
sys.path.insert(0, str(SCRIPTS))
import probe_two_host_safety as probe
import qualify_two_host_deployment as driver
import two_host_topology as model

IMAGE = driver.IMAGE


def fixture():
    rows = []
    services = {}
    for role, count in model.NORMAL_COUNTS.items():
        env = {
            "PATH": "/usr/bin",
            "JWT_SECRET": "a$literal#value",
            "RESERVATION_MODE": "postgres",
            "DB_POOL_MAX": "3" if role == "api" else "12",
            "API_PAYMENT_POOL_MAX": "0",
        }
        services[role] = {
            "build": {"context": "/repo"},
            "environment": {"SHADOW": "file-only-value"},
            "ports": [],
            "volumes": [],
        }
        for i in range(count):
            ports = (
                {"8000/tcp": [{"HostIp": "10.0.0.1", "HostPort": "8000"}]}
                if role == "load-balancer"
                else None
            )
            mounts = (
                [
                    {
                        "Type": "bind",
                        "Source": "/actual/runtime-nginx.conf",
                        "Destination": "/etc/nginx/nginx.conf",
                        "RW": False,
                    }
                ]
                if role == "load-balancer"
                else []
            )
            rows.append(
                {
                    "Id": f"{len(rows) + 1:064x}",
                    "Image": IMAGE,
                    "Config": {
                        "Env": [k + "=" + v for k, v in env.items()],
                        "Cmd": ["uvicorn", "ticketing.api:app"],
                        "Entrypoint": None,
                        "Healthcheck": None,
                        "Labels": {
                            "com.docker.compose.project": "flash-ticketing",
                            "com.docker.compose.service": role,
                        },
                    },
                    "HostConfig": {
                        "PortBindings": ports,
                        "RestartPolicy": {"Name": "unless-stopped", "MaximumRetryCount": 0},
                    },
                    "State": {"Running": True},
                    "Mounts": mounts,
                }
            )
    services["reservation-writer"] = {"build": {}, "environment": {}}
    return {"name": "flash-ticketing", "services": services}, rows


def test_runtime_values_override_file_only_values_and_restore_exactly():
    config, rows = fixture()
    saved = model.snapshot(config, rows, image_id=IMAGE)
    api = saved["model"]["services"]["api"]
    assert api["environment"]["JWT_SECRET"] == "a$literal#value"
    assert "SHADOW" not in api["environment"]
    lb = saved["model"]["services"]["load-balancer"]
    assert lb["ports"][0]["host_ip"] == "10.0.0.1"
    assert lb["volumes"][0]["source"] == "/actual/runtime-nginx.conf"
    assert model.restored(saved, copy.deepcopy(rows))["primary_api_count"] == 4


@pytest.mark.parametrize("change", ["environment", "command", "image", "binding", "mount", "count"])
def test_restore_detects_semantic_drift(change):
    config, rows = fixture()
    saved = model.snapshot(config, rows, image_id=IMAGE)
    if change == "environment":
        rows[0]["Config"]["Env"].append("EXTRA=changed")
    elif change == "command":
        rows[0]["Config"]["Cmd"] = ["wrong-command"]
    elif change == "image":
        rows[0]["Image"] = "sha256:" + "a" * 64
    elif change == "binding":
        rows[-1]["HostConfig"]["PortBindings"]["8000/tcp"][0]["HostIp"] = "0.0.0.0"
    elif change == "mount":
        rows[-1]["Mounts"][0]["Source"] = "/wrong"
    else:
        rows.pop()
    with pytest.raises(ValueError):
        model.restored(saved, rows)


@pytest.mark.parametrize("change", ["counts", "project", "mode", "pool", "mixed_env", "image"])
def test_snapshot_refuses_unexpected_initial_topology(change):
    config, rows = fixture()
    if change == "counts":
        rows.pop()
    elif change == "project":
        config["name"] = "another"
    elif change == "mode":
        rows[0]["Config"]["Env"] = [v.replace("postgres", "redis-first") for v in rows[0]["Config"]["Env"]]
    elif change == "pool":
        rows[0]["Config"]["Env"] = [
            v.replace("DB_POOL_MAX=3", "DB_POOL_MAX=8") for v in rows[0]["Config"]["Env"]
        ]
    elif change == "mixed_env":
        rows[0]["Config"]["Env"].append("NEW=only-on-one-replica")
    else:
        rows[0]["Image"] = "sha256:" + "b" * 64
    with pytest.raises(ValueError):
        model.snapshot(config, rows, image_id=IMAGE)


def test_deployment_is_copy_preserves_connections_and_background_placement():
    config, rows = fixture()
    saved = model.snapshot(config, rows, image_id=IMAGE)
    before = copy.deepcopy(saved)
    live = model.deployment_model(saved, primary_ip="10.0.0.1", nginx_path="/owned/nginx.conf")
    assert saved == before
    services = live["services"]
    assert services["api"]["environment"]["DB_POOL_MAX"] == "4"
    assert services["api"]["environment"]["API_PAYMENT_POOL_MAX"] == "2"
    assert services["consumer"]["environment"]["DB_POOL_MAX"] == "8"
    assert services["reservation-writer"]["environment"]["RESERVATION_WRITER_BATCH_SIZE"] == "4"
    assert services["pgbouncer"]["environment"]["DEFAULT_POOL_SIZE"] == "24"
    assert services["pgbouncer"]["ports"][0]["host_ip"] == "10.0.0.1"
    assert services["simulator"]["environment"]["SIMULATOR_DISPATCH_MODE"] == "refill"
    assert services["kafka"] == saved["model"]["services"]["kafka"]


def test_literal_dollar_is_escaped_recursively_without_secret_replacement():
    values = {"env": {"PASSWORD": "$word#quoted"}, "command": ["$HOME", "$" + "{LOOKUP}"], "count": 2}
    assert model.literal_model(values) == {
        "env": {"PASSWORD": "$$word#quoted"},
        "command": ["$$HOME", "$$" + "{LOOKUP}"],
        "count": 2,
    }
    assert values["env"]["PASSWORD"] == "$word#quoted"


def responses():
    return [
        {
            "index": i,
            "host_index": i % 2,
            "status": 202 if i == 4 else 409,
            "body": {
                "command_id": "00000000-0000-0000-0000-000000000001",
                "hold_id": "00000000-0000-0000-0000-000000000002",
                "order_id": "00000000-0000-0000-0000-000000000003",
            }
            if i == 4
            else {"code": "SEAT_UNAVAILABLE"},
        }
        for i in range(100)
    ]


def test_wave_exactly_one_owner_and_two_hosts():
    winner, counts = probe.wave_view(responses())
    assert winner["index"] == 4 and counts == {"409": 99, "202": 1}


@pytest.mark.parametrize(
    "change", ["two_winners", "no_winner", "capacity_error", "wrong_conflict", "one_host", "missing"]
)
def test_wave_cannot_hide_rejections_or_double_acceptance(change):
    rows = responses()
    if change == "two_winners":
        rows[0]["status"] = 202
    elif change == "no_winner":
        rows[4]["status"] = 409
    elif change == "capacity_error":
        rows[0]["status"] = 503
    elif change == "wrong_conflict":
        rows[0]["body"]["code"] = "OTHER"
    elif change == "one_host":
        for row in rows:
            row["host_index"] = 0
    else:
        rows.pop()
    with pytest.raises(ValueError):
        probe.wave_view(rows)


@pytest.mark.parametrize(
    "origins",
    [
        ["http://127.0.0.1:8101", "http://10.0.0.2:8101"],
        ["http://10.0.0.1:8101", "http://10.0.0.1:8102"],
        ["http://10.0.0.1:8000", "http://10.0.0.2:8101"],
        ["http://user:secret@10.0.0.1:8101", "http://10.0.0.2:8101"],
    ],
)
def test_probe_rejects_wrong_environment(origins):
    with pytest.raises(ValueError):
        probe.validate_origins(origins)


def test_embedded_global_audit_syntax():
    compile(driver.GLOBAL_AUDIT, "global_audit", "exec")


def test_failed_cloud_apply_always_restores_and_never_attempts_safety(monkeypatch, tmp_path):
    config, rows = fixture()
    actions = []

    class FakeSession:
        def __init__(self, settings, output, password):
            self.state = {"phases": [], "restore_pass": False, "safety_probe_attempted": False}

        def phase(self, name):
            self.state["phases"].append(name)

        def checkpoint(self):
            pass

        def close(self):
            actions.append(("close",))

        def call(self, role, code, timeout=180):
            if "generator_idle" in code:
                return {"generator_idle": True}
            if code == driver.INSPECT:
                return copy.deepcopy(rows)
            if "'config', 'format'" in code or "'config','--format','json'" in code:
                return config
            if "config','--format','json'" in code and ".env.rds" in code:
                return config
            if "Path(" in code and "read_text" in code:
                return "events {}"
            if "docker','ps','-aq'" in code:
                return []
            return {"ok": True}

        def api(self, *args):
            return {"pass": True, "kafka_members": 1}

        def put(self, *args):
            pass

        def up(self, role, path, counts, services):
            actions.append((role, path, list(services)))
            if path.endswith("/live.compose.json"):
                raise RuntimeError("injected first apply failure")

        def wait(self, role, count):
            return rows[:4]

    monkeypatch.setattr(driver, "Session", FakeSession)
    monkeypatch.setattr(driver.sys.stdin, "isatty", lambda: True)
    monkeypatch.setattr(driver.getpass, "getpass", lambda _: "synthetic")
    cloud = {
        "primary": {"repo": "/owned", "private_ipv4": "10.0.0.1"},
        "secondary": {"prepared_directory": "/owned-secondary", "private_ipv4": "10.0.0.2"},
    }
    result = driver.run(cloud, tmp_path)
    assert result["failure_phase"] == "four-primary-control-topology"
    assert result["restore_pass"] and not result["pass"]
    assert not result["safety_probe_attempted"]
    assert sum(str(a).count("restore.compose.json") for a in actions) == 2
    assert actions[-1] == ("close",)


def test_absent_role_is_scaled_to_zero_instead_of_started():
    session = driver.Session.__new__(driver.Session)
    captured = []
    session.call = lambda role, code: captured.append(code)
    session.up("primary", "/owned/restore.compose.json", model.NORMAL_COUNTS, ["reservation-writer", "api"])
    assert "reservation-writer=0" in captured[0]
    assert "api=4" in captured[0]


def test_failed_wave_retains_status_evidence_without_retry():
    import httpx

    records = []
    requests = []

    async def respond(request):
        requests.append(request)
        return httpx.Response(503, json={"code": "CAPACITY_EXCEEDED"})

    async def exercise():
        async with httpx.AsyncClient(transport=httpx.MockTransport(respond)) as client:
            with pytest.raises(ValueError, match="exactly one"):
                await probe.probe(
                    "00000000-0000-0000-0000-000000000001",
                    ["http://10.0.0.1:8101", "http://10.0.0.2:8101"],
                    [str(i) for i in range(100)],
                    "test-wave",
                    client=client,
                    evidence=lambda phase, values: records.append((phase, values)),
                )

    import asyncio

    asyncio.run(exercise())
    assert len(requests) == 100
    assert records[-1][0] == "hold-wave-validation"
    assert len(records[-1][1]["wave_responses"]) == 100
    assert {r["status"] for r in records[-1][1]["wave_responses"]} == {503}


@pytest.mark.parametrize("timeout", [False, True])
def test_probe_failure_or_timeout_copies_evidence_before_teardown(monkeypatch, tmp_path, timeout):
    import json
    import subprocess
    from types import SimpleNamespace

    calls = []

    def invoke(args, **kwargs):
        calls.append(args)
        if args[1] == "exec":
            if timeout:
                raise subprocess.TimeoutExpired(args, 110, stderr=b"bounded timeout")
            return SimpleNamespace(returncode=1, stderr="inner diagnostic")
        assert args[1] == "cp"
        Path(args[-1]).write_text(json.dumps({"pass": False, "phase": "hold-wave"}))
        return SimpleNamespace(returncode=0)

    monkeypatch.setattr(subprocess, "run", invoke)
    code = driver.probe_program("container", ["python", "/tmp/probe.py"], "/tmp/owned", str(tmp_path))
    exec(compile(code, "probe_capture", "exec"), {})  # noqa: S102 - execute trusted embedded program with mocked subprocess
    assert [c[1] for c in calls] == ["exec", "cp"]
    assert (tmp_path / "probe-result.private.json").exists()
    assert (tmp_path / "probe-stderr.private.log").read_text() == (
        "bounded timeout" if timeout else "inner diagnostic"
    )


def test_probe_main_keeps_failure_checkpoint(monkeypatch, tmp_path):
    import json

    output = tmp_path / "evidence.json"

    async def fail(*args, evidence, **kwargs):
        evidence("payment-replay", {"accepted_holds": 1})
        raise ValueError("synthetic failure")

    monkeypatch.setattr(probe, "probe", fail)
    monkeypatch.setenv("ENVIRONMENT", "development")
    monkeypatch.setenv("JWT_SECRET", "synthetic-test-secret-32-characters")
    monkeypatch.setattr(
        sys,
        "argv",
        [
            "probe",
            "--event-id",
            "00000000-0000-0000-0000-000000000001",
            "--origin",
            "http://10.0.0.1:8101",
            "--origin",
            "http://10.0.0.2:8101",
            "--output",
            str(output),
        ],
    )
    with pytest.raises(ValueError, match="synthetic"):
        probe.main()
    saved = json.loads(output.read_text())
    assert saved["pass"] is False and saved["phase"] == "payment-replay"
    assert saved["failure_type"] == "ValueError" and saved["accepted_holds"] == 1
    assert "JWT_SECRET" not in output.read_text()


@pytest.mark.parametrize("prefix", ["adr0147-topology", "adr0148-rate"])
def test_cleanup_removes_only_owned_snapshot_files(tmp_path, prefix):
    owner = tmp_path / (prefix + "-012345abcdef")
    owner.mkdir()
    for name in (
        "backup.private.json",
        "restore.compose.json",
        "live.compose.json",
        "probe-result.private.json",
    ):
        (owner / name).write_text("retained")
    code = driver.cleanup_program(str(owner), None)
    # Windows drive paths are absolute on the local test platform; remote inputs use POSIX.
    exec(compile(code, "owned_cleanup", "exec"), {})  # noqa: S102 - trusted pure cleanup program in temporary directory
    assert [p.name for p in owner.iterdir()] == ["probe-result.private.json"]


def test_cleanup_refuses_unowned_directory(tmp_path):
    with pytest.raises(ValueError, match="Owned"):
        exec(compile(driver.cleanup_program(str(tmp_path), None), "owned_cleanup", "exec"), {})  # noqa: S102 - trusted program

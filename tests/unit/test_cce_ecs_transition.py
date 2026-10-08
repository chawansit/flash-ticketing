"""Offline native routing/identity restoration checks; no SSH or cloud mutation."""

import copy
import json
import sys
from pathlib import Path
from types import SimpleNamespace

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[2] / "scripts"))
import cce_api_adapter as cce
import cce_ecs_transition as topology
import cce_paid_stage as paid
import work_envelope as policy

RUN = "adr0151-" + "a" * 12
KEY = "bounded_cce_paid_comparison__" + "f" * 12


def pod_receipts():
    return [
        {
            "pod_name": f"api-{i}",
            "pod_uid": f"uid-{i}",
            "private_ipv4": f"10.2.240.{i + 1}",
            "image_id": "docker-pullable://example@" + cce.dependency.MANIFEST,
            "started_at": "2026-10-08T11:00:00Z",
            "process_start_time_seconds": 123.0,
            "resources": cce.contract()["resources"],
            "startup_proof": {
                "run": RUN,
                "pod_uid": f"uid-{i}",
                "sources": cce.contract()["api_sources"],
                "environment_sha256": "a" * 64,
                "command_sha256": policy.digest(
                    [
                        "uvicorn",
                        "ticketing.api:app",
                        "--host",
                        "0.0.0.0",
                        "--port",
                        "8000",
                        "--limit-concurrency",
                        "256",
                        "--timeout-keep-alive",
                        "10",
                    ]
                ),
            },
        }
        for i in range(4)
    ]


def api_row(index):
    return {
        "Id": str(index) * 64,
        "Image": cce.dependency.INDEX,
        "Config": {
            "Image": cce.dependency.INDEX,
            "Labels": {"com.docker.compose.service": "api"},
            "Env": [k + "=" + v for k, v in cce.contract()["api_settings"].items()],
        },
        "HostConfig": {},
        "Mounts": [],
        "State": {"Running": True},
    }


@pytest.fixture
def area(monkeypatch):
    config = {
        "primary": {"repo": "/root/flash-ticketing", "private_ipv4": "10.1.137.69"},
        "secondary": {"private_ipv4": "10.1.207.148"},
    }
    owner = config["primary"]["repo"] + "/tmp/" + RUN
    routes = [
        {
            "container_id": str(i + 1) * 64,
            "host_role": "primary" if i == 0 else "secondary",
            "private_ipv4": config["primary" if i == 0 else "secondary"]["private_ipv4"],
            "port": 8101 + i,
        }
        for i in range(4)
    ]
    rows = {"primary": [api_row(1)], "secondary": [api_row(i) for i in range(2, 5)]}
    lb = {
        "Id": "b" * 64,
        "Image": "nginx-config",
        "Config": {"Labels": {"com.docker.compose.service": "load-balancer"}},
        "HostConfig": {},
        "Mounts": [{"Source": owner + "/nginx.conf", "Destination": "/etc/nginx/nginx.conf"}],
        "State": {"Running": True},
    }
    rows["primary"].append(lb)
    monkeypatch.setattr(
        policy, "PROFILES", {**policy.PROFILES, cce.PROFILE: ("bounded_cce_paid_comparison", "ADR0228")}
    )
    monkeypatch.setattr(policy, "envelope", lambda: {"qualified_profiles": [cce.PROFILE]})
    monkeypatch.setattr(cce, "authorized_creation", lambda value: None)
    guard = SimpleNamespace(
        key=KEY,
        binding={
            "configuration_sha256": policy.digest(config),
            "cce_transition_source_sha256": paid.sha(
                (policy.ROOT / "scripts/cce_ecs_transition.py").read_bytes()
            ),
        },
        check=lambda seconds: None,
    )
    route = {"text": topology.nginx_config(routes)}
    calls = []

    def call(role, program, timeout):
        compile(program, "remote-component", "exec")
        calls.append((role, program))
        if program in {topology.INSPECT, topology.SECONDARY_INSPECT}:
            return copy.deepcopy(rows[role])
        if "Path(" in program and ".read_text()" in program:
            return route["text"]
        if "verb=" in program or "verb =" in program:
            verb = "stop" if "verb='stop'" in program else "start"
            return {"owned_api_transition_verified": True, "running": verb == "start"}
        if "all_four_candidate_apis_ready" in program:
            return {"all_four_candidate_apis_ready": True}
        return {"owned_load_balancer_verified": True, "owned_nginx_reload_verified": True}

    session = SimpleNamespace(
        config=config,
        action_guard=guard,
        call=call,
        put=lambda role, path, text: route.update(text=text),
        checkpoint=lambda: None,
        begin_cleanup=lambda: calls.append(("cleanup", "begin")),
    )
    persisted = []
    transition = topology.Transition(session, guard, RUN, routes, owner, persisted.append)
    return transition, rows, route, calls, persisted


def test_native_route_preserves_baseline_policy_except_upstream_addresses():
    pods = pod_receipts()
    native = topology.native_route(pods)
    stock = topology.nginx_config(
        [{"private_ipv4": a, "port": 8101 + i} for i, a in enumerate(cce.endpoints(pods))]
    )
    for i, address in enumerate(cce.endpoints(pods)):
        stock = stock.replace(f"server {address}:{8101 + i};", f"server {address}:8000;")
    assert native == stock
    assert "proxy_next_upstream off;" in native
    assert "keepalive_timeout 5s;" in native


def test_capture_pause_route_and_restore_exact_candidate(area):
    transition, _rows, route, calls, persisted = area
    transition.capture()
    transition.activate(pod_receipts())
    assert transition.record["all_four_ecs_apis_stopped"] is True
    assert route["text"] == topology.native_route(pod_receipts())
    assert persisted[1]["api_stop_attempted"] is True
    restored = transition.restore()
    assert restored["candidate_restored"] is True
    assert route["text"] == transition.original_route
    changes = [program for role, program in calls if "verb=" in program]
    assert len(changes) == 4
    assert not any("docker compose" in program or "'pgbouncer'" in program for program in changes)


@pytest.mark.parametrize("fault", ["missing", "unexpected", "image", "settings", "lb_mount", "route"])
def test_capture_drift_prevents_any_stop_or_route_write(area, fault):
    transition, rows, route, calls, _persisted = area
    if fault == "missing":
        rows["secondary"].pop()
    elif fault == "unexpected":
        rows["primary"].append(api_row(7))
    elif fault == "image":
        rows["primary"][0]["Image"] = "other"
    elif fault == "settings":
        rows["secondary"][0]["Config"]["Env"] = []
    elif fault == "lb_mount":
        rows["primary"][1]["Mounts"][0]["Source"] = "/unrelated/nginx.conf"
    else:
        route["text"] = "wrong policy"
    with pytest.raises(ValueError):
        transition.capture()
    assert transition.record["api_stop_attempted"] is False
    assert not any("verb=" in program for _role, program in calls)


@pytest.mark.parametrize("fault", ["resources", "image", "run", "process", "sources", "command", "missing"])
def test_bad_native_receipt_never_stops_candidate_apis(area, fault):
    transition, _rows, _route, calls, _persisted = area
    transition.capture()
    pods = pod_receipts()
    if fault == "resources":
        pods[0]["resources"] = {}
    elif fault == "image":
        pods[0]["image_id"] = "unmatched"
    elif fault == "run":
        pods[0]["startup_proof"]["run"] = "foreign"
    elif fault == "process":
        pods[0]["process_start_time_seconds"] = float("nan")
    elif fault == "sources":
        pods[0]["startup_proof"]["sources"] = {}
    elif fault == "command":
        pods[0]["startup_proof"]["command_sha256"] = "0" * 64
    else:
        pods.pop()
    with pytest.raises(ValueError):
        transition.activate(pods)
    assert not any("verb=" in program for _role, program in calls)


def test_lost_stop_acknowledgement_still_restarts_both_hosts(area):
    transition, _rows, _route, calls, _persisted = area
    transition.capture()
    old = transition.session.call

    def call(role, program, timeout):
        if "verb='stop'" in program:
            raise TimeoutError("lost stop response")
        return old(role, program, timeout)

    transition.session.call = call
    with pytest.raises(TimeoutError):
        transition.activate(pod_receipts())
    assert transition.record["api_stop_attempted"] is True
    assert transition.restore()["candidate_restored"] is True
    assert sum("verb='start'" in program for _role, program in calls) == 2


def test_partial_route_transfer_is_repaired_before_nginx_validation(area):
    transition, _rows, route, _calls, _persisted = area
    transition.capture()
    old = transition.session.put

    def put(role, path, text):
        if text != transition.original_route:
            route["text"] = "partial invalid Nginx content"
            raise TimeoutError("lost transfer")
        old(role, path, text)

    transition.session.put = put
    with pytest.raises(TimeoutError):
        transition.activate(pod_receipts())
    assert transition.record["route_write_attempted"] is True
    assert transition.restore()["candidate_restored"] is True
    assert route["text"] == transition.original_route


def test_replaced_api_fails_recovery_but_other_host_and_route_are_attempted(area):
    transition, _rows, route, calls, _persisted = area
    transition.capture()
    transition.activate(pod_receipts())
    old = transition.session.call

    def call(role, program, timeout):
        if role == "primary" and "verb='start'" in program:
            raise ValueError("API replaced")
        return old(role, program, timeout)

    transition.session.call = call
    with pytest.raises(ValueError):
        transition.restore()
    assert transition.record["candidate_restored"] is False
    assert any(role == "secondary" and "verb='start'" in program for role, program in calls)
    assert route["text"] == transition.original_route


@pytest.mark.parametrize("fault", ["scope", "source", "configuration", "unregistered"])
def test_unqualified_transition_has_zero_cloud_calls(area, monkeypatch, fault):
    transition, _rows, _route, calls, _persisted = area
    if fault == "scope":
        transition.guard.key = "bounded_cce_dependency_probe__" + "f" * 12
    elif fault == "source":
        transition.guard.binding["cce_transition_source_sha256"] = "0" * 64
    elif fault == "configuration":
        transition.guard.binding["configuration_sha256"] = "0" * 64
    else:
        monkeypatch.setattr(policy, "PROFILES", {})
    with pytest.raises(ValueError):
        transition.capture()
    assert calls == []


@pytest.mark.parametrize("fault", [None, "identity_before", "identity_after", "still_running"])
def test_actual_generated_stop_program_verifies_before_and_after(monkeypatch, capsys, fault):
    rows = [api_row(1)]
    expected = copy.deepcopy(rows)
    commands = []
    if fault == "identity_before":
        rows[0]["Image"] = "replaced"
    import subprocess

    def check_output(args, **kw):
        return json.dumps(rows)

    def run(args, **kw):
        commands.append(args)
        rows[0]["State"]["Running"] = fault == "still_running"
        if fault == "identity_after":
            rows[0]["Image"] = "replaced"

    monkeypatch.setattr(subprocess, "check_output", check_output)
    monkeypatch.setattr(subprocess, "run", run)
    if fault:
        with pytest.raises(ValueError):
            exec(topology.change_program(expected, "stop"), {})  # noqa: S102 - Execute repository-generated code with mocked Docker.
        if fault == "identity_before":
            assert commands == []
    else:
        exec(topology.change_program(expected, "stop"), {})  # noqa: S102 - Execute repository-generated code with mocked Docker.
        assert json.loads(capsys.readouterr().out)["running"] is False
        assert commands == [["docker", "stop", "--time", "20", "1" * 64]]


@pytest.mark.parametrize("fault", [None, "replaced", "not_running"])
def test_actual_load_balancer_identity_check_does_not_parse_partial_route(area, monkeypatch, capsys, fault):
    transition, rows, _route, calls, _persisted = area
    transition.capture()
    transition.reload(validate_only=True)
    program = calls[-1][1]
    lb = copy.deepcopy(rows["primary"][1])
    if fault == "replaced":
        lb["Id"] = "c" * 64
    elif fault == "not_running":
        lb["State"]["Running"] = False
    import subprocess

    commands = []
    monkeypatch.setattr(subprocess, "check_output", lambda *args, **kw: json.dumps([lb]))
    monkeypatch.setattr(subprocess, "run", lambda *args, **kw: commands.append(args))
    if fault:
        with pytest.raises(ValueError):
            exec(program, {})  # noqa: S102 - Trusted generated lifecycle program, mocked Docker.
    else:
        exec(program, {})  # noqa: S102 - Trusted generated lifecycle program, mocked Docker.
        result = json.loads(capsys.readouterr().out)
        assert result["owned_load_balancer_verified"] is True
        assert result["owned_nginx_reload_verified"] is False
    assert commands == []

"""Owned bridge/audit creation and independent cleanup; simulated Docker only."""

import copy
import sys
from pathlib import Path
from types import SimpleNamespace

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[2] / "scripts"))
import cce_api_adapter as cce
import cce_paid_resources as resources
import cce_paid_stage as paid
import work_envelope as policy

RUN = "adr0151-" + "a" * 12


def row(role):
    bridge = role == "bridge"
    command = ["-u", "-c", resources.PROXY] if bridge else ["-u", "-c", "import time;time.sleep(3600)"]
    return {
        "Id": ("b" if bridge else "c") * 64,
        "Name": "/cce-" + ("pooler" if bridge else "audit") + "-" + RUN,
        "Image": cce.dependency.INDEX,
        "Config": {
            "Image": cce.dependency.INDEX,
            "Entrypoint": ["python"],
            "Cmd": command,
            "Labels": {
                "codex-owner": RUN,
                "codex-purpose": "cce-" + ("pooler" if bridge else "audit"),
                "com.docker.compose.project": "cce-" + RUN,
                "com.docker.compose.service": "pooler-bridge" if bridge else "audit-helper",
            },
            "Env": [
                "DATABASE_URL=postgresql://test@pgbouncer:5432/ticketing",
                *[k + "=" + v for k, v in cce.contract()["api_settings"].items()],
            ],
        },
        "HostConfig": {
            **({"NanoCpus": 1_000_000_000, "Memory": 64 * 1024 * 1024, "ReadonlyRootfs": True} if bridge else {}),
            "PortBindings": {"6432/tcp": [{"HostIp": "10.1.137.69", "HostPort": "6432"}]} if bridge else {}
        },
        "Mounts": [],
        "NetworkSettings": {"Networks": {"flash-ticketing_default": {}}},
        "State": {"Running": True, "StartedAt": "2026-10-08T12:00:00Z"},
    }


@pytest.fixture
def area(monkeypatch):
    cfg = {"primary": {"repo": "/root/flash-ticketing", "private_ipv4": "10.1.137.69"}}
    guard = SimpleNamespace(
        key="bounded_cce_paid_comparison__" + "f" * 12,
        binding={
            "configuration_sha256": policy.digest(cfg),
            "cce_bridge_cpu_limit": resources.BRIDGE_CPU_LIMIT,
            "cce_resource_source_sha256": paid.sha(
                (policy.ROOT / "scripts/cce_paid_resources.py").read_bytes()
            ),
        },
        check=lambda *args: None,
    )
    monkeypatch.setattr(
        policy, "PROFILES", {**policy.PROFILES, cce.PROFILE: ("bounded_cce_paid_comparison", "ADR0228")}
    )
    monkeypatch.setattr(policy, "envelope", lambda: {"qualified_profiles": [cce.PROFILE]})
    monkeypatch.setattr(cce, "authorized_creation", lambda *args: None)
    calls = []
    active = {}
    persisted = []
    pool = {
        "Id": "p" * 64,
        "Image": "pooler",
        "Config": {"Labels": {"com.docker.compose.service": "pgbouncer"}},
        "State": {"Running": True},
        "NetworkSettings": {"Networks": {"flash-ticketing_default": {}}},
    }

    def call(role, code, timeout):
        compile(code, "generated-resource", "exec")
        calls.append(code)
        if code == resources.INSPECT:
            return [pool]
        if "'docker','ps','-aq'" in code:
            key = "bridge" if "cce-pooler-" in code else "audit"
            return [copy.deepcopy(active[key])] if key in active else []
        if "args=" in code:
            key = "bridge" if "cce-pooler-" in code else "audit"
            active[key] = row(key)
            return {"created": True}
        if "['docker','rm','-f'" in code:
            key = "bridge" if "'Id': '" + "b" * 64 + "'" in code else "audit"
            active.pop(key)
            return {"owned_helper_removed": True}
        return {"helper_environment_removed": True}

    session = SimpleNamespace(
        config=cfg,
        action_guard=guard,
        call=call,
        put=lambda *args: calls.append("env transfer"),
        checkpoint=lambda: None,
        begin_cleanup=lambda: calls.append("cleanup"),
    )
    env = {"DATABASE_URL": "postgresql://test@pgbouncer:5432/ticketing"}
    value = resources.Resources(
        session, guard, RUN, cfg["primary"]["repo"] + "/tmp/" + RUN, env, persisted.append
    )
    return value, active, calls, persisted


def test_create_and_cleanup_only_owned_helpers(area):
    value, active, _calls, persisted = area
    assert value.create() == "c" * 64
    assert set(active) == {"audit", "bridge"}
    assert any(item["create_attempted"] == ["audit"] for item in persisted)
    assert value.cleanup()["owned_resources_removed"] is True
    assert active == {}
    assert value.record["helper_environment_removed"] is True


def test_lost_create_acknowledgement_recovers_identity_for_cleanup(area):
    value, active, _calls, _persisted = area
    old = value.session.call

    def call(role, code, timeout):
        result = old(role, code, timeout)
        if "args=" in code:
            raise TimeoutError("lost response")
        return result

    value.session.call = call
    with pytest.raises(TimeoutError):
        value.create()
    assert value.record["audit"]["container_id"] == "c" * 64
    assert value.cleanup()["owned_resources_removed"] is True
    assert active == {}


def test_unknown_creation_preserves_private_input_and_blocks_success(area):
    value, active, _calls, _persisted = area
    old = value.session.call

    def call(role, code, timeout):
        result = old(role, code, timeout)
        if "args=" in code:
            active["audit"]["Config"]["Labels"]["codex-owner"] = "foreign"
            raise TimeoutError("lost response")
        return result

    value.session.call = call
    with pytest.raises(ValueError):
        value.create()
    with pytest.raises(ValueError):
        value.cleanup()
    assert "audit" in active
    assert "helper_environment_removed" not in value.record


def test_replaced_bridge_does_not_prevent_owned_audit_cleanup(area):
    value, active, _calls, _persisted = area
    value.create()
    active["bridge"]["Id"] = "d" * 64
    with pytest.raises(ValueError):
        value.cleanup()
    assert set(active) == {"bridge"}
    assert value.record["owned_resources_removed"] is False


def test_stopped_owned_helpers_can_be_removed(area):
    value, active, _calls, _persisted = area
    value.create()
    for item in active.values():
        item["State"]["Running"] = False
    assert value.cleanup()["owned_resources_removed"] is True


@pytest.mark.parametrize("fault", ["image", "command", "network", "public_port", "owner"])
def test_helper_receipt_rejects_drift(fault):
    value = row("bridge")
    if fault == "image":
        value["Image"] = cce.dependency.CONFIG
    elif fault == "command":
        value["Config"]["Cmd"] = ["wrong"]
    elif fault == "network":
        value["NetworkSettings"]["Networks"] = {"unrelated": {}}
    elif fault == "public_port":
        value["HostConfig"]["PortBindings"]["6432/tcp"][0]["HostIp"] = "0.0.0.0"
    else:
        value["Config"]["Labels"]["codex-owner"] = "foreign"
    with pytest.raises(ValueError):
        resources.owned_receipt(
            value,
            value["Name"][1:],
            RUN,
            ["-u", "-c", resources.PROXY],
            pool_network="flash-ticketing_default",
            bridge=True,
        )


def test_unregistered_resources_do_not_call_remote(area, monkeypatch):
    value, _active, calls, _persisted = area
    monkeypatch.setattr(policy, "PROFILES", {})
    with pytest.raises(ValueError):
        value.create()
    assert calls == []


@pytest.mark.parametrize("role", ["audit", "bridge"])
def test_inherited_api_labels_are_rejected(role):
    value = row(role)
    value["Config"]["Labels"].update({"com.docker.compose.project": "flash-ticketing", "com.docker.compose.service": "api"})
    with pytest.raises(ValueError, match="Owned helper identity"):
        resources.owned_receipt(value, value["Name"][1:], RUN, value["Config"]["Cmd"], pool_network="flash-ticketing_default", bridge=role == "bridge")


def test_run_commands_override_inherited_api_labels(area):
    value, _active, calls, _persisted = area
    value.create()
    commands = [code for code in calls if "args=" in code]
    assert len(commands) == 2
    assert all("com.docker.compose.project=cce-" + RUN in code for code in commands)
    assert "com.docker.compose.service=audit-helper" in commands[0]
    assert "com.docker.compose.service=pooler-bridge" in commands[1]


@pytest.mark.parametrize("field,value", [("NanoCpus",125_000_000),("NanoCpus",2_000_000_000),("Memory",32*1024*1024),("ReadonlyRootfs",False)])
def test_bridge_allocation_is_exactly_verified(field,value):
    value_row=row("bridge");value_row["HostConfig"][field]=value
    with pytest.raises(ValueError):
        resources.owned_receipt(value_row,value_row["Name"][1:],RUN,value_row["Config"]["Cmd"],pool_network="flash-ticketing_default",bridge=True)


def test_bridge_run_command_and_binding_use_measured_correction(area):
    value,_active,calls,_persisted=area
    value.create()
    bridge=next(code for code in calls if "args=" in code and "cce-pooler-" in code)
    assert "'--cpus', '1'" in bridge and "'0.125'" not in bridge
    value.guard.binding["cce_bridge_cpu_limit"]=0.125
    with pytest.raises(ValueError):value.check()

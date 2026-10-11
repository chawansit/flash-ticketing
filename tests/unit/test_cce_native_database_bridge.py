"""Native opt-in, immutable ownership and reversible lifecycle regressions."""
import ast
import copy

import cce_native_database_bridge as native
import cce_paid_resources as resources
import cce_worker_rebalance_profile as profile
import pytest
import work_envelope as policy
from test_cce_paid_resources import RUN, area, row  # noqa: F401 - Shared simulated Docker fixture.


def native_row(owner):
    value = row("bridge")
    value["Image"] = native.IMAGE
    value["Config"].update(Image=native.IMAGE, Entrypoint=["nginx"], Cmd=list(native.COMMAND), User="10001:10001")
    value["HostConfig"]["Tmpfs"] = {"/tmp": native.TMPFS}
    value["Mounts"] = [{"Type": "bind", "Source": owner + "/cce-stream.conf",
                        "Destination": native.DESTINATION, "RW": False}]
    return value


def test_selector_rejects_unknown_or_unbound_changes():
    assert native.selected({}) is False
    goal = {"native_database_bridge_decision": "ADR0279", "native_database_bridge_sha256": policy.digest(native.contract()),
            "timeout_retention_decision": "ADR0278", "extension_decision": "ADR0271"}
    assert native.selected(goal)
    for key in goal:
        with pytest.raises(ValueError):
            native.selected({**goal, key: "wrong"})


@pytest.mark.parametrize("mutation", ["image", "command", "mount", "writable", "tmpfs", "user", "budget", "network"])
def test_unknown_native_helper_is_never_accepted(mutation):
    owner = "/root/flash-ticketing/tmp/" + RUN
    value = native_row(owner)
    if mutation == "image": value["Image"] = "sha256:" + "a" * 64
    if mutation == "command": value["Config"]["Cmd"] = ["other"]
    if mutation == "mount": value["Mounts"][0]["Source"] = "/unknown"
    if mutation == "writable": value["Mounts"][0]["RW"] = True
    if mutation == "tmpfs": value["HostConfig"]["Tmpfs"] = {}
    if mutation == "user": value["Config"]["User"] = "root"
    if mutation == "budget": value["HostConfig"]["NanoCpus"] = 2_000_000_000
    if mutation == "network": value["NetworkSettings"]["Networks"] = {"other": {}}
    with pytest.raises(ValueError):
        resources.owned_receipt(value, "cce-pooler-" + RUN, RUN, native.COMMAND,
                                pool_network="flash-ticketing_default", bridge=True, native_owner=owner)


def test_native_lifecycle_preserves_budget_and_cleans_owned_config(area):  # noqa: F811 - Imported pytest fixture.
    value, active, _calls, _ = area
    value.native_bridge = True
    value.commands["bridge"] = list(native.COMMAND)
    value.guard.binding.update(cce_native_database_bridge_decision="ADR0279", cce_native_database_bridge_sha256=policy.digest(native.contract()))
    original = value.session.call
    def call(role, code, timeout):
        if "Native bridge configuration drift" in code:
            return {"native_configuration_verified": True}
        if "native_configuration_removed" in code:
            return {"native_configuration_removed": True}
        if "Exact cached native bridge image required" in code:
            return {"native_image_verified": True}
        answer = original(role, code, timeout)
        if "args=" in code and "cce-pooler-" in code:
            options = ast.literal_eval(next(line[5:] for line in code.splitlines() if line.startswith("args=")))
            assert options[options.index("--entrypoint") + 1] == "nginx"
            assert options[options.index("--cpus") + 1] == "1"
            assert options[options.index("--memory") + 1] == "64m"
            assert "--env-file" not in options and native.IMAGE in options
            active["bridge"] = native_row(value.owner)
        return answer
    value.session.call = call
    value.create()
    assert active["bridge"]["Config"]["Cmd"] == native.COMMAND
    value.cleanup()
    assert not active and value.record["native_configuration_removed"]


def test_configuration_drift_blocks_removal(area):  # noqa: F811 - Imported pytest fixture.
    value, active, _, _ = area
    value.native_bridge = True
    value.network = "flash-ticketing_default"
    value.commands["bridge"] = list(native.COMMAND)
    active["bridge"] = native_row(value.owner)
    value.rows["bridge"] = copy.deepcopy(active["bridge"])
    value.record["create_attempted"] = ["bridge"]
    original = value.session.call
    def call(role, code, timeout):
        if "Native bridge configuration drift" in code:
            raise ValueError("configuration changed")
        return original(role, code, timeout)
    value.session.call = call
    with pytest.raises(ValueError, match="cleanup requires recovery"):
        value.cleanup()
    assert "bridge" in active


def test_native_plan_names_actual_single_factor(monkeypatch):
    goal = {"extension_decision": "ADR0271", "decision": "ADR0228", "profile": "cce_paid_comparison",
            "comparison_arm": "correction", "acquisition_budget": 20, "candidate_factor": profile.factor(),
            "timeout_retention_decision": "ADR0278", "native_database_bridge_decision": "ADR0279",
            "native_database_bridge_sha256": policy.digest(native.contract())}
    monkeypatch.setattr(profile.parent, "image", dict)
    monkeypatch.setattr(profile.parent, "plan", lambda _: {})
    plan = profile.plan(goal)
    assert plan["single_changed_factor"] == "database_tcp_bridge_implementation"
    assert plan["baseline_evidence"].endswith("timeout-baseline-result-2026-10-11.json")
    assert plan["maximum_pods"] == 18 and not plan["baseline_is_passing_control"]

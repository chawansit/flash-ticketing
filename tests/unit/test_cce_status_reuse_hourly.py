"""Hourly qualification must preserve the accepted runtime and complete observation."""
import copy
from types import SimpleNamespace

import cce_live_admission as live
import cce_native_workers as workers
import cce_status_refresh_reuse as reuse
import cce_status_reuse_hourly as hourly
import cce_transaction_profile as transaction
import pytest
import work_envelope as policy
from cce_paid_profiles import HOURLY, SHORT
from test_cce_live_admission import evidence
from test_cce_status_refresh_reuse import goal as short_goal


def goal():
    return {**short_goal(), "decision": "ADR0232", "profile": HOURLY.name,
            "offered_seconds": 3600, "hourly_qualification_decision": "ADR0281",
            "hourly_adapters_sha256": policy.digest(hourly.adapters()),
            "maximum_experiment_seconds": 5400, "maximum_paid_stages": 1,
            "generator_concurrency": 500, "maximum_pods": 18, "pod_vcpu": 1,
            "pod_memory_gib": 2, "worker_resources": {"cpu": "250m", "memory": "512Mi"},
            "baseline_sha256": hourly.BASELINE_SHA256}


def envelope():
    value = copy.deepcopy(policy.envelope())
    old = value["spending"]["temporary_cce_pilot_exception"]["goal_bounded_authorization"]
    old.update(goal())
    return value


def test_hourly_binding_retains_passing_runtime_and_full_hour_target(monkeypatch):
    value = envelope()
    monkeypatch.setattr(policy, "envelope", lambda: value)
    assert transaction.active()["profile"] == HOURLY.name
    assert reuse.selected(goal())
    plan = transaction.plan()
    assert plan["baseline_is_passing_control"] is True
    assert plan["common"] == {"buyer_journeys_per_second": 84, "duration_seconds": 3600}
    assert (plan["expected_terminal_tickets"], plan["minimum_issued_inside_hour"], plan["experiment_seconds_limit"]) == (302400, 300000, 5400)
    assert (plan["maximum_pods"], plan["pooler_server_connections"], plan["acquisition_budget"]) == (18, 24, 20)
    from cce_dependency_probe import authorized_today
    authorized_today(value)
    from cce_api_adapter import contract
    assert contract()["resources"]["limits"] == {"cpu": "1", "memory": "2Gi"}
    import run_cce_hourly_qualification as entry
    import run_cce_paid_comparison as runner
    assert runner.plan(profile=HOURLY) == entry.plan() == plan


@pytest.mark.parametrize("key,value", [("hourly_qualification_decision", "unknown"),
    ("offered_seconds", 300), ("offered_journeys_per_second", 85), ("generator_concurrency", 1000),
    ("maximum_experiment_seconds", 5401), ("maximum_paid_stages", 2), ("maximum_pods", 19),
    ("pod_memory_gib", 1), ("acquisition_budget", 21), ("baseline_sha256", "0"*64),
    ("hourly_adapters_sha256", "0"*64), ("status_refresh_reuse_decision", None),
    ("native_database_bridge_sha256", "0"*64), ("candidate_factor", {})])
def test_hourly_rejects_unapproved_scope_runtime_or_prerequisite(key, value):
    with pytest.raises(ValueError):
        hourly.active({**goal(), key: value})


def test_undeclared_hour_does_not_relax_short_reuse_contract():
    with pytest.raises(ValueError):
        reuse.selected({**short_goal(), "offered_seconds": 3600})
    assert reuse.selected(short_goal())
    with pytest.raises(ValueError):
        hourly.selected({"hourly_adapters_sha256": "orphan"})


def test_failed_prerequisite_cannot_start_hour(monkeypatch):
    original = policy.read
    def read(path):
        value = original(path)
        if path == policy.ROOT / hourly.BASELINE:
            value["status"] = "FAILED_RESTORED"
        return value
    monkeypatch.setattr(policy, "read", read)
    with pytest.raises(ValueError):
        hourly.active(goal())


def test_hourly_worker_lifetime_extends_through_offered_window_without_resource_change(monkeypatch):
    import cce_worker_identity as identity
    monkeypatch.setattr(workers, "environment", lambda role, values, ip, decision: values)
    envs = {role: {"TEST": "1"} for role in identity.specification("ADR0271")["counts"]}
    run = "adr0151-" + "a"*12
    short = workers.objects(run, envs, "10.1.137.69", "ADR0271", profile=SHORT)
    hour = workers.objects(run, envs, "10.1.137.69", "ADR0271", profile=HOURLY)
    for before, after in zip(short, hour, strict=True):
        if after["kind"] == "Pod":
            assert before["spec"]["activeDeadlineSeconds"] == 3000
            assert after["spec"]["activeDeadlineSeconds"] == 5400
            after["spec"]["activeDeadlineSeconds"] = 3000
        assert before == after


def test_hourly_admission_observation_covers_whole_hour_and_keeps_overlap_gate():
    now = [0.0]
    deployment = SimpleNamespace(pod_uids={f"api-{i}": f"uid-{i}" for i in range(4)},
                                 admission_diagnostics=lambda **kw: evidence([]))
    instance = live.Capture(deployment, now=lambda: now[0], profile=HOURLY)
    instance.started = 0
    for i in range(381):
        now[0] = i*10
        instance.read()
    assert len(instance.samples) == 381 and instance.finish()["complete"] is True
    now[0] += 21
    with pytest.raises(ValueError, match="gap"):
        instance.read()
    short = live.Capture(deployment)
    assert (short.maximum_seconds, short.maximum_samples) == (600, 64)


def test_hourly_adapter_imports_the_exact_timeout_retaining_leaf(tmp_path):
    import os
    import subprocess
    import sys
    from pathlib import Path

    from cce_paid_stage import generator_bundle
    from customer_recovery_bundle import qualify
    from customer_timeout_bundle import qualify as retained
    bundle, coordinator = qualify(generator_bundle())
    bundle, coordinator = retained(bundle, coordinator)
    for name in ("paid_ticket_load_generator.py", "paid_ticket_sharded_generator.py",
                 "customer_recovery_client.py", "paid_fixture_layout.py", "checkout_journey_probe.py"):
        (tmp_path / name).write_bytes(bundle["scripts/" + name])
    for name in hourly.adapters()["adapters"]:
        (tmp_path / Path(name).name).write_bytes((policy.ROOT / name).read_bytes())
    env = dict(os.environ)
    env.pop("PYTHONPATH", None)
    result = subprocess.run([sys.executable, "-E", str(tmp_path / "cce_hourly_paid_leaf.py"), "--help"],
                            cwd=tmp_path, env=env, capture_output=True, text=True, timeout=10, check=False)
    assert result.returncode == 0 and "--recovery-max-attempts" in result.stdout
    import hashlib

    from customer_timeout_bundle import MANIFEST
    receipt = policy.read(policy.ROOT / MANIFEST)
    assert hashlib.sha256(bundle["scripts/paid_ticket_load_generator.py"]).hexdigest() == receipt["files"]["artifacts/customer-timeout/paid_ticket_load_generator.py"]

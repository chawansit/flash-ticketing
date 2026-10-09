"""No cloud calls: CCE authorization, image drift, generated lifecycle and cleanup faults."""
import copy
import io
import json
import os
import ssl
import subprocess
import sys
import time
import urllib.error
import urllib.request
from datetime import UTC, datetime
from pathlib import Path
from types import SimpleNamespace

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[2] / "scripts"))
import cce_dependency_probe as cce
import work_envelope as policy


def proof():
    return {"pass": True, "registry_reference": cce.IMAGE,
            "original_local_image_index_digest": cce.INDEX,
            "docker_configuration_digest": cce.CONFIG,
            "manifest_config_verified": True, "platform": "linux/amd64"}


def test_exact_image_mapping_preserves_frozen_platform():
    assert "asyncio.open_connection('pgbouncer',5432)" in cce.REMOTE
    assert "struct.pack('!II',8,80877103)" in cce.REMOTE
    cce.validate_proof(proof())
    for field in proof():
        invalid = proof()
        invalid[field] = None
        with pytest.raises(ValueError):
            cce.validate_proof(invalid)


def legacy_envelope():
    value = copy.deepcopy(policy.envelope())
    value["spending"]["temporary_cce_pilot_exception"].update(
        boundary_mode="calendar_expiry", authorized_date_bangkok="2026-10-08",
        expires_at_bangkok="2026-10-08T23:59:59+07:00", no_spending_cap_explicitly_authorized=True,
    )
    return value


@pytest.mark.parametrize("stamp", ["2026-10-07T12:00:00+00:00", "2026-10-08T16:55:00+00:00",
                                  "2026-10-09T00:00:00+00:00", "2026-10-08T10:00:00"])
def test_spending_expiry_and_cleanup_margin(stamp):
    with pytest.raises(ValueError):
        cce.authorized_today(legacy_envelope(), datetime.fromisoformat(stamp))


def test_spending_valid_only_with_exact_exception():
    cce.authorized_today(legacy_envelope(), datetime(2026, 10, 8, 10, tzinfo=UTC))
    value = legacy_envelope()
    value["spending"]["temporary_cce_pilot_exception"]["no_spending_cap_explicitly_authorized"] = False
    with pytest.raises(ValueError):
        cce.authorized_today(value, datetime(2026, 10, 8, 10, tzinfo=UTC))


@pytest.mark.parametrize("run", ["adr0151-a", "adr0152-" + "a"*12, "../unsafe", "ADR0151-"+"a"*12])
def test_canonical_run_names(run):
    with pytest.raises(ValueError):
        cce.namespace_for(run)


@pytest.fixture
def area(tmp_path, monkeypatch):
    envelope = copy.deepcopy(policy.envelope())
    envelope["qualified_profiles"] = list(policy.PROFILES)
    for key in ("ENVELOPE", "JOURNAL", "STATE", "LOCK", "PAUSE"):
        monkeypatch.setattr(policy, key, tmp_path / key)
    policy.write(policy.ENVELOPE, envelope)
    policy.write(policy.JOURNAL, {"schema_version": 1, "envelope_id": envelope["envelope_id"],
                                "human_pause": False, "experiments": []})
    policy.write(policy.STATE, {"current_run": None, "human_pause": False})
    policy.write(policy.LOCK, {"pid": os.getpid(), "run": "adr0151-"+"a"*12})
    monkeypatch.setattr(cce, "authorized_today", lambda *args: None)
    return {"run_id": "adr0151-"+"a"*12, "configuration_sha256": envelope["existing_resource_configuration_sha256"],
            "cce_sources": cce.identity(), "image_proof_sha256": policy.digest(proof())}


def restored_report():
    return {"run": "adr0151-"+"a"*12, "customer_writes": 0, "capacity_stages_started": 0,
            "namespace_removed": True, "bridge_removed": True, "existing_runtime_unchanged": True,
            "generator_idle_after": True, "temporary_credentials_removed": True, "pass": True,
            "runtime_fingerprint_before": "a"*64, "runtime_fingerprint_after": "a"*64, "runtime_drift": []}


def test_probe_cannot_dispatch_customers_and_receipt_is_bound(area):
    entry = policy.reserve(area, cce.plan(), profile=cce.PROFILE)
    state = policy.read(policy.STATE)
    scope = state[entry["ledger"]]
    assert scope["paid_runs_authorized"] == scope["safety_tickets_authorized"] == 0
    report = restored_report()
    scope["cce_result_sha256"] = policy.digest(report)
    policy.write(policy.STATE, state)
    assert policy.ActionGuard(entry["ledger"], area).finish([report], 10)["status"] == "PASSED_RESTORED"


@pytest.mark.parametrize("fault", ["namespace_removed", "bridge_removed", "existing_runtime_unchanged",
                                  "generator_idle_after", "temporary_credentials_removed", "receipt"])
def test_unknown_cleanup_blocks_further_experiments(area, fault):
    entry = policy.reserve(area, cce.plan(), profile=cce.PROFILE)
    state = policy.read(policy.STATE)
    report = restored_report()
    if fault != "receipt":
        report[fault] = False
    state[entry["ledger"]]["cce_result_sha256"] = "wrong" if fault == "receipt" else policy.digest(report)
    policy.write(policy.STATE, state)
    assert policy.ActionGuard(entry["ledger"], area).finish([report], 10)["status"] == "RECOVERY_REQUIRED"
    with pytest.raises(ValueError):
        policy.reserve(area, cce.plan(), profile=cce.PROFILE)


def test_network_failure_with_verified_zero_writes_and_cleanup_can_close(area):
    entry = policy.reserve(area, cce.plan(), profile=cce.PROFILE)
    report = restored_report()
    report["pass"] = False
    state = policy.read(policy.STATE)
    state[entry["ledger"]]["cce_result_sha256"] = policy.digest(report)
    policy.write(policy.STATE, state)
    assert policy.ActionGuard(entry["ledger"], area).finish([report], 10)["status"] == "FAILED_RESTORED"


def test_changed_plan_and_sources_rejected_before_cloud(area):
    for field in ("kind", "image", "maximum_pods"):
        plan = cce.plan()
        plan[field] = "changed"
        with pytest.raises(ValueError):
            policy.reserve(area, plan, profile=cce.PROFILE)
    area["cce_sources"] = {}
    with pytest.raises(ValueError):
        policy.reserve(area, cce.plan(), profile=cce.PROFILE)


@pytest.mark.parametrize("fault", [None, "pod_image", "pod_startup", "network", "namespace_uid",
                                  "bridge_owner", "namespace_create_lost_response", "mount_order", "changed_config"])
def test_generated_remote_lifecycle_and_independent_cleanup(monkeypatch, capsys, fault):
    run = "adr0151-"+"b"*12
    clock = [0]
    namespace = [None]
    bridge = [None]
    calls = []
    pool = {"Id": "pool", "Image": "original", "State": {"Running": True},
            "Config": {"Labels": {}}, "HostConfig": {}, "Mounts": [],
            "NetworkSettings": {"Networks": {"original": {}}}}
    monkeypatch.setattr(time, "monotonic", lambda: clock[0])
    monkeypatch.setattr(time, "sleep", lambda n: clock.__setitem__(0, clock[0]+n))
    monkeypatch.setattr(ssl, "create_default_context", lambda **kwargs: SimpleNamespace(load_cert_chain=lambda *a: None))

    class Socket:
        def __enter__(self): return self
        def __exit__(self, *args): pass
        def bind(self, address): assert address == ("10.1.137.69", 6432)
    import socket
    monkeypatch.setattr(socket, "socket", Socket)

    def output(args, **kwargs):
        calls.append(args)
        if args[:2] == ["docker", "inspect"]:
            row = pool if args[2] == "pool" else {"Id": bridge[0], "Name": "/cce-pooler-"+run,
                   "Image": cce.INDEX, "Config": {"Labels": {"codex-owner": "wrong" if fault == "bridge_owner" else run}}}
            row = copy.deepcopy(row)
            if args[2] == "pool":
                count = sum(c[:2] == ["docker", "inspect"] for c in calls if isinstance(c,list))
                if fault == "mount_order":
                    row["Mounts"] = [{"Destination":"/one"},{"Destination":"/two"}]
                    if count % 2 == 0:row["Mounts"].reverse()
                if fault == "changed_config" and count > 2:row["Config"]["changed"] = True
            return json.dumps([row])
        if args[:2] == ["docker", "run"]:
            bridge[0] = "c"*64
            return bridge[0]
        if args[:2] == ["docker", "ps"]:
            if any("name=^/" in a for a in args): return bridge[0] or ""
            return "pool"
        raise AssertionError(args)
    def remove(args, **kwargs):
        assert args == ["docker", "rm", "-f", "c"*64]
        bridge[0] = None
        return SimpleNamespace(returncode=0)
    monkeypatch.setattr(subprocess, "check_output", output)
    monkeypatch.setattr(subprocess, "run", remove)

    class Response(io.BytesIO):
        def __enter__(self): return self
        def __exit__(self, *args): self.close()
    def response(value): return Response(json.dumps(value).encode())
    def urlopen(request, **kwargs):
        url = request if isinstance(request, str) else request.full_url
        method = "GET" if isinstance(request, str) else request.get_method()
        body = None if isinstance(request, str) or request.data is None else json.loads(request.data)
        calls.append((method, url))
        if url.startswith("http://"):
            return response({"all_tcp_connected": fault != "network", "customer_writes": 0})
        if method == "POST" and url.endswith("/namespaces"):
            namespace[0] = {"metadata": {"uid": "ns-uid", "labels": {"codex-owner": run}}}
            if fault == "namespace_create_lost_response":
                raise TimeoutError("lost response")
            return response(namespace[0])
        if method == "POST" and url.endswith("/secrets"):
            assert body["type"] == "kubernetes.io/dockerconfigjson"
            return response({})
        if url.endswith("/pods") and method == "POST":
            assert body["spec"]["containers"][0]["image"] == cce.IMAGE
            assert body["spec"]["containers"][0]["resources"]["limits"] == {"cpu": "1", "memory": "1Gi"}
            assert body["spec"]["activeDeadlineSeconds"] == 240
            assert body["spec"]["automountServiceAccountToken"] is False
            return response({"metadata": {"uid": "pod-uid"}})
        if url.endswith("/pods/dependency-probe"):
            return response({"metadata": {"uid": "pod-uid"}, "spec": {"containers": [{"resources": {}}]},
                "status": {"phase": "Pending" if fault == "pod_startup" else "Running", "podIP": "10.3.1.1",
                   "containerStatuses": [{"restartCount": 0, "imageID": "sha256:wrong" if fault == "pod_image" else cce.MANIFEST,
                       "state": {"waiting": {"reason": "ImagePullBackOff"}} if fault == "pod_startup" else {"running": {}}}]}})
        if method == "DELETE":
            assert body["preconditions"]["uid"] == "ns-uid"
            namespace[0] = None
            return response({})
        if namespace[0] is None:
            raise urllib.error.HTTPError(url, 404, "absent", {}, None)
        row = copy.deepcopy(namespace[0])
        if fault == "namespace_uid":
            row["metadata"]["uid"] = "replacement"
        return response(row)
    monkeypatch.setattr(urllib.request, "urlopen", urlopen)
    code = cce.remote_program({"ca.crt": "fake", "client.crt": "fake", "client.key": "fake"},
                              run, "test-user", "test-password")
    compile(code, "generated", "exec")
    exec(code, {})  # noqa: S102 - execute repository-generated lifecycle with mocked cloud APIs
    report = json.loads(capsys.readouterr().out)
    assert report["customer_writes"] == report["capacity_stages_started"] == 0
    assert report["temporary_credentials_removed"] is True
    assert report["existing_runtime_unchanged"] is (fault != "changed_config")
    assert report["namespace_removed"] is (fault not in {"namespace_uid", "namespace_create_lost_response"})
    assert report["bridge_removed"] is (fault != "bridge_owner")
    assert report["pass"] is (fault is None or fault in {"namespace_uid", "bridge_owner", "mount_order", "changed_config"})
    assert not any("rm" in c and "pool" in c for c in calls if isinstance(c, list))



def test_entrypoint_maps_generator_ack_and_records_attempt_before_remote_call(area, tmp_path, monkeypatch):
    import qualify_two_host_deployment as deployment
    import run_status_refresh_comparison as locking
    import stage_status_refresh_images as staging

    config = {"synthetic": "unchanged-resources"}
    envelope = policy.read(policy.ENVELOPE)
    envelope["existing_resource_configuration_sha256"] = policy.digest(config)
    policy.write(policy.ENVELOPE, envelope)
    config_path, proof_path = tmp_path / "config.json", tmp_path / "proof.json"
    policy.write(config_path, config)
    policy.write(proof_path, proof())
    monkeypatch.setattr(cce, "kube_material", lambda path: {})
    # Use the existing fixture's exclusive owned lock; no live locks or SSH.
    monkeypatch.setattr(locking, "RunLock", lambda run: SimpleNamespace(release=lambda: None))
    monkeypatch.setattr(staging, "new_stage_output", lambda: tmp_path / ("adr0153-parents-" + "a"*12))

    class Session:
        def __init__(self, *args, **kwargs): pass
        def phase(self, phase): pass
        def begin_cleanup(self): pass
        def close(self): pass
        def call(self, role, code, timeout):
            scope = list(policy.read(policy.STATE).values())[-1]
            assert scope["qualification_protocols_started"] == 1
            if role == "generator":
                return {"generator_idle": True}
            compile(code, "generated", "exec")
            import ast
            report = restored_report()
            report["run"] = ast.literal_eval(code.splitlines()[0].split("=",1)[1])["run"]
            del report["generator_idle_after"]
            return report
    monkeypatch.setattr(deployment, "Session", Session)
    result = cce.execute(config_path, proof_path, tmp_path, tmp_path / "kubeconfig",
                         "fake-ecs-password", "fake-registry-user", "fake-registry-password")
    assert result["status"] == "PASSED_RESTORED"
    assert policy.read(policy.JOURNAL)["experiments"][-1]["paid_runs_started"] == 0



def test_mismatched_run_cannot_close_current_scope():
    report = restored_report()
    binding = {"run_id": "adr0151-"+"b"*12}
    assert cce.outcome(report, {"binding":binding,"cce_result_sha256":policy.digest(report)}, binding) == (False,False,False)


@pytest.mark.parametrize("field", ["runtime_fingerprint_before", "runtime_fingerprint_after", "runtime_drift"])
def test_runtime_proof_is_mandatory(field):
    report = restored_report()
    report[field] = None
    binding = {"run_id": report["run"]}
    assert cce.outcome(report, {"binding":binding,"cce_result_sha256":policy.digest(report)}, binding) == (False,False,False)


def goal_envelope():
    value = legacy_envelope()
    exception = value["spending"]["temporary_cce_pilot_exception"]
    exception.pop("expires_at_bangkok")
    exception.update(boundary_mode="goal_bounded", goal_bounded_authorization={
        "decision": "ADR0228", "profile": "cce_paid_comparison", "maximum_paid_stages": 1,
        "maximum_pods": 4, "pod_vcpu": 1, "pod_memory_gib": 1,
        "offered_journeys_per_second": 84, "offered_seconds": 300,
        "maximum_experiment_seconds": 3600, "unlimited_cumulative_time_explicitly_authorized": True,
        "ledger_start_index": 0,
        "spending_allowance": {"status": "APPROVED", "maximum_additional_spend": None,
                               "no_spending_cap_explicitly_authorized": True},
    })
    return value


@pytest.mark.parametrize("stamp", ["2026-10-09T00:00:00+00:00", "2040-01-01T00:00:00+00:00"])
def test_goal_boundary_has_no_calendar_cutoff(stamp):
    cce.authorized_today(goal_envelope(), datetime.fromisoformat(stamp))


@pytest.mark.parametrize("fault", ["pending", "cap_unspecified", "finite_cap", "pod_count", "duration", "paid_stages", "naive", "unknown_mode"])
def test_goal_boundary_rejects_unapproved_spend_or_changed_scope(fault):
    value = goal_envelope()
    exception = value["spending"]["temporary_cce_pilot_exception"]
    goal = exception["goal_bounded_authorization"]
    now = datetime(2026, 10, 9, tzinfo=UTC)
    if fault == "pending":
        goal["spending_allowance"]["status"] = "PENDING"
    elif fault == "cap_unspecified":
        goal["spending_allowance"]["no_spending_cap_explicitly_authorized"] = False
    elif fault == "finite_cap":
        goal["spending_allowance"]["maximum_additional_spend"] = 10
    elif fault == "pod_count":
        goal["maximum_pods"] = 8
    elif fault == "duration":
        goal["maximum_experiment_seconds"] = 7200
    elif fault == "paid_stages":
        goal["maximum_paid_stages"] = 2
    elif fault == "naive":
        now = now.replace(tzinfo=None)
    else:
        exception["boundary_mode"] = "unknown"
    with pytest.raises(ValueError):
        cce.authorized_today(value, now)


@pytest.mark.parametrize("consumed_source", ["none", "journal", "scope", "unknown"])
def test_goal_creation_checks_single_paid_stage_across_fresh_attempts(monkeypatch, consumed_source):
    import cce_api_adapter as adapter

    value = goal_envelope()
    key = "bounded_cce_paid_comparison__" + "a" * 12
    entry = {"profile": "cce_paid_comparison", "ledger": key, "paid_runs_started": int(consumed_source == "journal")}
    state = {key: {"paid_runs_started": int(consumed_source == "scope")}}
    if consumed_source == "unknown":
        state[key]["paid_runs_started"] = "unknown"
    monkeypatch.setattr(policy, "journal", lambda _: {"experiments": [entry]})
    monkeypatch.setattr(policy, "read", lambda _: state)
    now = datetime(2040, 1, 1, tzinfo=UTC)
    if consumed_source == "none":
        adapter.authorized_creation(value, now)
    else:
        with pytest.raises(ValueError):
            adapter.authorized_creation(value, now)

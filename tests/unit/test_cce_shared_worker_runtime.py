"""Shared runtime admission, source drift and partial handover failures."""
import copy
import io
import json
import sys
from datetime import UTC, datetime, timedelta
from pathlib import Path
from types import SimpleNamespace

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[2] / "scripts"))
import cce_api_adapter as api
import cce_native_workers as pods
import cce_shared_worker_comparison as comparison
import cce_shared_worker_contract as contract
import cce_shared_worker_profile as profile
import cce_worker_identity as identity
import observe_cce_workers as observer
import work_envelope as policy
from cce_ecs_transition import Transition
from cce_worker_transition import WorkerTransition
from test_cce_api_adapter import RUN


def goal(arm="control"):
    return {"decision": "ADR0228", "extension_decision": "ADR0259", "profile": "cce_paid_comparison",
            "comparison_arm": arm, "acquisition_budget": 20,
            "candidate_factor": {"name": "worker_placement", "comparison_arm": arm,
                                 "placement_plan_sha256": comparison.digest(comparison.plan()),
                                 "shared_image_receipt_sha256": profile.proof_digest(), "database_connections_unchanged": True}}


def select(monkeypatch, arm="control"):
    envelope = copy.deepcopy(policy.envelope())
    envelope["spending"]["temporary_cce_pilot_exception"]["goal_bounded_authorization"] = goal(arm)
    monkeypatch.setattr(policy, "envelope", lambda: envelope)
    return envelope


def envs():
    base = {"DATABASE_URL": "postgresql://actor:secret@pgbouncer:5432/ticketing?sslmode=disable", "API_URL": "http://load-balancer:8000", "WORKER_METRICS_PORT": "9101"}
    return {role: {**base, "DB_POOL_MAX": str({"consumer": 8, "simulator": 10}.get(role, 12))} for role in identity.COUNTS}


def bundle():
    manifests = pods.objects(RUN, envs(), "10.1.137.69")
    rows = []
    for index, expected in enumerate(item for item in manifests if item["kind"] == "Pod"):
        pod = copy.deepcopy(expected)
        uid = str(index + 1)
        pod["metadata"]["uid"] = uid
        pod["status"] = {"phase": "Running", "podIP": "10.2.0." + str(index + 1),
                         "containerStatuses": [{"name": "worker", "restartCount": 0, "ready": True,
                                                 "state": {"running": {"startedAt": "2026-10-10T00:00:00Z"}}, "imageID": comparison.IMAGE}]}
        annotations = expected["metadata"]["annotations"]
        proof = {"run": RUN, "pod_uid": uid, "sources": comparison.selected_receipt()["runtime_source_sha256"],
                 "environment_sha256": annotations["codex-environment-sha256"], "command_sha256": annotations["codex-command-sha256"]}
        rows.append(pods.receipt(pod, expected, RUN, uid, "CCE_WORKER_STARTUP " + json.dumps(proof), {"process_start_time_seconds": 100 + index}))
    return {"decision": "ADR0259", "run": RUN, "sources": comparison.selected_receipt()["runtime_source_sha256"],
            "manifest_digest": identity.MANIFEST, "resources": identity.RESOURCES, "receipts": rows}


def test_same_shared_image_keeps_frozen_restore_and_budgets(monkeypatch):
    select(monkeypatch)
    worker = contract.worker_contract()
    assert worker.images["api"] == api.dependency.INDEX
    assert worker.images["confirmation"] == identity.ECS_IMAGE_ID
    for role in worker.background:
        assert worker.images[role] == identity.ECS_IMAGE_ID
        assert worker.source_map(role) == comparison.selected_receipt()["runtime_source_sha256"]
        assert worker.parents[role] != identity.CONFIG
    assert worker.background["confirmation"]["pool_per_replica"] == 2
    assert worker.settings("simulator")["SIMULATOR_CONCURRENCY"] == "12"
    assert worker.settings("simulator")["DB_POOL_MAX"] == "10"
    assert worker.settings("reservation-writer")["RESERVATION_WRITE_PIPELINE"] == "1"
    data = api.contract()
    assert data["resources"] == {k: {"cpu": "1", "memory": "2Gi"} for k in ("requests", "limits")}
    assert data["api_settings"]["ORDER_STATUS_READ_PIPELINE"] == "0"
    assert data["workload"]["customer_retries"] == 3
    assert api.admission_budget() == 20
    assert api.api_image() == comparison.IMAGE
    assert profile.plan(goal())["baseline_is_passing_control"] is False
    compile(worker.image_program(worker.roles), "<image-proof>", "exec")


@pytest.mark.parametrize("fault", ["source", "arm", "factor", "budget"])
def test_profile_rejects_drift(fault):
    value = goal()
    if fault == "source":
        value["candidate_factor"]["shared_image_receipt_sha256"] = "0" * 64
    elif fault == "arm":
        value["comparison_arm"] = "correction"
    elif fault == "factor":
        value["candidate_factor"]["database_connections_unchanged"] = False
    else:
        value["acquisition_budget"] = 21
    with pytest.raises(ValueError):
        profile.active(value)


def test_render_does_not_mutate_settings_or_disclose_secrets_in_receipts():
    before = envs()
    objects = pods.objects(RUN, before, "10.1.137.69")
    assert envs() == before
    assert len([o for o in objects if o["kind"] == "Pod"]) == 13
    assert len([o for o in objects if o["kind"] == "Secret"]) == 6
    proof = bundle()
    assert len(identity.validate_bundle(proof)) == 13
    assert "secret" not in json.dumps(proof)
    for item in objects:
        if item["kind"] == "Pod":
            spec = item["spec"]
            assert spec["hostAliases"] == [{"ip": "10.1.137.69", "hostnames": ["kafka"]}]
            assert spec["containers"][0]["image"] == comparison.IMAGE
            assert spec["containers"][0]["resources"] == identity.RESOURCES


@pytest.mark.parametrize("fault", ["source", "uid", "duplicate", "restart_identity", "command", "resource", "public_ip", "image"])
def test_native_bundle_rejects_wrong_runtime(fault):
    value = bundle()
    row = value["receipts"][0]
    if fault == "source":
        value["sources"][next(iter(value["sources"]))] = "0" * 64
    elif fault == "uid":
        row["startup_proof"]["pod_uid"] = "wrong"
    elif fault == "duplicate":
        row["private_ipv4"] = value["receipts"][1]["private_ipv4"]
    elif fault == "restart_identity":
        row["process_start_time_seconds"] = float("nan")
    elif fault == "command":
        row["startup_proof"]["command_sha256"] = "0" * 64
    elif fault == "resource":
        row["resources"] = {"requests": {"cpu": "2"}}
    elif fault == "public_ip":
        row["private_ipv4"] = "8.8.8.8"
    else:
        row["image_id"] = "registry@sha256:" + "0" * 64
    with pytest.raises(ValueError):
        identity.validate_bundle(value)


def test_metrics_adapter_reads_once_and_rejects_process_restart():
    value = bundle()
    calls = []
    original_fetch = object()
    original = SimpleNamespace(urlopen=original_fetch, api_replicas=lambda host, port: ["legacy"])
    def counters(role, operation, url):
        result = {}
        for addr in original.api_replicas("simulator", 9101):
            with original.urlopen("http://" + addr + ":9101/metrics", timeout=2) as response:
                result[addr] = response.read().decode()
        return result
    original.worker_counters = counters
    simulator = next(r for r in value["receipts"] if r["role"] == "simulator")
    start = simulator["process_start_time_seconds"]
    def fetch(url, timeout):
        calls.append(url)
        return io.BytesIO(f'process_start_time_seconds {start}\nprocess_cpu_seconds_total 1\nticketing_worker_operations_total{{operation="simulate_one",outcome="ok"}} 2\n'.encode())
    observer.install(original, value, fetch=fetch)
    result = original.worker_counters("simulator", "simulate_one", "http://simulator:9101/metrics")
    assert len(calls) == 1
    assert original.urlopen is original_fetch
    assert result["simulator_native_processes"][simulator["private_ipv4"]]["calls"] == 2
    start += 1
    with pytest.raises(ValueError, match="identity"):
        original.worker_counters("simulator", "simulate_one", "http://simulator:9101/metrics")
    assert original.urlopen is original_fetch


def samples(value):
    left = datetime(2026, 10, 10, tzinfo=UTC)
    rows = []
    for i in range(4):
        row = {"utc": (left + timedelta(seconds=i)).isoformat()}
        for role in identity.COUNTS:
            alias = "writer" if role == "reservation-writer" else role
            row[alias + "_native_processes"] = {r["private_ipv4"]: {"process_start_time_seconds": r["process_start_time_seconds"], "process_cpu_seconds_total": i * .1, "calls": i} for r in value["receipts"] if r["role"] == role}
        rows.append(row)
    return rows, left.isoformat(), (left + timedelta(seconds=3)).isoformat()


@pytest.mark.parametrize("fault", [None, "gap", "counter", "missing", "stalled"])
def test_worker_cpu_requires_full_window_and_every_worker_progress(fault):
    value = bundle()
    rows, left, right = samples(value)
    if fault == "gap":
        rows = [rows[0], rows[-1]]
    elif fault == "counter":
        next(iter(rows[2]["consumer_native_processes"].values()))["process_cpu_seconds_total"] = 0
    elif fault == "missing":
        rows[1].pop("writer_native_processes")
    elif fault == "stalled":
        for row in rows:
            next(iter(row["simulator_native_processes"].values()))["calls"] = 0
    if fault:
        with pytest.raises(ValueError):
            observer.summarize(rows, value, left, right)
    else:
        result = observer.summarize(rows, value, left, right)
        assert result["pass"] is True
        assert result["aggregate_worker_cpu_cores"] == pytest.approx(1.3)


@pytest.mark.parametrize("removed", [True, False])
def test_native_worker_absence_precedes_ecs_restart(monkeypatch, removed):
    order = []
    transition = WorkerTransition.__new__(WorkerTransition)
    transition.session = SimpleNamespace(begin_cleanup=lambda: None, call=lambda *a: order.append("ecs-start") or {"owned_worker_transition_verified": True, "running": True})
    transition.worker_stop_attempted = True
    transition.worker_runtime = SimpleNamespace(attempted=True)
    def cleanup():
        order.append("native-delete")
        return {"namespace_removed": removed}
    transition.deployment = SimpleNamespace(cleanup=cleanup)
    transition.record = {}
    transition.checkpoint = lambda: None
    transition.worker_rows = [{"Id": format(i, "064x"), "Image": identity.CONFIG, "Config": {}, "HostConfig": {}, "Mounts": []} for i in range(1, 14)]
    monkeypatch.setattr(Transition, "restore", lambda self: order.append("api-restore") or True)
    if removed:
        assert transition.restore() is True
        assert order == ["native-delete", "ecs-start", "api-restore"]
    else:
        with pytest.raises(ValueError, match="overlapping"):
            transition.restore()
        assert order == ["native-delete"]

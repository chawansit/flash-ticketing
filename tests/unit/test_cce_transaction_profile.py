"""Matched API image binding, control admission and stopped trace transport."""
import copy
import json
import sys
from pathlib import Path
from types import SimpleNamespace

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[2] / "scripts"))
import cce_api_adapter as api
import cce_reproduction_overlay as overlay
import cce_slot_trace_transport as transport
import cce_transaction_profile as profile
import run_cce_paid_comparison as core
import work_envelope as policy
from bounded_trace_transport import pack_trace
from cce_paid_profiles import HOURLY
from test_cce_api_adapter import RUN, service


def goal(arm="control"):
    return {"decision": "ADR0228", "profile": "cce_paid_comparison", "extension_decision": "ADR0242",
            "comparison_arm": arm, "acquisition_budget": 20,
            "candidate_factor": {"name": "redundant_explicit_begin", "comparison_arm": arm,
                                 "image_pair_receipt_sha256": profile.PROOF_SHA256,
                                 "database_connections_unchanged": True}}


def envelope(value):
    original = copy.deepcopy(policy.envelope())
    original["spending"]["temporary_cce_pilot_exception"]["goal_bounded_authorization"] = value
    return original


@pytest.mark.parametrize("arm", ["control", "candidate"])
def test_exact_pair_selects_api_only_and_preserves_worker_sources(monkeypatch, arm):
    value = envelope(goal(arm))
    monkeypatch.setattr(policy, "envelope", lambda: value)
    proof = profile.image_pair()
    assert profile.active() == value["spending"]["temporary_cce_pilot_exception"]["goal_bounded_authorization"]
    contract = api.contract()
    assert len(contract["api_sources"]) == 22
    assert len(api.legacy_contract()["api_sources"]) == 21
    assert contract["backend_images"] == api.legacy_contract()["backend_images"]
    assert api.api_image() == proof["images"][arm]["registry_image"]
    assert api.api_manifest() == proof["images"][arm]["registry_manifest_digest"]
    manifests = api.objects(RUN, service(), "10.1.137.69", "test", "test", acquisition_budget=20)
    pods = [v for v in manifests if v["kind"] == "Pod"]
    assert len(pods) == 4
    for pod in pods:
        assert pod["spec"]["imagePullSecrets"] == [{"name": "default-secret"}, {"name": "swr-pull"}]
        container = pod["spec"]["containers"][0]
        assert container["image"] == api.api_image()
        assert container["resources"] == {"requests": {"cpu": "1", "memory": "1Gi"},
                                           "limits": {"cpu": "1", "memory": "1Gi"}}
    if arm == "candidate":
        monkeypatch.setattr(profile, "control_receipt", lambda _: ("tmp/control/cce-comparison.private.json", {"pass": True}))
    chosen = core.plan()
    assert chosen["comparison_arm"] == arm
    assert chosen["common"] == {"buyer_journeys_per_second": 84, "duration_seconds": 300}
    assert chosen["acquisition_budget"] == 20 and chosen["pooler_server_connections"] == 24


def test_candidate_plan_requires_control_and_rejects_hourly(monkeypatch):
    value = envelope(goal("candidate"))
    monkeypatch.setattr(policy, "envelope", lambda: value)
    with pytest.raises(ValueError, match="control ledger"):
        core.plan()
    with pytest.raises(ValueError, match="separate hourly"):
        core.plan(profile=HOURLY)


@pytest.mark.parametrize("fault", ["arm", "factor", "budget", "receipt"])
def test_changed_comparison_discriminator_rejected(fault):
    value = goal()
    if fault == "arm": value["comparison_arm"] = "typo"
    elif fault == "factor": value["candidate_factor"]["name"] = "other"
    elif fault == "budget": value["acquisition_budget"] = 24
    else: value["candidate_factor"]["image_pair_receipt_sha256"] = "0" * 64
    with pytest.raises(ValueError, match="discriminator"):
        profile.active(envelope(value))


def test_registry_proof_drift_rejected_before_source_admission(monkeypatch, tmp_path):
    path = tmp_path / profile.PROOF
    path.parent.mkdir(parents=True)
    path.write_text("{}")
    monkeypatch.setattr(policy, "ROOT", tmp_path)
    with pytest.raises(ValueError, match="image pair receipt"):
        profile.image_pair()


@pytest.mark.parametrize("fault", [None, "failed", "cleanup", "source", "budget", "digest", "gate", "active"])
def test_candidate_requires_complete_same_runner_restored_control(monkeypatch, tmp_path, fault):
    value = envelope(goal("candidate"))
    ledger = "bounded_cce_paid_comparison__" + "b" * 12
    binding = {"cce_transaction_arm": "control", "cce_transaction_pair_sha256": profile.PROOF_SHA256,
               "cce_paid_entry_sources": core.identity(), "cce_paid_core_sources": core.paid.identity(),
               "configuration_sha256": value["existing_resource_configuration_sha256"], "cce_acquisition_budget": 20}
    report = {"pass": True, "binding_sha256": policy.digest(binding), "capacity_stages_started": 1,
              "restoration_complete": True, "integrity_verified": True, "transport_credentials_cleared": True,
              "native": {"cleanup_complete": True, "measurement_gates": {"paid": True, "durability": True}}}
    if fault == "cleanup": report["native"]["cleanup_complete"] = False
    if fault == "gate": report["native"]["measurement_gates"]["paid"] = False
    if fault == "source": binding["cce_paid_entry_sources"] = {}
    if fault == "budget": binding["cce_acquisition_budget"] = 24
    entry = {"ledger": ledger, "profile": "cce_paid_comparison", "status": "PASSED_RESTORED",
             "result_sha256": policy.digest(report)}
    if fault == "failed": entry["status"] = "FAILED_RESTORED"
    if fault == "digest": entry["result_sha256"] = "0" * 64
    state = {ledger: {"binding": binding, "cce_paid_result_sha256": policy.digest(report),
                      "paid_runs_started": 1, "active_run": "busy" if fault == "active" else None}}
    retained = tmp_path / "tmp/control/cce-comparison.private.json"
    retained.parent.mkdir(parents=True)
    retained.write_text(json.dumps(report))
    value["spending"]["temporary_cce_pilot_exception"]["goal_bounded_authorization"].update(
        control_ledger=ledger, control_report_path="tmp/control/cce-comparison.private.json")
    monkeypatch.setattr(core, "identity", lambda: original_entry)
    monkeypatch.setattr(core.paid, "identity", lambda: original_core)
    monkeypatch.setattr(policy, "envelope", lambda: value)
    monkeypatch.setattr(policy, "journal", lambda _: {"experiments": [entry]})
    monkeypatch.setattr(policy, "ROOT", tmp_path)
    original_read = policy.read
    monkeypatch.setattr(policy, "read", lambda p: state if p == policy.STATE else original_read(p))
    chosen = value["spending"]["temporary_cce_pilot_exception"]["goal_bounded_authorization"]
    if fault is None:
        assert profile.control_receipt(chosen) == ("tmp/control/cce-comparison.private.json", report)
    else:
        with pytest.raises(ValueError, match="passed and restored"):
            profile.control_receipt(chosen)


original_entry = core.identity()
original_core = core.paid.identity()


@pytest.mark.parametrize("fault", [None, "current", "historical", "helper", "lock"])
def test_overlay_detects_both_current_and_historical_drift(tmp_path, fault):
    names = overlay.OVERLAID | overlay.ADDITIONAL | {overlay.MANIFEST, "docs/capacity/cce/reproduction-lock-2026-10-09.json"}
    manifest = policy.read(policy.ROOT / overlay.MANIFEST)
    names |= {item["historical_blob"] for item in manifest["overlays"].values()}
    for name in names:
        target = tmp_path / name
        target.parent.mkdir(parents=True, exist_ok=True)
        target.write_bytes((policy.ROOT / name).read_bytes())
    lock = policy.read(policy.ROOT / "docs/capacity/cce/reproduction-lock-2026-10-09.json")
    if fault:
        name = {"current": "scripts/cce_api_adapter.py", "historical": next(iter(manifest["overlays"].values()))["historical_blob"],
                "helper": "scripts/cce_slot_trace_transport.py", "lock": "docs/capacity/cce/reproduction-lock-2026-10-09.json"}[fault]
        (tmp_path / name).write_bytes(b"drift")
        with pytest.raises(ValueError): overlay.verify(tmp_path, lock)
    else:
        assert overlay.verify(tmp_path, lock) == manifest


@pytest.mark.parametrize("fault", [None, "sha", "name", "directory"])
def test_stopped_trace_transport_preserves_exact_bytes(tmp_path, fault):
    raw = b'{"sample":true}\n' * 1000
    source = tmp_path / "source.jsonl"
    source.write_bytes(raw)
    packed = tmp_path / "source.jsonl.gz"
    receipt = pack_trace(source, packed)
    if fault == "sha": receipt["raw_sha256"] = "0" * 64
    stage = SimpleNamespace(run=RUN, cid="c" * 64, output=tmp_path / "out", check=lambda _: None,
                            session=SimpleNamespace(api=lambda *_: receipt))
    stage.output.mkdir()
    directory = "/tmp/" + RUN + "/cce-observers"
    name = "pipeline"
    if fault == "name": name = "../foreign"
    if fault == "directory": directory = "/tmp/foreign"
    if fault:
        with pytest.raises(ValueError): transport.collect(stage, directory, "/owned", name, lambda *_: packed.read_bytes())
    else:
        result, proof = transport.collect(stage, directory, "/owned", name, lambda *_: packed.read_bytes())
        assert result == raw and proof["compressed"] is True


@pytest.mark.parametrize("stop_failed", [False, True])
def test_compression_requires_stopped_jobs_and_keeps_slot_evidence_on_cpu_failure(monkeypatch, tmp_path, stop_failed):
    import cce_paid_observers as observers
    import database_wait_evidence as waits
    import observe_two_host_cpu as cpu
    import slot_failure_evidence as slots
    import summarize_paid_kafka_lag as kafka
    import summarize_paid_pipeline as pipeline
    from test_observe_cce_paid_pipeline import bundle

    calls = []
    def stop():
        calls.append("stop")
        if stop_failed: raise ValueError("unknown stop")
    stage = SimpleNamespace(profile=core.SHORT, run=RUN, cid="c" * 64, session=SimpleNamespace(),
                            output=tmp_path, record={}, checkpoint=lambda: None, stop_jobs=stop)
    obs = observers.Observers(stage, {}, None, None, "/owned")
    obs.native = bundle()
    obs.start_utc, obs.end_utc = "start", "end"
    monkeypatch.setattr(profile, "active", lambda: goal())
    monkeypatch.setattr(cpu, "compare_windows", lambda *args, **kwargs: {"pass": True})
    obs.record.update(primary_cpu={}, secondary_cpu={})
    def collect(*args):
        calls.append(args[3])
        assert calls[0] == "stop"
        return b'{"sample":true}\n', {"verified": {"pass": True}}
    monkeypatch.setattr(transport, "collect", collect)
    monkeypatch.setattr(pipeline, "summarize", lambda _: {"pass": True})
    monkeypatch.setattr(kafka, "summarize", lambda _: {"retained": True})
    monkeypatch.setattr(waits, "summarize", lambda _: {"complete": True})
    monkeypatch.setattr(slots, "summarize", lambda *_: {"pass": True, "retained_failures": 1})
    monkeypatch.setattr(observers, "api_cpu", lambda *a, **k: (_ for _ in ()).throw(ValueError("CPU gap")))
    with pytest.raises(ValueError, match="observation incomplete"):
        obs.collect()
    if stop_failed:
        assert calls == ["stop"]
        assert "slot_failure_capture" not in obs.record
    else:
        assert calls == ["stop", "pipeline", "kafka"]
        assert obs.record["slot_failure_capture"]["retained_failures"] == 1
        assert obs.record["kafka_summary"]["retained"] is True
    assert obs.record["observer_pass"] is False



def test_registered_control_reservation_and_guard_keep_exact_source_binding(tmp_path, monkeypatch):
    from test_work_envelope import area
    binding, _, _ = area.__wrapped__(tmp_path, monkeypatch)
    value = policy.read(policy.ENVELOPE)
    new_goal = copy.deepcopy(value["spending"]["temporary_cce_pilot_exception"]["goal_bounded_authorization"])
    new_goal.update(goal(), maximum_paid_stages=1, maximum_pods=4, pod_vcpu=1, pod_memory_gib=1,
                    offered_journeys_per_second=84, offered_seconds=300, maximum_experiment_seconds=3600,
                    ledger_start_index=0, unlimited_cumulative_time_explicitly_authorized=True)
    value["spending"]["temporary_cce_pilot_exception"]["goal_bounded_authorization"] = new_goal
    policy.write(policy.ENVELOPE, value)
    chosen = core.plan()
    binding.update(cce_paid_entry_sources=core.identity(), cce_paid_core_sources=core.paid.identity(),
                   cce_transaction_arm="control", cce_transaction_pair_sha256=profile.PROOF_SHA256,
                   baseline_sha256=chosen["baseline_sha256"], cce_acquisition_budget=20)
    binding.update({k: "a" * 64 for k in ("cce_manifest_sha256", "diagnostic_target_sha256",
                   "saved_api_service_sha256", "image_proof_sha256", "cce_resource_source_sha256",
                   "cce_transition_source_sha256")})
    entry = policy.reserve(binding, chosen, profile="cce_paid_comparison")
    guard = policy.ActionGuard(entry["ledger"], binding)
    guard.check()
    assert policy.read(policy.STATE)[entry["ledger"]]["paid_runs_authorized"] == 1
    binding["cce_paid_core_sources"] = {}
    with pytest.raises(ValueError): guard.check()


@pytest.mark.parametrize("arm", ["control", "candidate"])
def test_full_transition_keeps_ecs_settings_separate_from_native_pod_receipts(monkeypatch, arm):
    import cce_ecs_transition as topology
    from test_cce_ecs_transition import area, pod_receipts

    transition, rows, route, _calls, _ = area.__wrapped__(monkeypatch)
    selected = copy.deepcopy(api.legacy_contract())
    selected["api_settings"]["DB_FAILURE_DIAGNOSTICS"] = "1"
    selected["api_sources"] = profile.image_pair()["images"][arm]["runtime_sources_sha256"]
    monkeypatch.setattr(api, "contract", lambda: selected)
    monkeypatch.setattr(api, "api_manifest", lambda: profile.image_pair()["images"][arm]["registry_manifest_digest"])
    transition.capture()
    assert not any("DB_FAILURE_DIAGNOSTICS=" in item for host in rows.values() for row in host
                   if row["Config"]["Labels"].get("com.docker.compose.service") == "api"
                   for item in row["Config"]["Env"])
    receipts = pod_receipts()
    for receipt in receipts: receipt["image_id"] = "docker-pullable://example@" + api.api_manifest()
    transition.activate(receipts)
    assert transition.record["all_four_ecs_apis_stopped"] is True
    assert transition.restore()["candidate_restored"] is True
    assert route["text"] == transition.original_route
    receipts[0]["startup_proof"]["sources"] = api.legacy_contract()["api_sources"]
    with pytest.raises(ValueError, match="native API admission"):
        topology.receipt_gate(receipts, RUN)


def test_saved_ecs_service_stays_legacy_while_native_api_adds_common_diagnostics(monkeypatch):
    value = envelope(goal())
    monkeypatch.setattr(policy, "envelope", lambda: value)
    saved = {"model": {"services": {"api": service()}}}
    original = core.service_for(saved)
    assert "DB_FAILURE_DIAGNOSTICS" not in original["environment"]
    assert api.api_environment(original, "10.1.137.69", acquisition_budget=20)["DB_FAILURE_DIAGNOSTICS"] == "1"



def test_readiness_timeout_retains_all_four_owned_pod_reasons_before_cleanup(monkeypatch):
    persisted, calls = [], []
    manifests = api.objects(RUN, service(), "10.1.137.69", "test", "test")
    def request(method, path, body):
        calls.append(path)
        index = int(path.rsplit("-", 1)[1])
        pod = copy.deepcopy(manifests[index + 3])
        pod["metadata"]["uid"] = "uid-" + str(index)
        pod["status"] = {"phase": "Pending", "conditions": [{"type": "PodScheduled", "status": "False", "reason": "Unschedulable"}],
                         "containerStatuses": [{"name": "api", "state": {"waiting": {"reason": "ImagePullBackOff", "message": "private-message-must-not-be-persisted"}}}]}
        return pod
    deployment = core.ReadyDeployment(request, None, persisted.append)
    deployment.namespace = "flash-cce-" + "a" * 12
    deployment.pod_uids = {"api-" + str(i): "uid-" + str(i) for i in range(4)}
    monkeypatch.setattr(deployment, "authorize", lambda: None)
    clock = iter((0, 181))
    monkeypatch.setattr(core.time, "monotonic", lambda: next(clock))
    with pytest.raises(TimeoutError, match="readiness expired"):
        deployment.observe(manifests, None, None)
    assert len(calls) == 4
    views = persisted[-1]["readiness_timeout_views"]
    assert len(views) == 4 and {v["pod_name"] for v in views} == {"api-" + str(i) for i in range(4)}
    assert all(v["container_states"][0]["waiting_reason"] == "ImagePullBackOff" for v in views)
    assert all(v["readiness_conditions"][0]["reason"] == "Unschedulable" for v in views)
    assert "private-message" not in json.dumps(views)

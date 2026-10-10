"""ADR0271 exact worker placement, unchanged workload and rejection of stale images."""
import copy
import io
import json
from types import SimpleNamespace

import cce_api_adapter as api
import cce_event_lane_profile as lane
import cce_native_workers as pods
import cce_transaction_profile as transaction
import cce_worker_identity as identity
import cce_worker_rebalance_profile as profile
import observe_cce_workers as observer
import pytest
import work_envelope as policy
from test_cce_shared_worker_runtime import RUN, envs


def goal():
    return {"decision": "ADR0228", "extension_decision": "ADR0271", "profile": "cce_paid_comparison",
            "comparison_arm": "correction", "acquisition_budget": 20, "candidate_factor": profile.factor()}


def bundle():
    sources = lane.image()["runtime_source_sha256"]
    values = envs()
    values["projection-consumer"] = {**values["consumer"], "DB_POOL_MAX": "6"}
    manifests = pods.objects(RUN, values, "10.1.137.69", "ADR0271")
    rows = []
    for index, expected in enumerate(item for item in manifests if item["kind"] == "Pod"):
        pod = copy.deepcopy(expected)
        uid = str(index + 1)
        pod["metadata"]["uid"] = uid
        pod["status"] = {"phase": "Running", "podIP": f"10.2.0.{index+1}",
                         "containerStatuses": [{"name": "worker", "restartCount": 0, "ready": True,
                            "state": {"running": {"startedAt": "2026-10-10T00:00:00Z"}},
                            "imageID": lane.image()["registry_image"]}]}
        annotations = expected["metadata"]["annotations"]
        proof = {"run": RUN, "pod_uid": uid, "sources": sources,
                 "environment_sha256": annotations["codex-environment-sha256"],
                 "command_sha256": annotations["codex-command-sha256"]}
        rows.append(pods.receipt(pod, expected, RUN, uid, "CCE_WORKER_STARTUP " + json.dumps(proof),
                                {"process_start_time_seconds": 100+index}, "ADR0271"))
    return {"decision": "ADR0271", "run": RUN, "sources": sources,
            "manifest_digest": identity.specification("ADR0271")["manifest"],
            "resources": identity.RESOURCES, "receipts": rows}


def test_exact_eighteen_pod_plan_preserves_load_and_database_budgets(monkeypatch):
    value = profile.plan(goal())
    assert value["maximum_pods"] == 18
    assert value["total_requested_vcpu"] == 7.5 and value["total_requested_memory_gib"] == 15
    assert value["common"] == {"buyer_journeys_per_second": 84, "duration_seconds": 300}
    assert value["pooler_server_connections"] == 24 and value["acquisition_budget"] == 20
    assert value["consumer_local_pool_budget"] == 48
    assert value["simulator_concurrency"] == 16 and value["customer_recovery_max_attempts"] == 3
    assert value["baseline_is_passing_control"] is False
    envelope = copy.deepcopy(policy.envelope())
    envelope["spending"]["temporary_cce_pilot_exception"]["goal_bounded_authorization"] = goal()
    monkeypatch.setattr(policy, "envelope", lambda: envelope)
    assert transaction.active()["extension_decision"] == "ADR0271"
    assert transaction.plan() == value
    assert api.api_image() == lane.image()["registry_image"]
    assert api.admission_budget() == 20
    assert api.contract()["workload"]["customer_retries"] == 3


@pytest.mark.parametrize("fault", ["image", "source", "missing_projection", "duplicate_uid", "decision"])
def test_reject_stale_or_partial_worker_identity(fault):
    value = bundle()
    assert len(identity.validate_bundle(value)) == 14
    if fault == "image": value["manifest_digest"] = identity.MANIFEST
    elif fault == "source": value["sources"][next(iter(value["sources"]))] = "0"*64
    elif fault == "missing_projection": value["receipts"] = [r for r in value["receipts"] if r["role"] != "projection-consumer"]
    elif fault == "duplicate_uid": value["receipts"][0]["pod_uid"] = value["receipts"][1]["pod_uid"]
    else: value["decision"] = "ADR0259"
    with pytest.raises(ValueError): identity.validate_bundle(value)


def test_projection_native_progress_uses_real_consume_refresh_batch_metric():
    value = bundle()
    projection = next(r for r in value["receipts"] if r["role"] == "projection-consumer")
    module = SimpleNamespace(METRICS={}, urlopen=None, api_replicas=lambda host, port: [])
    def counters(role, operation, url):
        for address in module.api_replicas("projection-consumer", 9101):
            with module.urlopen(f"http://{address}:9101/metrics", timeout=2) as response:
                response.read()
        return {}
    module.worker_counters = counters
    def fetch(address, timeout):
        return io.BytesIO((f"process_start_time_seconds {projection['process_start_time_seconds']}\n"
                           "process_cpu_seconds_total 1\n"
                           'ticketing_worker_operations_total{operation="consume_refresh_batch",outcome="ok"} 3\n').encode())
    observer.install(module, value, fetch=fetch)
    assert module.METRICS["projection"][0] == "consume_refresh_batch"
    result = module.worker_counters("projection", *module.METRICS["projection"])
    assert result["projection_native_processes"][projection["private_ipv4"]]["calls"] == 3


def test_resource_authorization_requires_exact_eighteen_pod_envelope():
    from cce_dependency_probe import authorized_today
    envelope = copy.deepcopy(policy.envelope())
    old = envelope["spending"]["temporary_cce_pilot_exception"]["goal_bounded_authorization"]
    old.update(goal(), maximum_pods=18, worker_resources={"cpu": "250m", "memory": "512Mi"})
    authorized_today(envelope)
    old["maximum_pods"] = 19
    with pytest.raises(ValueError): authorized_today(envelope)

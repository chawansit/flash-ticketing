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
    old.update(goal(), maximum_pods=18, maximum_paid_stages=1, pod_vcpu=1, pod_memory_gib=2,
               offered_journeys_per_second=84, offered_seconds=300, maximum_experiment_seconds=3600,
               worker_resources={"cpu": "250m", "memory": "512Mi"})
    authorized_today(envelope)
    old["maximum_pods"] = 19
    with pytest.raises(ValueError): authorized_today(envelope)


@pytest.mark.parametrize("kind,expected", [("connection_refused", ConnectionRefusedError), ("timeout", TimeoutError)])
def test_startup_network_failure_reaches_existing_bounded_readiness_loop(kind, expected):
    from run_cce_paid_comparison import worker_metrics_reader
    session = SimpleNamespace(call=lambda *args: {"ready": False, "kind": kind})
    with pytest.raises(expected): worker_metrics_reader(session, "10.2.0.1")


def test_remote_refused_socket_is_structured_without_secrets(monkeypatch):
    import contextlib
    import urllib.error
    import urllib.request

    from run_cce_paid_comparison import WORKER_METRICS_READ
    def refused(*args, **kwargs):
        raise urllib.error.URLError(ConnectionRefusedError(111, "Connection refused"))
    monkeypatch.setattr(urllib.request, "urlopen", refused)
    output = io.StringIO()
    with contextlib.redirect_stdout(output):
        exec(compile(WORKER_METRICS_READ, "<bounded-worker-read>", "exec"), {"address": "10.2.0.1"})  # noqa: S102 - reviewed remote reader
    assert json.loads(output.getvalue()) == {"ready": False, "kind": "connection_refused"}


def test_invalid_worker_metrics_are_not_treated_as_startup_retry():
    from run_cce_paid_comparison import worker_metrics_reader
    session = SimpleNamespace(call=lambda *args: {"ready": True, "metrics": "process_cpu_seconds_total nan\nprocess_start_time_seconds 2\n"})
    with pytest.raises(ValueError): worker_metrics_reader(session, "10.2.0.1")


def test_remote_worker_filter_drops_arbitrary_log_content():
    namespace = {}
    exec(compile(api.WORKER_DIAGNOSTIC_FILTER, "<worker-filter>", "exec"), namespace)  # noqa: S102 - reviewed redaction
    value = namespace["filtered_worker_failure"](b"secret-password postgres://user:secret@host/db\nNoBrokersAvailable\n")
    assert value["categories"] == ["kafka_bootstrap_unavailable"]
    assert "secret" not in json.dumps(value) and "postgres://" not in json.dumps(value)


@pytest.mark.parametrize("log_failure", [False, True])
def test_failed_worker_retains_only_filtered_owner_bound_evidence(log_failure):
    saved = []
    def request(*args):
        if "/log?" not in args[1]: return copy.deepcopy(pod)
        if log_failure: raise TimeoutError()
        return {"schema_version": 1, "exception_types": ["NoBrokersAvailable"], "categories": ["kafka_bootstrap_unavailable"]}
    deployment = SimpleNamespace(run=RUN, verify_owner=lambda: None, request=request)
    runtime = pods.Workers(deployment, [], lambda _: {}, lambda _: None, "ADR0271", failure_persist=saved.append)
    runtime.uids = {"consumer-0": "uid-1"}
    pod = {"metadata": {"name": "consumer-0", "uid": "uid-1", "labels": {"codex-owner": RUN}},
           "spec": {"secret": "do-not-retain"}, "status": {"containerStatuses": [
               {"name": "worker", "restartCount": 0, "state": {"terminated": {"exitCode": 1, "reason": "Error", "message": "private-message"}}}]}}
    runtime.capture_startup_failure(pod, "/owned/consumer-0")
    assert saved[0]["pod_uid"] == "uid-1" and saved[0]["container_states"][0]["state"]["exit_code"] == 1
    assert "do-not-retain" not in json.dumps(saved) and "private-message" not in json.dumps(saved)
    assert ("capture_error" in saved[0]) is log_failure
    pod["metadata"]["uid"] = "replacement"
    with pytest.raises(ValueError): runtime.capture_startup_failure(pod, "/owned/consumer-0")


@pytest.mark.parametrize("attempts,missing_selector", [(3, False), (1, False), (None, False), (3, True)])
def test_validated_worker_goal_constructs_real_recovery_stage(tmp_path, monkeypatch, attempts, missing_selector):
    import cce_paid_stage as stage
    import customer_recovery_bundle as recovery

    envelope = copy.deepcopy(policy.envelope())
    envelope["spending"]["temporary_cce_pilot_exception"]["goal_bounded_authorization"] = goal()
    selected = transaction.active(envelope)
    parent = stage.generator_bundle()
    original = policy.ROOT
    files = {
        recovery.MANIFEST, recovery.COORDINATOR, *recovery.OVERLAYS,
        "scripts/run_synchronized_paid_generator.py",
        "docs/capacity/flash-sale-opening/frozen-baseline-cpu-diagnostic-plan-2026-10-05.json",
    }
    for name in files:
        target = tmp_path / name
        target.parent.mkdir(parents=True, exist_ok=True)
        target.write_bytes((original / name).read_bytes())
    monkeypatch.setattr(policy, "ROOT", tmp_path)
    monkeypatch.setattr(transaction, "active", lambda: selected)
    output = tmp_path / "tmp" / RUN / "candidate"
    output.mkdir(parents=True)
    binding = {} if attempts is None else {"cce_recovery_max_attempts": attempts}
    guard = SimpleNamespace(key="bounded_cce_paid_comparison__" + "f" * 12, binding=binding)
    session = SimpleNamespace(config={"generator": {"repo": "/root/flash-generator"}})
    if missing_selector:
        monkeypatch.setattr(recovery, "enabled", lambda _: False)
    if attempts != 3 or missing_selector:
        message = "profile and binding mismatch" if missing_selector else "Explicit short recovery binding"
        with pytest.raises(ValueError, match=message):
            stage.PaidStage(session, guard, RUN, "b" * 64, output, bundle=parent)
        return
    paid = stage.PaidStage(session, guard, RUN, "b" * 64, output, bundle=parent)
    assert paid.recovery_enabled is True
    assert paid.record["customer_recovery_max_attempts"] == 3
    assert len(paid.bundle) == 80
    assert paid.coordinator == (original / recovery.COORDINATOR).read_bytes()
    assert paid.bundle["scripts/customer_recovery_client.py"] == recovery.raw("scripts/customer_recovery_client.py")
    assert stage.generator_arguments(paid.gen, paid.origin, 0, profile=paid.profile,
                                     recovery_max_attempts=paid.record["customer_recovery_max_attempts"])[-2:] == ["--recovery-max-attempts", "3"]

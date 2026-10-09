"""Fixed native CCE caller tests; no cloud or customer traffic."""

import copy
import sys
from pathlib import Path
from types import SimpleNamespace

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[2] / "scripts"))
import cce_paid_inputs as inputs
import run_cce_paid_comparison as runner
import work_envelope as policy
from cce_paid_lifecycle import SAFETY
from test_observe_cce_paid_pipeline import bundle


def qualified():
    probe = {
        "requests_per_pod": [25] * 4,
        "wave_requests": 100,
        "accepted_holds": 1,
        "one_durable_owner_before_payment": True,
        "all_pod_hold_replay": True,
        "all_pod_payment_replay": True,
        "other_actor_denied_on_all_pods": True,
        "customer_ticket_confirmed": True,
    }
    financial = {
        "pass": True,
        "hold_deadlines_elapsed": True,
        "callback_delivery_attempts": 3,
        "callback_delivery_target": 3,
        "duplicate_booked_seats": 0,
        "multi_booking_orders": 0,
    }
    return probe, financial, {"pass": True, "kafka_members": 6}


def test_native_safety_requires_actual_complete_evidence():
    result = inputs.safety_result(*qualified())
    assert set(result["gates"]) == set(SAFETY) and all(result["gates"].values())


@pytest.mark.parametrize(
    "part,key,value",
    [
        (0, "requests_per_pod", [50, 50, 0, 0]),
        (0, "wave_requests", 99),
        (0, "accepted_holds", 2),
        (0, "one_durable_owner_before_payment", False),
        (0, "all_pod_hold_replay", False),
        (0, "all_pod_payment_replay", False),
        (0, "other_actor_denied_on_all_pods", False),
        (0, "customer_ticket_confirmed", False),
        (1, "pass", False),
        (1, "hold_deadlines_elapsed", False),
        (1, "callback_delivery_attempts", 2),
        (1, "callback_delivery_target", 4),
        (1, "duplicate_booked_seats", 1),
        (1, "multi_booking_orders", 1),
        (2, "pass", False),
        (2, "kafka_members", 5),
    ],
)
def test_safety_failure_cannot_qualify(part, key, value):
    records = qualified()
    records[part][key] = value
    with pytest.raises(ValueError, match="executed native safety"):
        inputs.safety_result(*records)


@pytest.mark.parametrize("shows", [1, 84])
def test_actual_fixture_program_verifies_frozen_producer_and_is_bounded(shows):
    text = inputs.fixture_program("/tmp/owned/" + str(shows), shows, "f" * 64)
    compile(text, "fixture", "exec")
    assert "hashlib.sha256" in text and "timeout=90" in text
    assert "'--seats','300','--sale-hours','1'" in text
    assert text.index("creation-intent.json") < text.index("subprocess.run")


def test_actual_native_safety_program_compiles_and_never_returns_tokens():
    text = inputs.safety_program("/tmp/owned", "a", bundle()["receipts"], "adr0147-" + "a" * 32)
    compile(text, "safety", "exec")
    assert "probe_actors(" in text and "json.dumps(tokens)" not in text
    assert "asyncio.run(probe(" in text and "evidence=evidence" in text
    compile(inputs.safety_audit_program(["a"]), "audit", "exec")
    assert "audit(conn,events,1,1,3)" in inputs.safety_audit_program(["a"])


def test_scope_consumed_before_any_cloud_call_and_never_replayed(tmp_path, monkeypatch):
    monkeypatch.setattr(policy, "STATE", tmp_path / "state.json")
    key = "bounded_cce_paid_comparison__" + "b" * 12
    policy.write(policy.STATE, {key: {"paid_runs_started": 0}})
    guard = SimpleNamespace(key=key, check=lambda _: None)
    runner.activate_scope(guard, "adr0151-" + "a" * 12)
    state = policy.read(policy.STATE)
    assert state[key]["safety_protocols_started"] == 2 and state[key]["paid_protocols_started"] == 1
    assert state["current_run"] == state[key]["active_run"] == "adr0151-" + "a" * 12
    with pytest.raises(ValueError, match="no replay"):
        runner.activate_scope(guard, "adr0151-" + "a" * 12)


def test_readiness_wait_observes_immutable_pods_before_admission(monkeypatch):
    names = [{"metadata": {"name": "api-" + str(i)}} for i in range(4)]
    calls = []

    def request(method, path, body):
        calls.append(path)
        name = path.rsplit("/", 1)[1]
        return {"metadata": {"uid": name}, "status": {"conditions": [{"type": "Ready", "status": "True"}]}}

    value = runner.ReadyDeployment(request, None, lambda _: None)
    value.namespace = "flash-cce-" + "a" * 12
    value.pod_uids = {"api-" + str(i): "api-" + str(i) for i in range(4)}
    monkeypatch.setattr(value, "authorize", lambda: None)
    monkeypatch.setattr(runner.native.Deployment, "observe", lambda *a, **k: "admitted")
    assert value.observe([{}, {}, {}, *names], None, None) == "admitted" and len(calls) == 4
    value.pod_uids["api-1"] = "replacement"
    with pytest.raises(ValueError, match="replaced"):
        value.observe([{}, {}, {}, *names], None, None)


@pytest.fixture
def composed(tmp_path, monkeypatch):
    output = tmp_path / ("adr0151-" + "a" * 12)
    output.mkdir()
    calls = []
    failure = set()
    saved = {"model": {"services": {"api": {"image": "original", "environment": {}}}}}
    config = {"primary": {"private_ipv4": "10.1.137.69"}}
    inventory = {"hosts": {r: {"machine_id_sha256": "f" * 64} for r in ("primary", "secondary")}}
    policy.write(output / "pre-safety-inventory.private.json", inventory)
    contract = SimpleNamespace(global_audit="audit", diagnostic_context=None)
    monkeypatch.setattr(runner.baseline, "GeneratorCompletionProbeContract", lambda *a: contract)
    monkeypatch.setattr(runner.baseline, "plan", lambda: {"artifact_receipt": {}})
    monkeypatch.setattr(runner.baseline, "source_contract", dict)
    monkeypatch.setattr(runner, "service_for", lambda s: s["model"]["services"]["api"])
    monkeypatch.setattr(runner, "api_semantics", lambda service: service)
    monkeypatch.setattr(runner.native, "objects", lambda *a, **kw: [{}])
    monkeypatch.setattr(runner, "activate_scope", lambda *a: calls.append("scope"))
    monkeypatch.setattr(runner, "restoration_complete", lambda r: r.get("restore_pass") is True)
    runner.REGISTRY_CREDENTIALS.update(username="fake", password="fake")
    guard = SimpleNamespace(
        check=lambda _: None,
        binding={
            "saved_api_service_sha256": policy.digest(saved["model"]["services"]["api"]),
            "cce_manifest_sha256": policy.digest([{}]),
        },
    )
    session = SimpleNamespace(call=lambda *a: [], begin_cleanup=lambda: calls.append("cleanup"))

    class Resources:
        def __init__(self, *a):
            self.record = {}

        def create(self):
            calls.append("create")
            if "create" in failure:
                raise ValueError("create")
            return "c" * 64

        def cleanup(self):
            calls.append("resource_cleanup")

    monkeypatch.setattr(runner, "Resources", Resources)
    monkeypatch.setattr(runner.paid, "PaidStage", lambda *a: SimpleNamespace(record={}))
    monkeypatch.setattr(runner, "Inputs", lambda *a: SimpleNamespace(safety=None, cleanup=None, paid=None))

    class Transport:
        def __init__(self, *a):
            self.request = None

        def close(self):
            calls.append("transport_closed")

    monkeypatch.setattr(runner.native, "KubernetesTransport", Transport)
    monkeypatch.setattr(runner, "ReadyDeployment", lambda *a: None)
    monkeypatch.setattr(runner, "Transition", lambda *a: None)
    monkeypatch.setattr(runner, "Observers", lambda *a: None)

    class Lifecycle:
        def __init__(self, *a, **k):
            calls.append("composed")
            self.record = {
                "pass": True,
                "cleanup_complete": True,
                "measurement_gates": dict.fromkeys(
                    ("post_ttl_financial", "zero_double_booking", "payment_durability", "full_queue_drain"),
                    True,
                ),
            }

        def run(self, *a):
            calls.append("native_run")
            if "native" in failure:
                self.record["pass"] = False
                raise ValueError("native")
            return self.record

    monkeypatch.setattr(runner, "Lifecycle", Lifecycle)

    def driver(config, output, stage_hook, runtime_policy, action_guard):
        assert runtime_policy is contract and action_guard is guard
        stage_hook(session, "control", [], saved, "/root/repo/tmp/" + output.name, output)
        try:
            stage_hook(session, "candidate", [], saved, "/root/repo/tmp/" + output.name, output)
        except ValueError:
            calls.append("original_failure_retained")
        calls.append("outer_restore")
        return {
            "restore_pass": True,
            "pass": "native" not in failure and "create" not in failure,
            "capacity_stages_started": 1 if "create" not in failure else 0,
            "post_ttl_financial": {"pass": True, "hold_deadlines_elapsed": True},
        }

    monkeypatch.setattr(runner.driver, "run", driver)
    yield output, config, guard, calls, failure
    runner.REGISTRY_CREDENTIALS.clear()


def test_concrete_entry_path_composes_once_and_restores(composed):
    output, config, guard, calls, _ = composed
    result = runner.run(config, output, guard, [{}], "private", None)
    assert result["pass"] and result["integrity_verified"] and result["restoration_complete"]
    assert calls == ["scope", "create", "composed", "native_run", "transport_closed", "outer_restore"]
    assert policy.read(output / "cce-comparison.private.json")["capacity_stages_started"] == 1


@pytest.mark.parametrize("fault", ["create", "native"])
def test_concrete_entry_failure_keeps_outer_restoration(composed, fault):
    output, config, guard, calls, failure = composed
    failure.add(fault)
    result = runner.run(config, output, guard, [{}], "private", None)
    assert result["pass"] is False and "outer_restore" in calls
    if fault == "create":
        assert "resource_cleanup" in calls and "native_run" not in calls
    else:
        assert "transport_closed" in calls


def test_bound_environment_mismatch_cannot_create_resources(composed):
    output, config, guard, calls, _ = composed
    guard.binding["saved_api_service_sha256"] = "changed"
    result = runner.run(config, output, guard, [{}], "private", None)
    assert "create" not in calls and "outer_restore" in calls
    assert not result["pass"]


@pytest.mark.parametrize(
    "fault", [None, "binding", "result", "sources", "cleanup", "integrity", "paid_count", "safety_count"]
)
def test_result_classification_cannot_close_unknown_recovery(monkeypatch, fault):
    binding = {"cce_paid_entry_sources": {"source": "fixed"}}
    monkeypatch.setattr(runner, "identity", lambda: {"source": "fixed"})
    report = {
        "pass": True,
        "binding_sha256": policy.digest(binding),
        "restoration_complete": True,
        "integrity_verified": True,
        "transport_credentials_cleared": True,
        "capacity_stages_started": 1,
        "native": {"cleanup_complete": True},
    }
    scope = {
        "binding": binding,
        "cce_paid_result_sha256": policy.digest(report),
        "paid_runs_started": 1,
        "qualification_protocols_started": 1,
        "paid_protocols_started": 1,
        "safety_protocols_started": 2,
    }
    if fault == "binding":
        scope["binding"] = {}
    if fault == "result":
        scope["cce_paid_result_sha256"] = "foreign"
    if fault == "sources":
        monkeypatch.setattr(runner, "identity", lambda: {"source": "changed"})
    if fault == "cleanup":
        report["native"]["cleanup_complete"] = False
    if fault == "integrity":
        report["integrity_verified"] = False
    if fault == "paid_count":
        scope["paid_runs_started"] = 0
    if fault == "safety_count":
        scope["safety_protocols_started"] = 1
    restored, integrity, passed = runner.outcome(report, scope, binding)
    if fault is None:
        assert restored and integrity and passed
    else:
        assert not passed


def test_read_only_http_evidence_has_exact_native_process_metrics():
    payload = "process_cpu_seconds_total 1.5\nprocess_start_time_seconds 100.0\n# help ignored\n"
    calls = []
    session = SimpleNamespace(call=lambda *a: calls.append(a) or payload)
    assert runner.http_reader(session, "10.2.1.4", "/metrics", metrics=True) == {
        "process_cpu_seconds_total": 1.5,
        "process_start_time_seconds": 100.0,
    }
    compile(calls[0][1], "http reader", "exec")
    assert "timeout=3" in calls[0][1] and "4194305" in calls[0][1]


def test_guarded_entry_does_not_accept_missing_private_inputs(monkeypatch):
    import run_work_envelope as entry

    monkeypatch.setattr(entry.sys.stdin, "isatty", lambda: False)
    with pytest.raises(ValueError, match="Protected CCE paid inputs"):
        entry.execute(None, None, None, profile_name="cce_paid_comparison")


import test_work_envelope as envelope_tests


@pytest.fixture
def reservation_area(tmp_path, monkeypatch):
    return envelope_tests.area.__wrapped__(tmp_path, monkeypatch)


def test_registered_profile_reserves_exact_single_paid_stage(reservation_area, monkeypatch):
    binding, _, _ = reservation_area
    envelope = policy.read(policy.ENVELOPE)
    envelope["qualified_profiles"] = list(policy.PROFILES)
    policy.write(policy.ENVELOPE, envelope)
    expected = {
        "decision": "ADR0228",
        "arms": ["candidate"],
        "common": {"buyer_journeys_per_second": 84, "duration_seconds": 300},
    }
    monkeypatch.setattr(runner, "plan", lambda: expected)
    monkeypatch.setattr(runner, "identity", lambda: {"entry": "fixed"})
    monkeypatch.setattr(runner.paid, "identity", lambda: {"core": "fixed"})
    monkeypatch.setattr(runner.native, "authorized_creation", lambda data: None)
    binding.update(cce_paid_entry_sources={"entry": "fixed"}, cce_paid_core_sources={"core": "fixed"})
    binding.update(
        {
            k: "a" * 64
            for k in (
                "cce_manifest_sha256",
                "diagnostic_target_sha256",
                "saved_api_service_sha256",
                "image_proof_sha256",
                "cce_resource_source_sha256",
                "cce_transition_source_sha256",
            )
        }
    )
    reserved = policy.reserve(binding, expected, profile="cce_paid_comparison")
    scope = policy.read(policy.STATE)[reserved["ledger"]]
    assert reserved["reserved_seconds"] == 3600 and scope["paid_runs_authorized"] == 1
    assert scope["safety_tickets_authorized"] == 2 and scope["qualification_runs_authorized"] == 1
    assert scope["paid_runs_started"] == scope["safety_protocols_started"] == 0


@pytest.mark.parametrize("fault", ["rate", "duration", "entry_sources", "manifest"])
def test_native_reservation_rejects_drift(reservation_area, monkeypatch, fault):
    binding, _, _ = reservation_area
    envelope = policy.read(policy.ENVELOPE)
    envelope["qualified_profiles"] = list(policy.PROFILES)
    policy.write(policy.ENVELOPE, envelope)
    expected = {
        "decision": "ADR0228",
        "arms": ["candidate"],
        "common": {"buyer_journeys_per_second": 84, "duration_seconds": 300},
    }
    monkeypatch.setattr(runner, "plan", lambda: expected)
    monkeypatch.setattr(runner, "identity", lambda: {"entry": "fixed"})
    monkeypatch.setattr(runner.paid, "identity", lambda: {"core": "fixed"})
    monkeypatch.setattr(runner.native, "authorized_creation", lambda data: None)
    binding.update(cce_paid_entry_sources={"entry": "fixed"}, cce_paid_core_sources={"core": "fixed"})
    binding.update(
        {
            k: "a" * 64
            for k in (
                "cce_manifest_sha256",
                "diagnostic_target_sha256",
                "saved_api_service_sha256",
                "image_proof_sha256",
                "cce_resource_source_sha256",
                "cce_transition_source_sha256",
            )
        }
    )
    changed = copy.deepcopy(expected)
    if fault == "rate":
        changed["common"]["buyer_journeys_per_second"] = 100
    if fault == "duration":
        changed["common"]["duration_seconds"] = 600
    if fault == "entry_sources":
        binding["cce_paid_entry_sources"] = {}
    if fault == "manifest":
        binding["cce_manifest_sha256"] = "invalid"
    with pytest.raises(ValueError, match="Exactly bound"):
        policy.reserve(binding, changed, profile="cce_paid_comparison")
    assert not policy.read(policy.JOURNAL)["experiments"]


def test_native_helper_upload_is_exclusive_and_runtime_owned(tmp_path, monkeypatch):
    import os
    if not hasattr(os, "getuid"):
        monkeypatch.setattr(os, "getuid", lambda: 0, raising=False)
    raw = b"print('proof')\n"
    program = inputs.helper_upload_program(str(tmp_path), inputs.HELPERS[0], raw)
    assert "chown" not in program and "docker" not in program
    exec(compile(program, "native_upload", "exec"), {})  # noqa: S102 - Execute the generated remote program against an isolated fixture.
    target = tmp_path / inputs.HELPERS[0]
    assert target.read_bytes() == raw
    with pytest.raises(FileExistsError):
        exec(compile(program, "native_upload", "exec"), {})  # noqa: S102 - Execute the generated remote program against an isolated fixture.
    with pytest.raises(ValueError):
        inputs.helper_upload_program(str(tmp_path), "../unknown.py", raw)


def test_remote_failures_preserve_each_original_diagnostic(tmp_path):
    import io

    import qualify_two_host_deployment as driver
    channel = SimpleNamespace(shutdown_write=lambda: None, recv_exit_status=lambda: 1)
    messages = iter([b"initial upload failure", b"restoration failure"])

    def execute(*args, **kwargs):
        stdin, stdout = io.StringIO(), io.BytesIO(b"")
        stdin.channel = stdout.channel = channel
        return stdin, stdout, io.BytesIO(next(messages))

    session = object.__new__(driver.Session)
    session.output = tmp_path
    session.state = {"phases": ["candidate-stage"]}
    session.clients = {"primary": SimpleNamespace(exec_command=execute)}
    session.cleanup_mode = True
    for _ in range(2):
        with pytest.raises(RuntimeError):
            session.call("primary", "pass", 5)
    assert (tmp_path / "primary-candidate-stage-1.private-error.log").read_text() == "initial upload failure"
    assert (tmp_path / "primary-candidate-stage-2.private-error.log").read_text() == "restoration failure"
    assert (tmp_path / "primary-candidate-stage.private-error.log").read_text() == "restoration failure"

"""Offline paid-stage allowance and failure recovery checks; no cloud calls."""

import json
import sys
from datetime import UTC, datetime, timedelta
from pathlib import Path
from types import SimpleNamespace
from uuid import uuid4

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[2] / "scripts"))
import cce_api_adapter as cce
import cce_paid_stage as stage
import work_envelope as policy

RUN = "adr0151-" + "a" * 12
KEY = "bounded_cce_paid_comparison__" + "f" * 12


@pytest.fixture(scope="module")
def frozen():
    return stage.generator_bundle()


@pytest.fixture
def area(tmp_path, monkeypatch, frozen):
    original = policy.ROOT
    baseline = cce.contract()
    source = stage.identity()
    monkeypatch.setattr(stage, "identity", lambda: source)
    monkeypatch.setattr(cce, "contract", lambda: baseline)
    monkeypatch.setattr(policy, "ROOT", tmp_path)
    monkeypatch.setattr(policy, "STATE", tmp_path / "state.json")
    for relative in [
        "docs/capacity/flash-sale-opening/frozen-baseline-cpu-diagnostic-plan-2026-10-05.json",
        "scripts/checkout_journey_probe.py",
        "scripts/run_synchronized_paid_generator.py",
    ]:
        target = tmp_path / relative
        target.parent.mkdir(parents=True, exist_ok=True)
        target.write_bytes((original / relative).read_bytes())
    monkeypatch.setattr(
        policy, "PROFILES", {**policy.PROFILES, cce.PROFILE: ("bounded_cce_paid_comparison", "ADR0228")}
    )
    monkeypatch.setattr(policy, "envelope", lambda: {"qualified_profiles": [cce.PROFILE]})
    monkeypatch.setattr(cce.dependency, "authorized_today", lambda value: None)
    output = tmp_path / "tmp" / RUN / "candidate"
    output.mkdir(parents=True)
    config = {"generator": {"repo": "/root/flash-ticketing"}}
    binding = {"configuration_sha256": policy.digest(config), "cce_paid_core_sources": source}
    guard = SimpleNamespace(key=KEY, binding=binding, check=lambda timeout: None)
    calls = []

    def api(cid, program, timeout):
        calls.append(("api", program))
        if "frozen_helpers_verified" in program:
            return {"frozen_helpers_verified": True}
        if "retired_shows" in program:
            return {"retired_shows": 84}
        return {"pass": True, "hold_deadlines_elapsed": True, "duplicate_booked_seats": 0}

    def call(role, program, timeout):
        calls.append((role, program))
        if program == stage.GENERATOR_IDLE:
            return {"generator_idle": True}
        if "private_manifests_removed" in program:
            return {"private_manifests_removed": True}
        if "transferred_generator_identity" in program:
            return {"transferred_generator_identity": True}
        return {"fresh": True}

    session = SimpleNamespace(
        config=config,
        action_guard=guard,
        state={"capacity_stages_started": 0},
        api=api,
        call=call,
        put=lambda *values: calls.append(("put", values[1])),
        checkpoint=lambda: None,
        begin_cleanup=lambda: calls.append(("cleanup", None)),
    )

    def launch(session, role, cid, args, directory, jobs, record):
        assert policy.read(policy.STATE)[KEY]["paid_runs_started"] == 1
        assert policy.read(output / "stage.private.json")["customers_dispatched"] is True
        calls.append(("launch", args))
        job = {"pid": 123, "name": "test"}
        jobs.append((role, job))
        return job

    jobs = SimpleNamespace(
        launch=launch, wait=lambda *args, **kw: None, stop=lambda *args: {"running": False}
    )
    paid = stage.PaidStage(session, guard, RUN, "b" * 64, output, bundle=frozen, jobs=jobs)
    policy.write(
        policy.STATE,
        {
            "current_run": RUN,
            KEY: {
                "active_run": RUN,
                "paid_protocols_started": 1,
                "paid_protocol_arms": ["candidate"],
                "paid_runs_started": 0,
                "paid_runs_authorized": 1,
            },
        },
    )
    monkeypatch.setattr(
        stage, "fetch", lambda *args: json.dumps({"pass": False, "customer_errors": 1}).encode()
    )
    monkeypatch.setattr(
        stage, "drain", lambda *args: {"pass": True, "kafka_members": 6, "kafka_total_lag": 0}
    )
    now = datetime.now(UTC)
    events = [str(uuid4()) for _ in range(84)]
    fixture = {
        "schema_version": 1,
        "environment": "development",
        "fixture_id": str(uuid4()),
        "created_at": now.isoformat(),
        "sale_ends": (now + timedelta(hours=1)).isoformat(),
        "shows": 84,
        "seats_per_show": 300,
        "fixture_layout": "distributed",
        "show_ids": events,
    }
    manifest = {
        "schema_version": 1,
        "environment": "development",
        "id": str(uuid4()),
        "origin": paid.origin,
        "expires_at": (now + timedelta(hours=1)).isoformat(),
        "show_ids": events,
        "viewer_tokens": ["viewer-" + str(i) for i in range(25200)],
        "seat_offset": 0,
        "seats_per_show": 300,
        "fixture_layout": "distributed",
    }
    receipts = [
        {
            "pod_name": f"api-{i}",
            "pod_uid": f"uid-{i}",
            "private_ipv4": f"10.2.240.{i + 1}",
            "resources": baseline["resources"],
            "image_id": "example@" + cce.dependency.MANIFEST,
            "started_at": "2026-10-08T11:00:00Z",
            "process_start_time_seconds": 123.0,
            "startup_proof": {
                "sources": baseline["api_sources"],
                "run": RUN,
                "pod_uid": f"uid-{i}",
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
    admission = {
        "run": RUN,
        "binding_sha256": policy.digest(binding),
        "all_required_pre_dispatch_gates_pass": True,
        "cce_api_receipts": receipts,
    }
    return paid, calls, fixture, manifest, admission


def test_exact_baseline_generator_arguments_and_no_retries():
    args = stage.generator_arguments(
        "/root/flash-ticketing/tmp/" + RUN + "-cce-candidate", "http://10.1.137.69:8000", 100
    )
    for flag, value in [
        ("--rate", "84"),
        ("--seconds", "300"),
        ("--concurrency", "500"),
        ("--http-client-count", "8"),
        ("--poll-seconds", "1"),
        ("--completion-deadline-seconds", "420"),
        ("--duplicates", "1"),
    ]:
        assert args[args.index(flag) + 1] == value
    assert not any("retry" in value for value in args)


def test_prepare_and_consume_allowance_before_possible_dispatch(area):
    paid, calls, fixture, manifest, admission = area
    paid.prepare(fixture, manifest)
    result = paid.dispatch(admission)
    assert result["pass"] is False  # Preserve customer failure; never replace with financial success.
    assert policy.read(policy.STATE)[KEY]["paid_runs_started"] == 1
    with pytest.raises(ValueError):
        paid.dispatch(admission)
    assert len([c for c in calls if c[0] == "launch"]) == 1
    assert (paid.output / "fixture-identity.json").exists()


def test_lost_launch_response_still_consumes_scope_and_triggers_audit(area):
    paid, _calls, fixture, manifest, admission = area
    paid.prepare(fixture, manifest)
    paid.jobs.launch = lambda *args: (_ for _ in ()).throw(TimeoutError("lost response"))
    with pytest.raises(TimeoutError):
        paid.dispatch(admission)
    assert policy.read(policy.STATE)[KEY]["paid_runs_started"] == 1
    assert paid.record["customers_dispatched"] is True
    assert paid.audit("global audit")["post_ttl_financial_pass"] is True
    with pytest.raises(ValueError):
        paid.dispatch(admission)


@pytest.mark.parametrize("fault", ["unregistered", "foreign_scope", "source_drift", "configuration_drift"])
def test_preparation_fails_before_remote_mutation(area, monkeypatch, fault):
    paid, calls, fixture, manifest, _admission = area
    if fault == "unregistered":
        monkeypatch.setattr(policy, "PROFILES", {})
    elif fault == "foreign_scope":
        paid.guard.key = "bounded_generator_completion_probe__" + "f" * 12
    elif fault == "source_drift":
        paid.guard.binding["cce_paid_core_sources"] = {}
    else:
        paid.guard.binding["configuration_sha256"] = "0" * 64
    with pytest.raises(ValueError):
        paid.prepare(fixture, manifest)
    assert calls == []


@pytest.mark.parametrize("fault", ["no_admission", "wrong_binding", "missing_pod", "already_consumed"])
def test_dispatch_preflight_drift_never_launches(area, fault):
    paid, calls, fixture, manifest, admission = area
    paid.prepare(fixture, manifest)
    if fault == "no_admission":
        admission["all_required_pre_dispatch_gates_pass"] = False
    elif fault == "wrong_binding":
        admission["binding_sha256"] = "0" * 64
    elif fault == "missing_pod":
        admission["cce_api_receipts"].pop()
    else:
        state = policy.read(policy.STATE)
        state[KEY]["paid_runs_started"] = 1
        policy.write(policy.STATE, state)
    with pytest.raises(ValueError):
        paid.dispatch(admission)
    assert not any(c[0] == "launch" for c in calls)


def test_financial_failure_does_not_skip_queue_audit_or_retirement_or_manifest_cleanup(area, monkeypatch):
    paid, _calls, fixture, manifest, admission = area
    paid.prepare(fixture, manifest)
    paid.dispatch(admission)
    old = paid.session.api

    def api(cid, program, timeout):
        if "hold_deadlines_elapsed" in program:
            raise TimeoutError("audit database unavailable")
        return old(cid, program, timeout)

    paid.session.api = api
    queue_calls = []
    monkeypatch.setattr(stage, "drain", lambda *args: queue_calls.append(args) or {"pass": True})
    with pytest.raises(ValueError):
        paid.cleanup("global audit")
    assert queue_calls
    assert paid.record["retired_shows"] == 84
    assert paid.record["private_manifests_removed"] is True
    assert paid.record["private_cleanup_pass"] is False
    assert (paid.output / "stage.private.json").exists()


def test_cleanup_runs_after_customer_failure_without_rewriting_it(area):
    paid, _calls, fixture, manifest, admission = area
    paid.prepare(fixture, manifest)
    paid.dispatch(admission)
    record = paid.cleanup("global audit")
    assert record["customer"]["pass"] is False
    assert record["private_cleanup_pass"] is True
    assert record["global_queues"]["pass"] is True
    assert record["capacity_stages_started"] == 1


def test_missing_fixture_identity_prevents_generator_transfer(area):
    paid, calls, fixture, manifest, _admission = area
    fixture["show_ids"] = [str(uuid4())] * 84
    with pytest.raises(ValueError):
        paid.prepare(fixture, manifest)
    assert calls == []


def test_unpaid_stage_never_runs_financial_audit(area):
    paid, calls, _fixture, _manifest, _admission = area
    assert paid.audit("global audit") is None
    assert calls == []


def test_unknown_generator_launch_identity_is_not_silently_cleaned(area):
    paid, _calls, fixture, manifest, admission = area
    paid.prepare(fixture, manifest)
    paid.dispatch(admission)
    paid.jobs.stop = lambda *args: (_ for _ in ()).throw(ValueError("PID changed"))
    with pytest.raises(ValueError):
        paid.cleanup("global audit")
    assert paid.record["private_cleanup_pass"] is False
    assert paid.record["financial"]["pass"] is True
    assert paid.record["global_queues"]["pass"] is True
    assert paid.record["private_inputs_preserved_for_recovery"] is True
    assert "private_manifests_removed" not in paid.record
    assert "retired_shows" not in paid.record


@pytest.mark.parametrize("idle_fault", ["busy", "unreachable"])
def test_lost_launch_receipt_with_unknown_idle_state_keeps_inputs_and_runs_audits(area, idle_fault):
    paid, calls, fixture, manifest, admission = area
    paid.prepare(fixture, manifest)
    paid.jobs.launch = lambda *args: (_ for _ in ()).throw(TimeoutError("lost launch receipt"))
    with pytest.raises(TimeoutError):
        paid.dispatch(admission)
    assert paid.job_list == []
    assert paid.record["generator_launch_attempted"] is True
    old = paid.session.call

    def call(role, program, timeout):
        if program == stage.GENERATOR_IDLE:
            if idle_fault == "unreachable":
                raise TimeoutError("idle state unavailable")
            return {"generator_idle": False}
        return old(role, program, timeout)

    paid.session.call = call
    with pytest.raises(ValueError):
        paid.cleanup("global audit")
    assert paid.record["financial"]["pass"] is True
    assert paid.record["global_queues"]["pass"] is True
    assert paid.record["private_cleanup_pass"] is False
    assert paid.record["private_inputs_preserved_for_recovery"] is True
    assert "retired_shows" not in paid.record
    assert not any("remove(p/" in program for role, program in calls if role == "generator")


def test_lost_launch_receipt_with_verified_idle_can_cleanup_but_not_replay(area):
    paid, _calls, fixture, manifest, admission = area
    paid.prepare(fixture, manifest)
    paid.jobs.launch = lambda *args: (_ for _ in ()).throw(TimeoutError("lost launch receipt"))
    with pytest.raises(TimeoutError):
        paid.dispatch(admission)
    record = paid.cleanup("global audit")
    assert record["generator_idle_after_stop"] is True
    assert record["private_cleanup_pass"] is True
    assert record["customers_dispatched"] is True
    assert "customer" not in record
    with pytest.raises(ValueError):
        paid.dispatch(admission)


def test_interrupted_financial_audit_still_checks_queue_and_removes_idle_inputs(area, monkeypatch):
    paid, _calls, fixture, manifest, admission = area
    paid.prepare(fixture, manifest)
    paid.dispatch(admission)
    old = paid.session.api

    def api(cid, program, timeout):
        if "hold_deadlines_elapsed" in program:
            raise KeyboardInterrupt
        return old(cid, program, timeout)

    paid.session.api = api
    with pytest.raises(ValueError):
        paid.cleanup("global audit")
    assert paid.record["financial_failure_type"] == "KeyboardInterrupt"
    assert paid.record["global_queues"]["pass"] is True
    assert paid.record["private_manifests_removed"] is True
    assert paid.record["private_cleanup_pass"] is False

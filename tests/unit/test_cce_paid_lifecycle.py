"""Composed native lifecycle stop gates and independent cleanup; no cloud."""

import sys
from pathlib import Path
from types import SimpleNamespace

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[2] / "scripts"))
import cce_paid_lifecycle as lifecycle
import run_two_host_paid_comparison as paid
from test_cce_paid_observers import snapshots
from test_observe_cce_paid_pipeline import bundle


@pytest.fixture
def area(monkeypatch):
    calls, saved = [], []
    fail = set()
    record = {"customers_dispatched": False, "financial": {"pass": True, "duplicate_booked_seats": 0}}

    def op(name, result=None):
        def run(*args, **kwargs):
            calls.append(name)
            if name in fail:
                raise ValueError(name)
            return result

        return run

    def stop():
        calls.append("stop")
        record["generator_idle_after_stop"] = "stop" not in fail
        record["job_cleanup_errors"] = ["unknown"] if "stop" in fail else []
        if "stop" in fail:
            raise ValueError("unknown stop")

    def dispatch(*args, **kwargs):
        record["customers_dispatched"] = True
        return op("dispatch", {"pass": True})(*args, **kwargs)

    session = SimpleNamespace(call=lambda role, *args: snapshots()[:-2] if role == "primary" else [])
    stage = SimpleNamespace(
        session=session,
        cid="c" * 64,
        run=bundle()["run"],
        guard=SimpleNamespace(binding={}),
        record=record,
        check=op("check"),
        checkpoint=lambda: None,
        prepare=op("prepare"),
        dispatch=dispatch,
        audit=op("audit", {"post_ttl_financial_pass": True, "global_queues_pass": True}),
        cleanup=op("stage_cleanup", {"private_cleanup_pass": True}),
        stop_jobs=stop,
    )
    helpers = {"audit": snapshots()[-2], "bridge": snapshots()[-1]}
    resources = SimpleNamespace(
        record={"audit": {"container_id": stage.cid}},
        rows=helpers,
        inspect_name=lambda role: [helpers[role]],
        cleanup=op("resource_cleanup", {"owned_resources_removed": True}),
    )
    deployment = SimpleNamespace(
        create=op("create"),
        observe=op("observe", bundle()["receipts"]),
        cleanup=op("namespace_cleanup", {"namespace_removed": True}),
    )
    transition = SimpleNamespace(
        capture=op("capture"), activate=op("activate"), restore=op("restore", {"candidate_restored": True})
    )
    observer = SimpleNamespace(
        start=op("observers"),
        launch_cpu=op("cpu"),
        collect=op("collect", {"observer_pass": True, "native_api_cpu": {"pass": True}}),
        cleanup=op("observer_cleanup"),
    )
    monkeypatch.setattr(paid, "drain", op("drain", {"pass": True}))
    value = lifecycle.Lifecycle(
        stage,
        resources,
        deployment,
        transition,
        observer,
        [],
        qualify_safety=op("safety", {"gates": dict.fromkeys(lifecycle.SAFETY, True)}),
        cleanup_safety=op("safety_cleanup"),
        prepare_fixture=op("fixture", ({}, {})),
        identities={},
        background_before=snapshots(),
        persist=saved.append,
    )
    return value, calls, fail, saved


def test_composition_preserves_gate_order_and_stops_before_restoration(area):
    value, calls, _, saved = area
    result = value.run(None, None, "global audit")
    assert result["pass"] is True and result["cleanup_complete"] is True
    assert calls.index("safety") < calls.index("prepare") < calls.index("observers") < calls.index("dispatch")
    assert calls.index("dispatch") < calls.index("collect") < calls.index("audit")
    assert calls.index("stop") < calls.index("restore") < calls.index("stage_cleanup")
    assert calls.index("namespace_cleanup") < calls.index("resource_cleanup")
    assert any(r.get("started") and not r.get("dispatch_attempted") for r in saved)


@pytest.mark.parametrize(
    "failure",
    ["capture", "create", "observe", "activate", "safety", "fixture", "prepare", "observers", "cpu"],
)
def test_failed_admission_cannot_launch_paid_customers(area, failure):
    value, calls, fail, _ = area
    fail.add(failure)
    with pytest.raises(ValueError, match=failure):
        value.run(None, None, "global audit")
    assert "dispatch" not in calls
    assert all(
        name in calls
        for name in (
            "stop",
            "safety_cleanup",
            "restore",
            "stage_cleanup",
            "drain",
            "observer_cleanup",
            "namespace_cleanup",
            "resource_cleanup",
        )
    )
    assert value.record["pass"] is False


@pytest.mark.parametrize("failure", ["dispatch", "collect", "audit"])
def test_failed_paid_work_still_runs_all_recovery_operations(area, failure):
    value, calls, fail, _ = area
    fail.add(failure)
    with pytest.raises(ValueError, match=failure):
        value.run(None, None, "global audit")
    assert value.record["customers_dispatched"] is True
    assert all(
        name in calls
        for name in (
            "stop",
            "safety_cleanup",
            "restore",
            "stage_cleanup",
            "drain",
            "observer_cleanup",
            "namespace_cleanup",
            "resource_cleanup",
        )
    )


@pytest.mark.parametrize(
    "failure",
    [
        "safety_cleanup",
        "restore",
        "stage_cleanup",
        "drain",
        "observer_cleanup",
        "namespace_cleanup",
        "resource_cleanup",
    ],
)
def test_cleanup_failure_is_preserved_and_other_cleanup_continues(area, failure):
    value, calls, fail, _ = area
    fail.add(failure)
    with pytest.raises(ValueError, match="recovery"):
        value.run(None, None, "global audit")
    assert value.record["cleanup_complete"] is False
    assert value.record["pass"] is False
    assert all(
        name in calls
        for name in (
            "restore",
            "stage_cleanup",
            "drain",
            "observer_cleanup",
            "namespace_cleanup",
            "resource_cleanup",
        )
    )


def test_unknown_job_ownership_preserves_native_inputs_and_helpers(area):
    value, calls, fail, _ = area
    fail.add("stop")
    with pytest.raises(ValueError, match="recovery"):
        value.run(None, None, "global audit")
    assert "namespace_cleanup" not in calls and "resource_cleanup" not in calls
    assert "restore" in calls and "stage_cleanup" in calls and "drain" in calls
    assert any(r["type"] == "UnknownJobOwnership" for r in value.record["cleanup_failures"])


@pytest.mark.parametrize("gate", lifecycle.SAFETY)
def test_each_safety_and_durability_gate_blocks_paid_dispatch(area, gate):
    value, calls, _, _ = area
    value.qualify_safety = lambda *args: {"gates": {key: key != gate for key in lifecycle.SAFETY}}
    with pytest.raises(ValueError, match="executed native safety"):
        value.run(None, None, "global audit")
    assert "dispatch" not in calls


def test_false_customer_gate_is_never_overwritten_by_successful_cleanup(area):
    value, _, _, _ = area
    original = value.stage.dispatch
    value.stage.dispatch = lambda *args, **kwargs: {**original(*args, **kwargs), "pass": False}
    result = value.run(None, None, "global audit")
    assert result["cleanup_complete"] is True and result["pass"] is False


def test_wrong_audit_identity_is_rejected_before_mutation(area):
    value, calls, _, _ = area
    value.resources.record["audit"]["container_id"] = "d" * 64
    with pytest.raises(ValueError, match="captured owned audit"):
        value.run(None, None, "global audit")
    assert calls == ["check"]


@pytest.mark.parametrize("fault", ["missing", "replacement", "restart", "stopped", "project_emulation"])
def test_snapshot_refuses_uncaptured_or_changed_helpers(area, fault):
    import copy

    value, _, _, _ = area
    captured = copy.deepcopy(value.resources.rows["audit"])
    actual = copy.deepcopy(captured)
    if fault == "missing":
        views = []
    else:
        if fault == "replacement":
            actual["Id"] = "e" * 64
        elif fault == "restart":
            actual["State"]["StartedAt"] = "2026-10-08T12:00:00Z"
        elif fault == "stopped":
            actual["State"]["Running"] = False
        views = [actual]
    original = value.resources.inspect_name
    value.resources.inspect_name = lambda role: views if role == "audit" else original(role)
    if fault == "project_emulation":
        value.session.call = lambda role, *args: snapshots() if role == "primary" else []
    with pytest.raises(ValueError):
        value.snapshots()

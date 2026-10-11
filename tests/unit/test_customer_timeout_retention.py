"""Executed timeout evidence tests with real child processes and no cloud access."""
import asyncio
import hashlib
import importlib.util
import json
from pathlib import Path
from types import SimpleNamespace

import cce_transaction_profile as transaction
import cce_worker_rebalance_profile as placement
import customer_recovery_bundle as recovery
import customer_timeout_bundle as timeout_bundle
import pytest
import work_envelope as policy
from cce_paid_stage import PaidStage, generator_bundle
from test_cce_paid_lifecycle import area  # noqa: F401 - Reuse the established isolated lifecycle fixture.

ROOT = Path(__file__).resolve().parents[2]


def load(path):
    spec = importlib.util.spec_from_file_location("timeout_test_" + path.stem, path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


@pytest.mark.parametrize("state", ["checkpoint", "missing", "invalid", "final"])
def test_real_hung_shards_retain_failed_summary(tmp_path, monkeypatch, state):
    coordinator = tmp_path / "paid_ticket_sharded_generator.py"
    coordinator.write_bytes((ROOT / "artifacts/customer-timeout/paid_ticket_sharded_generator.py").read_bytes())
    child = tmp_path / "paid_ticket_load_generator.py"
    result = {"scheduled": 1, "dispatched": 1, "completed": 0, "generator_drops": 0,
              "fulfilled": 0, "fulfilled_by_deadline": 0, "accounting_complete": False,
              "measurement_complete": False, "rate_target_per_second": 1, "dispatch_seconds": 1,
              "kind": "paid_journey_checkpoint", "pass": False}
    if state == "final":
        result.update(completed=1, fulfilled=1, fulfilled_by_deadline=1, distinct_orders=1,
                      distinct_tickets=1, accounting_complete=True, measurement_complete=True,
                      recovery_max_attempts=3, **{"pass": True})
    child.write_text("import argparse,json,time\nfrom pathlib import Path\np=argparse.ArgumentParser();p.add_argument('--output');a,_=p.parse_known_args()\n"
                     + ("Path(a.output" + ("+'.progress.json'" if state == "checkpoint" else "")
                        + ").write_text(" + (repr("{") if state == "invalid" else "json.dumps(" + repr(result) + ")") + ")\n"
                        if state != "missing" else "") + "time.sleep(30)\n", encoding="utf-8")
    module = load(coordinator)
    wait_for = asyncio.wait_for

    async def bounded_wait(awaitable, timeout):
        return await wait_for(awaitable, 0.6 if timeout == 29 else timeout)

    monkeypatch.setattr(module.asyncio, "wait_for", bounded_wait)
    args = SimpleNamespace(rate=2, seconds=1, concurrency=2, origin="http://unused.invalid",
                           completion_deadline_seconds=1, poll_seconds=0.1, duplicates=3,
                           recovery_max_attempts=3)
    manifest = {"schema_version": 1, "environment": "development", "show_ids": ["a", "b"],
                "viewer_tokens": ["one", "two"], "seats_per_show": 2, "seat_offset": 0}
    value = asyncio.run(module.run(args, manifest))
    assert value["pass"] is False
    assert value["coordinator_failure_type"] == "TimeoutError"
    assert value["shard_evidence_states"] == [state, state]
    assert all(code is not None and code != 0 for code in value["shard_exit_codes"])
    assert value["scheduled"] == 2
    assert value["measurement_complete"] is False
    assert value["hold_to_ticket_p95_ms"] is None
    if state == "final":
        assert value["dispatched"] == value["fulfilled"] == 2
    else:
        assert value["dispatched"] is None and value["final_customer_failures"] is None
        assert value["retained_lower_bound_counts"]["dispatched"] == (2 if state == "checkpoint" else 0)
    assert not list(tmp_path.glob("paid-shards-*"))


def test_child_client_close_failure_preserves_accounting(tmp_path, monkeypatch):
    module = load(ROOT / "artifacts/customer-timeout/paid_ticket_load_generator.py")
    monkeypatch.setattr(module, "validate", lambda *args: 2)

    class Client:
        def __init__(self, **kwargs):
            pass

        async def __aenter__(self):
            return self

        async def __aexit__(self, *args):
            raise RuntimeError("close_failure")

        async def get(self, *args):
            return SimpleNamespace(raise_for_status=lambda: None)

    async def journey(*args, **kwargs):
        return {"outcome": "fulfilled", "order_id": str(args[2]), "ticket_id": str(args[2])}

    monkeypatch.setattr(module.httpx, "AsyncClient", Client)
    args = SimpleNamespace(rate=2, seconds=1, concurrency=2, origin="http://unused.invalid",
                           completion_deadline_seconds=1, poll_seconds=0.1, duplicates=3,
                           recovery_max_attempts=1, start_at_epoch=None, timeout_seconds=2,
                           http_max_connections=2, output=tmp_path / "result.json")
    with pytest.raises(RuntimeError, match="close_failure"):
        asyncio.run(module.scheduled_journeys(args, {}, journey_fn=journey))
    value = json.loads((tmp_path / "result.json.progress.json").read_text())
    assert value["accounting_complete"] is True and value["measurement_complete"] is False
    assert value["scheduled"] == value["dispatched"] == value["completed"] == value["fulfilled"] == 2
    assert value["pass"] is False
    assert not (tmp_path / "result.json.progress.json.tmp").exists()


def test_overlay_preserves_historical_bytes_and_rejects_drift(monkeypatch):
    parent, coordinator = recovery.qualify(generator_bundle())
    original_hash = hashlib.sha256(parent["scripts/paid_ticket_sharded_generator.py"]).hexdigest()
    current, wrapper = timeout_bundle.qualify(parent, coordinator)
    assert len(current) == 80 and current != parent
    assert original_hash.encode() in coordinator
    assert hashlib.sha256(current["scripts/paid_ticket_sharded_generator.py"]).hexdigest().encode() in wrapper
    assert recovery.qualify(generator_bundle()) == (parent, coordinator)
    monkeypatch.setattr(timeout_bundle, "MANIFEST_SHA256", "0" * 64)
    with pytest.raises(ValueError, match="receipt"):
        timeout_bundle.qualify(parent, coordinator)


def test_actual_stage_constructor_transfers_selected_timeout_workload(monkeypatch, tmp_path):
    goal = {"decision": "ADR0228", "extension_decision": "ADR0271", "profile": "cce_paid_comparison",
            "comparison_arm": "correction", "acquisition_budget": 20, "candidate_factor": placement.factor(),
            "timeout_retention_decision": "ADR0278"}
    monkeypatch.setattr(transaction, "active", lambda: goal)
    original = policy.ROOT
    parent = generator_bundle()
    required = ["docs/capacity/flash-sale-opening/frozen-baseline-cpu-diagnostic-plan-2026-10-05.json",
                "scripts/run_synchronized_paid_generator.py", timeout_bundle.MANIFEST]
    required += list(json.loads((original / timeout_bundle.MANIFEST).read_text())["files"])
    for name in required:
        target = tmp_path / name
        target.parent.mkdir(parents=True, exist_ok=True)
        target.write_bytes((original / name).read_bytes())
    monkeypatch.setattr(policy, "ROOT", tmp_path)
    monkeypatch.setattr(recovery, "raw", lambda name: (original / name).read_bytes().replace(b"\r\n", b"\n"))
    run = "adr0151-" + "a" * 12
    output = tmp_path / "tmp" / run / "candidate"
    output.mkdir(parents=True)
    binding = {"cce_recovery_max_attempts": 3, "cce_timeout_retention_decision": "ADR0278",
               "cce_timeout_bundle_sha256": timeout_bundle.MANIFEST_SHA256}
    guard = SimpleNamespace(key="bounded_cce_paid_comparison__" + "b" * 12, binding=binding)
    session = SimpleNamespace(config={"generator": {"repo": "/root/flash-generator"}})
    stage = PaidStage(session, guard, run, "c" * 64, output, bundle=parent)
    assert b"coordinator_failure_type" in stage.bundle["scripts/paid_ticket_sharded_generator.py"]
    assert b"checkpoint_progress" in stage.bundle["scripts/paid_ticket_load_generator.py"]
    guard.binding = {**binding, "cce_timeout_bundle_sha256": "0" * 64}
    with pytest.raises(ValueError, match="binding"):
        PaidStage(session, guard, run, "c" * 64, output, bundle=parent)


@pytest.mark.parametrize("diagnostics_fail", [False, True])
def test_failed_dispatch_collects_diagnostics_before_restoration(area, diagnostics_fail):  # noqa: F811
    value, calls, fail, _ = area
    fail.add("dispatch")
    if diagnostics_fail:
        fail.add("collect")
    with pytest.raises(ValueError, match="dispatch"):
        value.run(None, None, "global audit")
    assert calls.index("collect") < calls.index("restore")
    assert calls.count("collect") == 1
    assert all(name in calls for name in ("stage_cleanup", "drain", "namespace_cleanup", "resource_cleanup"))
    assert value.record["pass"] is False
    if diagnostics_fail:
        assert value.record["failure_observer_collection_error"] == "ValueError"
    else:
        assert value.record["failure_observation_complete"] is True


def test_unknown_selector_and_invalid_cohort_fail_closed():
    assert timeout_bundle.selected(None) is False
    with pytest.raises(ValueError, match="Unknown"):
        timeout_bundle.selected({"timeout_retention_decision": "ADR9999"})
    with pytest.raises(ValueError, match="cohort"):
        timeout_bundle.issuance_program([], 1000)

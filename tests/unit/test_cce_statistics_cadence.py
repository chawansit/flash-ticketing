"""Measured cohort timing variation must not shift statistics behind SQL work."""
import json
import time
from datetime import UTC, datetime
from types import SimpleNamespace

import database_wait_evidence as waits
import independent_database_sampling as sampling
import pytest


def diagnostic():
    at = datetime.now(UTC).isoformat()
    item = {"complete": True, "collection_ms": 1, "activity_utc": at,
            "statistics_utc": at, "restricted_sessions": 0, "active": 2,
            "idle_in_transaction": 0, "waits": [],
            "track_wal_io_timing": False, "track_io_timing": False}
    for name, keys in (("wal", waits.WAL_KEYS), ("checkpointer", waits.CHECKPOINT_KEYS),
                       ("database", waits.DATABASE_KEYS)):
        item[name] = {"stats_reset": None, **dict.fromkeys(keys, 0)}
    return item


def run_delayed_cohort(tmp_path, monkeypatch, *, statistics_first=False, long_delay=1.51):
    monkeypatch.setattr(waits, "Collector", lambda: SimpleNamespace(collect=lambda conn: diagnostic()))
    delays = iter([.19, long_delay, .01])
    calls = []
    connection = SimpleNamespace(rollback=lambda: None)

    def cohort(conn, shows):
        assert conn is connection and shows == ["show"]
        calls.append(time.monotonic())
        time.sleep(next(delays))
        return {"issued_tickets": len(calls), "cohort_sample_complete": True,
                "cohort_sample_age_seconds": 0}

    module = SimpleNamespace(sample=cohort)
    if statistics_first:
        waits.install(module, statistics_first=True)
    else:
        waits.install(module)
    trace = tmp_path / ("first.jsonl" if statistics_first else "after.jsonl")
    sampler = sampling.Sampler(connection, module.sample, ["show"], trace, seconds=3.6)
    try:
        deadline = time.monotonic() + 5
        while time.monotonic() < deadline:
            if trace.exists() and len(trace.read_text().splitlines()) >= 3:
                break
            time.sleep(.01)
        else:
            pytest.fail("Three bounded samples not produced")
    finally:
        sampler.stop()
    return trace, calls


def test_reproduce_measured_gap_without_collection_errors(tmp_path, monkeypatch):
    trace, calls = run_delayed_cohort(tmp_path, monkeypatch)
    rows = [json.loads(line) for line in trace.read_text().splitlines()]
    result = waits.summarize(trace)
    assert len(calls) == 3
    assert result["error_counts"] == {}
    assert not result["complete"] and not result["counter_continuity"]
    timestamps = [datetime.fromisoformat(row["database_wait_diagnostics"]["statistics_utc"])
                  for row in rows]
    assert (timestamps[1] - timestamps[0]).total_seconds() > 2


def test_statistics_first_passes_same_measured_delay_and_budget(tmp_path, monkeypatch):
    trace, calls = run_delayed_cohort(tmp_path, monkeypatch, statistics_first=True)
    result = waits.summarize(trace)
    assert len(calls) == 3
    assert result["complete"] and result["counter_continuity"]
    assert result["error_counts"] == {} and result["discontinuity_counts"] == {}
    assert 1 < result["max_statistics_gap_seconds"] < 2


def test_larger_block_still_fails_original_gate(tmp_path, monkeypatch):
    trace, _ = run_delayed_cohort(tmp_path, monkeypatch, statistics_first=True, long_delay=2.15)
    result = waits.summarize(trace)
    assert not result["complete"] and result["discontinuity_counts"]["statistics_gap"] == 1
    assert result["max_statistics_gap_seconds"] > 2


@pytest.mark.parametrize("rollback_failure", [False, True])
def test_failed_cohort_keeps_statistics_without_synthetic_ticket_counts(monkeypatch, rollback_failure):
    order = []
    def collect(conn):
        order.append("statistics")
        return diagnostic()
    def cohort(conn, shows):
        order.append("cohort")
        raise TimeoutError("private query and credentials")
    def rollback():
        order.append("rollback")
        if rollback_failure:
            raise RuntimeError("private rollback details")
    monkeypatch.setattr(waits, "Collector", lambda: SimpleNamespace(collect=collect))
    module = SimpleNamespace(sample=cohort)
    waits.install(module, statistics_first=True)
    row = module.sample(SimpleNamespace(rollback=rollback), ["show"])
    assert order == ["statistics", "cohort", "rollback"]
    assert row["database_wait_diagnostics"]["complete"]
    assert row["database_error"] == "TimeoutError" and not row["cohort_sample_complete"]
    assert "issued_tickets" not in row and "private" not in json.dumps(row)
    assert ("database_rollback_error" in row) is rollback_failure


def test_statistics_failure_is_not_hidden_by_successful_cohort(monkeypatch):
    monkeypatch.setattr(waits, "Collector", lambda: SimpleNamespace(
        collect=lambda conn: {"complete": False, "error_type": "TimeoutError"}))
    module = SimpleNamespace(sample=lambda *args: {"issued_tickets": 9})
    waits.install(module, statistics_first=True)
    row = module.sample(None, ["show"])
    assert not row["database_wait_diagnostics"]["complete"] and row["issued_tickets"] == 9


def test_legacy_order_and_exception_behavior_unchanged(monkeypatch):
    order = []
    monkeypatch.setattr(waits, "Collector", lambda: SimpleNamespace(
        collect=lambda conn: order.append("statistics") or diagnostic()))
    def cohort(*args):
        order.append("cohort")
        return {"issued_tickets": 1}
    module = SimpleNamespace(sample=cohort)
    waits.install(module)
    module.sample(None, ["show"])
    assert order == ["cohort", "statistics"]
    def fail(*args):
        raise TimeoutError()
    module = SimpleNamespace(sample=fail)
    waits.install(module)
    with pytest.raises(TimeoutError):
        module.sample(None, ["show"])


def test_source_bound_inventory_requires_new_order():
    import cce_verification_contract as verification
    marker = verification.contract()
    inventory = {"verification_contract": marker}
    assert marker["data_connections"] == 1 and marker["statistics_gap_seconds"] == 2
    assert verification.validate_inventory(inventory, verification.digest(inventory))
    for key in ("statistics_schedule_decision", "statistics_order",
                "fixture_preparation_decision", "fixture_preparation_order"):
        changed = json.loads(json.dumps(inventory))
        changed["verification_contract"].pop(key)
        with pytest.raises(ValueError, match="inventory binding"):
            verification.validate_inventory(changed, verification.digest(changed))


def test_cohort_freshness_gate_is_independent_of_statistics_success(tmp_path, monkeypatch):
    monkeypatch.setattr(waits, "Collector", lambda: SimpleNamespace(collect=lambda conn: diagnostic()))
    module = SimpleNamespace(sample=lambda *args: {
        "issued_tickets": 1, "cohort_sample_complete": True, "cohort_sample_age_seconds": 11})
    waits.install(module, statistics_first=True)
    sampler = sampling.Sampler(SimpleNamespace(rollback=lambda: None), module.sample,
                               ["show"], tmp_path / "stale.jsonl", seconds=1)
    try:
        row = sampler.snapshot(["show"])
        assert row["database_wait_diagnostics"]["complete"]
        assert not row["cohort_sample_complete"] and row["database_error"] == "StaleCohortSnapshot"
    finally:
        sampler.stop()


def test_plan_identifies_statistics_correction_instead_of_repeating_old_capture_label():
    import cce_metric_admission as capture
    from test_cce_metric_admission import goal
    plan = capture.extend_plan({}, goal())
    assert plan["diagnostic_decision"] == "ADR0289"
    assert plan["single_changed_factor"] == "paid_fixture_preparation_order"
    assert plan["statistics_schedule_decision"] == "ADR0288"
    assert plan["fixture_preparation_order"] == "before_transition_admission"
    assert plan["statistics_order"] == "before_cohort"
    assert plan["metric_admission_capture"]["decision"] == "ADR0287"

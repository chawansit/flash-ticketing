"""Same snapshot, partial audit failures, exclusive SQL ownership and evidence gaps."""
import json
import threading
import time
from contextlib import nullcontext
from datetime import UTC, datetime, timedelta
from types import SimpleNamespace
from uuid import uuid4

import cce_bounded_financial_audit as audit
import cce_verification_contract as verification
import database_wait_evidence as waits
import independent_database_sampling as sampling
import pytest


@pytest.mark.parametrize("shows", [1, 84, 102, 120, 140, 1008])
def test_generated_audit_keeps_all_predicates_and_total_deadline(shows):
    source = audit.program([str(uuid4()) for _ in range(shows)], shows * 300)
    assert "REPEATABLE READ READ ONLY" in source
    assert "TOTAL_SECONDS=600" in source
    assert "set_timeout(conn, deadline, 60)" in source
    assert "aggregate_audit(conn" in source
    assert len(audit.relationship_queries()) == 7
    assert all("cohort AS NOT MATERIALIZED" in q and "scoped_bookings AS NOT MATERIALIZED" in q
               for q in audit.relationship_queries())
    compile(source, "audit", "exec")


@pytest.mark.parametrize("failure", [None, "count", "exception"])
def test_same_snapshot_financial_checks_do_not_drop_late_failures(failure):
    class Conn:
        transactions = 0
        queries = 0
        def transaction(self):
            self.transactions += 1
            return nullcontext()
        def execute(self, sql, params=None):
            if params is None:
                return None
            self.queries += 1
            if failure == "exception" and self.queries == 5:
                raise TimeoutError("secret query text must not leak")
            value = 1 if self.queries == 6 else int(failure == "count" and self.queries == 5)
            return SimpleNamespace(fetchone=lambda: (value,))
    conn = Conn()
    result = audit.financial_snapshot(conn, [str(uuid4())], 1, 1, 1,
                                     audit.relationship_queries(), lambda *args, **kwargs: {"pass": True})
    assert conn.transactions == 1
    assert result["pass"] is (failure is None)
    if failure == "exception":
        assert result["failed_predicate"] == "fulfilled_ticket_relationship_errors"
        assert result["failure_type"] == "TimeoutError"
        assert len(result["relationships"]) == 4
        assert "secret" not in json.dumps(result)
    elif failure == "count":
        assert result["relationships"]["fulfilled_ticket_relationship_errors"] == 1
        assert result["hold_deadlines_elapsed"]


def test_total_deadline_prevents_another_predicate(monkeypatch):
    moments = iter([10, 611])
    monkeypatch.setattr(audit.time, "monotonic", lambda: next(moments))
    conn = SimpleNamespace(transaction=nullcontext, execute=lambda *args: None)
    result = audit.financial_snapshot(conn, [str(uuid4())], 1, 1, 1,
                                     audit.relationship_queries(), lambda *args, **kwargs: {"pass": True})
    assert not result["pass"] and result["failure_type"] == "TimeoutError"
    assert not result["relationships"]


def test_metrics_delay_does_not_hold_sql_connection_or_sampling_clock(tmp_path):
    owners = []
    conn = SimpleNamespace(rollback=lambda: None)
    def collect(connection, shows):
        assert connection is conn and shows == ["show"]
        owners.append(threading.get_ident())
        return {"issued_tickets": len(owners), "database_wait_diagnostics": {"complete": True}}
    sampler = sampling.Sampler(conn, collect, ["show"], tmp_path / "db.jsonl", interval=.02, seconds=2)
    try:
        first = sampler.snapshot(["show"])
        time.sleep(.15)  # Foreground metrics stalls; SQL progresses independently.
        second = sampler.snapshot(["show"])
        assert second["issued_tickets"] >= first["issued_tickets"] + 3
        assert second["database_sample_age_seconds"] < .08
        with pytest.raises(ValueError, match="Original diagnostic cohort"):
            sampler.snapshot(["other"])
    finally:
        sampler.stop()
    assert len(set(owners)) == 1 and owners[0] != threading.get_ident()
    assert len((tmp_path / "db.jsonl").read_text().splitlines()) >= 4
    assert not sampler.thread.is_alive()


def test_existing_connection_is_exclusively_owned_and_joined_before_close(tmp_path):
    closed, connections = [], []
    class Conn:
        def __enter__(self): return self
        def __exit__(self, *args):
            assert not wrapper.sampler.thread.is_alive()
            closed.append(True)
        def execute(self, sql): return None
    raw = Conn()
    def connect(dsn, **kwargs):
        connections.append(dsn)
        return raw if dsn == "data" else "admin"
    module = SimpleNamespace(psycopg=SimpleNamespace(connect=connect), sample=lambda *args: {"issued_tickets": 1})
    sampling.install(module, "data", tmp_path / "db.jsonl", 480)
    assert module.psycopg.connect("admin", autocommit=True) == "admin"
    with module.psycopg.connect("data", autocommit=True) as wrapper:
        wrapper.execute("SET statement_timeout='3s'")
        assert module.sample(wrapper, ["show"])["issued_tickets"] == 1
        with pytest.raises(ValueError, match="exclusively"):
            wrapper.execute("SELECT 1")
        with pytest.raises(ValueError, match="Exactly one"):
            module.psycopg.connect("data", autocommit=True)
    assert connections == ["admin", "data"] and closed == [True]


def test_sql_errors_are_retained_without_reconnect_or_synthetic_counts(tmp_path):
    rolled = []
    conn = SimpleNamespace(rollback=lambda: rolled.append(True))
    def fail(*args): raise RuntimeError("password/query not for evidence")
    sampler = sampling.Sampler(conn, fail, ["show"], tmp_path / "db.jsonl", seconds=1)
    try:
        row = sampler.snapshot(["show"])
        assert row["database_error"] == "RuntimeError" and "issued_tickets" not in row
    finally:
        sampler.stop()
    assert rolled == [True]
    assert "password/query" not in (tmp_path / "db.jsonl").read_text()


def diagnostic(at):
    item = {"complete": True, "collection_ms": 1, "activity_utc": at, "statistics_utc": at,
            "restricted_sessions": 0, "active": 2, "idle_in_transaction": 0, "waits": [],
            "track_wal_io_timing": False, "track_io_timing": False}
    for name, keys in (("wal", waits.WAL_KEYS), ("checkpointer", waits.CHECKPOINT_KEYS),
                       ("database", waits.DATABASE_KEYS)):
        item[name] = {"stats_reset": None, **dict.fromkeys(keys, 0)}
    return {"database_wait_diagnostics": item}


@pytest.mark.parametrize("gap", [1, 2.012074])
def test_original_statistics_gap_gate_is_preserved(tmp_path, gap):
    start = datetime(2026, 10, 12, tzinfo=UTC)
    times = [start, start + timedelta(seconds=gap), start + timedelta(seconds=gap + 1)]
    path = tmp_path / "db.jsonl"
    path.write_text("".join(json.dumps(diagnostic(at.isoformat())) + "\n" for at in times))
    result = sampling.summarize(path, start.isoformat(), times[-1].isoformat())
    assert result["complete"] is (gap == 1)
    assert result["offered_window_covered"]


def test_complete_internal_samples_do_not_prove_offered_window_coverage(tmp_path):
    start = datetime(2026, 10, 12, tzinfo=UTC)
    path = tmp_path / "db.jsonl"
    path.write_text("".join(json.dumps(diagnostic((start + timedelta(seconds=i)).isoformat())) + "\n"
                            for i in range(2)))
    result = sampling.summarize(path, start.isoformat(), (start + timedelta(seconds=3)).isoformat())
    assert not result["complete"] and not result["offered_window_covered"]


def test_verification_is_explicit_and_rejects_source_drift():
    assert not verification.selected(None) and not verification.selected({})
    goal = {"verification_decision": "ADR0285", "verification_contract_sha256": verification.digest(verification.contract())}
    assert verification.selected(goal)
    goal["verification_contract_sha256"] = "0" * 64
    with pytest.raises(ValueError, match="unchanged"):
        verification.selected(goal)


def test_integrated_stage_selects_bounded_audit_only_for_sealed_verification(monkeypatch):
    import cce_paid_stage as stages
    import cce_transaction_profile as transaction
    calls = []
    goal = {"verification_decision": "ADR0285", "verification_contract_sha256": verification.digest(verification.contract())}
    monkeypatch.setattr(transaction, "active", lambda: goal)
    monkeypatch.setattr(stages, "drain", lambda *args: {"pass": True})
    def execute(cid, source, timeout):
        calls.append((source, timeout))
        return {"pass": True, "hold_deadlines_elapsed": True}
    stage = SimpleNamespace(record={"customers_dispatched": True}, outcome_enabled=False,
            events=[str(uuid4())], cid="owned",
            profile=SimpleNamespace(expected=1, duration=0, name="test"),
            session=SimpleNamespace(api=execute), checkpoint=lambda: None)
    assert stages.PaidStage.audit(stage, "queues") == {"post_ttl_financial_pass": True, "global_queues_pass": True}
    assert len(calls) == 1 and calls[0][1] == 780
    assert "NOT MATERIALIZED" in calls[0][0] and "failed_predicate" in calls[0][0]


def test_diagnostic_collection_uses_raw_database_stream_not_repeated_foreground_snapshots(tmp_path, monkeypatch):
    import cce_paid_observers as observers
    start = datetime(2026, 10, 12, tzinfo=UTC)
    raw = tmp_path / "database-waits.jsonl"
    raw.write_text("".join(json.dumps(diagnostic((start + timedelta(seconds=i)).isoformat())) + "\n"
                          for i in range(3)))
    cached = tmp_path / "pipeline.jsonl"
    cached.write_text("invalid repeated statistics evidence")
    monkeypatch.setattr(observers, "api_cpu", lambda *args, **kwargs: {"pass": True})
    record = {}
    failures = observers.summarize_independent_diagnostics(record, [], cached, {}, start.isoformat(),
            (start + timedelta(seconds=2)).isoformat(), hourly=False, summarize_waits=lambda *args, **kwargs: pytest.fail(
                "Foreground evidence must not qualify raw statistics"), diagnostic_path=raw)
    assert failures == [] and record["database_wait_capture"]["complete"]
    assert record["database_wait_capture"]["rows"] == 3


def test_existing_trace_is_not_overwritten_and_shutdown_retains_failure(tmp_path):
    path = tmp_path / "database-waits.jsonl"
    path.write_text("retained evidence")
    sampler = sampling.Sampler(SimpleNamespace(), lambda *args: {}, ["show"], path, seconds=1)
    with pytest.raises(RuntimeError, match="FileExistsError"):
        sampler.snapshot(["show"])
    with pytest.raises(RuntimeError, match="FileExistsError"):
        sampler.stop()
    assert path.read_text() == "retained evidence" and not sampler.thread.is_alive()

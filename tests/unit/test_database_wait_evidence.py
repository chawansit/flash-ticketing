"""ADR0173 diagnostic safety and exact control regressions; no cloud load."""
import json
import sys
from contextlib import contextmanager
from datetime import UTC, datetime, timedelta
from pathlib import Path
from types import SimpleNamespace

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[2] / "scripts"))
import database_wait_contract as contract
import database_wait_evidence as evidence
import run_database_wait_diagnostics as profile
import work_envelope as envelope


def records(index=0):
    utc = (datetime(2026, 10, 6, tzinfo=UTC) + timedelta(seconds=index)).isoformat()
    activity = {"utc": utc, "restricted_sessions": 0, "active": 2, "idle_in_transaction": 1,
                "waits": [{"type": "IO", "event": "WALSync", "count": 2}], "query": "secret"}
    stats = {"utc": utc, "track_wal_io_timing": False, "track_io_timing": True}
    for group, keys in (("wal", evidence.WAL_KEYS), ("checkpointer", evidence.CHECKPOINT_KEYS), ("database", evidence.DATABASE_KEYS)):
        stats[group] = {k: 10 + index for k in keys}
        stats[group]["stats_reset"] = None
    return activity, stats


def row(index=0):
    item = evidence.sanitize(*records(index))
    item.update(complete=True, collection_ms=15)
    return {"utc": item["activity_utc"], "database_wait_diagnostics": item}


class Connection:
    autocommit = True

    def __init__(self, failure=None):
        self.commands = []
        self.failure = failure
        self.closed_transactions = 0

    @contextmanager
    def transaction(self):
        try:
            yield
        finally:
            self.closed_transactions += 1

    def execute(self, sql):
        self.commands.append(sql)
        if sql.startswith("SET"):
            return None
        if self.failure:
            raise self.failure
        if sql == evidence.CAPABILITY_SQL:
            result = {"server_version_num": 170011, "wal_view": True, "checkpointer_view": True,
                      "track_activities": True, "full_statistics_visibility": True,
                      "track_wal_io_timing": False, "track_io_timing": True}
        else:
            result = records()[0 if sql == evidence.ACTIVITY_SQL else 1]
        return SimpleNamespace(fetchone=lambda: [result])


def test_collector_reuses_connection_and_read_only_local_timeout():
    conn = Connection()
    collector = evidence.Collector()
    assert collector.collect(conn)["complete"]
    assert collector.collect(conn)["complete"]
    assert conn.closed_transactions == 5  # Capabilities once, two queries each sample.
    assert conn.commands.count("SET LOCAL statement_timeout='100ms'") == 5
    assert conn.commands.count("SET TRANSACTION READ ONLY") == 5
    assert conn.commands.count(evidence.CAPABILITY_SQL) == 1


def test_failed_query_exits_transaction_and_redacts_error():
    conn = Connection(RuntimeError("secret password and SQL"))
    result = evidence.Collector().collect(conn)
    assert result["complete"] is False and result["error_type"] == "RuntimeError"
    assert "secret" not in json.dumps(result)
    assert conn.closed_transactions == 1


def test_transactional_connection_rejected():
    conn = Connection()
    conn.autocommit = False
    assert evidence.Collector().collect(conn)["complete"] is False
    assert conn.commands == []


def test_capabilities_fail_before_activity_when_view_missing():
    conn = Connection()
    original = conn.execute
    def missing(sql):
        value = original(sql)
        if sql == evidence.CAPABILITY_SQL:
            result = value.fetchone()[0]
            result["checkpointer_view"] = False
            return SimpleNamespace(fetchone=lambda: [result])
        return value
    conn.execute = missing
    assert evidence.Collector().collect(conn)["complete"] is False
    assert evidence.ACTIVITY_SQL not in conn.commands


def test_timing_disabled_zero_does_not_prove_low_wal_latency():
    activity, stats = records()
    stats["wal"]["wal_write_time"] = stats["wal"]["wal_sync_time"] = 0
    assert evidence.sanitize(activity, stats)["wal_timing_available"] is False
    stats["track_wal_io_timing"] = True
    assert evidence.sanitize(activity, stats)["wal_timing_available"] is True
    stats["wal"]["wal_write_time"] = None  # PostgreSQL 18 absent column.
    assert evidence.sanitize(activity, stats)["wal_timing_available"] is False


@pytest.mark.parametrize("mutation", ["opaque", "label", "nan", "negative", "many", "timestamp", "missing"])
def test_rejects_incomplete_or_unbounded_evidence(mutation):
    activity, stats = records()
    if mutation == "opaque": activity["restricted_sessions"] = 1
    if mutation == "label": activity["waits"][0]["event"] = "secret sql text"
    if mutation == "nan": stats["wal"]["wal_bytes"] = float("nan")
    if mutation == "negative": stats["database"]["deadlocks"] = -1
    if mutation == "many": activity["waits"] *= 65
    if mutation == "timestamp": activity["utc"] = "2026-10-06T00:00:00"
    if mutation == "missing": stats["database"]["blks_read"] = None
    with pytest.raises(ValueError): evidence.sanitize(activity, stats)


def test_only_allowlisted_data_exported():
    checked = evidence.sanitize(*records())
    assert "secret" not in json.dumps(checked) and "query" not in checked
    assert all(k not in evidence.ACTIVITY_SQL for k in ("client_addr", "query_start", "query_id"))


def trace(tmp_path, rows):
    path = tmp_path / "pipeline.jsonl"
    path.write_text("\n".join(json.dumps(r) for r in rows))
    return path


def test_summary_measures_deltas_and_states_scope(tmp_path):
    result = evidence.summarize(trace(tmp_path, [row(0), row(1), row(2)]))
    assert result["complete"] and result["counter_deltas"]["wal"]["wal_bytes"] == 2
    assert result["wal_timing_available_throughout"] is False
    assert result["observed_active_wait_peaks"] == {"IO:WALSync": 2}


@pytest.mark.parametrize("mutation", ["reset", "decrease", "gap", "missing", "overhead"])
def test_summary_fails_instead_of_bridging_invalid_samples(tmp_path, mutation):
    rows = [row(0), row(1)]
    item = rows[1]["database_wait_diagnostics"]
    if mutation == "reset": item["wal"]["stats_reset"] = item["activity_utc"]
    if mutation == "decrease": item["wal"]["wal_bytes"] = 0
    if mutation == "gap": rows[1] = row(4)
    if mutation == "missing": rows[1] = {"utc": item["activity_utc"]}
    if mutation == "overhead": item["collection_ms"] = 251
    assert evidence.summarize(trace(tmp_path, rows))["complete"] is False


def test_install_preserves_paid_counts_and_uses_same_connection():
    conn = Connection()
    module = SimpleNamespace(sample=lambda connection, show_ids: {"issued_tickets": 18000, "ids": show_ids})
    evidence.install(module)
    result = module.sample(conn, ["show"])
    assert result["issued_tickets"] == 18000 and result["database_wait_diagnostics"]["complete"]


def test_exact_new_profile_keeps_workload_images_and_budgets():
    import slow_database_contract
    before, after = slow_database_contract.plan(), contract.plan()
    for key in ("common", "artifact_receipt", "expected_runtime_source_sha256", "arm_settings", "allowance"):
        assert before[key] == after[key]
    engine = profile.create_runner()
    c = engine.StatusRefreshContract(after["artifact_receipt"], "control", after["expected_runtime_source_sha256"])
    engine.RefreshStages(False, {}, c, "adr0151-" + "b" * 12)
    assert "scripts/database_wait_evidence.py" in engine.identity()
    with pytest.raises(ValueError):
        engine.StatusRefreshContract(after["artifact_receipt"], "candidate", after["expected_runtime_source_sha256"])


def test_unknown_dynamic_scope_does_not_get_normalized():
    key = "bounded_database_wait_diagnostics__" + "a" * 12
    assert envelope.base_ledger(key) == profile.LEDGER
    assert envelope.base_ledger(key + "typo") == key + "typo"


def test_preflight_compiles_and_closes_transient_connection():
    program = evidence.preflight_program()
    compile(program, "preflight", "exec")
    assert "connect_timeout=5" in program and "assert all(r['complete']" in program


def test_failure_classifier_retains_counts_without_weakening_visibility_gate():
    conn = Connection()
    original = conn.execute
    def hidden(sql):
        result = original(sql)
        if sql == evidence.ACTIVITY_SQL:
            data = result.fetchone()[0]
            data.update(restricted_sessions=1, restricted_backends=[
                {"backend_type": "parallel worker", "own_role": True, "count": 1, "query": "secret"}])
            return SimpleNamespace(fetchone=lambda: [data])
        return result
    conn.execute = hidden
    result = evidence.Collector().collect(conn)
    assert result["complete"] is False
    assert result["error_phase"] == "validation" and result["error_code"] == "activity_visibility_or_bound"
    assert result["failure_context"]["restricted_backends"] == [
        {"backend_type": "parallel worker", "own_role": True, "count": 1}]
    assert "secret" not in json.dumps(result)


def test_failure_context_rejects_arbitrary_fields_labels_and_bounds():
    assert evidence.failure_context({"restricted_backends": [
        {"backend_type": "secret:password", "own_role": True, "count": 1}]}) == {"restricted_backends": []}
    assert evidence.failure_context({"restricted_backends": [{}] * 17}) == {}


def test_unavailable_backend_type_is_retained_without_weakening_visibility():
    group = {"backend_type": None, "own_role": False, "count": 1, "usename": "secret"}
    result = evidence.failure_context({"restricted_sessions": 1, "restricted_backends": [group]})
    assert result["restricted_backends"] == [{"backend_type": "unavailable", "own_role": False, "count": 1}]
    activity, stats = records(); activity["restricted_sessions"] = 1
    with pytest.raises(ValueError, match="visibility"):
        evidence.sanitize(activity, stats)
    assert "secret" not in json.dumps(result)


@pytest.mark.parametrize("privilege", [False, None, 1, "true"])
def test_missing_effective_statistics_privilege_stops_before_activity(privilege):
    conn = Connection()
    original = conn.execute
    def restricted(sql):
        result = original(sql)
        if sql == evidence.CAPABILITY_SQL:
            value = result.fetchone()[0]
            value["full_statistics_visibility"] = privilege
            return SimpleNamespace(fetchone=lambda: [value])
        return result
    conn.execute = restricted
    result = evidence.Collector().collect(conn)
    assert result["complete"] is False
    assert result["error_phase"] == "capabilities"
    assert result["error_code"] == "statistics_privilege_missing"
    assert evidence.ACTIVITY_SQL not in conn.commands and evidence.STATS_SQL not in conn.commands
    assert conn.closed_transactions == 1

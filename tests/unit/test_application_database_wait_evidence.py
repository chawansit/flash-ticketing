"""ADR0176 scope, identity, privacy and continuity regressions; no cloud calls."""
import json
import sys
from datetime import UTC, datetime, timedelta
from pathlib import Path

import pytest
from test_database_wait_evidence import Connection, records

sys.path.insert(0, str(Path(__file__).resolve().parents[2] / "scripts"))
import application_database_wait_evidence as scoped
import database_wait_evidence as full


def binding():
    return scoped.RoleCoverage(scoped.role_digest("application", "ticketing"), tuple(sorted(scoped.EXPECTED_REPLICAS.items())))


def activity(index=0):
    return {"utc": (datetime(2026, 10, 6, tzinfo=UTC) + timedelta(seconds=index)).isoformat(),
            "observer_role": "application", "database": "ticketing", "sessions_total": 7,
            "application_sessions": 4, "foreign_sessions": 3, "unknown_sessions": 0,
            "restricted_application_sessions": 0, "restricted_foreign_sessions": 2,
            "active": 2, "idle_in_transaction": 1,
            "waits": [{"type": "IO", "event": "WALSync", "count": 2}], "query": "private SQL"}


def sample(index=0):
    item = scoped.sanitize(activity(index), records(index)[1], binding())
    item["collection_ms"] = 15
    return item


def trace(tmp_path, values):
    path = tmp_path / "pipeline.jsonl"
    path.write_text("\n".join(json.dumps({"application_database_wait_diagnostics": x}) for x in values))
    return path


def rows():
    return [{"Id": str(i), "State": {"Running": True},
             "Config": {"Labels": {"com.docker.compose.service": role},
                        "Env": ["DATABASE_URL=postgresql://application:private@pool/ticketing"]}}
            for i, role in enumerate(role for role, count in scoped.EXPECTED_REPLICAS.items() for _ in range(count))]


def test_binding_checks_every_api_and_database_worker_and_exports_no_credentials():
    proof = scoped.prove_role_coverage(rows(), scoped.EXPECTED_REPLICAS, "postgresql://application:private@pool/ticketing")
    assert proof == binding() and sum(dict(proof.replicas).values()) == 18
    assert "private" not in str(proof) and "application" not in str(proof)


@pytest.mark.parametrize("mutation", ["missing", "role", "database", "duplicate", "stopped", "incomplete_policy"])
def test_binding_rejects_incomplete_or_changed_application_authority(mutation):
    data, expected = rows(), dict(scoped.EXPECTED_REPLICAS)
    if mutation == "missing": data.pop()
    if mutation == "role": data[-1]["Config"]["Env"] = ["DATABASE_URL=postgresql://other@pool/ticketing"]
    if mutation == "database": data[-1]["Config"]["Env"] = ["DATABASE_URL=postgresql://application@pool/other"]
    if mutation == "duplicate": data[-1]["Id"] = data[0]["Id"]
    if mutation == "stopped": data[-1]["State"]["Running"] = False
    if mutation == "incomplete_policy": expected.pop("confirmation")
    with pytest.raises(ValueError):
        scoped.prove_role_coverage(data, expected, "postgresql://application@pool/ticketing")


def test_foreign_masking_is_explicit_and_does_not_pass_historical_global_gate():
    result = sample()
    assert result["application_visibility_complete"] is True
    assert result["full_database_visibility_complete"] is False
    assert result["restricted_foreign_sessions"] == 2
    assert "complete" not in result and "restricted_sessions" not in result
    assert "observer_role" not in result and "query" not in result and "private SQL" not in str(result)


@pytest.mark.parametrize("mutation", ["unknown", "hidden_own", "role", "database", "partition", "too_many_masked", "waits", "label"])
def test_invalid_ownership_or_application_activity_fails_closed(mutation):
    a = activity()
    if mutation == "unknown": a.update(unknown_sessions=1, sessions_total=8)
    if mutation == "hidden_own": a["restricted_application_sessions"] = 1
    if mutation == "role": a["observer_role"] = "different"
    if mutation == "database": a["database"] = "different"
    if mutation == "partition": a["sessions_total"] = 10
    if mutation == "too_many_masked": a["restricted_foreign_sessions"] = 4
    if mutation == "waits": a["waits"][0]["count"] = 3
    if mutation == "label": a["waits"][0]["event"] = "private SQL"
    with pytest.raises(ValueError): scoped.sanitize(a, records()[1], binding())


def test_scoped_collector_uses_bounded_existing_connection_without_full_stats_access():
    conn = Connection(); original = conn.execute
    def execute(sql):
        from types import SimpleNamespace
        if sql == scoped.ACTIVITY_SQL:
            conn.commands.append(sql)
            return SimpleNamespace(fetchone=lambda: [activity()])
        value = original(sql)
        if sql == full.CAPABILITY_SQL:
            value.fetchone()[0]["full_statistics_visibility"] = False
        return value
    conn.execute = execute
    result = scoped.Collector(binding()).collect(conn)
    assert result["application_visibility_complete"] and not result["full_database_visibility_complete"]
    assert conn.closed_transactions == 3
    assert conn.commands.count("SET LOCAL statement_timeout='100ms'") == 3
    assert conn.commands.count("SET TRANSACTION READ ONLY") == 3
    assert full.ACTIVITY_SQL not in conn.commands


def test_summary_keeps_scoped_pass_and_global_visibility_failure_separate(tmp_path):
    report = scoped.summarize(trace(tmp_path, [sample(0), sample(1)]), binding())
    assert report["application_database_wait_evidence_complete"]
    assert not report["full_database_visibility_complete"] and "complete" not in report
    assert report["masked_foreign_sample_count"] == 2 and report["masked_foreign_sessions_peak"] == 2
    assert report["counter_deltas"]["wal"]["wal_bytes"] == 1
    assert report["application_active_wait_peaks"] == {"IO:WALSync": 2}
    assert report["wal_timing_available_throughout"] is False


@pytest.mark.parametrize("mutation", ["scope", "binding", "gap", "reset", "decrease", "error", "overhead"])
def test_summary_cannot_bridge_failed_or_drifted_evidence(tmp_path, mutation):
    values = [sample(0), sample(1)]
    if mutation == "scope": values[1]["diagnostic_scope"] = "full_database"
    if mutation == "binding": values[1]["role_identity_sha256"] = "0" * 64
    if mutation == "gap": values[1] = sample(4)
    if mutation == "reset": values[1]["wal"]["stats_reset"] = values[1]["statistics_utc"]
    if mutation == "decrease": values[1]["wal"]["wal_bytes"] = 0
    if mutation == "error": values[1]["application_visibility_complete"] = False
    if mutation == "overhead": values[1]["collection_ms"] = 251
    report = scoped.summarize(trace(tmp_path, values), binding())
    assert not report["application_database_wait_evidence_complete"]


def test_global_visibility_flag_cannot_contradict_foreign_masking(tmp_path):
    values = [sample(0), sample(1)]; values[1]["full_database_visibility_complete"] = True
    with pytest.raises(ValueError): scoped.summarize(trace(tmp_path, values), binding())


def test_single_materialized_snapshot_and_no_query_identity_export():
    assert scoped.ACTIVITY_SQL.count("FROM pg_catalog.pg_stat_activity") == 1
    assert "AS MATERIALIZED" in scoped.ACTIVITY_SQL
    assert all(k not in scoped.ACTIVITY_SQL for k in ("client_addr", "application_name", "query_start", "query_id"))


def test_missing_binding_and_invalid_capabilities_rejected():
    with pytest.raises(ValueError): scoped.Collector(None)
    conn = Connection(); original = conn.execute
    def execute(sql):
        value = original(sql)
        if sql == full.CAPABILITY_SQL: value.fetchone()[0]["track_activities"] = False
        return value
    conn.execute = execute
    result = scoped.Collector(binding()).collect(conn)
    assert result["application_visibility_complete"] is False and result["error_phase"] == "capabilities"
    assert scoped.ACTIVITY_SQL not in conn.commands


def test_failed_query_does_not_export_exception_text():
    result = scoped.Collector(binding()).collect(Connection(RuntimeError("private credentials and SQL")))
    assert not result["application_visibility_complete"] and "private" not in str(result)


@pytest.mark.parametrize("bad", [scoped.RoleCoverage("private identity", tuple(sorted(scoped.EXPECTED_REPLICAS.items()))),
                                 scoped.RoleCoverage("0" * 64, (("api", 4),))])
def test_malformed_binding_is_not_coverage_proof(bad):
    with pytest.raises(ValueError): scoped.Collector(bad)


def test_fully_visible_samples_can_report_global_visibility_without_global_gate_alias(tmp_path):
    values = []
    for index in (0, 1):
        a = activity(index); a["restricted_foreign_sessions"] = 0
        value = scoped.sanitize(a, records(index)[1], binding()); value["collection_ms"] = 10
        values.append(value)
    result = scoped.summarize(trace(tmp_path, values), binding())
    assert result["application_database_wait_evidence_complete"] and result["full_database_visibility_complete"]
    assert "complete" not in result


def test_existing_cloud_profiles_remain_full_visibility_and_scope_is_unregistered():
    import api_placement_contract
    import work_envelope
    assert "diagnostic_scope" not in api_placement_contract.plan()
    assert all("application_role" not in p for p in work_envelope.envelope()["qualified_profiles"])
    assert "application_database_wait_evidence" not in Path("scripts/run_work_envelope.py").read_text()

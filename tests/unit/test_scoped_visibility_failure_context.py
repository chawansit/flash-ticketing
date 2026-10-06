"""ADR0179 preserves diagnostic failure, privacy and bounded context; no cloud calls."""
import json
import sys
from pathlib import Path
from types import SimpleNamespace

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[2] / "scripts"))
import application_database_wait_evidence as scoped
import database_wait_evidence as full
from test_application_database_wait_evidence import activity, binding, sample, trace
from test_database_wait_evidence import Connection


def backend(**extra):
    return {"backend_type": "autovacuum worker", "own_role": None, "state_hidden": False, "count": 1, **extra}


@pytest.mark.parametrize("kind", ["unknown", "hidden_application"])
def test_real_collector_preserves_rejection_and_attribution_without_extra_queries(tmp_path, kind):
    value = activity()
    if kind == "unknown":
        value.update(sessions_total=8, unknown_sessions=1, failure_backends=[backend()])
    else:
        value.update(restricted_application_sessions=1, failure_backends=[backend(backend_type="client backend", own_role=True, state_hidden=True)])
    value.update(observer_role="application", database="ticketing", password="secret", SQL="secret")
    conn = Connection();original = conn.execute
    def execute(sql):
        if sql == scoped.ACTIVITY_SQL:
            conn.commands.append(sql)
            return SimpleNamespace(fetchone=lambda: [value])
        return original(sql)
    conn.execute = execute
    item = scoped.Collector(binding()).collect(conn)
    assert item["application_visibility_complete"] is False and item["error_code"] == "application_visibility_missing"
    assert item["error_phase"] == "validation" and item["error_type"] == "ValueError"
    assert conn.closed_transactions == 3 and conn.commands.count(scoped.ACTIVITY_SQL) == 1
    assert conn.commands.count(full.STATS_SQL) == 1
    assert conn.commands.count("SET LOCAL statement_timeout='100ms'") == 3
    assert conn.commands.count("SET TRANSACTION READ ONLY") == 3
    context = item["failure_context"]
    assert context["unknown_sessions" if kind == "unknown" else "restricted_application_sessions"] == 1
    assert context["backend_groups_complete"]
    assert "secret" not in json.dumps(item) and "observer_role" not in json.dumps(item)
    report = scoped.summarize(trace(tmp_path, [sample(0), item, sample(3)]), binding())
    assert report["application_database_wait_evidence_complete"] is False
    assert report["counter_deltas"] is None and report["counter_continuity"] is False
    assert report["error_counts"] == {"MissingOrInvalidScope": 1}
    assert report["collector_failure_categories"] == {"validation:application_visibility_missing": 1}
    assert report["failure_context_samples"] == 1
    label = "autovacuum worker:unknown:visible" if kind == "unknown" else "client backend:application:hidden"
    assert report["failure_context_peaks"]["backend_groups"] == {label: 1}


@pytest.mark.parametrize("mutation", ["limit", "invalid_count", "missing_role", "string_role", "invalid_state", "invalid_group"])
def test_malformed_or_oversized_context_is_bounded_and_never_completes(mutation):
    groups = [backend()]
    if mutation == "limit": groups *= 17
    elif mutation == "invalid_count": groups[0]["count"] = True
    elif mutation == "missing_role": del groups[0]["own_role"]
    elif mutation == "string_role": groups[0]["own_role"] = "secret"
    elif mutation == "invalid_state": groups[0]["state_hidden"] = "secret"
    else: groups[0] = "secret"
    result = scoped.failure_context({"unknown_sessions": True, "failure_backends": groups})
    assert "unknown_sessions" not in result and not result["backend_groups_complete"]
    assert not result["backend_groups"] and "secret" not in json.dumps(result)


def test_context_and_summary_sanitize_arbitrary_labels_and_fields_again(tmp_path):
    value = {"unknown_sessions": 1, "restricted_application_sessions": 0, "failure_backends": [backend(backend_type="password secret", password="secret")], "pid": 123}
    context = scoped.failure_context(value)
    assert context["backend_groups"][0]["backend_type"] == "other"
    assert "secret" not in json.dumps(context) and "pid" not in context
    context.update(password="secret")
    item = {"diagnostic_scope": scoped.SCOPE, "application_visibility_complete": False,
            "error_phase": "password secret", "error_code": ["secret"], "failure_context": context}
    report = scoped.summarize(trace(tmp_path, [sample(), item]), binding())
    assert not report["application_database_wait_evidence_complete"]
    assert report["collector_failure_categories"] == {"other:other": 1}
    assert report["failure_context_peaks"]["backend_groups"] == {"other:unknown:visible": 1}
    assert "secret" not in json.dumps(report)


def test_successful_samples_and_missing_historical_context_keep_original_gates(tmp_path):
    report = scoped.summarize(trace(tmp_path, [sample(0), sample(1)]), binding())
    assert report["application_database_wait_evidence_complete"] and report["failure_context_samples"] == 0
    item = {"diagnostic_scope": scoped.SCOPE, "application_visibility_complete": False,
            "error_phase": "validation", "error_code": "application_visibility_missing"}
    report = scoped.summarize(trace(tmp_path, [sample(), item]), binding())
    assert report["error_counts"] == {"MissingOrInvalidScope": 1} and report["failure_context_samples"] == 0
    assert not report["application_database_wait_evidence_complete"] and report["counter_deltas"] is None


def test_grouping_uses_single_snapshot_and_no_identity_or_payload_columns():
    sql = scoped.ACTIVITY_SQL
    assert sql.count("FROM pg_catalog.pg_stat_activity") == 1 and "AS MATERIALIZED" in sql
    assert "LIMIT 16" in sql and "backend_type" in sql
    assert all(key not in sql for key in ("client_addr", "application_name", "query_start", "query_id"))


def test_context_completeness_requires_all_rejected_sessions_and_no_truncated_groups():
    result = scoped.failure_context({"unknown_sessions": 2, "restricted_application_sessions": 0,
                                     "failure_backends": [backend()]})
    assert not result["backend_groups_complete"]
    result = scoped.failure_context({"unknown_sessions": 16, "restricted_application_sessions": 0,
                                     "failure_backends": [backend()] * 16})
    assert len(result["backend_groups"]) == 16 and not result["backend_groups_complete"]

"""Bounded evidence never controls financial cleanup or exposes raw log fields."""
import json
import sys
from pathlib import Path
from types import SimpleNamespace

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[2] / "scripts"))
import admission_failure_evidence as evidence


def failure():
    return {"event": "db_acquisition_failure", "time": "2026-10-06T01:00:00+00:00",
            "request_id": "00000000-0000-4000-8000-000000000001", "role": "payment",
            "reason": "native_timeout", "capture": "native_failure_before_release", "elapsed_ms": 500.3,
            "native_pool": {"pool_size": 2, "pool_available": 0, "requests_waiting": 3, "pool_max": 2},
            "guard": {"maximum": 12, "used": 4, "acquiring": 3, "retained": 1, "callback_reserved": 0,
                      "partial_timeout_reclaim": True, "counts": {"general": 0, "payment": 4},
                      "retained_by_role": {"general": 0, "payment": 1}, "limits": {"general": 10, "payment": 10}}}


def apis():
    return [{"container_id": x * 64, "image_id": "sha256:" + "c" * 64,
             "started_at": "2026-10-06T00:00:00.000000000Z"} for x in ("a", "b")]


def trace(path, count=1):
    values = {f"acquisition_failure:{r}:{why}": 0 for r in evidence.ROLES for why in evidence.REASONS}
    rows = [{"api_replicas": {"api": values}},
            {"api_replicas": {"api": {**values, "acquisition_failure:payment:native_timeout": count}}}]
    path.write_text("\n".join(json.dumps(r) for r in rows) + "\n")


def test_allowlist_removes_secrets_sql_and_arbitrary_nested_fields():
    raw = failure();raw.update(password="secret", DSN="postgres://secret", exception="SQL customer payload")
    raw["native_pool"]["connection"] = "postgres://secret"
    raw["guard"]["untrusted"] = {"password": "secret"}
    result = evidence.sanitize(raw)
    assert "secret" not in json.dumps(result) and "SQL" not in json.dumps(result)
    assert result["request_id"] == raw["request_id"] and result["guard"]["used"] == 4
    assert evidence.sanitize({"event": "other", "secret": "secret"}) is None


@pytest.mark.parametrize("field,value", [("role", "unknown"), ("reason", "unknown"), ("elapsed_ms", float("nan")),
                                        ("elapsed_ms", -1), ("native_pool", None), ("request_id", "token-secret"),
                                        ("time", "2026-10-06T01:00:00")])
def test_missing_or_invalid_snapshot_fails_closed(field, value):
    row = failure();row[field] = value
    with pytest.raises((ValueError, TypeError)):
        evidence.sanitize(row)


def test_remote_program_is_compilable_and_contains_fixed_bounds():
    program = evidence.program(apis(), "2026-10-06T00:00:00+00:00")
    compile(program, "remote-diagnostics", "exec")
    assert "docker" in program and "logs" in program and "inspect" in program
    assert "MAX_BYTES=67108864" in program and "MAX_RECORDS=128" in program
    assert "process.kill()" in program and "identities() != expected" in program


@pytest.mark.parametrize("change", ["id", "image", "start", "since"])
def test_remote_inputs_are_exact_identities(change):
    data = apis();since = "2026-10-06T00:00:00+00:00"
    if change == "id":data[0]["container_id"] = "arbitrary"
    elif change == "image":data[0]["image_id"] = "mutable:latest"
    elif change == "start":data[0]["started_at"] = None
    else:since = "2026-10-06T00:00:00"
    with pytest.raises(ValueError):evidence.program(data, since)


def test_snapshot_counts_cover_counters_and_reject_missing_reset(tmp_path):
    path = tmp_path / "trace.jsonl";trace(path)
    assert evidence.counter_coverage({"api": [failure()]}, path)
    assert not evidence.counter_coverage({"api": []}, path)
    trace(path, -1)
    with pytest.raises(ValueError):evidence.counter_coverage({"api": [failure()]}, path)


@pytest.mark.parametrize("truncated", [False, True])
def test_collector_sanitizes_again_and_exposes_incomplete_evidence(tmp_path, truncated):
    path = tmp_path / "pipeline.jsonl"
    trace(path, 1)
    rows = [json.loads(line) for line in path.read_text().splitlines()]
    for row in rows:
        metric = row["api_replicas"]["api"]
        row["api_replicas"] = {h + ":" + a["container_id"]: metric for h in ("primary", "secondary") for a in apis()}
    path.write_text("\n".join(json.dumps(row) for row in rows) + "\n")
    data = {"captured_at": "2026-10-06T00:00:00+00:00",
            "apis": [{"host_role": h, **a} for h in ("primary", "secondary") for a in apis()]}
    class Session:
        def call(self, role, code, timeout):
            compile(code, "remote", "exec");assert timeout == 45
            row = {**failure(), "SQL": "secret"}
            return {"complete": not truncated, "DSN": "secret", "apis": [
                {"container_id": a["container_id"], "complete": not truncated, "input_bytes": 100, "records": [row]}
                for a in apis()]}
    result = evidence.collect(Session(), data, tmp_path)
    assert result["complete"] is (not truncated) and result["record_count"] == 4
    assert "secret" not in (tmp_path / "admission-failure-evidence.json").read_text()


def test_stream_filter_bounds_and_container_identity(monkeypatch):
    import os
    import select
    import subprocess

    raw = "2026-10-06T01:00:00Z " + json.dumps(failure()) + "\n"
    rows = [{"Id": a["container_id"], "Image": a["image_id"], "State": {"Running": True, "StartedAt": a["started_at"]},
             "Config": {"Labels": {"com.docker.compose.service": "api", "com.docker.compose.project": "flash-ticketing"}}}
            for a in apis()]
    monkeypatch.setattr(subprocess, "check_output", lambda *args, **kwargs: json.dumps(rows).encode())
    chunks = []
    class Process:
        def __init__(self, *args, **kwargs):
            chunks[:] = [raw.encode(), b""]
            self.stdout = SimpleNamespace(fileno=lambda: 99, close=lambda: None)
        def wait(self, **kwargs):return 0
        def poll(self):return 0
    monkeypatch.setattr(subprocess, "Popen", Process)
    monkeypatch.setattr(select, "select", lambda *args: ([1], [], []))
    monkeypatch.setattr(os, "read", lambda *args: chunks.pop(0))
    result = evidence.remote_collect(apis(), "2026-10-06T00:00:00+00:00")
    assert result["complete"] and sum(len(a["records"]) for a in result["apis"]) == 2
    monkeypatch.setattr(evidence, "MAX_BYTES", 1)
    assert not evidence.remote_collect(apis(), "2026-10-06T00:00:00+00:00")["complete"]
    rows[0]["Image"] = "wrong"
    with pytest.raises(ValueError, match="identity changed"):
        evidence.remote_collect(apis(), "2026-10-06T00:00:00+00:00")


def test_hook_captures_before_cleanup_and_keeps_financial_audits_after_failure():
    source = Path("scripts/run_two_host_paid_comparison.py").read_text()
    finally_source = source[source.index("            cleanup_errors = []"):]
    capture = finally_source.index("record.update(collect_profile_failure_evidence(session, inventory, local, base_ledger(self.ledger_key)))")
    assert capture < finally_source.index("self.stop(session, role, cid, job)")
    assert capture < finally_source.index('("financial", lambda:')
    assert 'record["pass"] = False' in finally_source[:finally_source.index("self.stop(session, role, cid, job)")]
    assert 'record["private_cleanup_pass"] = not cleanup_errors' in finally_source


def test_other_replica_records_cannot_hide_missing_snapshot(tmp_path):
    path = tmp_path / "trace.jsonl";trace(path)
    rows = [json.loads(line) for line in path.read_text().splitlines()]
    for row in rows:
        row["api_replicas"]["other"] = {key: 0 for key in row["api_replicas"]["api"]}
    path.write_text("\n".join(json.dumps(row) for row in rows) + "\n")
    assert not evidence.counter_coverage({"api": [], "other": [failure()]}, path)


def test_failure_after_old_byte_ceiling_is_retained_without_raw_logs(monkeypatch):
    import itertools
    import os
    import select
    import subprocess

    api = apis()[0]
    identity = [{"Id": api["container_id"], "Image": api["image_id"],
                 "State": {"Running": True, "StartedAt": api["started_at"]},
                 "Config": {"Labels": {"com.docker.compose.service": "api",
                                       "com.docker.compose.project": "flash-ticketing"}}}]
    monkeypatch.setattr(subprocess, "check_output", lambda *args, **kwargs: json.dumps(identity).encode())
    unrelated = b"2026-10-06T01:00:00Z unrelated private-content-must-not-be-retained\n"
    chunk = unrelated * (65536 // len(unrelated))
    count = (32 * 2**20) // len(chunk) + 2
    record = ("2026-10-06T01:00:00Z " + json.dumps(failure()) + "\n").encode()
    stream = itertools.chain(itertools.repeat(chunk, count), (record, b""))

    class Process:
        stdout = SimpleNamespace(fileno=lambda: 99, close=lambda: None)
        def __init__(self, *args, **kwargs): pass
        def wait(self, **kwargs): return 0
        def poll(self): return 0

    monkeypatch.setattr(subprocess, "Popen", Process)
    monkeypatch.setattr(select, "select", lambda *args: ([1], [], []))
    monkeypatch.setattr(os, "read", lambda *args: next(stream))
    proof = evidence.remote_collect([api], "2026-10-06T00:00:00+00:00")
    captured = proof["apis"][0]
    assert proof["complete"] is True
    assert 32 * 2**20 < captured["input_bytes"] < 64 * 2**20
    assert captured["records"] == [evidence.sanitize(failure())]
    assert "private-content" not in json.dumps(proof)
    program = evidence.program([api], "2026-10-06T00:00:00+00:00", expected_api_count=1)
    assert "MAX_BYTES=67108864" in program
    assert "MAX_LINE=16384" in program and "MAX_RECORDS=128" in program
    assert "remaining = 10 -" in program

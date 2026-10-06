"""ADR0171 bounded diagnostics preserve a frozen control and historical scopes."""
import json
import sys
from pathlib import Path
from types import SimpleNamespace

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[2] / "scripts"))
import admission_failure_evidence as failure
import run_payment_stall_diagnostics as previous_runner
import run_slow_database_diagnostics as runner
import slow_database_contract as policy
import slow_database_evidence as evidence
from test_admission_failure_evidence import apis
from test_admission_failure_evidence import failure as failure_record


def slow(phase="commit", **extra):
    return {"event": "slow_db_phase", "time": "2026-10-06T01:00:01+00:00", "phase": phase,
            "duration_ms": 120.5, "outcome": "ok", **extra}


def trace(path, failure_count=0):
    labels = [host + ":" + a["container_id"] for host in ("primary", "secondary") for a in apis()]
    rows = []
    for second in range(4):
        metric = {key: 0 for key in evidence.GAUGES}
        metric.update(process_start_time_seconds=100)
        for phase in evidence.DURATIONS:
            metric["duration:" + phase + ":count"] = second * 10
            metric["duration:" + phase + ":sum"] = second * .2
        for role in failure.ROLES:
            for reason in failure.REASONS:
                metric[f"acquisition_failure:{role}:{reason}"] = failure_count if second and role == "payment" and reason == "native_timeout" else 0
        rows.append({"utc": f"2026-10-06T01:00:0{second}+00:00", "db_lock_waiters": 0,
                     "api_replicas": {label: dict(metric) for label in labels}})
    path.write_text("\n".join(json.dumps(r) for r in rows) + "\n")
    return labels, rows


def inventory():
    return {"captured_at": "2026-10-06T01:00:00+00:00", "apis": [
        {"host_role": host, **api} for host in ("primary", "secondary") for api in apis()]}


def test_slow_allowlist_never_keeps_request_payload_or_invents_correlation():
    row = slow(SQL="secret", request_id="secret", message="secret", exception="secret", nested={"dsn": "secret"})
    assert evidence.sanitize(row) == slow()
    assert "secret" not in json.dumps(evidence.sanitize(row))
    assert evidence.sanitize(failure_record()) == failure.sanitize(failure_record())
    assert evidence.sanitize({"event": "unrelated", "password": "secret"}) is None


@pytest.mark.parametrize("field,value", [("phase", "SELECT customer"), ("outcome", "unknown"),
                                         ("duration_ms", 99), ("duration_ms", float("nan")),
                                         ("time", "2026-10-06T01:00:00"), ("duration_ms", True)])
def test_invalid_slow_records_rejected(field, value):
    with pytest.raises((ValueError, TypeError)):
        evidence.sanitize(slow(**{field: value}))


def test_remote_program_compiles_and_keeps_distinct_limits():
    code = evidence.program(apis(), "2026-10-06T01:00:00+00:00")
    compile(code, "remote", "exec")
    assert "'db_acquisition_failure': 128" in code and "'slow_db_phase': 512" in code
    assert "identities() != expected" in code and "failure_sanitize=sanitize" in code


def test_context_is_replica_specific_and_interval_means_are_not_percentiles(tmp_path):
    labels, _ = trace(tmp_path / "pipeline.jsonl")
    samples = evidence.trace_context(tmp_path / "pipeline.jsonl", labels)
    event = slow()
    contexts, complete = evidence.temporal_context([(labels[0], event)], samples)
    assert complete and contexts[0]["context_bracketed"]
    assert len(contexts[0]["nearby_samples"]) <= 5
    assert contexts[0]["nearby_samples"][1]["interval_mean_ms"]["db_commit:all"] == 20
    assert "request_id" not in contexts[0]["record"]
    failure_row = failure_record();failure_row["time"] = "2026-10-06T00:00:00+00:00"
    contexts, complete = evidence.temporal_context([(labels[0], failure_row)], samples)
    assert not complete and not contexts[0]["context_bracketed"]


@pytest.mark.parametrize("drift", ["gap", "reverse", "restart", "missing_replica", "counter_reset", "missing_metric", "sum_without_count", "nan"])
def test_trace_rejects_drift_without_using_other_replica_as_substitute(tmp_path, drift):
    path = tmp_path / "pipeline.jsonl";labels, rows = trace(path)
    metric = rows[2]["api_replicas"][labels[0]]
    if drift == "gap": rows[2]["utc"] = "2026-10-06T01:00:09+00:00"
    elif drift == "reverse": rows[2]["utc"] = rows[0]["utc"]
    elif drift == "restart": metric["process_start_time_seconds"] = 101
    elif drift == "missing_replica": del rows[2]["api_replicas"][labels[0]]
    elif drift == "counter_reset": metric["duration:db_commit:all:count"] = 0
    elif drift == "missing_metric": del metric[evidence.GAUGES[0]]
    elif drift == "sum_without_count": metric["duration:db_commit:all:count"] = 10
    else: metric[evidence.GAUGES[0]] = float("nan")
    path.write_text("\n".join(json.dumps(r) for r in rows) + "\n")
    with pytest.raises((ValueError, KeyError)):
        evidence.trace_context(path, labels)


@pytest.mark.parametrize("bound", ["MAX_TRACE_BYTES", "MAX_TRACE_LINE", "MAX_TRACE_ROWS"])
def test_trace_bounds(monkeypatch, tmp_path, bound):
    path = tmp_path / "pipeline.jsonl";labels, _ = trace(path)
    monkeypatch.setattr(evidence, bound, 1)
    with pytest.raises(ValueError): evidence.trace_context(path, labels)


@pytest.mark.parametrize("truncated,missing_failure", [(False, False), (True, False), (False, True)])
def test_combined_collection_preserves_failure_coverage_and_sanitizes_twice(tmp_path, truncated, missing_failure):
    trace(tmp_path / "pipeline.jsonl", 1)
    class Session:
        def call(self, role, code, timeout):
            compile(code, "remote", "exec");assert timeout == 45
            f = failure_record();f["time"] = "2026-10-06T01:00:01+00:00"
            rows = [slow(SQL="secret")] + ([] if missing_failure else [f])
            return {"complete": not truncated, "apis": [{"container_id": a["container_id"], "complete": not truncated,
                                                          "input_bytes": 500, "records": rows} for a in apis()]}
    result = evidence.collect(Session(), inventory(), tmp_path)
    assert result["complete"] is (not truncated and not missing_failure)
    assert result["slow_phase_count"] == 4
    assert result["failure_count"] == (0 if missing_failure else 4)
    assert "secret" not in (tmp_path / "slow-database-evidence.json").read_text()
    legacy = json.loads((tmp_path / "admission-failure-evidence.json").read_text())
    assert legacy["record_count"] == result["failure_count"]
    assert all(row["event"] == "db_acquisition_failure" for h in legacy["hosts"] for a in h["apis"] for row in a["records"])


def test_real_generated_stream_handles_mixed_records_and_limits(monkeypatch):
    import os
    import select
    import subprocess
    code = evidence.program(apis(), "2026-10-06T01:00:00+00:00")
    namespace = {};exec(code.rsplit("\nprint(", 1)[0], namespace)  # noqa: S102 - execute locally generated, inspected parser code only
    rows = [{"Id": a["container_id"], "Image": a["image_id"], "State": {"Running": True, "StartedAt": a["started_at"]},
             "Config": {"Labels": {"com.docker.compose.service": "api", "com.docker.compose.project": "flash-ticketing"}}} for a in apis()]
    monkeypatch.setattr(subprocess, "check_output", lambda *a, **k: json.dumps(rows).encode())
    chunks = []; payloads = [slow(), failure_record()]
    class Process:
        def __init__(self, *a, **k):
            chunks[:] = [("".join("2026-10-06T01:00:01Z " + json.dumps(p) + "\n" for p in payloads)).encode(), b""]
            self.stdout = SimpleNamespace(fileno=lambda: 99, close=lambda: None)
        def wait(self, **k): return 0
        def poll(self): return 0
    monkeypatch.setattr(subprocess, "Popen", Process)
    monkeypatch.setattr(select, "select", lambda *a: ([1], [], []))
    monkeypatch.setattr(os, "read", lambda *a: chunks.pop(0))
    collect = namespace["remote_collect"]
    result = collect(apis(), "2026-10-06T01:00:00+00:00", record_limits={"slow_db_phase": 1, "db_acquisition_failure": 1})
    assert result["complete"] and all(len(a["records"]) == 2 for a in result["apis"])
    payloads[:] = [slow(), slow(), failure_record()]
    assert not collect(apis(), "2026-10-06T01:00:00+00:00", record_limits={"slow_db_phase": 1, "db_acquisition_failure": 1})["complete"]


def test_profile_only_control_same_sources_budgets_and_new_identity():
    p = policy.plan();engine = runner.create_runner();old = previous_runner.create_runner()
    assert engine.ARMS == ("control",) and engine.LEDGER != old.LEDGER
    c = policy.SlowDatabaseContract(p["artifact_receipt"], "control", p["expected_runtime_source_sha256"])
    engine.RefreshStages(False, {}, c, "adr0151-" + "a" * 12)
    original = previous_runner.policy.PaymentStallContract(p["artifact_receipt"], "control", p["expected_runtime_source_sha256"])
    assert all(c.settings(role) == original.settings(role) for role in c.roles)
    assert c.images == original.images and c.inventory_marker() == original.inventory_marker()
    assert "scripts/slow_database_evidence.py" in engine.identity()
    with pytest.raises(ValueError): policy.SlowDatabaseContract(p["artifact_receipt"], "candidate", p["expected_runtime_source_sha256"])
    gate = engine.stage_gates({}, {}, {}, c)
    assert not gate["confirmation"]["slow_database_evidence_complete"]


@pytest.mark.parametrize("payload", [b'{"event":"slow_db_phase",broken}', b'["slow_db_phase"]'])
def test_malformed_slow_event_marks_stream_incomplete(monkeypatch, payload):
    import os
    import select
    import subprocess

    rows = [{"Id": a["container_id"], "Image": a["image_id"], "State": {"Running": True, "StartedAt": a["started_at"]},
             "Config": {"Labels": {"com.docker.compose.service": "api", "com.docker.compose.project": "flash-ticketing"}}} for a in apis()]
    monkeypatch.setattr(subprocess, "check_output", lambda *a, **k: json.dumps(rows).encode())
    chunks = []
    class Process:
        def __init__(self, *a, **k):
            chunks[:] = [b"2026-10-06T01:00:01Z " + payload + b"\n", b""]
            self.stdout = SimpleNamespace(fileno=lambda: 99, close=lambda: None)
        def wait(self, **k): return 0
        def poll(self): return 0
    monkeypatch.setattr(subprocess, "Popen", Process)
    monkeypatch.setattr(select, "select", lambda *a: ([1], [], []))
    monkeypatch.setattr(os, "read", lambda *a: chunks.pop(0))
    result = failure.remote_collect(apis(), "2026-10-06T01:00:00+00:00", classifier=evidence.sanitize,
                                    record_limits={"slow_db_phase": 512, "db_acquisition_failure": 128})
    assert not result["complete"]


def test_context_failure_still_retains_available_allowlisted_logs(tmp_path):
    class Session:
        def call(self, *args):
            return {"complete": True, "apis": [{"container_id": a["container_id"], "complete": True,
                                                "input_bytes": 500, "records": [slow(SQL="secret")]} for a in apis()]}
    with pytest.raises(FileNotFoundError): evidence.collect(Session(), inventory(), tmp_path)
    saved = (tmp_path / "slow-database-evidence.json").read_text()
    assert not json.loads(saved)["complete"] and "secret" not in saved


def test_hook_selects_diagnostics_only_for_new_ledger_before_cleanup():
    source = Path("scripts/run_two_host_paid_comparison.py").read_text()
    final = source[source.index("            cleanup_errors = []"):]
    assert 'self.ledger_key == "bounded_slow_database_diagnostics"' in final
    assert final.index("collect_slow(session, inventory, local)") < final.index("self.stop(session, role, cid, job)")
    assert final.index("collect_slow(session, inventory, local)") < final.index('("financial", lambda:')
    assert 'record["pass"] = False' in final


def test_old_consumed_scope_does_not_authorize_new_ledger():
    old = json.loads(Path("docs/capacity/CURRENT_STATE.json").read_text())
    engine = runner.create_runner()
    binding = {"test": "exact"}
    old["cloud_load_requires_resume"] = False
    with pytest.raises(ValueError): engine.validate_release(old, binding, execute=False)


@pytest.mark.parametrize("drift", ["unused_candidate", "unknown_metadata", "wrong_control_setting"])
def test_plan_rejects_unused_or_drifted_metadata(monkeypatch, tmp_path, drift):
    data = policy.plan()
    if drift == "unused_candidate": data["arm_settings"]["candidate"] = data["arm_settings"]["control"]
    elif drift == "unknown_metadata": data["previous_failed_protocol"] = "old"
    else: data["arm_settings"]["control"]["api"]["API_PARTIAL_TIMEOUT_RECLAIM"] = "1"
    path = tmp_path / "plan.json";path.write_text(json.dumps(data));monkeypatch.setattr(policy, "PLAN", path)
    with pytest.raises(ValueError): policy.plan()

"""ADR0178 exercises the real generated collection path for both declared placements."""
import contextlib
import copy
import io
import json
import os
import select
import subprocess
import sys
from pathlib import Path
from types import SimpleNamespace

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[2] / "scripts"))
import admission_failure_evidence as failure
import api_placement_contract as placement
import application_role_rebalance_contract as scoped
import slow_database_evidence as slow
from test_admission_failure_evidence import failure as failure_record
from test_slow_database_evidence import slow as slow_record
from test_slow_database_evidence import trace


def inventory(decision, arm):
    import callback_routing_contract as routing
    import shared_callback_placement_contract as shared
    import shared_callback_rate_probe_contract as probe
    policy, cls = {"ADR0174": (placement, placement.ApiPlacementContract),
                   "ADR0177": (scoped, scoped.ApplicationRoleRebalanceContract),
                   "ADR0216": (routing, routing.CallbackRoutingContract),
                   "ADR0217": (shared, shared.SharedCallbackPlacementContract),
                   "ADR0219": (probe, probe.SharedCallbackRateProbeContract)}[decision]
    data = policy.plan()
    contract = cls(data["artifact_receipt"], arm, data["expected_runtime_source_sha256"])
    rows = []
    for host, count in contract.measured_api_counts.items():
        for _ in range(count):
            rows.append({"host_role": host, "container_id": format(len(rows) + 1, "064x"),
                         "image_id": contract.images["api"], "started_at": "2026-10-06T00:00:00.000000000Z"})
    return {"captured_at": "2026-10-06T01:00:00+00:00", "apis": rows,
            "status_refresh_contract": contract.inventory_marker()}


@pytest.mark.parametrize("decision,arm", [(d,a) for d in ("ADR0174", "ADR0177", "ADR0216", "ADR0217") for a in ("control", "candidate")] + [("ADR0219", "candidate")])
@pytest.mark.parametrize("collector", [failure, slow])
def test_real_collectors_execute_exact_generated_streams(monkeypatch, tmp_path, decision, arm, collector):
    inv = inventory(decision, arm)
    _, rows = trace(tmp_path / "pipeline.jsonl", 1)
    labels = [a["host_role"] + ":" + a["container_id"] for a in inv["apis"]]
    for row in rows:
        metric = next(iter(row["api_replicas"].values()))
        row["api_replicas"] = {label: copy.deepcopy(metric) for label in labels}
    (tmp_path / "pipeline.jsonl").write_text("\n".join(json.dumps(r) for r in rows) + "\n")
    calls, log_ids, chunks = [], [], []
    active = []

    def inspect_rows(*args, **kwargs):
        assert args[0] == ["docker", "inspect", *[a["container_id"] for a in active]]
        return json.dumps([{"Id": a["container_id"], "Image": a["image_id"],
                            "State": {"Running": True, "StartedAt": a["started_at"]},
                            "Config": {"Labels": {"com.docker.compose.service": "api",
                                                  "com.docker.compose.project": "flash-ticketing"}}}
                           for a in active]).encode()

    class Process:
        def __init__(self, args, **kwargs):
            assert args[:4] == ["docker", "logs", "--timestamps", "--since"]
            assert args[4] == inv["captured_at"]
            log_ids.append(args[5])
            event = failure_record();event["time"] = "2026-10-06T01:00:01+00:00"
            payload = [event] + ([slow_record(SQL="secret")] if collector is slow else [])
            chunks[:] = [("".join("2026-10-06T01:00:01Z " + json.dumps(p) + "\n" for p in payload)).encode(), b""]
            self.stdout = SimpleNamespace(fileno=lambda: 99, close=lambda: None)
        def wait(self, **kwargs): return 0
        def poll(self): return 0

    class Session:
        def call(self, role, code, timeout):
            assert timeout == 45
            calls.append(role)
            active[:] = [a for a in inv["apis"] if a["host_role"] == role]
            output = io.StringIO()
            with monkeypatch.context() as patch, contextlib.redirect_stdout(output):
                patch.setattr(subprocess, "check_output", inspect_rows)
                patch.setattr(subprocess, "Popen", Process)
                patch.setattr(select, "select", lambda *args: ([1], [], []))
                patch.setattr(os, "read", lambda *args: chunks.pop(0))
                exec(compile(code, "qualified-local-collector", "exec"), {})  # noqa: S102
            return json.loads(output.getvalue())

    result = collector.collect(Session(), inv, tmp_path)
    assert result["complete"] and result["counter_coverage"]
    assert calls == ["primary", "secondary"]
    assert log_ids == [a["container_id"] for a in inv["apis"]]
    assert result["failure_count" if collector is slow else "record_count"] == 4
    if collector is slow:
        assert result["slow_phase_count"] == 4 and result["failure_context_complete"]
    for path in tmp_path.glob("*-evidence.json"):
        assert "secret" not in path.read_text()


@pytest.mark.parametrize("collector", [failure, slow])
@pytest.mark.parametrize("drift", ["missing", "duplicate", "extra", "host", "unknown_decision", "unknown_arm",
                                   "wrong_counts", "boolean_count", "factor", "reclaim", "unknown_field"])
def test_allocation_drift_rejected_before_remote_call(tmp_path, collector, drift):
    inv = inventory("ADR0177", "candidate")
    marker = inv["status_refresh_contract"]
    if drift == "missing": inv["apis"].pop()
    elif drift == "extra": inv["apis"].append(copy.deepcopy(inv["apis"][-1]))
    elif drift == "duplicate": inv["apis"][-1]["container_id"] = inv["apis"][-2]["container_id"]
    elif drift == "host": inv["apis"][-1]["host_role"] = "primary"
    elif drift == "unknown_decision": marker["decision"] = "ADR9999"
    elif drift == "unknown_arm": marker["arm"] = "future"
    elif drift == "wrong_counts": marker["api_counts"] = {"primary": 2, "secondary": 2}
    elif drift == "boolean_count": marker["api_counts"]["primary"] = True
    elif drift == "factor": marker["factor"] = "unknown"
    elif drift == "reclaim": marker["partial_timeout_reclaim"] = "1"
    else: marker["extra"] = True
    class Session:
        def call(self, *args): pytest.fail("Invalid allocation reached remote collection")
    with pytest.raises((ValueError, KeyError)):
        collector.collect(Session(), inv, tmp_path)


@pytest.mark.parametrize("collector", [failure, slow])
def test_three_replica_program_requires_explicit_exact_bound(collector):
    apis = [{k: a[k] for k in ("container_id", "image_id", "started_at")}
            for a in inventory("ADR0177", "candidate")["apis"] if a["host_role"] == "secondary"]
    stamp = "2026-10-06T01:00:00+00:00"
    with pytest.raises(ValueError): collector.program(apis, stamp)
    compile(collector.program(apis, stamp, expected_api_count=3), "remote", "exec")
    for count in (True, 0, 2, 4):
        with pytest.raises(ValueError): collector.program(apis, stamp, expected_api_count=count)
    with pytest.raises(ValueError): collector.program([apis[0]] * 3, stamp, expected_api_count=3)

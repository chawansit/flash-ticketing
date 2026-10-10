"""ADR0270 topology binding must survive real CPU collection and summary."""
import hashlib
from datetime import UTC, datetime, timedelta
from pathlib import Path

import observe_two_host_cpu as cpu
import pytest


def specification():
    roles = ["consumer"] * 6 + ["reservation-writer"] * 3 + ["projection-consumer",
        "maintenance", "publisher", "reconciler", "simulator", "confirmation", "kafka",
        "pgbouncer", "load-balancer", "cce-audit", "cce-pooler"]
    entries = []
    for i, role in enumerate(roles):
        helper = role.startswith("cce-")
        entries.append({"id": f"{i+1:064x}", "role": role, "image_id": "sha256:" + "a" * 64,
            "started_at": "2026-10-10T00:00:00+00:00", "pid": i+1, "process_start_ticks": 1,
            "owner": "adr0151-123456789abc" if helper else None,
            "project": "cce-adr0151-123456789abc" if helper else "flash-ticketing"})
    return {"schema": 1, "host_role": "primary", "arm": "candidate", "placement": "cce-api-isolation",
        "decision": "ADR0228", "event_lane_decision": "ADR0266", "inventory_sha256": "b" * 64,
        "instance_uuid_sha256": hashlib.sha256(b"test-instance").hexdigest(), "containers": entries}


def data():
    spec = specification()
    start = datetime(2026, 10, 10, tzinfo=UTC)
    return {**spec, "seconds": 1, "interval": 1, "hourly": False, "requested_start_utc": start.isoformat(),
        "samples": [{"utc": (start + timedelta(seconds=i)).isoformat(), "elapsed_seconds": i,
            "host_ticks": [i*100, 0, 0, i*100, 0, 0, 0, 0],
            "container_cpu_usec": {e["id"]: i*1000 for e in spec["containers"]}} for i in range(2)]}


def test_collection_retains_preflight_event_lane_binding(monkeypatch):
    spec = specification()
    def read(path, *args, **kwargs):
        if path.as_posix() == "/sys/class/dmi/id/product_uuid":
            return "test-instance"
        if path.as_posix() == "/proc/stat":
            return "cpu 100 0 0 100 0 0 0 0"
        return "usage_usec 1000"
    monkeypatch.setattr(Path, "read_text", read)
    monkeypatch.setattr(cpu, "inspect_containers", lambda entries: {
        e["id"]: {**e, "path": Path("mock-cgroup")} for e in entries})
    monkeypatch.setattr(cpu, "proc_identity", lambda pid: 1)
    monkeypatch.setattr(cpu.time, "sleep", lambda seconds: None)
    result = cpu.collect(spec, (datetime.now(UTC)+timedelta(seconds=1)).isoformat(), seconds=1, interval=1)
    assert result["event_lane_decision"] == "ADR0266"
    assert result["inventory_sha256"] == spec["inventory_sha256"]
    assert len(result["containers"]) == 20


def test_summary_preserves_binding_and_checks_projection_role():
    result = cpu.summarize(data())
    assert result["pass"] is True
    assert result["event_lane_decision"] == "ADR0266"
    assert result["cpu_cores_by_role"]["projection-consumer"] == pytest.approx(0.001)


@pytest.mark.parametrize("binding", [None, "ADR0265"])
def test_unbound_projection_evidence_still_rejected(binding):
    value = data()
    if binding is None:
        value.pop("event_lane_decision")
    else:
        value["event_lane_decision"] = binding
    with pytest.raises(ValueError, match="Explicit event-lane CPU binding"):
        cpu.summarize(value)

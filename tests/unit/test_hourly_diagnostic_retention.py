"""ADR0257 exercise long trace bounds, missing diagnostics and independent retention."""
import json
import sys
from datetime import UTC, datetime, timedelta
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[2] / "scripts"))
import cce_paid_observers as observers
import database_wait_evidence as waits


def item(index):
    stamp = (datetime(2026, 10, 10, tzinfo=UTC) + timedelta(seconds=index)).isoformat()
    result = {"complete": True, "collection_ms": 1, "activity_utc": stamp, "statistics_utc": stamp,
                  "restricted_sessions": 0, "active": 1, "idle_in_transaction": 0, "waits": [],
                  "track_wal_io_timing": True, "track_io_timing": True}
    for group, keys in (("wal", waits.WAL_KEYS), ("checkpointer", waits.CHECKPOINT_KEYS), ("database", waits.DATABASE_KEYS)):
        result[group] = {"stats_reset": None, **dict.fromkeys(keys, index)}
    return {"database_wait_diagnostics": result}


def test_hourly_trace_accepts_more_than_short_row_limit_and_still_rejects_gaps(tmp_path):
    path = tmp_path / "long.jsonl"
    rows = [item(i) for i in range(2001)]
    path.write_text("".join(json.dumps(row) + "\n" for row in rows))
    with pytest.raises(ValueError, match="row or line"):
        waits.summarize(path)
    assert waits.summarize(path, hourly=True)["complete"] is True
    rows[-1] = item(2010)
    path.write_text("".join(json.dumps(row) + "\n" for row in rows))
    assert waits.summarize(path, hourly=True)["complete"] is False


def test_hourly_mode_does_not_waive_missing_samples(tmp_path):
    path = tmp_path / "missing.jsonl"
    path.write_text(json.dumps(item(0)) + "\n{}\n" + json.dumps(item(2)) + "\n")
    result = waits.summarize(path, hourly=True)
    assert result["complete"] is False and result["error_counts"] == {"MissingOrOverhead": 1}


@pytest.mark.parametrize("hourly", [None, 1, "hourly"])
def test_explicit_hourly_mode_required(tmp_path, hourly):
    with pytest.raises(ValueError, match="Explicit hourly"):
        waits.summarize(tmp_path / "trace", hourly=hourly)


def test_hourly_trace_keeps_byte_and_row_ceiling(tmp_path, monkeypatch):
    path = tmp_path / "large.jsonl"
    path.write_text("{}\n" * 3)
    monkeypatch.setattr(waits, "HOURLY_MAX_BYTES", 8)
    with pytest.raises(ValueError, match="byte bound"):
        waits.summarize(path, hourly=True)
    monkeypatch.setattr(waits, "HOURLY_MAX_BYTES", 32)
    monkeypatch.setattr(waits, "HOURLY_MAX_ROWS", 2)
    with pytest.raises(ValueError, match="row or line"):
        waits.summarize(path, hourly=True)


@pytest.mark.parametrize("cpu_failure", [False, True])
def test_database_summary_failure_cannot_discard_cpu_attempt(monkeypatch, cpu_failure):
    calls = []
    def database(path, *, hourly):
        assert hourly is True
        calls.append("database")
        raise ValueError("hourly evidence incomplete")
    def cpu(*args, **kwargs):
        calls.append("cpu")
        assert kwargs["hourly"] is True
        if cpu_failure:
            raise RuntimeError("missing process evidence")
        return {"aggregate_api_cpu_cores": 1.3}
    monkeypatch.setattr(observers, "api_cpu", cpu)
    record = {}
    failures = observers.summarize_independent_diagnostics(record, [], "trace", {}, "start", "end",
                                                          hourly=True, summarize_waits=database)
    assert calls == ["database", "cpu"]
    assert failures[0] == {"operation": "database_wait_capture", "type": "ValueError"}
    if cpu_failure:
        assert failures[-1] == {"operation": "native_api_cpu", "type": "RuntimeError"}
    else:
        assert record["native_api_cpu"]["aggregate_api_cpu_cores"] == 1.3

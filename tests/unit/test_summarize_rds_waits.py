import importlib.util
import json
from pathlib import Path

SCRIPT = Path(__file__).resolve().parents[2] / "scripts" / "summarize_rds_waits.py"
SPEC = importlib.util.spec_from_file_location("summarize_rds_waits", SCRIPT)
assert SPEC and SPEC.loader
module = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(module)


def test_summary_retains_only_aggregate_waits_and_wal_deltas():
    rows = [
        json.dumps({"type": "metadata", "wal_timing_enabled": False}),
        json.dumps({"type": "sample", "utc": "2026-09-20T01:42:22Z",
                    "activity": {"wait_types": {"CPU": 2, "Client": 10, "IO": 1},
                                 "wait_events": {"WALSync": 1}},
                    "wal": {"sync_ms": 100}, "query_ms": 3.5,
                    "observer_wake_lag_ms": 1, "password": "private"}),
        json.dumps({"type": "sample", "utc": "2026-09-20T01:42:22.200Z",
                    "activity": {"wait_types": {"CPU": 1, "Client": 10, "IO": 2},
                                 "wait_events": {"WALSync": 2}},
                    "wal": {"sync_ms": 175}, "query_ms": 4.0,
                    "observer_wake_lag_ms": 2}),
    ]
    result = module.summarize(rows)
    assert result["samples"] == 2
    assert result["max_interesting_waiters"] == 2
    assert result["max_wal_sync_delta_ms"] == 75
    assert result["wait_type_sample_counts"] == {"IO": 2}
    assert result["wait_event_sample_counts"] == {"WALSync": 2}
    assert result["wal_timing_enabled"] is False
    assert "private" not in json.dumps(result)
    assert result["sample_errors"] == 0

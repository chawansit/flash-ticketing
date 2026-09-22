import importlib.util
from pathlib import Path

SCRIPT = Path(__file__).resolve().parents[2] / "scripts" / "summarize_drain_trace.py"
SPEC = importlib.util.spec_from_file_location("summarize_drain_trace", SCRIPT)
assert SPEC and SPEC.loader
summary = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(summary)


def test_drain_trace_preserves_peak_and_actual_zero_time():
    rows = [
        {"utc": "2026-09-22T00:00:00+00:00", "overdue_active_holds": 0, "pending_refresh": 0},
        {"utc": "2026-09-22T00:01:00+00:00", "overdue_active_holds": 20, "pending_refresh": 3},
        {"utc": "2026-09-22T00:01:30+00:00", "error": "TimeoutError"},
        {"utc": "2026-09-22T00:02:00+00:00", "overdue_active_holds": 5, "pending_refresh": 1},
        {"utc": "2026-09-22T00:03:00+00:00", "overdue_active_holds": 0, "pending_refresh": 0},
    ]
    result = summary.summarize(rows)
    assert result["samples"] == 4
    assert result["sample_errors"] == 1
    assert result["overdue_active_holds"]["max"] == 20
    assert result["pending_refresh"]["max"] == 3
    assert result["observed_drain_after_peak_seconds"] == 120


def test_drain_trace_does_not_invent_zero_when_observation_ends_early():
    rows = [
        {"utc": "2026-09-22T00:00:00+00:00", "overdue_active_holds": 4},
        {"utc": "2026-09-22T00:01:00+00:00", "overdue_active_holds": 2},
    ]
    result = summary.summarize(rows)
    assert result["overdue_active_holds"]["last"] == 2
    assert result["observed_drain_after_peak_seconds"] is None

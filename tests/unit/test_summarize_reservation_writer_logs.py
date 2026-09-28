from __future__ import annotations

import importlib.util
from pathlib import Path

import pytest

SCRIPT = Path(__file__).resolve().parents[2] / "scripts" / "summarize_reservation_writer_logs.py"
SPEC = importlib.util.spec_from_file_location("summarize_reservation_writer_logs", SCRIPT)
assert SPEC and SPEC.loader
summary = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(summary)


def test_summarizes_bounded_failure_data_without_command_identifiers() -> None:
    result = summary.summarize(
        [
            '{"message":"startup"}',
            '{"time":"t1","event":"reservation_command_failed","error_code":"HOLD_EXPIRED","command_age_seconds":121.5,"command_id":"secret-one"}',
            '{"time":"t2","event":"reservation_command_failed","error_code":"SEAT_UNAVAILABLE","command_age_seconds":2.0,"command_id":"secret-two"}',
            '{"time":"t3","event":"reservation_command_metadata_expired","command_age_seconds":301.0,"command_id":"secret-three"}',
            '{"time":"t4","message":"worker_iteration_failed","role":"reservation-writer"}',
            "not-json",
        ],
        limit=2,
    )

    assert result["structured_log_lines"] == 5
    assert result["events"] == {
        "reservation_command_failed": 2,
        "reservation_command_metadata_expired": 1,
        "worker_iteration_failed": 1,
    }
    assert result["failure_codes"] == {
        "HOLD_EXPIRED": 1,
        "METADATA_EXPIRED": 1,
        "SEAT_UNAVAILABLE": 1,
    }
    assert result["failed_command_age"]["count"] == 3
    assert result["failed_command_age"]["avg_seconds"] == pytest.approx(141.5)
    assert result["failed_command_age"]["p95_seconds"] == 301.0
    assert len(result["last_failures"]) == 2
    assert "command_id" not in str(result)


def test_empty_logs_are_a_valid_zero_failure_summary() -> None:
    result = summary.summarize([])

    assert result["structured_log_lines"] == 0
    assert result["events"] == {}
    assert result["failure_codes"] == {}
    assert result["failed_command_age"] == {
        "count": 0,
        "avg_seconds": None,
        "p95_seconds": None,
        "max_seconds": None,
    }
    assert result["last_failures"] == []

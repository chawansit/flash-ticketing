from __future__ import annotations

import importlib.util
from pathlib import Path

import pytest

SCRIPT = Path(__file__).resolve().parents[2] / "scripts" / "summarize_reservation_writer_metrics.py"
SPEC = importlib.util.spec_from_file_location("summarize_reservation_writer_metrics", SCRIPT)
assert SPEC and SPEC.loader
summary = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(summary)


def test_aggregates_two_writer_snapshots_and_histograms() -> None:
    lines = [
        'ticketing_reservation_persistence_total{outcome="durable"} 10',
        'ticketing_reservation_persistence_failures_total{code="HOLD_EXPIRED"} 2',
        "ticketing_reservation_command_age_seconds_count 12",
        "ticketing_reservation_command_age_seconds_sum 24",
        'ticketing_reservation_command_age_seconds_bucket{le="1"} 5',
        'ticketing_reservation_command_age_seconds_bucket{le="5"} 12',
        'ticketing_reservation_command_age_seconds_bucket{le="+Inf"} 12',
        'ticketing_reservation_persistence_phase_seconds_count{outcome="ok",phase="postgres"} 10',
        'ticketing_reservation_persistence_phase_seconds_sum{outcome="ok",phase="postgres"} 0.5',
        'ticketing_reservation_persistence_phase_seconds_bucket{le="0.1",outcome="ok",phase="postgres"} 10',
        'ticketing_reservation_persistence_phase_seconds_bucket{le="+Inf",outcome="ok",phase="postgres"} 10',
        'ticketing_reservation_persistence_total{outcome="durable"} 7',
        'ticketing_reservation_persistence_failures_total{code="HOLD_EXPIRED"} 3',
        "ticketing_reservation_command_age_seconds_count 8",
        "ticketing_reservation_command_age_seconds_sum 8",
        'ticketing_reservation_command_age_seconds_bucket{le="1"} 8',
        'ticketing_reservation_command_age_seconds_bucket{le="5"} 8',
        'ticketing_reservation_command_age_seconds_bucket{le="+Inf"} 8',
    ]

    result = summary.summarize(summary.parse_metrics(lines))

    assert result["persistence_outcomes"] == {"durable": 17}
    assert result["failure_codes"] == {"HOLD_EXPIRED": 5}
    assert result["command_age"]["count"] == 20
    assert result["command_age"]["avg_seconds"] == pytest.approx(1.6)
    assert result["command_age"]["p95_upper_seconds"] == 5
    assert result["phases"]["postgres:ok"]["count"] == 10
    assert result["phases"]["postgres:ok"]["avg_ms"] == pytest.approx(50)
    assert result["phases"]["postgres:ok"]["p95_upper_ms"] == 100


def test_empty_histogram_is_explicit() -> None:
    result = summary.histogram({}, "missing")

    assert result == {"count": 0.0, "avg": None, "p95_upper": None, "p99_upper": None}

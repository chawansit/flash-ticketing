from __future__ import annotations

import importlib.util
import math
from pathlib import Path

import pytest

spec = importlib.util.spec_from_file_location(
    "cloud_pressure_observe", Path(__file__).resolve().parents[2] / "scripts/cloud_pressure_observe.py"
)
assert spec and spec.loader
observer = importlib.util.module_from_spec(spec)
spec.loader.exec_module(observer)


def test_parse_metrics_keeps_only_pressure_series() -> None:
    parsed = observer.parse_metrics(
        "# HELP ignored ignored\n"
        "ticketing_hold_inflight 3\n"
        'ticketing_hold_admission_total{outcome="rejected"} 2\n'
        'ticketing_db_pool_state{state="requests_waiting"} 1\n'
        'ticketing_db_pool_acquire_seconds_count{outcome="ok"} 40\n'
        "ticketing_db_connection_hold_seconds_sum 1.25\n"
        'ticketing_event_loop_lag_seconds_bucket{le="0.01"} 9\n'
        "python_gc_objects_collected_total 99\n"
        "ticketing_hold_limit NaN\n"
    )

    assert parsed["ticketing_hold_inflight"] == 3.0
    assert parsed['ticketing_hold_admission_total{outcome="rejected"}'] == 2.0
    assert parsed['ticketing_db_pool_state{state="requests_waiting"}'] == 1.0
    assert parsed['ticketing_db_pool_acquire_seconds_count{outcome="ok"}'] == 40.0
    assert parsed["ticketing_db_connection_hold_seconds_sum"] == 1.25
    assert parsed['ticketing_event_loop_lag_seconds_bucket{le="0.01"}'] == 9.0
    assert math.isnan(parsed["ticketing_hold_limit"])
    assert "python_gc_objects_collected_total" not in parsed


def test_metric_value_handles_labels_and_missing_series() -> None:
    metrics = {'ticketing_hold_admission_total{outcome="admitted"}': 12.0}

    assert observer.metric_value(
        metrics, "ticketing_hold_admission_total", '{outcome="admitted"}'
    ) == 12.0
    assert observer.metric_value(metrics, "ticketing_hold_inflight") == 0.0


def test_percentile_uses_nearest_rank() -> None:
    assert observer.percentile([4.0, 1.0, 3.0, 2.0], 0.95) == 4.0
    assert observer.percentile([], 0.95) is None


def test_histogram_window_reports_window_average_and_upper_bounds() -> None:
    start = {
        "metric_count": 10.0,
        "metric_sum": 0.1,
        'metric_bucket{le="0.01"}': 8.0,
        'metric_bucket{le="0.05"}': 10.0,
        'metric_bucket{le="+Inf"}': 10.0,
    }
    end = {
        "metric_count": 20.0,
        "metric_sum": 0.3,
        'metric_bucket{le="0.01"}': 16.0,
        'metric_bucket{le="0.05"}': 20.0,
        'metric_bucket{le="+Inf"}': 20.0,
    }

    result = observer.histogram_window(start, end, "metric")

    assert result["count"] == 10.0
    assert result["avg_ms"] == pytest.approx(20.0)
    assert result["p95_upper_ms"] == 50.0
    assert result["p99_upper_ms"] == 50.0


def test_histogram_window_filters_labeled_series() -> None:
    start = {
        'metric_count{outcome="ok"}': 5,
        'metric_sum{outcome="ok"}': 0.05,
        'metric_bucket{le="0.01",outcome="ok"}': 4,
        'metric_bucket{le="+Inf",outcome="ok"}': 5,
        'metric_bucket{le="0.01",outcome="error"}': 9,
    }
    end = {
        'metric_count{outcome="ok"}': 10,
        'metric_sum{outcome="ok"}': 0.15,
        'metric_bucket{le="0.01",outcome="ok"}': 8,
        'metric_bucket{le="+Inf",outcome="ok"}': 10,
        'metric_bucket{le="0.01",outcome="error"}': 99,
    }

    result = observer.histogram_window(
        start, end, "metric", '{outcome="ok"}', {"outcome": "ok"}
    )

    assert result["count"] == 5
    assert result["avg_ms"] == pytest.approx(20)
    assert math.isinf(result["p95_upper_ms"])

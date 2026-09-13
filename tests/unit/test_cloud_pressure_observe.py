from __future__ import annotations

import importlib.util
import math
from pathlib import Path

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
        "python_gc_objects_collected_total 99\n"
        "ticketing_hold_limit NaN\n"
    )

    assert parsed["ticketing_hold_inflight"] == 3.0
    assert parsed['ticketing_hold_admission_total{outcome="rejected"}'] == 2.0
    assert parsed['ticketing_db_pool_state{state="requests_waiting"}'] == 1.0
    assert parsed['ticketing_db_pool_acquire_seconds_count{outcome="ok"}'] == 40.0
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

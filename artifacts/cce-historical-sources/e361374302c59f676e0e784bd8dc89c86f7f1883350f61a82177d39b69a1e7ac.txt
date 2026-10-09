import importlib.util
from pathlib import Path


def load_script(name):
    path = Path(__file__).resolve().parents[2] / "scripts" / name
    spec = importlib.util.spec_from_file_location(name.removesuffix(".py"), path)
    assert spec and spec.loader
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


lag = load_script("kafka_lag_observe.py")
summary = load_script("summarize_refresh_pipeline.py")


def test_parse_describe_sums_partition_lag_and_members():
    output = """
GROUP TOPIC PARTITION CURRENT-OFFSET LOG-END-OFFSET LAG CONSUMER-ID HOST CLIENT-ID
g ticketing.events 0 100 110 10 member-a /host client
g ticketing.events 1 205 210 5 member-b /host client
"""
    result = lag.parse_describe(output)
    assert result["total_lag"] == 15
    assert result["max_partition_lag"] == 10
    assert result["assigned_partitions"] == 2
    assert result["members"] == 2


def test_refresh_pipeline_reports_rates_and_failed_samples():
    backend = [
        {
            "utc": "2026-09-22T00:00:00+00:00",
            "refresh_generation_total": 100,
            "refresh_completed_generation_total": 90,
            "outbox_rows_inserted": 1000,
            "consumer_inbox_rows_inserted": 900,
            "pending_refresh": 10,
            "oldest_refresh_seconds": 2,
        },
        {"utc": "2026-09-22T00:00:01+00:00", "error": "TimeoutError"},
        {
            "utc": "2026-09-22T00:00:10+00:00",
            "refresh_generation_total": 150,
            "refresh_completed_generation_total": 140,
            "outbox_rows_inserted": 1060,
            "consumer_inbox_rows_inserted": 950,
            "pending_refresh": 4,
            "oldest_refresh_seconds": 3,
        },
    ]
    kafka = [
        {
            "utc": "2026-09-22T00:00:00+00:00",
            "total_lag": 2,
            "max_partition_lag": 2,
            "members": 1,
        },
        {"utc": "2026-09-22T00:00:01+00:00", "error_type": "TimeoutExpired"},
        {
            "utc": "2026-09-22T00:00:10+00:00",
            "total_lag": 8,
            "max_partition_lag": 5,
            "members": 1,
        },
        {
            "utc": "2026-09-22T00:00:20+00:00",
            "total_lag": 0,
            "max_partition_lag": 0,
            "members": 1,
        },
    ]
    result = summary.summarize(backend, kafka)
    assert result["backend_sample_errors"] == 1
    assert result["kafka_sample_errors"] == 1
    assert result["kafka_total_lag"]["max"] == 8
    assert result["kafka_drain_after_peak_seconds"] == 10
    assert result["refresh_generated"] == {"delta": 50, "per_second": 5.0}
    assert result["refresh_completed"] == {"delta": 50, "per_second": 5.0}
    assert result["pending_refresh_last"] == 4
    assert result["oldest_refresh_seconds_max"] == 3

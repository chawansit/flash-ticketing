"""Kafka summary identifies lag versus group membership without raw IDs."""

from scripts.summarize_paid_kafka_lag import summarize


def test_paid_kafka_lag_summary_tracks_peak_drain_and_partition_skew():
    rows = [
        {
            "utc": "2026-09-29T00:00:00+00:00",
            "total_lag": 0,
            "max_partition_lag": 0,
            "members": 2,
            "partitions": [{"partition": 0, "lag": 0}, {"partition": 1, "lag": 0}],
        },
        {
            "utc": "2026-09-29T00:00:02+00:00",
            "total_lag": 9,
            "max_partition_lag": 8,
            "members": 2,
            "partitions": [{"partition": 0, "lag": 8}, {"partition": 1, "lag": 1}],
        },
        {"utc": "2026-09-29T00:00:03+00:00", "error_type": "TimeoutExpired"},
        {
            "utc": "2026-09-29T00:00:06+00:00",
            "total_lag": 0,
            "max_partition_lag": 0,
            "members": 2,
            "partitions": [{"partition": 0, "lag": 0}, {"partition": 1, "lag": 0}],
        },
    ]
    result = summarize(rows)
    assert result["max_total_lag"] == 9
    assert result["max_partition_lag"] == 8
    assert result["partition_lag_peaks"] == {"0": 8, "1": 1}
    assert result["drain_after_peak_seconds"] == 4
    assert result["sample_errors"] == 1


def test_idle_group_has_zero_drain_time():
    rows = [
        {
            "utc": "2026-09-29T00:00:00+00:00",
            "total_lag": 0,
            "max_partition_lag": 0,
            "members": 1,
            "partitions": [{"partition": 0, "lag": 0}],
        }
    ]
    assert summarize(rows)["drain_after_peak_seconds"] == 0

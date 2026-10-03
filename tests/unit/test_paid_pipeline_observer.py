"""Paid-stage diagnostics preserve fixed metric labels and aggregate replicas."""

from io import BytesIO

from scripts import observe_paid_pipeline, summarize_paid_pipeline


def test_api_metrics_classifies_checkout_503_and_pool_pressure(monkeypatch):
    payload = b"""# HELP anything
ticketing_http_requests_total{route="/v1/orders/{order_id}",method="GET",status="503"} 3
ticketing_http_requests_total{route="/health/ready",method="GET",status="503"} 10
ticketing_http_requests_total{route="/v1/payments/simulate",method="POST",status="503"} 2
ticketing_db_unavailable_total{cause="PoolTimeout"} 4
ticketing_db_pool_acquire_seconds_count{outcome="timeout"} 7
ticketing_db_pool_acquire_seconds_sum{outcome="timeout"} 3.5
ticketing_db_commit_seconds_count 20
ticketing_db_commit_seconds_sum 0.2
ticketing_event_loop_lag_current_seconds 0.015
ticketing_db_pool_state{state="requests_waiting"} 5
ticketing_db_pool_in_use 12
ticketing_order_status_cache_total{outcome="hit"} 5
ticketing_order_status_cache_total{outcome="redis_error"} 2
"""
    monkeypatch.setattr(observe_paid_pipeline, "urlopen", lambda *_args, **_kwargs: BytesIO(payload))
    result = observe_paid_pipeline.api_metrics("127.0.0.1")
    assert result["http_503:/v1/orders/{order_id}:GET"] == 3
    assert result["http_503:/v1/payments/simulate:POST"] == 2
    assert "http_503:/health/ready:GET" not in result
    assert result["db_503:PoolTimeout"] == 4
    assert result["pool_acquire:timeout"] == 7
    assert result["duration:db_pool_acquire:timeout:count"] == 7
    assert result["duration:db_pool_acquire:timeout:sum"] == 3.5
    assert result["duration:db_commit:all:count"] == 20
    assert result["event_loop_lag_current"] == 0.015
    assert result["pool_state:requests_waiting"] == 5
    assert result["pool_in_use"] == 12
    assert result["order_cache:hit"] == 5
    assert result["order_cache:redis_error"] == 2


def test_summary_aggregates_counter_deltas_and_per_replica_peaks():
    rows = [
        {
            "api_replicas": {
                "a": {"db_503:PoolTimeout": 2, "pool_in_use": 4,
                      "duration:db_commit:all:count": 10, "duration:db_commit:all:sum": 0.1,
                      "event_loop_lag_current": 0.01},
                "b": {"db_503:PoolTimeout": 5, "pool_in_use": 6,
                      "duration:db_commit:all:count": 10, "duration:db_commit:all:sum": 0.1},
            }
        },
        {
            "api_replicas": {
                "a": {"db_503:PoolTimeout": 4, "pool_in_use": 7,
                      "duration:db_commit:all:count": 20, "duration:db_commit:all:sum": 0.3,
                      "event_loop_lag_current": 0.02},
                "b": {"db_503:PoolTimeout": 8, "pool_in_use": 3,
                      "duration:db_commit:all:count": 30, "duration:db_commit:all:sum": 0.5},
            }
        },
    ]
    api = summarize_paid_pipeline.summarize(rows)["api"]
    assert api["observed_replicas"] == 2
    assert api["counter_deltas"]["db_503:PoolTimeout"] == 5
    assert api["pool_peaks_per_replica"]["pool_in_use"] == 7
    assert round(api["duration_mean_ms"]["db_commit:all"], 3) == 20.0
    assert api["event_loop_lag_current_peak_ms"] == 20.0
    assert api["counter_reset_detected"] is False


def test_worker_counters_sum_both_consumer_replicas(monkeypatch):
    monkeypatch.setattr(observe_paid_pipeline, "api_replicas", lambda host, port: ["replica-a", "replica-b"])

    def open_metric(url, timeout):
        count = 10 if "replica-a" in url else 12
        return BytesIO(
            (
                'ticketing_worker_operations_total{operation="consume_event",outcome="ok"} '
                f"{count}\n"
                'ticketing_worker_busy_seconds_total{operation="consume_event"} '
                f"{count / 2}\n"
                'ticketing_worker_active{operation="consume_event"} 1\n'
            ).encode()
        )

    monkeypatch.setattr(observe_paid_pipeline, "urlopen", open_metric)
    result = observe_paid_pipeline.worker_counters(
        "consumer", "consume_event", "http://consumer:9101/metrics"
    )
    assert result == {
        "consumer_replicas": 2,
        "consumer_calls": 22,
        "consumer_busy_seconds": 11,
        "consumer_active": 2,
    }

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
                "a": {"db_503:PoolTimeout": 2, "pool_in_use": 4, "order_cache:hit": 3,
                      "duration:db_commit:all:count": 10, "duration:db_commit:all:sum": 0.1,
                      "event_loop_lag_current": 0.01},
                "b": {"db_503:PoolTimeout": 5, "pool_in_use": 6,
                      "duration:db_commit:all:count": 10, "duration:db_commit:all:sum": 0.1},
            }
        },
        {
            "api_replicas": {
                "a": {"db_503:PoolTimeout": 4, "pool_in_use": 7, "order_cache:hit": 8,
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
    assert api["counter_deltas"]["order_cache:hit"] == 5
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


def test_consumer_phases_aggregate_replicas_and_preserve_histogram_bounds(monkeypatch):
    monkeypatch.setattr(observe_paid_pipeline, "api_replicas", lambda *_: ["a", "b"])
    labels = 'phase="commit",partition="all",outcome="ok"'
    metric = "\n".join([
        f'ticketing_consumer_phase_seconds_count{{{labels}}} 10',
        f'ticketing_consumer_phase_seconds_sum{{{labels}}} 0.5',
        f'ticketing_consumer_phase_seconds_bucket{{{labels},le="0.05"}} 9',
        f'ticketing_consumer_phase_seconds_bucket{{{labels},le="0.1"}} 10',
        f'ticketing_consumer_phase_seconds_bucket{{{labels},le="+Inf"}} 10',
    ]).encode()
    monkeypatch.setattr(observe_paid_pipeline, "urlopen", lambda *_args, **_kwargs: BytesIO(metric))
    counters = observe_paid_pipeline.worker_counters("consumer", "consume_event", "http://consumer:9101/metrics")
    assert counters["consumer_phases"]["commit:all:ok:count"] == 20
    result = summarize_paid_pipeline.summarize_consumer_phases([
        {"consumer_phases": {}}, counters])
    assert result["phases"]["commit:all:ok"] == {
        "calls": 20, "total_seconds": 1, "mean_ms": 50, "p95_upper_bound_ms": 100}
    assert not result["counter_reset_detected"]


def test_phase_summary_does_not_hide_counter_reset():
    result = summarize_paid_pipeline.summarize_consumer_phases([
        {"consumer_phases": {"poll:all:ok:count": 10, "poll:all:ok:sum": 2}},
        {"consumer_phases": {"poll:all:ok:count": 2, "poll:all:ok:sum": .5}},
    ])
    assert result["counter_reset_detected"] and result["phases"] == {}
    assert not summarize_paid_pipeline.summarize_consumer_phases([])["observed"]


def test_mid_window_reset_is_reported_even_when_final_counter_recovers():
    result = summarize_paid_pipeline.summarize_consumer_phases([
        {"consumer_phases": {"commit:all:ok:count": count, "commit:all:ok:sum": count / 10}}
        for count in (10, 2, 20)
    ])
    assert result["counter_reset_detected"] and not result["phases"]

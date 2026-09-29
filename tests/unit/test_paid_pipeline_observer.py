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
ticketing_db_pool_state{state="requests_waiting"} 5
ticketing_db_pool_in_use 12
"""
    monkeypatch.setattr(observe_paid_pipeline, "urlopen", lambda *_args, **_kwargs: BytesIO(payload))
    result = observe_paid_pipeline.api_metrics("127.0.0.1")
    assert result["http_503:/v1/orders/{order_id}:GET"] == 3
    assert result["http_503:/v1/payments/simulate:POST"] == 2
    assert "http_503:/health/ready:GET" not in result
    assert result["db_503:PoolTimeout"] == 4
    assert result["pool_acquire:timeout"] == 7
    assert result["pool_state:requests_waiting"] == 5
    assert result["pool_in_use"] == 12


def test_summary_aggregates_counter_deltas_and_per_replica_peaks():
    rows = [
        {
            "api_replicas": {
                "a": {"db_503:PoolTimeout": 2, "pool_in_use": 4},
                "b": {"db_503:PoolTimeout": 5, "pool_in_use": 6},
            }
        },
        {
            "api_replicas": {
                "a": {"db_503:PoolTimeout": 4, "pool_in_use": 7},
                "b": {"db_503:PoolTimeout": 8, "pool_in_use": 3},
            }
        },
    ]
    api = summarize_paid_pipeline.summarize(rows)["api"]
    assert api["observed_replicas"] == 2
    assert api["counter_deltas"]["db_503:PoolTimeout"] == 5
    assert api["pool_peaks_per_replica"]["pool_in_use"] == 7
    assert api["counter_reset_detected"] is False

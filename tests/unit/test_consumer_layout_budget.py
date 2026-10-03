import json
from types import SimpleNamespace

from scripts import (
    kafka_lag_observe,
    observe_paid_pipeline,
    summarize_paid_kafka_lag,
    summarize_paid_pipeline,
)
from scripts.verify_consumer_pool_budget import budget_view
from ticketing import observability, workers


def test_six_member_ownership_is_credential_free_and_one_partition_each():
    header = "GROUP TOPIC PARTITION CURRENT-OFFSET LOG-END-OFFSET LAG CONSUMER-ID HOST CLIENT-ID"
    lines = [f"g ticketing.events {i} 10 11 1 private-member-{i} /private-host private-client" for i in range(6)]
    row = kafka_lag_observe.parse_describe(header + "\n" + "\n".join(lines))
    assert row["members"] == 6 and len(row["member_partitions"]) == 6
    assert all(len(parts) == 1 for parts in row["member_partitions"].values())
    assert "private-member" not in json.dumps(row) and "private-host" not in json.dumps(row)
    row["utc"] = "2026-10-03T00:00:00+00:00"
    result = summarize_paid_kafka_lag.summarize([row])
    assert result["ownership_observed"] and result["max_partitions_per_member"] == 1
    assert result["last_member_partition_groups"] == [[i] for i in range(6)]


def test_budget_verifier_preserves_aggregate_ceiling_and_redacts_secrets():
    pooler = {"DEFAULT_POOL_SIZE": "24", "RESERVE_POOL_SIZE": "0", "MAX_CLIENT_CONN": "160",
              "POOL_MODE": "transaction", "DB_PASSWORD": "must-not-emit"}
    result = budget_view(pooler, [{"DB_POOL_MAX": "8", "DATABASE_URL": "secret"}] * 6, 6, 8)
    assert result["pass"] and result["aggregate_consumer_pool_ceiling"] == 48
    assert "must-not-emit" not in json.dumps(result) and "secret" not in json.dumps(result)
    assert not budget_view({**pooler, "DEFAULT_POOL_SIZE": "25"}, [{"DB_POOL_MAX": "8"}] * 6, 6, 8)["pass"]
    assert not budget_view(pooler, [{"DB_POOL_MAX": "12"}] * 6, 6, 8)["pass"]
    assert not budget_view(pooler, [{"DB_POOL_MAX": "8"}] * 5, 6, 8)["pass"]


def test_retry_sqlstate_measurement_preserves_existing_backoff_and_recovery(monkeypatch):
    class Deadlock(RuntimeError):
        sqlstate = "40P01"

    calls, waits = [], []

    def handle(*args):
        calls.append(True)
        if len(calls) == 1:
            raise Deadlock("private database text")

    metric = observability.CONSUMER_BATCH_FAILURES.labels("40P01")
    before = metric._value.get()
    monkeypatch.setattr(workers, "consume_events", handle)
    monkeypatch.setattr(workers.time, "sleep", waits.append)
    workers.consume_kafka_messages(None, None, [SimpleNamespace(value=b"{}")])
    assert len(calls) == 2 and waits == [.2]
    assert metric._value.get() == before + 1


def test_first_failed_attempt_is_not_lost_by_observer_summary(monkeypatch):
    from io import BytesIO
    monkeypatch.setattr(observe_paid_pipeline, "api_replicas", lambda *_: ["a"])
    payload = b'ticketing_consumer_batch_failures_total{sqlstate="40P01"} 1'
    monkeypatch.setattr(observe_paid_pipeline, "urlopen", lambda *_args, **_kwargs: BytesIO(payload))
    row = observe_paid_pipeline.worker_counters("consumer", "consume_event", "http://consumer:9101/metrics")
    assert row["consumer_batch_failures"] == {"40P01": 1}
    result = summarize_paid_pipeline.summarize([{"consumer_batch_failures": {}}, row])
    assert result["consumer_batch_failure_counts"] == {"40P01": 1}

import json
import sys

import pytest

from scripts import observe_paid_pipeline as observer


def test_bounded_duration_contract_covers_five_minutes_and_launch_grace():
    assert observer.paid_observer_seconds(300) == observer.MAX_PAID_OBSERVER_SECONDS == 480
    assert observer.paid_observer_seconds(60) == 240
    for value in [0, 301]:
        with pytest.raises(ValueError):
            observer.paid_observer_seconds(value)


def test_pipeline_first_sample_rejects_missing_errors_and_wrong_topology():
    row = {"utc": "2026-10-03T00:00:00+00:00", "issued_tickets": 0,
           "api_replicas": {f"private-host-{i}": {"secret": "private-text"} for i in range(4)},
           "consumer_replicas": 6, "simulator_replicas": 1, "publisher_replicas": 1}
    result = observer.pipeline_startup_view(row, 6)
    assert result["pass"] and "private" not in json.dumps(result)
    assert not observer.pipeline_startup_view({}, 6)["pass"]
    assert not observer.pipeline_startup_view(row, 4)["pass"]
    assert not observer.pipeline_startup_view({**row, "database_error": "private-error"}, 6)["pass"]
    assert not observer.pipeline_startup_view({**row, "api_replicas": {}}, 6)["pass"]
    assert not observer.pipeline_startup_view({**row, "simulator_replicas": 0}, 6)["pass"]


def test_kafka_first_sample_requires_members_and_valid_lag():
    row = {"utc": "2026-10-03T00:00:00+00:00", "members": 6, "total_lag": 0}
    assert observer.kafka_startup_view(row, 6)["pass"]
    assert not observer.kafka_startup_view({}, 6)["pass"]
    assert not observer.kafka_startup_view(row, 4)["pass"]
    result = observer.kafka_startup_view({**row, "error_type": "private-exception"}, 6)
    assert not result["pass"] and "private" not in json.dumps(result)


def test_five_minute_cli_duration_is_accepted_before_database_access(monkeypatch, tmp_path):
    fixture = tmp_path / "fixture.json"
    fixture.write_text(json.dumps({"environment": "development", "show_ids": ["isolated"]}))
    args = ["observer", "--manifest", str(fixture), "--output", str(tmp_path / "fresh.jsonl"), "--seconds"]
    monkeypatch.delenv("TEST_DATABASE_URL", raising=False)
    for seconds in [420, 480]:
        monkeypatch.setattr(sys, "argv", args + [str(seconds)])
        with pytest.raises(RuntimeError, match="TEST_DATABASE_URL required"):
            observer.main()
    monkeypatch.setattr(sys, "argv", args + ["481"])
    with pytest.raises(SystemExit) as rejected:
        observer.main()
    assert rejected.value.code == 2

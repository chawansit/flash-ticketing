"""Bulk fixture delay must precede fresh admission, with all stop gates retained."""
import pytest
from cce_paid_profiles import HOURLY
from test_cce_paid_lifecycle import area  # noqa: F401 - reuse the composed lifecycle fixture.


@pytest.fixture
def hourly_area(area):  # noqa: F811
    value, calls, failures, saved = area
    value.stage.profile = HOURLY
    value.stage.record["hourly_issuance"] = {"pass": True}
    return value, calls, failures, saved


def test_slow_fixture_does_not_age_fresh_transition_inventory(hourly_area, monkeypatch):
    value, calls, _, _ = hourly_area
    now = [100.0]
    captured = []
    monkeypatch.setattr("cce_paid_lifecycle.time.monotonic", lambda: now[0])

    def fixture():
        calls.append("fixture")
        now[0] += 400  # Exceeds the unchanged300-second admission freshness gate.
        return {}, {}

    def refresh():
        calls.append("refresh")
        captured.append(now[0])
        return {"captured_at": now[0], "sha256": "fresh"}

    original_safety = value.qualify_safety
    def safety(*args):
        now[0] += 130
        return original_safety(*args)

    original_start = value.observers.start
    def start(*args):
        assert 0 <= now[0] - captured[0] <= 300
        return original_start(*args)

    value.prepare_fixture = fixture
    value.refresh_inventory = refresh
    value.qualify_safety = safety
    value.observers.start = start
    result = value.run(None, None, "audit")
    assert result["pass"]
    assert result["paid_fixture_preparation_seconds"] == 400
    assert calls.index("fixture") < calls.index("refresh") < calls.index("activate")
    assert calls.index("safety") < calls.index("prepare") < calls.index("observers") < calls.index("dispatch")


def test_fixture_failure_blocks_admission_and_dispatch_and_runs_cleanup(hourly_area):
    value, calls, failures, _ = hourly_area
    failures.add("fixture")
    value.refresh_inventory = lambda: pytest.fail("Failed fixture must not reach fresh admission")
    with pytest.raises(ValueError, match="fixture"):
        value.run(None, None, "audit")
    assert "activate" not in calls and "safety" not in calls and "dispatch" not in calls
    assert all(name in calls for name in ("stop", "safety_cleanup", "restore",
                                        "stage_cleanup", "observer_cleanup", "namespace_cleanup", "resource_cleanup"))
    assert value.record["cleanup_complete"]


def test_failed_fresh_admission_never_activates_prepared_fixture(hourly_area):
    value, calls, _, _ = hourly_area
    def refresh():
        raise ValueError("stale admission")
    value.refresh_inventory = refresh
    with pytest.raises(ValueError, match="stale"):
        value.run(None, None, "audit")
    assert "fixture" in calls and "activate" not in calls and "dispatch" not in calls
    assert value.record["paid_fixture_prepared_before_admission"]
    assert value.record["cleanup_complete"]

def test_short_fixture_is_fresh_after_mandatory_expiry_wait(area, monkeypatch):  # noqa: F811
    from datetime import UTC, datetime, timedelta
    from uuid import uuid4

    from fixture_identity_evidence import fixture_identity

    value, calls, _, _ = area
    now = [100.0]
    monkeypatch.setattr("cce_paid_lifecycle.time.monotonic", lambda: now[0])
    original_safety = value.qualify_safety
    def safety(*args):
        now[0] += 130
        return original_safety(*args)

    def fixture():
        calls.append("fixture")
        now[0] += 5
        created = datetime.fromtimestamp(now[0], UTC)
        return {"schema_version": 1, "environment": "development", "fixture_layout": "distributed",
                "fixture_id": str(uuid4()), "shows": 84, "seats_per_show": 300,
                "show_ids": [str(uuid4()) for _ in range(84)], "created_at": created.isoformat(),
                "sale_ends": (created + timedelta(hours=1)).isoformat()}, {}

    def prepare(fixture, manifest):
        calls.append("prepare")
        fixture_identity(fixture, 84, now=datetime.fromtimestamp(now[0], UTC))
        with pytest.raises(ValueError, match="Fresh open fixture"):
            fixture_identity(fixture, 84, now=datetime.fromtimestamp(now[0] + 121, UTC))

    value.qualify_safety, value.prepare_fixture, value.stage.prepare = safety, fixture, prepare
    result = value.run(None, None, "audit")
    assert result["pass"] and not result["paid_fixture_prepared_before_admission"]
    assert calls.index("safety") < calls.index("fixture") < calls.index("prepare") < calls.index("dispatch")
    assert calls.count("fixture") == 1

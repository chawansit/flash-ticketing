"""ADR0294 early collector preparation must preserve all dispatch gates."""
from types import SimpleNamespace

import cce_paid_observers as observers
import pytest
from cce_paid_profiles import HOURLY
from test_cce_paid_lifecycle import area  # noqa: F401 - reuse composed gate fixture.


@pytest.fixture
def early_area(area):  # noqa: F811
    value, calls, failures, saved = area
    value.stage.profile = HOURLY
    value.stage.record["hourly_issuance"] = {"pass": True}
    value.early_hourly_observers = True
    value.observers.ensure_offered_coverage = lambda start: calls.append("coverage")
    return value, calls, failures, saved


def test_expiry_wait_cannot_age_inventory_before_observer_admission(early_area, monkeypatch):
    value, calls, _, _ = early_area
    now = [100.0]
    captured = []
    monkeypatch.setattr("cce_paid_lifecycle.time.monotonic", lambda: now[0])
    value.refresh_inventory = lambda: captured.append(now[0]) or {"fresh": True}
    activate = value.transition.activate
    def slow_activate(*args):
        now[0] += 190
        return activate(*args)
    value.transition.activate = slow_activate
    start = value.observers.start
    def fresh_start(*args):
        assert 0 <= now[0] - captured[0] <= 300
        return start(*args)
    value.observers.start = fresh_start
    safety = value.qualify_safety
    def expiry(*args):
        now[0] += 130
        return safety(*args)
    value.qualify_safety = expiry
    result = value.run(None, None, "audit")
    assert result["pass"]
    assert calls.index("fixture") < calls.index("prepare") < calls.index("observers")
    assert calls.index("observers") < calls.index("safety") < calls.index("coverage") < calls.index("dispatch")
    assert calls.count("prepare") == calls.count("observers") == 1


@pytest.mark.parametrize("failure", ["observers", "safety"])
def test_early_collection_never_bypasses_failed_safety(early_area, failure):
    value, calls, failures, _ = early_area
    failures.add(failure)
    with pytest.raises(ValueError, match=failure):
        value.run(None, None, "audit")
    assert "dispatch" not in calls
    assert value.record["cleanup_complete"]


def test_exhausted_collector_lifetime_blocks_dispatch(early_area):
    value, calls, _, _ = early_area
    def reject(start):
        raise ValueError("coverage exhausted")
    value.observers.ensure_offered_coverage = reject
    with pytest.raises(ValueError, match="coverage exhausted"):
        value.run(None, None, "audit")
    assert "dispatch" not in calls and value.record["cleanup_complete"]


@pytest.mark.parametrize("elapsed,delay,expected", [(180,30,True), (568,30,True), (569,30,False), (10,-1,False)])
def test_exact_remaining_hour_and_two_second_tail_required(monkeypatch, elapsed, delay, expected):
    value = observers.Observers.__new__(observers.Observers)
    value.stage = SimpleNamespace(profile=HOURLY)
    value.hourly_early_bootstrap = value.started = True
    value.collector_started_monotonic = 100
    value.record = {}
    value.checkpoint = lambda: None
    monkeypatch.setattr(observers.time, "monotonic", lambda: 100+elapsed)
    monkeypatch.setattr(observers.time, "time", lambda: 1000)
    if expected:
        assert value.ensure_offered_coverage(1000+delay)
    else:
        with pytest.raises(ValueError, match="complete offered hour"):
            value.ensure_offered_coverage(1000+delay)
    assert value.record["early_observer_coverage"]["pass"] is expected


def test_sampler_accepts_only_explicit_bounded_hourly_lifetimes():
    import observe_cce_paid_pipeline as sampling
    module = SimpleNamespace(sample=lambda conn, shows: None,
        PAID_COHORT_QUERY="FROM payment_attempts p JOIN cohort o ON o.id=p.order_id FROM tickets t JOIN bookings b ON b.id=t.booking_id")
    sampling.install_hourly_sampling(module, observer_seconds=4200)
    assert module.MAX_PAID_OBSERVER_SECONDS == 4200
    module = SimpleNamespace(sample=lambda conn, shows: None,
        PAID_COHORT_QUERY="FROM payment_attempts p JOIN cohort o ON o.id=p.order_id FROM tickets t JOIN bookings b ON b.id=t.booking_id")
    with pytest.raises(ValueError, match="lifetime"):
        sampling.install_hourly_sampling(module, observer_seconds=5400)


@pytest.mark.parametrize("seconds,bound,accepted", [(3780,3780,True), (4200,3780,False),
                                                  (4200,4200,True), (4201,4200,False),
                                                  (5400,5400,False)])
def test_independent_sql_sampler_keeps_default_bound_and_explicit_extension(monkeypatch, tmp_path, seconds, bound, accepted):
    import independent_database_sampling as sampling
    monkeypatch.setattr(sampling.threading, "Thread", lambda **kwargs: SimpleNamespace(start=lambda: None))
    module = SimpleNamespace(psycopg=SimpleNamespace(connect=lambda *args, **kwargs: None),
                             sample=lambda *args: {})
    if accepted:
        value = sampling.Sampler(None, module.sample, ["show"], tmp_path/"db.jsonl",
                                 seconds=seconds, max_seconds=bound)
        assert value.seconds == seconds
        sampling.install(module, "data", tmp_path/"db.jsonl", seconds, max_seconds=bound)
    else:
        with pytest.raises(ValueError, match="Bounded sampler"):
            sampling.Sampler(None, module.sample, ["show"], tmp_path/"db.jsonl",
                             seconds=seconds, max_seconds=bound)
        with pytest.raises(ValueError, match="Bounded sampler"):
            sampling.install(module, "data", tmp_path/"db.jsonl", seconds, max_seconds=bound)

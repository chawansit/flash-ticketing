"""Measure actual serial observer costs while preserving error and row semantics."""
import sys
from pathlib import Path
from types import SimpleNamespace

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[2] / "scripts"))
from observe_two_host_pipeline import install_phase_timings


class Clock:
    def __init__(self):
        self.now = 0.0

    def __call__(self):
        return self.now

    def operation(self, seconds, value):
        def run(*args, **kwargs):
            self.now += seconds
            return value.copy() if isinstance(value, dict) else value
        return run


def observer(clock):
    return SimpleNamespace(
        host_cpu_ticks=clock.operation(0.001, [1] * 8),
        pgbouncer_view=clock.operation(2.35, {"stats": {}}),
        sample=clock.operation(0.04, {"issued_tickets": 10}),
        worker_counters=clock.operation(0.02, {"consumer_calls": 12}),
        api_metrics=clock.operation(0.003, {"requests": 4}),
    )


def test_serial_order_values_and_later_phase_timings_are_retained():
    clock = Clock()
    module = observer(clock)
    install_phase_timings(module, clock=clock)
    assert module.host_cpu_ticks() == [1] * 8
    assert module.pgbouncer_view(None, "ticketing") == {"stats": {}}
    row = module.sample(None, ["show"])
    assert row["issued_tickets"] == 10
    assert module.worker_counters("consumer", "consume_event", "url") == {"consumer_calls": 12}
    assert module.api_metrics("primary:api-1") == {"requests": 4}
    assert row["observer_phase_ms"] == pytest.approx(
        {"host_cpu": 1, "pgbouncer": 2350, "paid_cohort": 40,
         "worker:consumer": 20, "api:primary:api-1": 3})


def test_failed_resource_call_is_timed_and_original_error_propagates():
    clock = Clock()
    module = observer(clock)
    error = OSError("resource unavailable")
    def failing(*args):
        clock.now += 2.1
        raise error
    module.pgbouncer_view = failing
    install_phase_timings(module, clock=clock)
    module.host_cpu_ticks()
    with pytest.raises(OSError) as caught:
        module.pgbouncer_view(None, "ticketing")
    assert caught.value is error
    assert module.sample(None, ["show"])["observer_phase_ms"]["pgbouncer"] == pytest.approx(2100)


def test_next_iteration_does_not_retain_old_api_or_worker_phases():
    clock = Clock()
    module = observer(clock)
    install_phase_timings(module, clock=clock)
    module.host_cpu_ticks()
    first = module.sample(None, ["show"])
    module.api_metrics("primary:api-1")
    retained = dict(first["observer_phase_ms"])
    module.host_cpu_ticks()
    second = module.sample(None, ["show"])
    assert "api:primary:api-1" in retained
    assert set(second["observer_phase_ms"]) == {"host_cpu", "paid_cohort"}


def test_outer_wait_collection_does_not_enter_cohort_query_time():
    clock = Clock()
    module = observer(clock)
    install_phase_timings(module, clock=clock)
    original = module.sample
    def with_waits(*args):
        row = original(*args)
        clock.now += 0.05
        row["database_wait_diagnostics"] = {"collection_ms": 50}
        return row
    module.sample = with_waits
    module.host_cpu_ticks()
    row = module.sample(None, ["show"])
    assert row["observer_phase_ms"]["paid_cohort"] == pytest.approx(40)
    assert row["database_wait_diagnostics"]["collection_ms"] == 50

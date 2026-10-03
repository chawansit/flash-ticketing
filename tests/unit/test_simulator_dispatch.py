"""A straggler must not park a healthy delivery slot or create unbounded work."""

import threading
import time
from concurrent.futures import ThreadPoolExecutor
from dataclasses import replace

import pytest

from ticketing import workers
from ticketing.config import Settings
from ticketing.observability import SIMULATOR_BATCH_BARRIER_SECONDS


@pytest.mark.parametrize("mode,expected_refill", [("batch", False), ("refill", True)])
def test_one_straggler_leaves_other_slot_reusable_only_in_refill(monkeypatch, mode, expected_refill):
    slow_started, release_slow, third_started, stop = [threading.Event() for _ in range(4)]
    lock = threading.Lock()
    calls = active = peak = 0

    def task(*args):
        nonlocal calls, active, peak
        with lock:
            calls += 1
            index = calls
            active += 1
            peak = max(peak, active)
        try:
            if index == 1:
                slow_started.set()
                assert release_slow.wait(3)
            elif index == 3:
                third_started.set()
                stop.set()
            return True
        finally:
            with lock:
                active -= 1

    monkeypatch.setattr(workers, "simulate_one", task)
    settings = replace(Settings(), simulator_concurrency=2)
    barrier_before = SIMULATOR_BATCH_BARRIER_SECONDS._value.get()
    errors = []

    def dispatch():
        try:
            with ThreadPoolExecutor(max_workers=2) as executor:
                if mode == "batch":
                    workers.simulate_batch(None, settings, executor)
                else:
                    workers.simulate_refill(None, settings, executor, lambda: not stop.is_set())
        except Exception as exc:  # noqa: BLE001 - surface worker-thread failures in the test
            errors.append(exc)

    thread = threading.Thread(target=dispatch)
    thread.start()
    try:
        assert slow_started.wait(1)
        assert third_started.wait(.15) is expected_refill
        assert peak <= 2
    finally:
        stop.set()
        release_slow.set()
        thread.join(3)
    assert not thread.is_alive() and not errors
    assert calls == (3 if expected_refill else 2)
    if mode == "batch":
        assert SIMULATOR_BATCH_BARRIER_SECONDS._value.get() > barrier_before


@pytest.mark.parametrize("fails", [False, True])
def test_idle_and_failed_slots_back_off_without_hot_loop(monkeypatch, fails):
    calls = 0
    def task(*args):
        nonlocal calls
        calls += 1
        if fails:
            raise RuntimeError("intentional test failure")
        return False
    monkeypatch.setattr(workers, "simulate_one", task)
    until = time.monotonic() + .24
    with ThreadPoolExecutor(max_workers=1) as executor:
        workers.simulate_refill(None, replace(Settings(), simulator_concurrency=1), executor,
                                lambda: time.monotonic() < until)
    assert 1 <= calls <= 3


def test_stopped_scheduler_submits_no_work(monkeypatch):
    def forbidden(*args):
        raise AssertionError("No submission after stop")
    monkeypatch.setattr(workers, "simulate_one", forbidden)
    with ThreadPoolExecutor(max_workers=2) as executor:
        workers.simulate_refill(None, replace(Settings(), simulator_concurrency=2), executor, lambda: False)


def test_invalid_dispatch_mode_rejected():
    with pytest.raises(RuntimeError, match="SIMULATOR_DISPATCH_MODE"):
        replace(Settings(), simulator_dispatch_mode="unbounded").validate()

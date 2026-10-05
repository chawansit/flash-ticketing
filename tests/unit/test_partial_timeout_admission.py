"""Conservative queue bounds and immutable failure-time admission evidence."""
import asyncio
import json
import logging
import threading
from concurrent.futures import ThreadPoolExecutor
from dataclasses import replace
from types import SimpleNamespace
from unittest.mock import Mock

import pytest
from psycopg_pool import PoolTimeout, TooManyRequests

from ticketing import api
from ticketing.config import Settings
from ticketing.infrastructure import postgres as pg
from ticketing.observability import DB_ACQUISITION_FAILURES, REQUEST_ID


class Native:
    timeout = .3

    def __init__(self):
        self.waiting = 0
        self.error = None
        self.deadline = None

    def get_stats(self):
        return {"requests_waiting": self.waiting, "pool_size": 2,
                "pool_available": 0, "pool_max": 2,
                "conninfo": "SECRET_DSN_MUST_NOT_APPEAR"}

    def getconn(self, timeout=None):
        self.deadline = timeout
        if self.error:
            raise self.error
        return object()

    def putconn(self, conn):
        pass


def setup(candidate=True, maximum=8, limits=None, aliases=False):
    native = Native()
    pools = {"general": Native(), "payment": native}
    if aliases:
        pools["callback"] = native
    limits = limits or {role: 6 for role in pools}
    budget = pg.SharedAcquisitionBudget(maximum, limits, pools,
                                        callback_reserved=1 if aliases else 0,
                                        reclaim_partial_timeouts=candidate)
    return budget, pools, pg.AcquisitionLimitedPool(native, budget, "payment")


@pytest.mark.parametrize("candidate,retained", [(False, 4), (True, 1)])
def test_partial_pruning_releases_only_excess_and_preserves_live_attempt(candidate, retained):
    budget, pools, _ = setup(candidate)
    pools["payment"].waiting = 6
    for _ in range(4):
        budget.acquire("payment")
        budget.release("payment", timed_out=True)
    budget.acquire("payment")
    pools["payment"].waiting = 1  # Public native FIFO after expired-prefix handoff.
    state = budget.snapshot()
    assert state["retained"] == retained
    assert state["acquiring"] == 1
    assert state["used"] == retained + 1
    budget.release("payment")
    pools["payment"].waiting = 0
    assert budget.snapshot()["used"] == 0


@pytest.mark.parametrize("waiting", range(7))
def test_reconciliation_stays_conservative_without_subtracting_live_attempts(waiting):
    budget, pools, _ = setup()
    pools["payment"].waiting = 6
    for _ in range(3):
        budget.acquire("payment")
        budget.release("payment", timed_out=True)
    for _ in range(2):
        budget.acquire("payment")
    pools["payment"].waiting = waiting
    state = budget.snapshot()
    assert state["acquiring"] == 2
    # Existing stale count and all physical positions are independent upper
    # bounds. Every possible surviving stale subset must remain budgeted.
    for actual_expired in range(min(3, waiting) + 1):
        assert state["used"] >= 2 + actual_expired
    assert 0 <= state["retained"] <= 3
    assert state["used"] <= 8
    for _ in range(2):
        budget.release("payment")
    pools["payment"].waiting = 0
    assert budget.snapshot()["used"] == 0


def test_alias_reclamation_does_not_transfer_retained_ownership_or_break_callback_reserve():
    budget, pools, _ = setup(aliases=True)
    pools["payment"].waiting = 6
    for role in ("payment", "payment", "callback", "callback"):
        budget.acquire(role)
        budget.release(role, timed_out=True)
    pools["payment"].waiting = 1
    state = budget.snapshot()
    assert state["counts"] == {"general": 0, "payment": 1, "callback": 1}
    assert state["retained"] == 2  # Both role bounds remain conservative.
    for _ in range(4):
        budget.acquire("payment")
    with pytest.raises(TooManyRequests) as failure:
        budget.acquire("payment")
    assert failure.value.acquisition_failure["reason"] == "callback_reserve"
    assert budget.snapshot()["used"] == 6
    for _ in range(4):
        budget.release("payment")
    pools["payment"].waiting = 0
    assert budget.snapshot()["used"] == 0


@pytest.mark.parametrize("global_full", [False, True])
def test_guard_failure_snapshot_is_exact_stable_and_has_fixed_metric_reason(global_full):
    budget, _pools, wrapper = setup(maximum=2 if global_full else 8,
                                  limits={"general": 6, "payment": 2})
    for _ in range(2):
        budget.acquire("payment")
    reason = "global_limit" if global_full else "role_limit"
    counter = DB_ACQUISITION_FAILURES.labels("payment", reason)
    before = counter._value.get()
    with pytest.raises(TooManyRequests) as rejected:
        wrapper.getconn()
    evidence = rejected.value.acquisition_failure
    assert evidence["reason"] == reason
    assert evidence["capture"] == "guard_rejection"
    assert evidence["guard"]["used"] == 2
    assert evidence["guard"]["counts"]["payment"] == 2
    assert evidence["native_pool"]["pool_max"] == 2
    assert evidence["elapsed_ms"] >= 0
    assert counter._value.get() == before + 1
    encoded = json.dumps(evidence)
    assert "SECRET_DSN" not in encoded
    assert "conninfo" not in encoded
    budget.release("payment")
    budget.release("payment")
    assert evidence["guard"]["used"] == 2
    assert budget.snapshot()["used"] == 0


@pytest.mark.parametrize("native_error,reason", [(PoolTimeout, "native_timeout"),
                                                (TooManyRequests, "native_limit")])
def test_native_failure_snapshot_precedes_release_and_keeps_original_error(native_error, reason):
    budget, pools, wrapper = setup()
    exc = native_error("SECRET_DRIVER_MESSAGE_MUST_NOT_APPEAR")
    pools["payment"].error = exc
    with pytest.raises(native_error) as failed:
        wrapper.getconn(timeout=.3)
    assert failed.value is exc
    evidence = exc.acquisition_failure
    assert evidence["reason"] == reason
    assert evidence["guard"]["used"] == 1
    assert evidence["capture"] == "native_failure_before_release"
    assert "SECRET_DRIVER" not in json.dumps(evidence)
    assert 0 < pools["payment"].deadline <= .3
    assert budget.snapshot()["used"] == 0


def test_diagnostic_counter_failure_cannot_replace_driver_error_or_leak_slot(monkeypatch):
    budget, pools, wrapper = setup()
    exc = PoolTimeout("original")
    pools["payment"].error = exc
    monkeypatch.setattr(pg, "DB_ACQUISITION_FAILURES", Mock(labels=Mock(side_effect=RuntimeError("metric"))))
    with pytest.raises(PoolTimeout) as failure:
        wrapper.getconn()
    assert failure.value is exc
    assert budget.snapshot()["used"] == 0


def test_default_off_and_factory_rejects_flag_without_guard_before_opening_resources(monkeypatch):
    assert not Settings().api_partial_timeout_reclaim
    with pytest.raises(RuntimeError, match="API_PARTIAL_TIMEOUT_RECLAIM"):
        replace(Settings(), api_partial_timeout_reclaim=True, api_pool_shared_waiting=False).validate()
    ctor = Mock()
    monkeypatch.setattr(pg, "Postgres", ctor)
    with pytest.raises(ValueError, match="Partial"):
        pg.create_api_databases("unused", 4, 300, 8, 2, False, reclaim_partial_timeouts=True)
    ctor.assert_not_called()


def test_failure_log_has_request_correlation_without_sql_or_driver_secrets(caplog):
    budget, pools, wrapper = setup()
    pools["payment"].error = PoolTimeout("SECRET_DRIVER")
    db = pg.Postgres.borrow_pool(wrapper)
    token = REQUEST_ID.set("test-request-correlation")
    try:
        with caplog.at_level(logging.WARNING), pytest.raises(PoolTimeout), db.connection():
            pass
    finally:
        REQUEST_ID.reset(token)
    records = [r for r in caplog.records if r.message == "db_acquisition_failure"]
    assert len(records) == 1
    assert records[0].fields["request_id"] == "test-request-correlation"
    assert records[0].fields["reason"] == "native_timeout"
    assert "SECRET" not in json.dumps(records[0].fields)
    assert budget.snapshot()["used"] == 0



def test_api_keeps_failure_evidence_internal_and_preserves_retryable_response():
    budget, _, wrapper = setup(maximum=1, limits={"general": 1, "payment": 1})
    budget.acquire("payment")
    with pytest.raises(TooManyRequests) as rejected:
        wrapper.getconn()
    request = SimpleNamespace(state=SimpleNamespace(request_id="test-request"))
    response = asyncio.run(api.unavailable_error(request, rejected.value))
    assert response.status_code == 503
    assert response.headers["Retry-After"] == "1"
    assert json.loads(response.body) == {"code": "DATABASE_UNAVAILABLE", "request_id": "test-request"}
    assert request.state.db_acquisition_failure["reason"] == "global_limit"
    budget.release("payment")


def test_logging_failure_does_not_replace_timeout_or_leak_slot(monkeypatch):
    budget, pools, wrapper = setup()
    exc = PoolTimeout("original")
    pools["payment"].error = exc
    db = pg.Postgres.borrow_pool(wrapper)
    monkeypatch.setattr(pg.logger, "warning", Mock(side_effect=RuntimeError("logging")))
    with pytest.raises(PoolTimeout) as failure, db.connection():
        pass
    assert failure.value is exc
    assert budget.snapshot()["used"] == 0


@pytest.mark.parametrize("candidate,expected_live", [(False, 4), (True, 10)])
def test_mixed_concurrent_admissions_keep_same_total_ceiling_after_partial_pruning(candidate, expected_live):
    budget, pools, _ = setup(candidate, maximum=12, limits={"general": 10, "payment": 10})
    for role in ("general", "payment"):
        pools[role].waiting = 4
        for _ in range(4):
            budget.acquire(role)
            budget.release(role, timed_out=True)
        pools[role].waiting = 1
    start = threading.Barrier(41)
    attempted = threading.Barrier(41)
    release = threading.Event()
    admitted = []
    lock = threading.Lock()

    def attempt(index):
        role = "payment" if index % 2 else "general"
        start.wait(timeout=5)
        try:
            budget.acquire(role)
        except TooManyRequests:
            attempted.wait(timeout=5)
            return
        try:
            with lock:
                admitted.append(role)
            attempted.wait(timeout=5)
            assert release.wait(timeout=5)
        finally:
            budget.release(role)

    with ThreadPoolExecutor(max_workers=40) as executor:
        futures = [executor.submit(attempt, i) for i in range(40)]
        start.wait(timeout=5)
        try:
            attempted.wait(timeout=5)
            assert len(admitted) == expected_live
            state = budget.snapshot()
            assert state["used"] == 12
            assert all(count <= 10 for count in state["counts"].values())
        finally:
            release.set()
        for f in futures:
            f.result(timeout=5)
    for pool in pools.values():
        pool.waiting = 0
    assert budget.snapshot()["used"] == 0

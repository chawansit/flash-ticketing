"""Callback headroom without a third physical pool or an extra queue."""
import asyncio
import hashlib
import hmac
import json
import threading
import time
from concurrent.futures import ThreadPoolExecutor
from dataclasses import replace
from types import SimpleNamespace
from unittest.mock import Mock
from uuid import uuid4

import pytest
from fastapi.testclient import TestClient
from psycopg_pool import PoolTimeout, TooManyRequests

from ticketing import api
from ticketing.config import Settings
from ticketing.infrastructure import postgres as pg


class Native:
    timeout = .15

    def __init__(self):
        self.waiting = 0
        self.error = None
        self.closed = 0

    def get_stats(self):
        return {"requests_waiting": self.waiting}

    def getconn(self, timeout=None):
        if self.error:
            raise self.error
        return object()

    def close(self):
        self.closed += 1
        self.waiting = 0


def guard():
    native = {"general": Native(), "payment": Native()}
    native["callback"] = native["payment"]
    budget = pg.SharedAcquisitionBudget(12, {role: 10 for role in native}, native, 2)
    return budget, native


@pytest.mark.parametrize("reserve,shared,payment,waiting", [(-1, True, 2, 12), (3, True, 2, 12),
    (1, False, 2, 12), (1, False, 0, 12), (2, True, 2, 4), (1, True, 1, 4)])
def test_invalid_reserve_fails_before_pool_creation(monkeypatch, reserve, shared, payment, waiting):
    constructor = Mock()
    monkeypatch.setattr(pg, "Postgres", constructor)
    with pytest.raises(ValueError, match="Callback reserve"):
        pg.create_api_databases("unused", 4, 150, waiting, payment, shared, reserve)
    constructor.assert_not_called()
    with pytest.raises(RuntimeError, match="API_CALLBACK_ACQUISITION_RESERVE"):
        replace(Settings(), pool_max=4, pool_max_waiting=waiting, api_payment_pool_max=payment,
                api_pool_shared_waiting=shared, api_callback_acquisition_reserve=reserve,
                simulator_concurrency=1).validate()


@pytest.mark.parametrize("general,payment", [(10, 0), (2, 8), (5, 5)])
def test_callbacks_admitted_after_noncallback_saturation(general, payment):
    budget, _ = guard()
    for role, count in [("general", general), ("payment", payment)]:
        for _ in range(count):
            budget.acquire(role)
    for role in ("general", "payment"):
        with pytest.raises(TooManyRequests):
            budget.acquire(role)
    budget.acquire("callback")
    budget.acquire("callback")
    assert budget.snapshot()["used"] == 12
    for role in budget.limits:
        with pytest.raises(TooManyRequests):
            budget.acquire(role)
    for role, count in budget.snapshot()["counts"].items():
        for _ in range(count):
            budget.release(role)
    assert budget.snapshot()["used"] == 0


def test_callback_flood_preserves_general_headroom_and_shared_native_limit():
    budget, _ = guard()
    for _ in range(10):
        budget.acquire("callback")
    for role in ("callback", "payment"):
        with pytest.raises(TooManyRequests):
            budget.acquire(role)
    budget.acquire("general")
    budget.acquire("general")
    assert budget.snapshot()["used"] == 12


def test_retained_submission_timeouts_do_not_take_callback_reserve_or_expand_fifo():
    budget, native = guard()
    pool = pg.AcquisitionLimitedPool(native["payment"], budget, "payment")
    native["payment"].waiting = 8
    native["payment"].error = PoolTimeout()
    budget.acquire("general")
    budget.acquire("general")
    for _ in range(8):
        with pytest.raises(PoolTimeout):
            pool.getconn()
    with pytest.raises(TooManyRequests):
        pool.getconn()
    for _ in range(2):
        budget.acquire("callback")
        budget.release("callback", timed_out=True)
    with pytest.raises(TooManyRequests):
        budget.acquire("callback")
    assert budget.snapshot()["retained"] == 10
    native["payment"].waiting = 0
    assert budget.snapshot()["used"] == 2
    budget.release("general")
    budget.release("general")
    assert budget.snapshot()["used"] == 0


@pytest.mark.parametrize("error", [RuntimeError("failed"), KeyboardInterrupt(), PoolTimeout()])
def test_callback_exception_releases_slot(error):
    budget, native = guard()
    native["callback"].error = error
    with pytest.raises(type(error)):
        pg.AcquisitionLimitedPool(native["callback"], budget, "callback").getconn()
    assert budget.snapshot()["used"] == 0


def test_concurrent_noncallbacks_cannot_steal_two_callback_positions():
    budget, _ = guard()
    started = threading.Barrier(41)
    attempted = threading.Barrier(41)
    release = threading.Event()

    def noncallback(index):
        role = "general" if index % 2 else "payment"
        started.wait(timeout=3)
        admitted = False
        try:
            try:
                budget.acquire(role)
                admitted = True
            except TooManyRequests:
                pass
            attempted.wait(timeout=3)
            assert release.wait(timeout=3)
        finally:
            if admitted:
                budget.release(role)

    with ThreadPoolExecutor(max_workers=40) as executor:
        futures = [executor.submit(noncallback, i) for i in range(40)]
        started.wait(timeout=3)
        try:
            attempted.wait(timeout=3)
            assert budget.snapshot()["used"] == 10
            budget.acquire("callback")
            budget.acquire("callback")
            assert budget.snapshot()["used"] == 12
            budget.release("callback")
            budget.release("callback")
        finally:
            release.set()
        for future in futures:
            future.result(timeout=3)
    assert budget.snapshot()["used"] == 0


def test_factory_reuses_native_pool_and_borrowed_close_never_closes_owner(monkeypatch):
    created = []

    def init(self, url, **kwargs):
        self.pool = Native()
        self._owns_pool = True
        self.callback_database = self
        created.append((self.pool, kwargs))

    monkeypatch.setattr(pg.Postgres, "__init__", init)
    general, payment = pg.create_api_databases("unused", 4, 150, 12, 2, True, 2)
    callback = payment.callback_database
    assert len(created) == 2
    assert sum(kwargs["maximum"] for _, kwargs in created) == 4
    assert callback.pool._native is payment.pool._native
    callback.close()
    assert created[1][0].closed == 0
    callback.pool.getconn()
    assert general._shared_acquisition_budget.snapshot()["used"] == 0
    general.close()
    payment.close()
    assert [native.closed for native, _ in created] == [1, 1]


@pytest.mark.parametrize("route,method,expected", [("callback", "POST", "callback"),
    ("payment", "POST", "payment"), ("get_order", "GET", "general"), ("callback", "GET", "general")])
def test_only_callback_posts_select_reserved_adapter(monkeypatch, route, method, expected):
    monkeypatch.setattr(api, "settings", replace(api.settings, api_callback_acquisition_reserve=2))
    services = {role: object() for role in ("general", "payment", "callback")}
    state = SimpleNamespace(reservations=services["general"], payment_reservations=services["payment"],
                            callback_reservations=services["callback"])
    request = SimpleNamespace(method=method, scope={"route": SimpleNamespace(name=route)},
                              app=SimpleNamespace(state=state))
    assert asyncio.run(api.service(request)) is services[expected]


@pytest.mark.parametrize("valid_signature", [False, True])
def test_reserved_callback_signature_and_no_fallback_on_exhaustion(monkeypatch, valid_signature):
    monkeypatch.setattr(api, "settings", replace(api.settings, api_callback_acquisition_reserve=2))
    general, payment, callback = Mock(), Mock(), Mock()
    monkeypatch.setattr(api.app.state, "reservations", general, raising=False)
    monkeypatch.setattr(api.app.state, "payment_reservations", payment, raising=False)
    monkeypatch.setattr(api.app.state, "callback_reservations", callback, raising=False)
    callback.callback.side_effect = TooManyRequests("bounded")
    body = {"callback_id": str(uuid4()), "payment_id": str(uuid4()), "order_id": str(uuid4()),
            "amount": 100, "currency": "THB", "outcome": "SUCCEEDED"}
    raw = json.dumps(body).encode()
    timestamp = str(int(time.time()))
    signature = hmac.new(api.settings.webhook_secret.encode(), timestamp.encode()+b"."+raw, hashlib.sha256).hexdigest()
    response = TestClient(api.app).post("/v1/webhooks/payments", content=raw, headers={
        "Content-Type": "application/json", "X-Payment-Timestamp": timestamp,
        "X-Payment-Signature": signature if valid_signature else "invalid"})
    assert response.status_code == (503 if valid_signature else 401)
    if valid_signature:
        assert response.headers["Retry-After"] == "1"
        callback.callback.assert_called_once_with(body)
    else:
        callback.callback.assert_not_called()
    general.callback.assert_not_called()
    payment.callback.assert_not_called()



def test_enabled_lifespan_wires_borrowed_callback_service_and_closes_two_pools(monkeypatch):
    created = []

    def init(self, url, **kwargs):
        self.pool = Native()
        self._owns_pool = True
        self.callback_database = self
        created.append(self.pool)

    monkeypatch.setattr(pg.Postgres, "__init__", init)
    monkeypatch.setattr(api, "RedisSeats", Mock(return_value=Mock()))
    monkeypatch.setattr(api, "RedisReservationIntake", Mock())
    monkeypatch.setattr(api, "settings", replace(api.settings, pool_max=4, pool_max_waiting=12,
        api_payment_pool_max=2, api_pool_shared_waiting=True, api_callback_acquisition_reserve=2,
        simulator_concurrency=1))
    isolated = SimpleNamespace(state=SimpleNamespace())

    async def run():
        async with api.lifespan(isolated):
            callback = isolated.state.callback_reservations.store.db
            payment = isolated.state.payment_db
            assert callback is payment.callback_database
            assert callback.pool._native is payment.pool._native
            assert not callback._owns_pool
            assert isolated.state.payment_reservations is not isolated.state.callback_reservations

    asyncio.run(run())
    assert len(created) == 2
    assert [pool.closed for pool in created] == [1, 1]



def test_payment_only_flood_preserves_callback_positions_in_native_fifo():
    budget, _ = guard()
    for _ in range(8):
        budget.acquire("payment")
    with pytest.raises(TooManyRequests):
        budget.acquire("payment")
    budget.acquire("callback")
    budget.acquire("callback")
    budget.acquire("general")
    budget.acquire("general")
    assert budget.snapshot()["used"] == 12
    assert budget.snapshot()["counts"] == {"general": 2, "payment": 8, "callback": 2}

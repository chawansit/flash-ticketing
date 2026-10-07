import asyncio
import json
import threading
from contextlib import contextmanager
from dataclasses import replace
from datetime import UTC, datetime
from types import SimpleNamespace
from unittest.mock import Mock
from uuid import uuid4

import pytest
from fastapi.testclient import TestClient

from ticketing import api, workers
from ticketing.config import Settings
from ticketing.infrastructure.payment_simulator_latency import confirmation_delay, sample_delay


@pytest.mark.parametrize("phase,lower,upper", [
    ("initiation", .05, .15), ("callback_network", .02, .1),
])
def test_uniform_jitter_is_bounded_and_disabled_profile_has_no_wait(phase, lower, upper):
    values = [sample_delay("bank-like", phase) for _ in range(1000)]
    assert all(lower <= value <= upper for value in values)
    assert len(set(values)) > 900
    assert sample_delay("none", phase) == 0


def test_confirmation_is_bounded_stable_and_identity_components_are_unambiguous():
    values = [confirmation_delay("bank-like", "actor", "order", str(i)) for i in range(1000)]
    assert all(1 <= value <= 3 for value in values)
    assert len(set(values)) == 1000
    assert confirmation_delay("bank-like", "a", "b", "c") == confirmation_delay("bank-like", "a", "b", "c")
    assert confirmation_delay("bank-like", "ab", "c", "d") != confirmation_delay("bank-like", "a", "bc", "d")
    assert confirmation_delay("bank-like", "a", "b", "c") != confirmation_delay("bank-like", "other", "b", "c")
    assert confirmation_delay("none", "a", "b", "c") == 1


@pytest.mark.parametrize("delay", [0, 1, 2.5, 600])
def test_explicit_confirmation_override_keeps_original_type_for_idempotency(delay):
    for profile in ("bank-like", "none"):
        value = confirmation_delay(profile, "actor", "order", "key", delay)
        assert value == delay
        assert type(value) is type(delay)
        assert type(api.PaymentInput(delay_seconds=delay).delay_seconds) is type(delay)


def test_invalid_profile_is_rejected():
    with pytest.raises(RuntimeError, match="SIMULATOR_LATENCY_PROFILE"):
        replace(Settings(), simulator_latency_profile="unknown").validate()
    with pytest.raises(ValueError):
        sample_delay("unknown", "initiation")
    with pytest.raises(ValueError):
        confirmation_delay("unknown", "a", "b", "c")


def test_initiation_yields_event_loop_before_database_work(monkeypatch):
    main_thread = threading.get_ident()
    calls = []
    original_sleep = asyncio.sleep

    async def wait(delay):
        assert delay == .1
        assert calls == []
        await original_sleep(0)
        calls.append("gateway_accepted")

    def persist(*args):
        assert threading.get_ident() != main_thread
        assert calls == ["gateway_accepted"]
        calls.append("persist")
        return {"payment_id": "test"}

    monkeypatch.setattr(api, "settings", replace(Settings(), simulator_latency_profile="bank-like"))
    monkeypatch.setattr(api, "sample_delay", lambda *_: .1)
    monkeypatch.setattr(api.asyncio, "sleep", wait)
    result = asyncio.run(api.payment(uuid4(), api.PaymentInput(), "actor",
                                    SimpleNamespace(initiate_payment=persist), "key"))
    assert result == {"payment_id": "test"}
    assert calls == ["gateway_accepted", "persist"]


def test_route_replays_use_same_delay_and_disabled_simulator_cannot_sleep(monkeypatch):
    service = Mock()
    service.initiate_payment.return_value = {"payment_id": str(uuid4())}
    monkeypatch.setattr(api.app.state, "payment_reservations", service, raising=False)
    monkeypatch.setattr(api, "settings", replace(Settings(), simulator_latency_profile="none"))
    api.app.dependency_overrides[api.actor] = lambda: "actor"
    try:
        client = TestClient(api.app)
        path = "/v1/orders/" + str(uuid4()) + "/payments"
        for _ in range(2):
            assert client.post(path, json={}, headers={"Idempotency-Key": "same"}).status_code == 202
        assert service.initiate_payment.call_args_list[0] == service.initiate_payment.call_args_list[1]
        assert service.initiate_payment.call_args.args[-2] == 1
        monkeypatch.setattr(api, "settings", replace(Settings(), environment="production"))
        monkeypatch.setattr(api, "sample_delay", lambda *_: pytest.fail("Production simulator must not run"))
        assert client.post(path, json={}, headers={"Idempotency-Key": "same"}).status_code == 403
        assert service.initiate_payment.call_count == 2
    finally:
        api.app.dependency_overrides.clear()


@pytest.mark.parametrize("delivery_fails", [False, True])
def test_callback_transport_delay_is_outside_transactions_and_errors_are_not_acknowledged(
    monkeypatch, delivery_fails,
):
    now = datetime.now(UTC)
    row = {"id": uuid4(), "order_id": uuid4(), "total": 100, "currency": "THB",
           "outcome": "SUCCEEDED", "due_at": now, "claim_observed_at": now}
    active, statements, sleeps, delivered = [], [], [], []

    def execute(sql, params):
        assert active
        statements.append(sql)
        return SimpleNamespace(fetchone=lambda: row)

    @contextmanager
    def transaction():
        active.append(True)
        try:
            yield SimpleNamespace(execute=execute)
        finally:
            active.pop()

    def wait(delay):
        assert not active
        assert len(statements) == 1
        sleeps.append(delay)

    class Transport:
        def post(self, raw, _headers):
            assert not active
            assert sleeps == [.07]
            delivered.append(json.loads(raw))
            if delivery_fails:
                raise ConnectionError("Response lost")

    monkeypatch.setattr(workers, "sample_delay", lambda *_: .07)
    monkeypatch.setattr(workers.time, "sleep", wait)
    db = SimpleNamespace(transaction=transaction)
    if delivery_fails:
        with pytest.raises(ConnectionError):
            workers.simulate_one(db, Settings(), Transport())
        assert len(statements) == 1
    else:
        assert workers.simulate_one(db, Settings(), Transport())
        assert len(statements) == 2
    assert delivered[0]["callback_id"] == delivered[0]["payment_id"] == str(row["id"])

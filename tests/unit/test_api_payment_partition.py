import asyncio
import hashlib
import hmac
import json
import time
from contextlib import contextmanager
from dataclasses import replace
from types import SimpleNamespace
from unittest.mock import Mock
from uuid import uuid4

import pytest
from fastapi.testclient import TestClient
from psycopg_pool import PoolTimeout, TooManyRequests

from ticketing import api
from ticketing.config import Settings
from ticketing.infrastructure import postgres
from ticketing.observability import DB_POOL_STATE


@pytest.mark.parametrize("maximum,waiting,payment", [(4,12,1),(4,12,2),(3,None,1),(12,None,3),(2,2,1),(1,1,0)])
def test_partition_preserves_total_connections_and_bounded_waiters(maximum, waiting, payment):
    budgets = postgres.api_pool_budgets(maximum, waiting, payment)
    pools = [v for v in budgets.values() if v is not None]
    assert sum(v["maximum"] for v in pools) == maximum
    assert sum(v["maximum_waiting"] for v in pools) == (maximum if waiting is None else waiting)
    assert all(v["maximum"] >= 1 and v["maximum_waiting"] >= 1 for v in pools)


@pytest.mark.parametrize("maximum,waiting,payment", [(1,1,1),(4,1,1),(4,12,-1),(4,12,4),(4,0,1)])
def test_invalid_partition_fails_before_any_pool_opens(monkeypatch, maximum, waiting, payment):
    constructor = Mock()
    monkeypatch.setattr(postgres, "Postgres", constructor)
    with pytest.raises(ValueError):
        postgres.create_api_databases("unused", maximum, 150, waiting, payment)
    constructor.assert_not_called()


@pytest.mark.parametrize("payment", [-1,4,5])
def test_settings_reject_payment_partition_outside_total_budget(payment):
    with pytest.raises(RuntimeError, match="API_PAYMENT_POOL_MAX"):
        replace(Settings(), pool_max=4, simulator_concurrency=1, api_payment_pool_max=payment).validate()


def test_settings_reject_unbounded_queue_partition_and_keep_shared_one_connection():
    with pytest.raises(RuntimeError, match="waiter"):
        replace(Settings(), pool_max=4, pool_max_waiting=1, api_payment_pool_max=1).validate()
    replace(Settings(), pool_max=1, simulator_concurrency=1, api_payment_pool_max=0).validate()


def test_factory_disabled_returns_one_owned_adapter(monkeypatch):
    adapter = SimpleNamespace(pool=object(), close=Mock())
    constructor = Mock(return_value=adapter)
    monkeypatch.setattr(postgres, "Postgres", constructor)
    general, payment = postgres.create_api_databases("unused", 4, 150, 12, 0)
    assert general is payment is adapter
    adapter.close.assert_not_called()
    constructor.assert_called_once_with("unused", wait_ms=150, maximum=4, maximum_waiting=12)


def test_factory_closes_first_pool_if_second_creation_fails(monkeypatch):
    general = Mock()
    constructor = Mock(side_effect=[general, RuntimeError("second pool failed")])
    monkeypatch.setattr(postgres, "Postgres", constructor)
    with pytest.raises(RuntimeError, match="second pool"):
        postgres.create_api_databases("unused", 4, 150, 12, 1)
    general.close.assert_called_once()


def test_aggregate_gauges_sum_partition_without_counting_an_adapter_twice():
    class Pool:
        def __init__(self, stats):
            self.stats = stats
        def get_stats(self):
            return self.stats

    general = Pool({"pool_max":3,"pool_size":3,"pool_available":1,"requests_waiting":2})
    payment = Pool({"pool_max":1,"pool_size":1,"pool_available":1,"requests_waiting":0})
    db = postgres.Postgres.__new__(postgres.Postgres)
    db.pool = general
    db._metric_pool_roles = {"general":general,"payment":payment}
    db.sample_pool()
    assert DB_POOL_STATE.labels("pool_max")._value.get() == 4
    assert DB_POOL_STATE.labels("pool_available")._value.get() == 2
    assert DB_POOL_STATE.labels("requests_waiting")._value.get() == 2
    assert DB_POOL_STATE.labels("general_pool_max")._value.get() == 3
    assert DB_POOL_STATE.labels("payment_pool_max")._value.get() == 1
    assert DB_POOL_STATE.labels("payment_requests_waiting")._value.get() == 0


def signed_payload():
    body = {"callback_id":str(uuid4()),"payment_id":str(uuid4()),"order_id":str(uuid4()),
            "amount":100,"currency":"THB","outcome":"SUCCEEDED"}
    raw = json.dumps(body).encode()
    timestamp = str(int(time.time()))
    signature = hmac.new(api.settings.webhook_secret.encode(),timestamp.encode()+b"."+raw,hashlib.sha256).hexdigest()
    return body,raw,{"X-Payment-Timestamp":timestamp,"X-Payment-Signature":signature,"Content-Type":"application/json"}


@pytest.fixture
def routed_services(monkeypatch):
    general,payment = Mock(),Mock()
    monkeypatch.setattr(api.app.state,"reservations",general,raising=False)
    monkeypatch.setattr(api.app.state,"payment_reservations",payment,raising=False)
    api.app.dependency_overrides[api.actor] = lambda:"owner"
    try:
        yield general,payment
    finally:
        api.app.dependency_overrides.clear()


def test_real_routes_select_financial_service_only_for_payments(routed_services):
    general,payment = routed_services
    oid = str(uuid4())
    general.get_order.return_value = {"id":oid,"status":"PENDING","tickets":[]}
    general.checkout.return_value = {"order_id":oid}
    payment.initiate_payment.return_value = {"payment_id":str(uuid4()),"order_id":oid}
    payment.callback.return_value = {"status":"duplicate"}
    client = TestClient(api.app)
    assert client.get("/v1/orders/"+oid).status_code == 200
    assert client.post("/v1/orders",json={"hold_id":str(uuid4())},headers={"Idempotency-Key":"checkout"}).status_code == 201
    assert client.post("/v1/orders/"+oid+"/payments",json={"outcome":"SUCCEEDED"},headers={"Idempotency-Key":"pay"}).status_code == 202
    body,raw,headers = signed_payload()
    assert client.post("/v1/webhooks/payments",content=raw,headers=headers).status_code == 200
    payment.callback.assert_called_once_with(body)
    payment.initiate_payment.assert_called_once()
    general.get_order.assert_called_once()
    general.checkout.assert_called_once()
    general.callback.assert_not_called()
    general.initiate_payment.assert_not_called()
    payment.get_order.assert_not_called()


@pytest.mark.parametrize("failure",[PoolTimeout("bounded"),TooManyRequests("bounded")])
def test_payment_pool_errors_remain_visible_and_do_not_fall_back(routed_services,failure):
    general,payment = routed_services
    payment.callback.side_effect = failure
    _,raw,headers = signed_payload()
    response = TestClient(api.app).post("/v1/webhooks/payments",content=raw,headers=headers)
    assert response.status_code == 503
    assert response.json()["code"] == "DATABASE_UNAVAILABLE"
    assert response.headers["retry-after"] == "1"
    general.callback.assert_not_called()
    assert payment.callback.call_count == 1


def test_invalid_signature_never_uses_either_database_service(routed_services):
    general,payment = routed_services
    _,raw,headers = signed_payload()
    headers["X-Payment-Signature"] = "invalid"
    assert TestClient(api.app).post("/v1/webhooks/payments",content=raw,headers=headers).status_code == 401
    general.callback.assert_not_called()
    payment.callback.assert_not_called()


@pytest.mark.parametrize("distinct",[False,True])
def test_readiness_checks_each_owned_pool_once(distinct):
    calls = []
    class Database:
        def __init__(self,name):
            self.name = name
        @contextmanager
        def transaction(self):
            calls.append(self.name)
            yield Mock()
    general = Database("general")
    payment = Database("payment") if distinct else general
    request = SimpleNamespace(app=SimpleNamespace(state=SimpleNamespace(db=general,payment_db=payment,cache=Mock())))
    assert api.ready(request) == {"status":"ready"}
    assert calls == (["general","payment"] if distinct else ["general"])


@pytest.mark.parametrize("distinct",[False,True])
@pytest.mark.parametrize("fail_body",[False,True])
def test_lifespan_closes_all_owned_resources_once(monkeypatch,distinct,fail_body):
    general = Mock()
    payment = Mock() if distinct else general
    cache = Mock()
    monkeypatch.setattr(api,"create_api_databases",lambda *args:(general,payment))
    monkeypatch.setattr(api,"RedisSeats",lambda *args,**kwargs:cache)
    monkeypatch.setattr(api,"RedisReservationIntake",Mock())
    isolated = SimpleNamespace(state=SimpleNamespace())
    async def run():
        async with api.lifespan(isolated):
            if fail_body:
                raise RuntimeError("request lifecycle failed")
    if fail_body:
        with pytest.raises(RuntimeError,match="lifecycle"):
            asyncio.run(run())
    else:
        asyncio.run(run())
    general.close.assert_called_once()
    if distinct:
        payment.close.assert_called_once()
    cache.redis.close.assert_called_once()


def test_failed_cache_setup_closes_both_created_pools(monkeypatch):
    general,payment = Mock(),Mock()
    monkeypatch.setattr(api,"create_api_databases",lambda *args:(general,payment))
    monkeypatch.setattr(api,"RedisSeats",Mock(side_effect=RuntimeError("cache setup")))
    async def run():
        async with api.lifespan(SimpleNamespace(state=SimpleNamespace())):
            pytest.fail("failed setup must not yield")
    with pytest.raises(RuntimeError,match="cache setup"):
        asyncio.run(run())
    general.close.assert_called_once()
    payment.close.assert_called_once()


def test_two_slot_factory_owns_two_equal_bounded_pools(monkeypatch):
    general,payment=Mock(),Mock()
    constructor=Mock(side_effect=[general,payment])
    monkeypatch.setattr(postgres,'Postgres',constructor)
    assert postgres.create_api_databases('unused',4,150,12,2)==(general,payment)
    assert constructor.call_count==2
    for call in constructor.call_args_list:
        assert call.args==('unused',)
        assert call.kwargs=={'wait_ms':150,'maximum':2,'maximum_waiting':6}
    assert general._metric_waiter_limits==payment._metric_waiter_limits=={'general':6,'payment':6}
    general.close.assert_not_called()
    payment.close.assert_not_called()


def test_two_slot_settings_remain_opt_in_with_shared_default():
    assert Settings().api_payment_pool_max==0
    replace(Settings(),pool_max=4,pool_max_waiting=12,simulator_concurrency=1,api_payment_pool_max=2).validate()
    assert postgres.api_pool_budgets(4,12,2)=={
        'general':{'maximum':2,'maximum_waiting':6},
        'payment':{'maximum':2,'maximum_waiting':6},
    }

@pytest.mark.parametrize("total,waiting,payment,financial", [(4,12,2,11),(4,12,2,1),(4,None,2,3)])
def test_explicit_waiter_allocation_preserves_totals(total,waiting,payment,financial):
    budgets=postgres.api_pool_budgets(total,waiting,payment,financial)
    assert budgets["payment"]["maximum_waiting"]==financial
    assert budgets["general"]["maximum_waiting"]==(waiting or total)-financial
    assert sum(v["maximum"] for v in budgets.values())==total


@pytest.mark.parametrize("payment,financial", [(0,1),(2,-1),(2,12),(2,13)])
def test_invalid_explicit_waiters_fail_before_resources_and_settings(monkeypatch,payment,financial):
    constructor=Mock()
    monkeypatch.setattr(postgres,"Postgres",constructor)
    with pytest.raises(ValueError,match="waiter"):
        postgres.create_api_databases("unused",4,150,12,payment,financial)
    constructor.assert_not_called()
    with pytest.raises(RuntimeError,match="API_PAYMENT_POOL_MAX_WAITING"):
        replace(Settings(),pool_max=4,pool_max_waiting=12,simulator_concurrency=1,
                api_payment_pool_max=payment,api_payment_pool_max_waiting=financial).validate()


def test_explicit_waiter_factory_metrics_and_lifespan_configuration(monkeypatch):
    class Pool:
        def get_stats(self):
            return {"pool_max":2,"pool_size":2,"pool_available":2,"requests_waiting":0}
    general=postgres.Postgres.__new__(postgres.Postgres)
    payment=postgres.Postgres.__new__(postgres.Postgres)
    general.pool,payment.pool=Pool(),Pool()
    general.close,payment.close=Mock(),Mock()
    constructor=Mock(side_effect=[general,payment])
    monkeypatch.setattr(postgres,"Postgres",constructor)
    assert postgres.create_api_databases("unused",4,150,12,2,11)==(general,payment)
    assert constructor.call_args_list[0].kwargs=={"wait_ms":150,"maximum":2,"maximum_waiting":1}
    assert constructor.call_args_list[1].kwargs=={"wait_ms":150,"maximum":2,"maximum_waiting":11}
    general.sample_pool()
    for state,value in {"pool_max":4,"pool_max_waiting":12,"general_max_waiting":1,
                        "payment_max_waiting":11}.items():
        assert DB_POOL_STATE.labels(state)._value.get()==value
    assert Settings().api_payment_pool_max_waiting==0

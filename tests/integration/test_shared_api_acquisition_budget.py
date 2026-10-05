"""Real native queues: shared burst admission, retained timeout slots and financial replay."""
import hashlib
import hmac
import json
import os
import time
from concurrent.futures import ThreadPoolExecutor
from contextlib import ExitStack
from dataclasses import replace
from uuid import uuid4

import jwt
import pytest
from fastapi.testclient import TestClient
from psycopg_pool import PoolClosed, PoolTimeout, TooManyRequests

from ticketing import api
from ticketing.infrastructure.postgres import create_api_databases
from ticketing.workers import consume_event

pytestmark=pytest.mark.integration

def wait_queue(pool,count,deadline):
    while pool.get_stats()["requests_waiting"]!=count:
        assert time.monotonic()<deadline,"Native queue did not reach controlled boundary"
        time.sleep(.001)

@pytest.mark.parametrize("shared,reclaim",[(False,False),(True,False),(True,True)])
@pytest.mark.parametrize("readers,payers",[(7,5),(5,7)])
def test_mixed_http_burst_static_boundary_vs_shared_exact_financial_replay(system,monkeypatch,shared,reclaim,readers,payers):
    svc,db,show=system
    with db.transaction() as conn:
        for seat in "DEFG":
            conn.execute("INSERT INTO event_seats(event_id,seat_id,price) VALUES (%s,%s,100)",(show,seat))
    orders=[svc.reserve("owner",show,[seat],str(uuid4())) for seat in "ABCDEFG"]
    keys=[str(uuid4()) for _ in range(payers)]
    monkeypatch.setattr(api,"settings",replace(
        api.settings,database_url=db.pool.conninfo,redis_url=os.environ["TEST_REDIS_URL"],
        pool_max=4,pool_max_waiting=12,pool_wait_ms=1000,api_payment_pool_max=2,
        api_pool_shared_waiting=shared,api_partial_timeout_reclaim=reclaim,
        simulator_concurrency=1,reservation_mode="postgres",order_status_cache_ms=0,
    ))
    names=("db","payment_db","cache","reservations","payment_reservations","reservation_intake")
    previous={n:getattr(api.app.state,n) for n in names if hasattr(api.app.state,n)}
    token=jwt.encode({"sub":"owner","exp":int(time.time())+60,"aud":"ticketing","iss":"ticketing"},
                     api.settings.jwt_secret,algorithm="HS256")
    authorization={"Authorization":"Bearer "+token}
    def pay(client,index):
        return client.post("/v1/orders/"+orders[index]["order_id"]+"/payments",
                           json={"outcome":"SUCCEEDED"},
                           headers={**authorization,"Idempotency-Key":keys[index]})
    try:
        with TestClient(api.app) as client:
            general,payment=api.app.state.db,api.app.state.payment_db
            for pool in (general.pool,payment.pool):
                pool.resize(2,2)
                pool.wait(timeout=10)
            with ExitStack() as held,ThreadPoolExecutor(max_workers=12) as executor:
                for adapter in (general,payment):
                    held.enter_context(adapter.transaction())
                    held.enter_context(adapter.transaction())
                reading=[executor.submit(client.get,"/v1/orders/"+orders[0]["order_id"],headers=authorization)
                         for _ in range(readers)]
                paying=[executor.submit(pay,client,i) for i in range(payers)]
                deadline=time.monotonic()+.7
                wait_queue(general.pool,readers if shared else min(readers,6),deadline)
                wait_queue(payment.pool,payers if shared else min(payers,6),deadline)
                assert general.pool.get_stats()["requests_waiting"]+payment.pool.get_stats()["requests_waiting"]<=12
                with db.transaction() as conn:
                    assert conn.execute("SELECT count(*) AS n FROM payment_attempts").fetchone()["n"]==0
                if shared:
                    state=general._shared_acquisition_budget.snapshot()
                    assert state["counts"]=={"general":readers,"payment":payers}
                    assert state["used"]==12
                held.close()
                reads=[f.result(timeout=3) for f in reading]
                payments=[f.result(timeout=3) for f in paying]
            expected_rejected=0 if shared else 1
            assert sum(r.status_code==503 for r in reads+payments)==expected_rejected
            assert all(r.status_code in (200,503) for r in reads)
            for i,response in enumerate(payments):
                if response.status_code==503:
                    assert response.json()["code"]=="DATABASE_UNAVAILABLE"
                    assert response.headers["Retry-After"]=="1"
                    response=pay(client,i)  # Explicit historical-control recovery only.
                assert response.status_code==202
                duplicate=pay(client,i)
                assert duplicate.status_code==202
                assert duplicate.json()["payment_id"]==response.json()["payment_id"]
                body={"callback_id":str(uuid4()),"payment_id":response.json()["payment_id"],
                      "order_id":orders[i]["order_id"],"amount":100,"currency":"THB","outcome":"SUCCEEDED"}
                raw=json.dumps(body).encode()
                timestamp=str(int(time.time()))
                signature=hmac.new(api.settings.webhook_secret.encode(),
                                   timestamp.encode()+b"."+raw,hashlib.sha256).hexdigest()
                headers={"X-Payment-Timestamp":timestamp,"X-Payment-Signature":signature,"Content-Type":"application/json"}
                assert client.post("/v1/webhooks/payments",content=raw,headers=headers).json()["status"]=="book"
                assert client.post("/v1/webhooks/payments",content=raw,headers=headers).json()["status"]=="duplicate"
                envelope={"event_id":str(uuid4()),"schema_version":1,"event_type":"OrderPaid",
                          "payload":{"order_id":orders[i]["order_id"]}}
                consume_event(db,None,envelope)
                consume_event(db,None,envelope)
                assert svc.get_order("owner",orders[i]["order_id"])["status"]=="FULFILLED"
            with db.transaction() as conn:
                for table in ("payment_attempts","payment_callbacks","bookings","tickets"):
                    assert conn.execute(f"SELECT count(*) AS n FROM {table}").fetchone()["n"]==payers
                assert conn.execute("SELECT count(*) AS n FROM (SELECT event_id,seat_id FROM bookings "
                                    "GROUP BY event_id,seat_id HAVING count(*)>1) duplicates").fetchone()["n"]==0
            if shared:
                assert general._shared_acquisition_budget.snapshot()["used"]==0
                metrics=client.get("/metrics").text
                for marker in ['state="pool_max_waiting"} 12.0','state="general_max_waiting"} 10.0',
                               'state="payment_max_waiting"} 10.0','state="shared_acquisition_limit"} 12.0']:
                    assert marker in metrics
        assert general.pool.closed and payment.pool.closed
    finally:
        for name in names:
            if name in previous:setattr(api.app.state,name,previous[name])
            elif hasattr(api.app.state,name):delattr(api.app.state,name)

@pytest.mark.parametrize("dominant",["general","payment"])
def test_shared_native_headroom_combined_overflow_timeout_and_recovery(system,dominant):
    _,db,_=system
    general,payment=create_api_databases(db.pool.conninfo,4,500,12,2,True)
    adapters={"general":general,"payment":payment}
    other="payment" if dominant=="general" else "general"
    for adapter in adapters.values():
        adapter.pool.resize(2,2)
        adapter.pool.wait(timeout=10)
    budget=general._shared_acquisition_budget
    try:
        with ExitStack() as held,ThreadPoolExecutor(max_workers=12) as executor:
            for adapter in adapters.values():
                held.enter_context(adapter.transaction())
                held.enter_context(adapter.transaction())
            futures=[executor.submit(adapters[dominant].pool.getconn) for _ in range(10)]
            deadline=time.monotonic()+.3
            wait_queue(adapters[dominant].pool,10,deadline)
            with pytest.raises(TooManyRequests):
                adapters[dominant].pool.getconn()
            futures.extend(executor.submit(adapters[other].pool.getconn) for _ in range(2))
            wait_queue(adapters[other].pool,2,deadline)
            assert budget.snapshot()["used"]==12
            with pytest.raises(TooManyRequests):
                adapters[other].pool.getconn()
            for future in futures:
                with pytest.raises(PoolTimeout):
                    future.result(timeout=2)
            # Native waiting positions remain until return. Their budget slots
            # must remain occupied even after the requesting threads timed out.
            assert sum(a.pool.get_stats()["requests_waiting"] for a in adapters.values())==12
            assert budget.snapshot()["retained"]==budget.snapshot()["used"]==12
            with pytest.raises(TooManyRequests):
                adapters[other].pool.getconn()
            held.close()
        assert all(a.pool.get_stats()["requests_waiting"]==0 for a in adapters.values())
        assert budget.snapshot()["used"]==0
        for adapter in adapters.values():
            with adapter.transaction() as conn:
                assert conn.execute("SELECT 1 AS n").fetchone()["n"]==1
        assert budget.snapshot()["used"]==0
    finally:
        payment.close()
        general.close()

@pytest.mark.parametrize("role",["general","payment"])
def test_shared_native_fifo_and_other_purpose_progress(system,role):
    _,db,_=system
    general,payment=create_api_databases(db.pool.conninfo,4,1000,12,2,True)
    target=general if role=="general" else payment
    other=payment if role=="general" else general
    for adapter in (general,payment):
        adapter.pool.resize(2,2)
        adapter.pool.wait(timeout=10)
    order=[]
    def checkout(index):
        with target.connection() as conn:
            order.append(index)
            conn.execute("SELECT 1")
    try:
        with ExitStack() as held,ExitStack() as primary,ThreadPoolExecutor(max_workers=10) as executor:
            held.enter_context(target.connection())
            primary.enter_context(target.connection())
            futures=[]
            deadline=time.monotonic()+.7
            for i in range(10):
                futures.append(executor.submit(checkout,i))
                wait_queue(target.pool,i+1,deadline)
            with other.transaction() as conn:
                assert conn.execute("SELECT 1 AS n").fetchone()["n"]==1
            primary.close()
            for f in futures:f.result(timeout=2)
        assert order==list(range(10))
        assert target._shared_acquisition_budget.snapshot()["used"]==0
    finally:
        payment.close()
        general.close()

def test_shared_pool_close_unblocks_waiters_without_leaking_budget(system):
    _,db,_=system
    general,payment=create_api_databases(db.pool.conninfo,4,1000,12,2,True)
    payment.pool.resize(2,2)
    payment.pool.wait(timeout=10)
    budget=payment._shared_acquisition_budget
    try:
        with payment.connection(),payment.connection(),ThreadPoolExecutor(max_workers=2) as executor:
            queued=[executor.submit(payment.pool.getconn) for _ in range(2)]
            wait_queue(payment.pool,2,time.monotonic()+.7)
            payment.close()
            for f in queued:
                with pytest.raises(PoolClosed):f.result(timeout=2)
            assert budget.snapshot()["used"]==0
    finally:
        payment.close()
        general.close()

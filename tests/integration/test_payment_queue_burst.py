"""Reproduce the measured fast payment queue rejection with real HTTP/SQL."""
from concurrent.futures import ThreadPoolExecutor
from contextlib import ExitStack
import hashlib
import hmac
import json
import time
from uuid import uuid4

import jwt
import pytest
from fastapi.testclient import TestClient

from conftest import NoShield
from ticketing import api
from ticketing.application.reservations import Reservations
from ticketing.infrastructure.postgres import Postgres
from ticketing.infrastructure.reservations import PostgresReservations
from ticketing.observability import DB_UNAVAILABLE
from ticketing.workers import consume_event

pytestmark = pytest.mark.integration


@pytest.mark.parametrize("payment_waiters", [6, 11])
def test_payment_queue_boundary_and_exact_recovery_replay(system, payment_waiters):
    svc,db,show=system
    with db.transaction() as conn:
        for seat in "DEFG":
            conn.execute("INSERT INTO event_seats(event_id,seat_id,price) VALUES (%s,%s,100)",(show,seat))
    orders=[svc.reserve("owner",show,[seat],str(uuid4())) for seat in "ABCDEFG"]
    keys=[str(uuid4()) for _ in orders]
    from ticketing.infrastructure.postgres import create_api_databases
    general,payment=create_api_databases(db.pool.conninfo,4,1000,12,2,payment_waiters)
    for pool in (general.pool,payment.pool):
        pool.resize(2,2)
        pool.wait(timeout=10)
    normal=Reservations(PostgresReservations(general,NoShield(),120))
    financial=Reservations(PostgresReservations(payment,NoShield(),120))
    previous=dict(api.app.dependency_overrides)
    api.app.dependency_overrides[api.service]=lambda:financial
    token=jwt.encode({"sub":"owner","exp":int(time.time())+60,"aud":"ticketing","iss":"ticketing"},
                     api.settings.jwt_secret,algorithm="HS256")
    client=TestClient(api.app)
    cause_before=DB_UNAVAILABLE.labels("TooManyRequests")._value.get()
    def initiate(index):
        return client.post("/v1/orders/"+orders[index]["order_id"]+"/payments",
                           json={"outcome":"SUCCEEDED"},
                           headers={"Authorization":"Bearer "+token,"Idempotency-Key":keys[index]})
    try:
        # A one-second test-only acquisition fence keeps the six held waiters
        # observable despite test-client scheduling; production deadlines stay fixed.
        with ExitStack() as held, ThreadPoolExecutor(max_workers=7) as executor:
            held.enter_context(payment.transaction())
            held.enter_context(payment.transaction())
            queued=[executor.submit(initiate,i) for i in range(6)]
            deadline=time.monotonic()+.7
            while payment.pool.get_stats()["requests_waiting"]!=6:
                assert time.monotonic()<deadline,"Six waiters did not reach the reproduction boundary"
                time.sleep(.001)
            assert normal.get_order("owner",orders[6]["order_id"])["status"]=="PENDING"
            assert general.pool.get_stats()["pool_available"]==2
            if payment_waiters == 6:
                rejected=initiate(6)
                assert rejected.status_code==503
                assert rejected.json()["code"]=="DATABASE_UNAVAILABLE"
                assert rejected.headers["Retry-After"]=="1"
                assert DB_UNAVAILABLE.labels("TooManyRequests")._value.get()==cause_before+1
                assert payment.pool.get_stats()["requests_errors"]==1
            else:
                queued.append(executor.submit(initiate,6))
                while payment.pool.get_stats()["requests_waiting"] != 7:
                    assert time.monotonic()<deadline
                    time.sleep(.001)
                assert DB_UNAVAILABLE.labels("TooManyRequests")._value.get()==cause_before
                assert payment.pool.get_stats().get("requests_errors",0)==0
            with db.transaction() as conn:
                assert conn.execute("SELECT count(*) AS n FROM payment_attempts").fetchone()["n"]==0
            held.close()
            responses=[f.result(timeout=3) for f in queued]
            assert all(r.status_code==202 for r in responses)
        if payment_waiters == 6:
            responses.append(initiate(6))
        assert responses[-1].status_code==202
        for i,response in enumerate(responses):
            repeated=initiate(i)
            assert repeated.status_code==202
            assert repeated.json()["payment_id"]==response.json()["payment_id"]
            body={"callback_id":str(uuid4()),"payment_id":response.json()["payment_id"],
                  "order_id":orders[i]["order_id"],"amount":100,"currency":"THB","outcome":"SUCCEEDED"}
            raw=json.dumps(body).encode()
            timestamp=str(int(time.time()))
            signature=hmac.new(api.settings.webhook_secret.encode(),
                               timestamp.encode()+b"."+raw,hashlib.sha256).hexdigest()
            headers={"X-Payment-Timestamp":timestamp,"X-Payment-Signature":signature,
                     "Content-Type":"application/json"}
            assert client.post("/v1/webhooks/payments",content=raw,headers=headers).json()["status"]=="book"
            assert client.post("/v1/webhooks/payments",content=raw,headers=headers).json()["status"]=="duplicate"
            envelope={"event_id":str(uuid4()),"schema_version":1,"event_type":"OrderPaid",
                      "payload":{"order_id":orders[i]["order_id"]}}
            consume_event(db,None,envelope)
            consume_event(db,None,envelope)
            assert svc.get_order("owner",orders[i]["order_id"])["status"]=="FULFILLED"
        with db.transaction() as conn:
            for table in ("payment_attempts","payment_callbacks","bookings","tickets"):
                assert conn.execute(f"SELECT count(*) AS n FROM {table}").fetchone()["n"]==7
            assert conn.execute("SELECT count(*) AS n FROM (SELECT event_id,seat_id FROM bookings "
                                "GROUP BY event_id,seat_id HAVING count(*)>1) duplicated").fetchone()["n"]==0
        assert payment.pool.get_stats()["pool_available"]==2
        assert general.pool.max_size+payment.pool.max_size==4
    finally:
        api.app.dependency_overrides.clear()
        api.app.dependency_overrides.update(previous)
        client.close()
        payment.close()
        general.close()

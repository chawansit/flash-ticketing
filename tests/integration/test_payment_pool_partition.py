"""Controlled pool exhaustion is separate from cloud capacity qualification."""
from concurrent.futures import ThreadPoolExecutor
from contextlib import contextmanager
from threading import Barrier, Event
from uuid import uuid4

import pytest
from conftest import NoShield
from psycopg_pool import PoolTimeout

from ticketing.application.reservations import Reservations
from ticketing.infrastructure.postgres import Postgres
from ticketing.infrastructure.reservations import PostgresReservations

pytestmark = pytest.mark.integration


def make_payment(svc, show):
    hold = svc.reserve("owner", show, ["A"], str(uuid4()))
    payment = svc.initiate_payment("owner", hold["order_id"], str(uuid4()), "SUCCEEDED", 0, 1)
    payload = {"callback_id": str(uuid4()), "payment_id": payment["payment_id"],
               "order_id": hold["order_id"], "amount": 100, "currency": "THB", "outcome": "SUCCEEDED"}
    return hold, payload


class PausedReads:
    def __init__(self, db, count):
        self.db = db
        self.arrived = Barrier(count + 1)
        self.release = Event()

    @contextmanager
    def transaction(self):
        with self.db.transaction() as conn:
            owner = self

            class Connection:
                def execute(self, query, params=None):
                    cursor = conn.execute(query, params)
                    if "FROM orders" in query:
                        owner.arrived.wait(timeout=2)
                        assert owner.release.wait(timeout=2), "Read boundary was not released"
                    return cursor

            yield Connection()


def test_shared_pool_reads_can_exhaust_payment_capacity(system):
    svc, db, show = system
    hold, payload = make_payment(svc, show)
    small = Postgres(db.pool.conninfo, maximum=4, wait_ms=50, maximum_waiting=4)
    small.pool.resize(4, 4)
    small.pool.wait(timeout=10)
    reads = PausedReads(small, 4)
    reader = Reservations(PostgresReservations(reads, NoShield(), 120))
    financial = Reservations(PostgresReservations(small, NoShield(), 120))
    try:
        with ThreadPoolExecutor(max_workers=4) as executor:
            futures = [executor.submit(reader.get_order, "owner", hold["order_id"]) for _ in range(4)]
            try:
                reads.arrived.wait(timeout=2)
                assert small.pool.get_stats()["pool_available"] == 0
                with pytest.raises(PoolTimeout):
                    financial.initiate_payment("owner", hold["order_id"], str(uuid4()), "SUCCEEDED", 0, 1)
                with pytest.raises(PoolTimeout):
                    financial.callback(payload)
            finally:
                reads.release.set()
            assert all(f.result(timeout=2)["status"] == "PENDING" for f in futures)
        assert financial.callback(payload)["status"] == "book"
        assert financial.callback(payload)["status"] == "duplicate"
        with db.transaction() as conn:
            assert conn.execute("SELECT count(*) AS n FROM bookings").fetchone()["n"] == 1
            assert conn.execute("SELECT count(*) AS n FROM payment_callbacks").fetchone()["n"] == 1
        assert small.pool.get_stats()["pool_available"] == 4
    finally:
        small.close()

def test_general_read_saturation_leaves_payment_initiation_and_callback_usable(system):
    from ticketing.infrastructure.postgres import create_api_databases
    from ticketing.workers import consume_event

    svc, db, show = system
    hold, payload = make_payment(svc, show)
    general, payment = create_api_databases(db.pool.conninfo,4,50,12,1)
    general.pool.resize(3,3)
    general.pool.wait(timeout=10)
    payment.pool.wait(timeout=10)
    reads = PausedReads(general,3)
    reader = Reservations(PostgresReservations(reads,NoShield(),120))
    financial = Reservations(PostgresReservations(payment,NoShield(),120))
    try:
        with ThreadPoolExecutor(max_workers=3) as executor:
            futures = [executor.submit(reader.get_order,"owner",hold["order_id"]) for _ in range(3)]
            try:
                reads.arrived.wait(timeout=2)
                assert general.pool.get_stats()["pool_available"] == 0
                attempt = financial.initiate_payment("owner",hold["order_id"],str(uuid4()),"SUCCEEDED",0,1)
                assert attempt["payment_id"] == payload["payment_id"]
                assert financial.callback(payload)["status"] == "book"
                assert financial.callback(payload)["status"] == "duplicate"
                assert general.pool.get_stats()["pool_available"] == 0
            finally:
                reads.release.set()
            assert all(f.result(timeout=2)["status"] == "PENDING" for f in futures)
        envelope = {"event_id":str(uuid4()),"schema_version":1,"event_type":"OrderPaid",
                    "payload":{"order_id":hold["order_id"]}}
        consume_event(db,None,envelope)
        consume_event(db,None,envelope)
        assert svc.get_order("owner",hold["order_id"])["status"] == "FULFILLED"
        with db.transaction() as conn:
            for table in ("bookings","tickets","payment_attempts","payment_callbacks"):
                assert conn.execute(f"SELECT count(*) AS n FROM {table}").fetchone()["n"] == 1
        assert general.pool.max_size + payment.pool.max_size == 4
        assert general.pool.get_stats()["pool_available"] == 3
        assert payment.pool.get_stats()["pool_available"] == 1
    finally:
        payment.close()
        general.close()


def test_payment_saturation_has_bounded_waiters_visible_errors_and_no_borrowing(system):
    import time

    from psycopg_pool import TooManyRequests

    from ticketing.infrastructure.postgres import create_api_databases

    svc,db,show = system
    hold,payload = make_payment(svc,show)
    general,payment = create_api_databases(db.pool.conninfo,4,150,4,1)
    financial = Reservations(PostgresReservations(payment,NoShield(),120))
    normal = Reservations(PostgresReservations(general,NoShield(),120))
    general.pool.wait(timeout=10)
    payment.pool.wait(timeout=10)
    try:
        with payment.transaction(), ThreadPoolExecutor(max_workers=1) as executor:
            queued = executor.submit(financial.callback,payload)
            deadline = time.monotonic()+1
            while payment.pool.get_stats()["requests_waiting"] != 1:
                assert time.monotonic()<deadline
                time.sleep(.001)
            with pytest.raises(TooManyRequests):
                financial.callback(payload)
            assert normal.get_order("owner",hold["order_id"])["status"] == "PENDING"
            # The only financial waiter times out visibly; it must not borrow
            # an available general connection or silently retry.
            with pytest.raises(PoolTimeout):
                queued.result(timeout=1)
        assert financial.callback(payload)["status"] == "book"
        assert financial.callback(payload)["status"] == "duplicate"
        with db.transaction() as conn:
            assert conn.execute("SELECT count(*) AS n FROM bookings").fetchone()["n"] == 1
            assert conn.execute("SELECT count(*) AS n FROM payment_callbacks").fetchone()["n"] == 1
    finally:
        payment.close()
        general.close()

def test_real_api_routes_complete_payment_while_general_reads_hold_every_general_connection(system, monkeypatch):
    import hashlib
    import hmac
    import json
    import os
    import time
    from dataclasses import replace

    import jwt
    from fastapi.testclient import TestClient

    from ticketing import api

    svc,db,show = system
    hold,payload = make_payment(svc,show)
    monkeypatch.setattr(api,"settings",replace(
        api.settings,database_url=db.pool.conninfo,redis_url=os.environ["TEST_REDIS_URL"],
        pool_max=4,pool_max_waiting=12,api_payment_pool_max=1,simulator_concurrency=1,
        reservation_mode="postgres",order_status_cache_ms=0,
    ))
    names=("db","payment_db","cache","reservations","payment_reservations","reservation_intake")
    previous={n:getattr(api.app.state,n) for n in names if hasattr(api.app.state,n)}
    token=jwt.encode({"sub":"owner","exp":int(time.time())+60,"aud":"ticketing","iss":"ticketing"},
                    api.settings.jwt_secret,algorithm="HS256")
    authorization={"Authorization":"Bearer "+token}
    try:
        with TestClient(api.app) as client:
            general=api.app.state.db
            financial=api.app.state.payment_db
            assert general is not financial
            general.pool.resize(3,3)
            general.pool.wait(timeout=10)
            financial.pool.wait(timeout=10)
            reads=PausedReads(general,3)
            api.app.state.reservations.store.db=reads
            with ThreadPoolExecutor(max_workers=3) as executor:
                futures=[executor.submit(client.get,"/v1/orders/"+hold["order_id"],headers=authorization)
                         for _ in range(3)]
                try:
                    reads.arrived.wait(timeout=2)
                    assert general.pool.get_stats()["pool_available"]==0
                    response=client.post(
                        "/v1/orders/"+hold["order_id"]+"/payments",json={"outcome":"SUCCEEDED"},
                        headers={**authorization,"Idempotency-Key":str(uuid4())},
                    )
                    assert response.status_code==202
                    assert response.json()["payment_id"]==payload["payment_id"]
                    raw=json.dumps(payload).encode()
                    timestamp=str(int(time.time()))
                    signature=hmac.new(api.settings.webhook_secret.encode(),
                                       timestamp.encode()+b"."+raw,hashlib.sha256).hexdigest()
                    headers={"X-Payment-Timestamp":timestamp,"X-Payment-Signature":signature,
                             "Content-Type":"application/json"}
                    response=client.post("/v1/webhooks/payments",content=raw,headers=headers)
                    assert response.status_code==200 and response.json()["status"]=="book"
                    assert general.pool.get_stats()["pool_available"]==0
                finally:
                    reads.release.set()
                assert all(f.result(timeout=2).status_code==200 for f in futures)
            api.app.state.reservations.store.db=general
            response=client.get("/metrics")
            for marker in ['state="pool_max"} 4.0','state="general_pool_max"} 3.0',
                           'state="payment_pool_max"} 1.0','state="pool_max_waiting"} 12.0',
                           'state="general_max_waiting"} 9.0','state="payment_max_waiting"} 3.0']:
                assert marker in response.text
            assert client.post("/v1/webhooks/payments",content=raw,headers=headers).json()["status"]=="duplicate"
            other=jwt.encode({"sub":"other","exp":int(time.time())+60,"aud":"ticketing","iss":"ticketing"},
                             api.settings.jwt_secret,algorithm="HS256")
            assert client.get("/v1/orders/"+hold["order_id"],headers={"Authorization":"Bearer "+other}).status_code==404
        assert general.pool.closed and financial.pool.closed
        with db.transaction() as conn:
            assert conn.execute("SELECT count(*) AS n FROM bookings").fetchone()["n"]==1
            assert conn.execute("SELECT count(*) AS n FROM payment_callbacks").fetchone()["n"]==1
    finally:
        for name in names:
            if name in previous:
                setattr(api.app.state,name,previous[name])
            elif hasattr(api.app.state,name):
                delattr(api.app.state,name)

"""Real SQL and signed HTTP recovery at ambiguous commit/acknowledgement boundaries.

The loopback proxy drops a socket response only after the actual ASGI route
returns. Explicit recovery calls do not change the no-retry capacity generator.
"""
import hashlib
import hmac
import json
import os
import socket
import time
from concurrent.futures import ThreadPoolExecutor
from contextlib import contextmanager
from dataclasses import replace
from http.client import RemoteDisconnected
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from threading import Event, Thread
from uuid import uuid4

import httpx
import jwt
import pytest
from fastapi.testclient import TestClient

from ticketing import api, workers
from ticketing.infrastructure.payment_transport import CallbackTransport

pytestmark = pytest.mark.integration


class ApiProcess:
    """Recreate API lifespan/pools; the OS process itself is not restarted."""

    def __init__(self):
        self.client = None
        self.restart()

    def restart(self):
        if self.client is not None:
            old_general = api.app.state.db
            old_payment = api.app.state.payment_db
            self.client.__exit__(None, None, None)
            assert old_general.pool.closed and old_payment.pool.closed
        self.client = TestClient(api.app)
        self.client.__enter__()
        for adapter in {api.app.state.db, api.app.state.payment_db}:
            adapter.pool.wait(timeout=10)

    def close(self):
        self.client.__exit__(None, None, None)


@pytest.fixture(params=["normal", "isolated_shared"])
def process(system, monkeypatch, request):
    _, db, _ = system
    redis_url = os.environ.get("TEST_REDIS_URL")
    if not redis_url:
        pytest.skip("TEST_REDIS_URL not configured")
    isolated = request.param == "isolated_shared"
    monkeypatch.setattr(api, "settings", replace(
        api.settings, environment="development", database_url=db.pool.conninfo,
        redis_url=redis_url, reservation_mode="postgres", order_status_cache_ms=0, simulator_concurrency=1,
        pool_max=4 if isolated else 3, pool_max_waiting=12 if isolated else 3,
        api_payment_pool_max=2 if isolated else 0, api_pool_shared_waiting=isolated,
    ))
    names = ("db", "payment_db", "cache", "reservations", "payment_reservations", "reservation_intake")
    previous = {name: getattr(api.app.state, name) for name in names if hasattr(api.app.state, name)}
    instance = None
    try:
        instance = ApiProcess()
        yield instance
        if isolated:
            assert api.app.state.db._shared_acquisition_budget.snapshot()["used"] == 0
    finally:
        if instance is not None:
            instance.close()
        for name in names:
            if name in previous:
                setattr(api.app.state, name, previous[name])
            elif hasattr(api.app.state, name):
                delattr(api.app.state, name)


@contextmanager
def proxy(process, drop_path=None):
    """Actual loopback sockets; the application remains an in-process ASGI app."""

    class Handler(BaseHTTPRequestHandler):
        protocol_version = "HTTP/1.1"

        def do_POST(self):
            raw = self.rfile.read(int(self.headers["Content-Length"]))
            forwarded = {name: value for name, value in self.headers.items()
                         if name.lower() not in {"host", "connection", "content-length"}}
            response = process.client.post(self.path, content=raw, headers=forwarded)
            server.responses.append((self.path, response.status_code, response.json()))
            server.bodies.append(raw)
            if server.drop_path == self.path:
                server.drop_path = None
                self.close_connection = True
                self.connection.shutdown(socket.SHUT_RDWR)
                self.connection.close()
                return
            self.send_response(response.status_code)
            self.send_header("Content-Type", "application/json")
            self.send_header("Content-Length", str(len(response.content)))
            self.end_headers()
            self.wfile.write(response.content)

        def log_message(self, *_):
            pass

    server = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
    server.daemon_threads = True
    server.drop_path = drop_path
    server.responses = []
    server.bodies = []
    thread = Thread(target=server.serve_forever)
    thread.start()
    try:
        yield server, f"http://127.0.0.1:{server.server_port}"
    finally:
        server.shutdown()
        server.server_close()
        thread.join(3)
        assert not thread.is_alive()


def authorization(actor="owner"):
    token = jwt.encode(
        {"sub": actor, "exp": int(time.time()) + 600, "aud": "ticketing", "iss": "ticketing"},
        api.settings.jwt_secret, algorithm="HS256",
    )
    return {"Authorization": "Bearer " + token}


def initiate(process, order, key, actor="owner", **changes):
    return process.client.post(
        "/v1/orders/" + order["order_id"] + "/payments",
        json={"outcome": "SUCCEEDED", "delay_seconds": 0, "duplicates": 1, **changes},
        headers={**authorization(actor), "Idempotency-Key": key},
    )


def hold_deadline(db):
    with db.transaction() as conn:
        return conn.execute("SELECT expires_at FROM holds").fetchone()["expires_at"]


def callback_body(order, attempt):
    return {"callback_id": attempt["payment_id"], "payment_id": attempt["payment_id"],
            "order_id": order["order_id"], "amount": order["total"],
            "currency": order["currency"], "outcome": "SUCCEEDED"}


def signed_callback(process, body):
    raw = json.dumps(body).encode()
    timestamp = str(int(time.time()))
    signature = hmac.new(api.settings.webhook_secret.encode(),
                         timestamp.encode() + b"." + raw, hashlib.sha256).hexdigest()
    return process.client.post(
        "/v1/webhooks/payments", content=raw,
        headers={"Content-Type": "application/json", "X-Payment-Timestamp": timestamp,
                 "X-Payment-Signature": signature},
    )


def envelope(db, kind):
    with db.transaction() as conn:
        rows = conn.execute("SELECT id,payload FROM outbox_events WHERE event_type=%s", (kind,)).fetchall()
        assert len(rows) == 1
    return {"event_id": str(rows[0]["id"]), "schema_version": 1,
            "event_type": kind, "payload": rows[0]["payload"]}


def fulfill(db):
    message = envelope(db, "OrderPaid")
    workers.consume_event(db, None, message)
    workers.consume_event(db, None, message)


def assert_exact_paid(db, deadline):
    with db.transaction() as conn:
        for table in ("payment_attempts", "payment_callbacks", "bookings", "tickets"):
            assert conn.execute(f"SELECT count(*) AS n FROM {table}").fetchone()["n"] == 1
        assert conn.execute("SELECT status FROM payment_attempts").fetchone()["status"] == "SUCCEEDED"
        assert conn.execute("SELECT status FROM orders").fetchone()["status"] == "FULFILLED"
        for kind in ("OrderPaid", "TicketsIssued"):
            assert conn.execute("SELECT count(*) AS n FROM outbox_events WHERE event_type=%s",
                                (kind,)).fetchone()["n"] == 1
        assert conn.execute("SELECT count(*) AS n FROM refund_requests").fetchone()["n"] == 0
        assert conn.execute("SELECT count(*) AS n FROM (SELECT event_id,seat_id FROM bookings "
                            "GROUP BY event_id,seat_id HAVING count(*)>1) d").fetchone()["n"] == 0
    assert hold_deadline(db) == deadline


def test_lost_payment_response_replays_after_pool_restart_without_another_payment(system, process):
    svc, db, show = system
    order = svc.reserve("owner", show, ["A"], "hold")
    deadline = hold_deadline(db)
    key = "payment-" + str(uuid4())
    path = "/v1/orders/" + order["order_id"] + "/payments"
    with proxy(process, drop_path=path) as (server, origin):
        with (httpx.Client(base_url=origin, timeout=3, trust_env=False) as client,
              pytest.raises(httpx.RemoteProtocolError)):
                client.post(path, json={"outcome": "SUCCEEDED", "delay_seconds": 0, "duplicates": 1},
                            headers={**authorization(), "Idempotency-Key": key})
        assert len(server.responses) == 1 and server.responses[0][1] == 202
        committed = server.responses[0][2]
    with db.transaction() as conn:
        assert conn.execute("SELECT count(*) AS n FROM payment_attempts").fetchone()["n"] == 1
        assert conn.execute("SELECT response FROM idempotency_records WHERE operation='payment' "
                            "AND key=%s", (key,)).fetchone()["response"] == committed
    process.restart()
    recovered = initiate(process, order, key)
    assert recovered.status_code == 202 and recovered.json() == committed
    changed = initiate(process, order, key, duplicates=2)
    assert changed.status_code == 409 and changed.json()["code"] == "IDEMPOTENCY_MISMATCH"
    unauthorized = initiate(process, order, key, actor="another")
    assert unauthorized.status_code == 404 and unauthorized.json()["code"] == "ORDER_NOT_FOUND"
    assert signed_callback(process, callback_body(order, committed)).json()["status"] == "book"
    fulfill(db)
    assert_exact_paid(db, deadline)
    with db.transaction() as conn:
        assert conn.execute("SELECT count(*) AS n FROM idempotency_records "
                            "WHERE operation='payment'").fetchone()["n"] == 1


def expire_lease(db):
    # Advance only this isolated fixture; never change production lease constants.
    with db.transaction() as conn:
        conn.execute("UPDATE payment_attempts SET lease_until=clock_timestamp()-interval '1 second'")


def delivery_row(db):
    with db.transaction() as conn:
        return conn.execute("SELECT deliveries,lease_token,lease_until FROM payment_attempts").fetchone()


def test_lost_callback_response_commits_once_then_recovers_through_existing_lease(system, process):
    svc, db, show = system
    order = svc.reserve("owner", show, ["A"], "hold")
    assert initiate(process, order, "payment").status_code == 202
    deadline = hold_deadline(db)
    path = "/v1/webhooks/payments"
    with proxy(process, drop_path=path) as (server, origin):
        with_transport = CallbackTransport(origin + path, 2, timeout=3)
        try:
            with pytest.raises(RemoteDisconnected):
                workers.simulate_one(db, api.settings, with_transport)
            assert len(server.responses) == 1 and server.responses[0][2]["status"] == "book"
            assert delivery_row(db)["deliveries"] == 0
            assert delivery_row(db)["lease_until"] is not None
            assert not workers.simulate_one(db, api.settings, with_transport)
            assert len(server.responses) == 1  # No inline or pre-expiry retry.
            process.restart()
            expire_lease(db)
            assert workers.simulate_one(db, api.settings, with_transport)
            assert not workers.simulate_one(db, api.settings, with_transport)
            assert len(server.responses) == 2 and server.responses[1][2]["status"] == "duplicate"
            assert json.loads(server.bodies[0]) == json.loads(server.bodies[1])
        finally:
            with_transport.close()
    assert delivery_row(db)["deliveries"] == 1 and delivery_row(db)["lease_until"] is None
    fulfill(db)
    assert_exact_paid(db, deadline)


class TransactionFault:
    """Fail a selected public transaction before or after its durable commit."""

    def __init__(self, db, ordinal, after_commit=False):
        self.db, self.ordinal, self.after_commit = db, ordinal, after_commit
        self.calls = 0

    def __getattr__(self, name):
        return getattr(self.db, name)

    @contextmanager
    def transaction(self):
        self.calls += 1
        selected = self.calls == self.ordinal
        with self.db.transaction() as conn:
            yield conn
            if selected and not self.after_commit:
                raise RuntimeError("injected before acknowledgement commit")
        if selected and self.after_commit:
            raise RuntimeError("injected after financial commit")


def test_callback_acknowledgement_failure_retains_lease_then_replays_exactly(system, process):
    svc, db, show = system
    order = svc.reserve("owner", show, ["A"], "hold")
    assert initiate(process, order, "payment").status_code == 202
    deadline = hold_deadline(db)
    path = "/v1/webhooks/payments"
    with proxy(process) as (server, origin):
        transport = CallbackTransport(origin + path, 2, timeout=3)
        try:
            fault = TransactionFault(db, ordinal=2)
            with pytest.raises(RuntimeError, match="before acknowledgement"):
                workers.simulate_one(fault, api.settings, transport)
            assert fault.calls == 2
            assert server.responses[0][2]["status"] == "book"
            assert delivery_row(db)["deliveries"] == 0 and delivery_row(db)["lease_until"] is not None
            assert not workers.simulate_one(db, api.settings, transport)
            assert len(server.responses) == 1
            expire_lease(db)
            assert workers.simulate_one(db, api.settings, transport)
            assert not workers.simulate_one(db, api.settings, transport)
            assert len(server.responses) == 2 and server.responses[1][2]["status"] == "duplicate"
        finally:
            transport.close()
    assert delivery_row(db)["deliveries"] == 1 and delivery_row(db)["lease_until"] is None
    fulfill(db)
    assert_exact_paid(db, deadline)


def test_stale_callback_worker_cannot_acknowledge_a_new_lease_owner(system, process):
    svc, db, show = system
    order = svc.reserve("owner", show, ["A"], "hold")
    assert initiate(process, order, "payment").status_code == 202
    deadline = hold_deadline(db)
    delivered, release = Event(), Event()
    path = "/v1/webhooks/payments"
    with proxy(process) as (server, origin):
        transport = CallbackTransport(origin + path, 2, timeout=3)

        class DelayedAcknowledgement:
            def post(self, body, headers):
                transport.post(body, headers)
                delivered.set()
                assert release.wait(5)

        try:
            with ThreadPoolExecutor(max_workers=1) as executor:
                first = executor.submit(workers.simulate_one, db, api.settings, DelayedAcknowledgement())
                try:
                    assert delivered.wait(5)
                    old_token = delivery_row(db)["lease_token"]
                    expire_lease(db)
                    assert workers.simulate_one(db, api.settings, transport)
                    replacement = delivery_row(db)
                    assert replacement["lease_token"] != old_token
                    assert replacement["deliveries"] == 1 and replacement["lease_until"] is None
                finally:
                    release.set()
                assert first.result(timeout=5)
            assert delivery_row(db) == replacement  # Stale UPDATE changed nothing.
            assert not workers.simulate_one(db, api.settings, transport)
            assert len(server.responses) == 2
        finally:
            release.set()
            transport.close()
    fulfill(db)
    assert_exact_paid(db, deadline)


def test_fulfillment_commit_lost_acknowledgement_replays_same_inbox_event(system, process):
    svc, db, show = system
    order = svc.reserve("owner", show, ["A"], "hold")
    attempt = initiate(process, order, "payment").json()
    deadline = hold_deadline(db)
    assert signed_callback(process, callback_body(order, attempt)).json()["status"] == "book"
    message = envelope(db, "OrderPaid")
    fault = TransactionFault(db, ordinal=1, after_commit=True)
    with pytest.raises(RuntimeError, match="after financial commit"):
        workers.consume_event(fault, None, message)
    assert fault.calls == 1
    assert_exact_paid(db, deadline)
    process.restart()
    # A fresh worker adapter has no process-local inbox/deduplication memory.
    recovered_db = api.app.state.payment_db
    workers.consume_event(recovered_db, None, message)
    assert_exact_paid(db, deadline)
    with db.transaction() as conn:
        assert conn.execute("SELECT count(*) AS n FROM consumer_inbox "
                            "WHERE event_id=%s", (message["event_id"],)).fetchone()["n"] == 1


def test_expired_hold_payment_replay_preserves_deadline_and_refunds_late_capture(system, process):
    svc, db, show = system
    order = svc.reserve("owner", show, ["A"], "hold")
    attempt = initiate(process, order, "payment").json()
    with db.transaction() as conn:
        conn.execute("UPDATE holds SET expires_at=clock_timestamp()-interval '1 second'")
        conn.execute("UPDATE event_seats SET reserved_until=clock_timestamp()-interval '1 second' "
                     "WHERE hold_id IS NOT NULL")
    expired_deadline = hold_deadline(db)
    process.restart()
    recovered = initiate(process, order, "payment")
    assert recovered.status_code == 202 and recovered.json() == attempt
    assert hold_deadline(db) == expired_deadline
    payload = callback_body(order, attempt)
    assert signed_callback(process, payload).json()["status"] == "refund"
    assert signed_callback(process, payload).json()["status"] == "duplicate"
    message = envelope(db, "RefundRequested")
    workers.consume_event(db, None, message)
    workers.consume_event(db, None, message)
    with db.transaction() as conn:
        for table in ("payment_attempts", "payment_callbacks", "refund_requests"):
            assert conn.execute(f"SELECT count(*) AS n FROM {table}").fetchone()["n"] == 1
        for table in ("bookings", "tickets"):
            assert conn.execute(f"SELECT count(*) AS n FROM {table}").fetchone()["n"] == 0
        assert conn.execute("SELECT status FROM orders").fetchone()["status"] == "REFUNDED"
        assert conn.execute("SELECT status FROM refund_requests").fetchone()["status"] == "REFUNDED"
        assert conn.execute("SELECT count(*) AS n FROM event_seats WHERE hold_id IS NOT NULL "
                            "OR booked_order_id IS NOT NULL").fetchone()["n"] == 0
    assert hold_deadline(db) == expired_deadline

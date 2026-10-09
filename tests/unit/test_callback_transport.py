"""Real HTTP tests for connection ownership and ambiguous failure without replay."""

import socket
import threading
from concurrent.futures import ThreadPoolExecutor
from http.client import RemoteDisconnected
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from urllib.error import HTTPError

import pytest

from ticketing.infrastructure.payment_transport import CallbackTransport


@pytest.fixture
def endpoint():
    class Handler(BaseHTTPRequestHandler):
        protocol_version = "HTTP/1.1"

        def do_POST(self):
            body = self.rfile.read(int(self.headers["Content-Length"]))
            with self.server.guard:
                self.server.requests.append((body, dict(self.headers), self.path))
                self.server.active += 1
                self.server.peak = max(self.server.peak, self.server.active)
                if self.server.active == 2:
                    self.server.two_active.set()
                if self.server.active == 12:
                    self.server.twelve_active.set()
            try:
                if self.server.gate is not None:
                    assert self.server.gate.wait(3)
                if self.server.drop:
                    self.connection.shutdown(socket.SHUT_RDWR)
                    self.connection.close()
                    return
                self.send_response(self.server.status)
                self.send_header("Content-Length", "2")
                if self.server.force_close:
                    self.send_header("Connection", "close")
                    self.close_connection = True
                self.end_headers()
                self.wfile.write(b"{}")
            finally:
                with self.server.guard:
                    self.server.active -= 1

        def log_message(self, *_args):
            pass

    class Server(ThreadingHTTPServer):
        daemon_threads = True
        request_queue_size = 32

        def get_request(self):
            connection = super().get_request()
            self.accepted += 1
            return connection

    server = Server(("127.0.0.1", 0), Handler)
    server.guard = threading.Lock()
    server.two_active = threading.Event()
    server.twelve_active = threading.Event()
    server.accepted = server.active = server.peak = 0
    server.requests = []
    server.status = 200
    server.force_close = server.drop = False
    server.gate = None
    thread = threading.Thread(target=server.serve_forever)
    thread.start()
    try:
        yield server, f"http://127.0.0.1:{server.server_port}/v1/webhooks/payments"
    finally:
        if server.gate is not None:
            server.gate.set()
        server.shutdown()
        server.server_close()
        thread.join(3)
        assert not thread.is_alive()


def test_reuses_connection_and_preserves_signed_bytes_headers_and_path(endpoint):
    server, url = endpoint
    transport = CallbackTransport(url, 2)
    raw = b'{"callback_id": "same-id", "amount": 100}'
    headers = {
        "Content-Type": "application/json",
        "X-Payment-Timestamp": "123",
        "X-Payment-Signature": "signed-test-bytes",
    }
    try:
        for _ in range(6):
            transport.post(raw, headers)
        assert server.accepted == 1
        assert len(server.requests) == 6
        for body, observed, path in server.requests:
            assert body == raw and path == "/v1/webhooks/payments"
            assert all(observed[key] == value for key, value in headers.items())
            assert observed.get("Connection") != "close"
    finally:
        transport.close()
    with pytest.raises(RuntimeError, match="closed"):
        transport.post(raw, headers)


def test_server_close_reconnects_only_for_a_later_delivery(endpoint):
    server, url = endpoint
    server.force_close = True
    transport = CallbackTransport(url, 1)
    try:
        transport.post(b"{}", {})
        transport.post(b"{}", {})
        assert server.accepted == len(server.requests) == 2
    finally:
        transport.close()


@pytest.mark.parametrize("status", [302, 429, 503])
def test_non_success_and_redirect_do_not_retry_or_lose_pool_slot(endpoint, status):
    server, url = endpoint
    transport = CallbackTransport(url, 1)
    try:
        server.status = status
        with pytest.raises(HTTPError) as error:
            transport.post(b"{}", {})
        assert error.value.code == status
        assert len(server.requests) == 1
        server.status = 200
        transport.post(b"{}", {})
        assert len(server.requests) == 2
    finally:
        transport.close()


def test_dropped_response_is_not_replayed_inside_transport(endpoint):
    server, url = endpoint
    transport = CallbackTransport(url, 1)
    try:
        server.drop = True
        with pytest.raises(RemoteDisconnected):
            transport.post(b"{}", {})
        assert len(server.requests) == 1
        server.drop = False
        transport.post(b"{}", {})
        assert len(server.requests) == 2
    finally:
        transport.close()


def test_concurrent_requests_never_share_a_connection_or_exceed_pool(endpoint):
    server, url = endpoint
    server.gate = threading.Event()
    transport = CallbackTransport(url, 2, timeout=2)
    try:
        with ThreadPoolExecutor(max_workers=6) as executor:
            futures = [executor.submit(transport.post, str(i).encode(), {}) for i in range(6)]
            assert server.two_active.wait(1)
            # Acquisition must be bounded even while both connections are occupied.
            with pytest.raises(TimeoutError, match="acquisition"):
                # Bound acquisition separately from the existing socket timeout.
                old_timeout = transport.timeout
                transport.timeout = 0.05
                try:
                    transport.post(b"extra", {})
                finally:
                    transport.timeout = old_timeout
            server.gate.set()
            for future in futures:
                future.result(timeout=3)
        assert server.peak <= 2 and server.accepted == 2
        assert len(server.requests) == 6
        assert {body for body, *_ in server.requests} == {str(i).encode() for i in range(6)}
    finally:
        server.gate.set()
        transport.close()


@pytest.mark.parametrize(
    "url",
    [
        "ftp://example.com/callback",
        "http:///callback",
        "http://user:password@example.com/callback",
        "http://example.com/callback?token=private",
        "http://example.com/callback#fragment",
    ],
)
def test_unsafe_or_ambiguous_endpoint_rejected_without_network(url):
    with pytest.raises(ValueError, match="endpoint"):
        CallbackTransport(url, 1)


def test_invalid_capacity_rejected():
    with pytest.raises(ValueError, match="bounds"):
        CallbackTransport("http://example.com/callback", 0)


def test_twelve_bounded_http_connections_can_deliver_concurrently(endpoint):
    server, url = endpoint
    server.gate = threading.Event()
    transport = CallbackTransport(url, 12, timeout=3)
    try:
        with ThreadPoolExecutor(max_workers=12) as executor:
            futures = [executor.submit(transport.post, str(i).encode(), {}) for i in range(12)]
            try:
                assert server.twelve_active.wait(3)
                assert server.peak == 12
            finally:
                server.gate.set()
            for future in futures:
                future.result(timeout=5)
        assert server.accepted == len(server.requests) == 12
        assert {body for body, *_ in server.requests} == {str(i).encode() for i in range(12)}
    finally:
        server.gate.set()
        transport.close()

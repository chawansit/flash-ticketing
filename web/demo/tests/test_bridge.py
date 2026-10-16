import importlib.util
import json
import threading
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from urllib.error import HTTPError
from urllib.request import Request, urlopen

import pytest

spec = importlib.util.spec_from_file_location('encore_bridge', Path(__file__).parents[1] / 'serve.py')
bridge = importlib.util.module_from_spec(spec)
spec.loader.exec_module(bridge)
EVENT = '10000000-0000-4000-8000-000000000001'


@pytest.fixture
def running():
    calls = []
    class Engine(BaseHTTPRequestHandler):
        def log_message(self, *args):
            pass
        def do_GET(self):
            self.respond()
        def do_POST(self):
            self.respond()
        def respond(self):
            calls.append({'method': self.command, 'path': self.path, 'auth': self.headers.get('Authorization'), 'key': self.headers.get('Idempotency-Key')})
            if self.path == '/metrics':
                body = b'ticketing_http_requests_total{route="/v1/orders/{id}",method="GET",status="503"} 2\nticketing_http_requests_total{route="/v1/holds",method="POST",status="201"} 5\nticketing_db_unavailable_total{cause="slot"} 2\n# SECRET COMMENT\nunrelated_metric{secret="hidden"} 100\n'
            else:
                body = json.dumps({'ok': True}).encode()
            self.send_response(200);self.send_header('Content-Type', 'application/json');self.send_header('ETag', 'W/"map1"');self.send_header('X-Poll-Interval-Ms', '1200');self.end_headers();self.wfile.write(body)
    engine = ThreadingHTTPServer(('127.0.0.1', 0), Engine)
    server = bridge.DemoServer(('127.0.0.1', 0), f'http://127.0.0.1:{engine.server_port}')
    threads = [threading.Thread(target=s.serve_forever, daemon=True) for s in (engine, server)]
    for thread in threads:
        thread.start()
    try:
        yield server, calls
    finally:
        for s in (server, engine):
            s.shutdown();s.server_close()
        for thread in threads:
            thread.join(2)


def get(server, path, method='GET', headers=None, body=None):
    data = json.dumps(body).encode() if body is not None else None
    request = Request(f'http://127.0.0.1:{server.server_port}'+path, data=data, method=method,
                      headers={'X-Demo-Client': 'encore', **({'Content-Type': 'application/json'} if body is not None else {}), **(headers or {})})
    try:
        response = urlopen(request, timeout=3)
    except HTTPError as error:
        response = error
    with response:
        return response.status, response.headers, response.read()


def test_static_preview_and_config_keep_upstream_private(running):
    server, calls = running
    status, _, body = get(server, '/')
    assert status == 200 and b'Encore' in body
    _, _, body = get(server, '/demo-config')
    assert json.loads(body)['engineConfigured'] and b'127.0.0.1' not in body and not calls


def test_existing_contract_headers_and_status_poll_hint_survive(running):
    server, calls = running
    status, headers, _ = get(server, '/engine/v1/holds', method='POST', headers={'Authorization': 'Bearer synthetic', 'Idempotency-Key': 'same-key'}, body={'event_id': EVENT, 'seat_ids': ['A1']})
    assert status == 200 and headers['X-Poll-Interval-Ms'] == '1200'
    assert calls == [{'method': 'POST', 'path': '/v1/holds', 'auth': 'Bearer synthetic', 'key': 'same-key'}]


@pytest.mark.parametrize('path', ['/engine/v1/webhooks/payments', '/engine/health/ready', '/engine/v1/events?url=http://other', '/engine/https://other', '/engine/v1/events/../../metrics'])
def test_unsupported_route_never_reaches_engine(running, path):
    server, calls = running
    assert get(server, path)[0] == 404 and not calls


def test_origin_and_host_guards_block_remote_mutation(running):
    server, calls = running
    for headers in [{'Origin': 'https://other.example'}, {'Host': 'evil.example'}, {'Sec-Fetch-Site': 'cross-site'}]:
        assert get(server, '/engine/v1/holds', method='POST', headers=headers, body={})[0] == 403
    assert not calls


def test_protected_routes_require_customer_token(running):
    server, calls = running
    assert get(server, f'/engine/v1/orders/{EVENT}')[0] == 401 and not calls


def test_metrics_relay_returns_only_allowlisted_aggregate_values(running):
    server, _ = running
    status, _, raw = get(server, '/engine/metrics')
    value = json.loads(raw)
    assert status == 200 and value['httpRequests'] == 7 and value['httpErrors'] == 2 and value['dbUnavailable'] == 2
    assert b'hidden' not in raw and b'SECRET' not in raw and b'/v1/orders' not in raw


def test_no_arbitrary_bind_or_credentialed_upstream():
    with pytest.raises(ValueError):
        bridge.DemoServer(('0.0.0.0', 0))
    for value in ['file:///tmp', 'https://user:pass@host', 'http://host/path', 'http://host?query=1']:
        with pytest.raises(ValueError):
            bridge.validate_upstream(value)

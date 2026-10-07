"""Native local Nginx forwarding only; this does not qualify financial capacity."""
import hashlib
import hmac
import json
import subprocess
import sys
import time
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from urllib.error import HTTPError
from urllib.request import urlopen
from uuid import uuid4

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[2] / "scripts"))
from prepare_two_host_scaling import nginx_config

from ticketing.infrastructure.payment_transport import CallbackTransport

SERVER = r"""
import hashlib, hmac, json, sys
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
class Handler(BaseHTTPRequestHandler):
    protocol_version = 'HTTP/1.1'
    def log_message(self, *args): pass
    def do_POST(self):
        raw = self.rfile.read(int(self.headers['Content-Length']))
        timestamp = self.headers.get('X-Payment-Timestamp', '')
        expected = hmac.new(b'local-route-test', timestamp.encode() + b'.' + raw, hashlib.sha256).hexdigest()
        valid = hmac.compare_digest(expected, self.headers.get('X-Payment-Signature', ''))
        path_ok = self.path == '/v1/webhooks/payments'
        print(json.dumps({'backend': sys.argv[1], 'body': raw.decode(), 'timestamp': timestamp, 'valid': valid, 'path_ok': path_ok}), flush=True)
        self.send_response(200 if valid and path_ok else 401)
        self.send_header('Content-Length', '0')
        self.end_headers()
ThreadingHTTPServer(('0.0.0.0', 8101), Handler).serve_forever()
"""


def docker(*args):
    return subprocess.run(['docker', *args], capture_output=True, text=True, check=True, timeout=30).stdout.strip()


@pytest.mark.integration
def test_signed_duplicate_callbacks_reach_all_four_backends_without_proxy_retry(tmp_path):
    docker('info', '--format', '{{.ServerVersion}}')
    for image in ('python:3.12-slim', 'nginx:1.27.5-alpine'):
        docker('image', 'inspect', image)
    prefix = 'callback-route-local-' + uuid4().hex[:12]
    network = prefix + '-network'
    owned = []
    transport = None
    docker('network', 'create', network)
    try:
        apis = []
        for i in range(4):
            name = prefix + '-api-' + str(i)
            owned.append(name)
            docker('run', '-d', '--name', name, '--network', network, 'python:3.12-slim',
                   'python', '-u', '-c', SERVER, str(i))
            row = json.loads(docker('inspect', name))[0]
            apis.append({'private_ipv4': row['NetworkSettings']['Networks'][network]['IPAddress'], 'port': 8101})
        path = tmp_path / 'nginx.conf'
        path.write_text(nginx_config(apis))
        proxy = prefix + '-proxy'
        owned.append(proxy)
        docker('run', '-d', '--name', proxy, '--network', network, '-p', '127.0.0.1::8000',
               '--mount', 'type=bind,source=' + str(path.resolve()) + ',target=/etc/nginx/nginx.conf,readonly',
               'nginx:1.27.5-alpine')
        row = json.loads(docker('inspect', proxy))[0]
        port = row['NetworkSettings']['Ports']['8000/tcp'][0]['HostPort']
        origin = 'http://127.0.0.1:' + port
        deadline = time.monotonic() + 15
        while True:
            try:
                with urlopen(origin + '/lb-health', timeout=1) as response:
                    assert response.status == 200
                break
            except OSError:
                if time.monotonic() >= deadline: raise
                time.sleep(.1)
        raw = json.dumps({'callback_id': 'same-delivery', 'payment_id': 'same-payment', 'outcome': 'success'}).encode()
        timestamp = '1791417600'
        signature = hmac.new(b'local-route-test', timestamp.encode() + b'.' + raw, hashlib.sha256).hexdigest()
        headers = {'Content-Type': 'application/json', 'X-Payment-Timestamp': timestamp, 'X-Payment-Signature': signature}
        transport = CallbackTransport(origin + '/v1/webhooks/payments', 8, timeout=5)
        with ThreadPoolExecutor(max_workers=8) as workers:
            list(workers.map(lambda _: transport.post(raw, headers), range(32)))
        # A rejected callback is forwarded once, not retried at the proxy/transport.
        with pytest.raises(HTTPError) as rejection:
            transport.post(raw, {**headers, 'X-Payment-Signature': 'invalid'})
        assert rejection.value.code == 401
        logs = [json.loads(line) for name in owned[:-1] for line in docker('logs', name).splitlines() if line.startswith('{')]
        assert len(logs) == 33
        assert sum(r['valid'] for r in logs) == 32
        assert {r['backend'] for r in logs if r['valid']} == {'0', '1', '2', '3'}
        assert all(r['path_ok'] and r['body'].encode() == raw and r['timestamp'] == timestamp for r in logs)
    finally:
        if transport is not None: transport.close()
        for name in reversed(owned):
            docker('rm', '-f', name)
        docker('network', 'rm', network)

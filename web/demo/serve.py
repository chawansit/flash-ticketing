"""Loopback-only Encore development preview and allowlisted engine bridge. Not a production gateway."""
import argparse
import json
import os
import re
from http.server import SimpleHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from urllib.error import HTTPError, URLError
from urllib.parse import unquote, urlsplit
from urllib.request import HTTPRedirectHandler, Request, build_opener

ASSETS = Path(__file__).resolve().parent / 'dist'
UUID = r'[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}'
ROUTES = {
    'GET': [r'/v1/events', rf'/v1/events/{UUID}/(?:layout|availability)',
            rf'/v1/reservation-commands/{UUID}/{UUID}', rf'/v1/orders/{UUID}(?:/payment-operation)?'],
    'POST': [r'/v1/holds', rf'/v1/orders/{UUID}/payments'],
    'DELETE': [rf'/v1/holds/{UUID}'],
}
BODY_LIMIT = 16384
RESPONSE_LIMIT = 8 * 1024 * 1024


class NoRedirect(HTTPRedirectHandler):
    def redirect_request(self, *_args, **_kwargs):
        return None


def validate_upstream(value):
    if not value:
        return ''
    parsed = urlsplit(value)
    if (parsed.scheme not in {'http', 'https'} or not parsed.hostname or parsed.username or parsed.password
            or parsed.path not in {'', '/'} or parsed.query or parsed.fragment):
        raise ValueError('DEMO_ENGINE_URL must be a fixed HTTP origin without credentials, path or query')
    return value.rstrip('/')


def metrics_snapshot(raw):
    values = {'httpRequests': 0, 'httpErrors': 0, 'dbUnavailable': 0}
    names = {'ticketing_http_requests_total': 'httpRequests', 'ticketing_db_unavailable_total': 'dbUnavailable'}
    for line in raw.splitlines():
        if line.startswith('#'):
            continue
        match = re.fullmatch(r'([a-zA-Z_:][a-zA-Z0-9_:]*)(\{.*\})?\s+([0-9.eE+\-]+)(?:\s+\d+)?', line)
        if not match or match[1] not in names:
            continue
        number = float(match[3])
        if not 0 <= number < 10**16:
            continue
        values[names[match[1]]] += number
        if match[1] == 'ticketing_http_requests_total' and re.search(r'(?:^|[,{])status="5\d\d"', match[2] or ''):
            values['httpErrors'] += number
    return {**values, 'scope': 'One upstream API process. Not cluster totals or a derived throughput rate.'}


class DemoServer(ThreadingHTTPServer):
    daemon_threads = True

    def __init__(self, address, upstream=''):
        if address[0] not in {'127.0.0.1', '::1'}:
            raise ValueError('Development bridge must bind to loopback')
        self.upstream = validate_upstream(upstream)
        self.opener = build_opener(NoRedirect())
        super().__init__(address, DemoHandler)


class DemoHandler(SimpleHTTPRequestHandler):
    extensions_map = {**SimpleHTTPRequestHandler.extensions_map, '.mjs': 'text/javascript'}

    def __init__(self, *args, **kwargs):
        super().__init__(*args, directory=str(ASSETS), **kwargs)

    def log_message(self, _format, *_args):
        pass  # Never log URLs, customer headers, tokens or upstream response bodies.

    def end_headers(self):
        self.send_header('X-Content-Type-Options', 'nosniff')
        self.send_header('Referrer-Policy', 'no-referrer')
        self.send_header('Cache-Control', 'no-store')
        super().end_headers()

    def json(self, status, body):
        raw = json.dumps(body, allow_nan=False).encode()
        self.send_response(status)
        self.send_header('Content-Type', 'application/json')
        self.send_header('Content-Length', str(len(raw)))
        self.end_headers()
        self.wfile.write(raw)

    def allowed_origin(self):
        host = self.headers.get('Host', '')
        allowed = {f'127.0.0.1:{self.server.server_port}', f'localhost:{self.server.server_port}'}
        if host not in allowed:
            self.json(403, {'code': 'LOCAL_HOST_REQUIRED'})
            return False
        origin = self.headers.get('Origin')
        if origin and origin not in {'http://' + name for name in allowed}:
            self.json(403, {'code': 'SAME_ORIGIN_REQUIRED'})
            return False
        if self.headers.get('Sec-Fetch-Site') == 'cross-site':
            self.json(403, {'code': 'SAME_ORIGIN_REQUIRED'})
            return False
        return True

    def do_GET(self):
        if not self.allowed_origin():
            return
        if self.path == '/demo-config':
            return self.json(200, {'engineConfigured': bool(self.server.upstream), 'scope': 'loopback development bridge'})
        if self.path.startswith('/engine'):
            return self.proxy()
        parsed = urlsplit(self.path)
        decoded = unquote(parsed.path)
        if any(part in {'..', '.git', '.env', '.openai'} for part in decoded.replace('\\', '/').split('/')):
            return self.json(404, {'code': 'NOT_FOUND'})
        candidate = ASSETS / decoded.lstrip('/')
        if not candidate.resolve().is_relative_to(ASSETS.resolve()) or candidate.is_dir() and decoded != '/':
            return self.json(404, {'code': 'NOT_FOUND'})
        return super().do_GET()

    def do_POST(self):
        if self.allowed_origin():
            self.proxy()

    def do_DELETE(self):
        if self.allowed_origin():
            self.proxy()

    def proxy(self):
        if not self.server.upstream:
            return self.json(503, {'code': 'LIVE_ENGINE_NOT_CONFIGURED'})
        parsed = urlsplit(self.path)
        path = parsed.path.removeprefix('/engine')
        metrics = self.command == 'GET' and path == '/metrics'
        if (not self.path.startswith('/engine/') or parsed.query or parsed.fragment
                or not metrics and not any(re.fullmatch(pattern, path) for pattern in ROUTES.get(self.command, []))):
            return self.json(404, {'code': 'UNSUPPORTED_DEMO_ROUTE'})
        if self.headers.get('X-Demo-Client') != 'encore':
            return self.json(403, {'code': 'DEMO_CLIENT_REQUIRED'})
        if not metrics and path != '/v1/events' and not re.fullmatch(rf'/v1/events/{UUID}/(?:layout|availability)', path):
            if not self.headers.get('Authorization', '').startswith('Bearer '):
                return self.json(401, {'code': 'CUSTOMER_TOKEN_REQUIRED'})
        body = None
        if self.command == 'POST':
            try:
                length = int(self.headers.get('Content-Length', '0'))
                if not 0 < length <= BODY_LIMIT or self.headers.get('Content-Type', '').split(';')[0] != 'application/json':
                    raise ValueError()
                body = self.rfile.read(length)
                json.loads(body)
            except (ValueError, json.JSONDecodeError):
                return self.json(400, {'code': 'INVALID_DEMO_BODY'})
        headers = {key: self.headers[key] for key in ('Authorization', 'Idempotency-Key', 'If-None-Match') if key in self.headers}
        if body is not None:
            headers['Content-Type'] = 'application/json'
        request = Request(self.server.upstream + path, data=body, headers=headers, method=self.command)
        try:
            response = self.server.opener.open(request, timeout=10)
        except HTTPError as error:
            response = error
        except (URLError, TimeoutError, OSError):
            return self.json(502, {'code': 'ENGINE_RESPONSE_UNKNOWN'})
        with response:
            raw = response.read(RESPONSE_LIMIT + 1)
            if len(raw) > RESPONSE_LIMIT:
                return self.json(502, {'code': 'ENGINE_RESPONSE_TOO_LARGE'})
            if metrics:
                if response.status != 200:
                    return self.json(502, {'code': 'ENGINE_METRICS_UNAVAILABLE'})
                return self.json(200, metrics_snapshot(raw.decode('utf-8', errors='replace')))
            if response.status not in {200, 201, 202, 204, 304}:
                try:
                    value = json.loads(raw)
                    code = value.get('code', 'ENGINE_REQUEST_FAILED')
                    code = code if isinstance(code, str) and re.fullmatch(r'[A-Z_]{1,80}', code) else 'ENGINE_REQUEST_FAILED'
                except (ValueError, AttributeError):
                    code = 'ENGINE_REQUEST_FAILED'
                self.send_response(response.status if 400 <= response.status <= 599 else 502)
                retry = response.headers.get('Retry-After')
                if retry:
                    self.send_header('Retry-After', retry)
                raw = json.dumps({'code': code}).encode()
                self.send_header('Content-Type', 'application/json')
            else:
                self.send_response(response.status)
                self.send_header('Content-Type', 'application/json')
                for key in ('ETag', 'X-Poll-Interval-Ms', 'X-Poll-Jitter-Percent'):
                    if key in response.headers:
                        self.send_header(key, response.headers[key])
            self.send_header('Content-Length', str(len(raw)))
            self.end_headers()
            self.wfile.write(raw)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--port', type=int, default=4080)
    args = parser.parse_args()
    server = DemoServer(('127.0.0.1', args.port), os.environ.get('DEMO_ENGINE_URL', ''))
    print(f'Encore demo: http://127.0.0.1:{server.server_port}', flush=True)
    print('Live engine bridge configured.' if server.upstream else 'Sample mode. Set DEMO_ENGINE_URL to enable the live bridge.', flush=True)
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        pass
    finally:
        server.server_close()


if __name__ == '__main__':
    main()

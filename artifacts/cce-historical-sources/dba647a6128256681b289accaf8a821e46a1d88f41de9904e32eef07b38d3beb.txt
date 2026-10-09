import argparse
import asyncio
import importlib.util
import json
import socket
import time
from datetime import UTC, datetime, timedelta
from pathlib import Path
from threading import Thread
from types import SimpleNamespace

import pytest
import uvicorn

spec = importlib.util.spec_from_file_location('expiry_generator', Path(__file__).parents[2] / 'scripts/http_load_generator.py')
generator = importlib.util.module_from_spec(spec)
spec.loader.exec_module(generator)


@pytest.mark.parametrize('value', ['0', '-1', 'nan', 'inf', '61'])
def test_invalid_expiry_rejected(value):
    with pytest.raises(argparse.ArgumentTypeError):
        generator.expiry_seconds(value)


def test_load_client_expiry_changes_real_connection_churn(tmp_path, monkeypatch):
    for key in ('HTTP_PROXY', 'HTTPS_PROXY', 'ALL_PROXY', 'http_proxy', 'https_proxy', 'all_proxy'):
        monkeypatch.delenv(key, raising=False)

    async def app(scope, receive, send):
        if scope['method'] == 'POST':
            while (await receive()).get('more_body', False):
                pass
        await send({'type': 'http.response.start', 'status': 201 if scope['method'] == 'POST' else 200,
                    'headers': [(b'etag', b'"v1"'), (b'content-length', b'2')]})
        await send({'type': 'http.response.body', 'body': b'OK'})

    listener = socket.socket()
    listener.bind(('127.0.0.1', 0))
    listener.listen(16)
    server = uvicorn.Server(uvicorn.Config(app, http='httptools', timeout_keep_alive=5,
                                         lifespan='off', access_log=False, log_level='critical'))
    thread = Thread(target=server.run, kwargs={'sockets': [listener]}, daemon=True)
    thread.start()
    deadline = time.monotonic() + 10
    while not server.started:
        assert time.monotonic() < deadline
        time.sleep(.01)
    host, port = listener.getsockname()
    origin = f'http://{host}:{port}'
    manifest = tmp_path / 'manifest.json'
    manifest.write_text(json.dumps({'schema_version': 1, 'environment': 'development', 'id': 'test',
                                   'expires_at': (datetime.now(UTC) + timedelta(minutes=5)).isoformat(),
                                   'origin': origin, 'show_ids': ['test'], 'viewer_tokens': ['test'],
                                   'seat_offset': 0, 'seats_per_show': 300}))
    outputs = []
    try:
        for expiry in (5, .01):
            output = tmp_path / f'{expiry}.json'
            args = SimpleNamespace(manifest=manifest, origin=origin, output=output, rate=10,
                                   seconds=2, inflight=64, burst=False, start_at=None,
                                   topology='same-host', transport_diagnostics=True, keepalive_expiry=expiry)
            asyncio.run(generator.run(args))
            result = json.loads(output.read_text())
            assert result['workload_gate_pass']
            assert result['keepalive_expiry_seconds'] == expiry
            counts = result['measured_tcp_connect_events']
            started = sum(c.get('started', 0) for c in counts.values())
            assert started == sum(c.get('complete', 0) for c in counts.values())
            outputs.append(started)
        assert outputs[0] <= 2  # Bootstrap connection is reused; excluded from measured counters.
        assert outputs[1] >= 15  # Early expiry causes fresh connections between scheduled requests.
    finally:
        server.should_exit = True
        thread.join(10)
        listener.close()
    assert not thread.is_alive()

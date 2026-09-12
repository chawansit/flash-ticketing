"""Real httptools socket tests for protocol stamps, keep-alive and pipeline wait."""
import asyncio
import importlib.util
import socket
import time
from pathlib import Path
from threading import Thread

import pytest
import uvicorn

spec=importlib.util.spec_from_file_location("ingress",Path(__file__).resolve().parents[2]/"scripts/ingress_server.py")
ingress=importlib.util.module_from_spec(spec)
spec.loader.exec_module(ingress)


@pytest.fixture
def server():
    records=[]
    async def app(scope,receive,send):
        records.append((scope["path"],scope["state"][ingress.STAMP],time.perf_counter()))
        if scope['path']=='/slow':
            await asyncio.sleep(.04)
        await send({'type':'http.response.start','status':200,'headers':[(b'content-length',b'2'),(b'server-timing',b'app;dur=1')]})
        await send({'type':'http.response.body','body':b'OK'})
    listener=socket.socket()
    listener.bind(('127.0.0.1',0))
    listener.listen(16)
    runner=uvicorn.Server(uvicorn.Config(ingress.IngressTiming(app),http=ingress.TimedHttpToolsProtocol,
                                        lifespan='off',access_log=False,log_level='critical'))
    thread=Thread(target=runner.run,kwargs={'sockets':[listener]},daemon=True)
    thread.start()
    deadline=time.monotonic()+10
    while not runner.started:
        assert time.monotonic()<deadline
        time.sleep(.01)
    try:
        yield listener.getsockname(),records
    finally:
        runner.should_exit=True
        thread.join(timeout=10)
        listener.close()


def read_responses(client,count):
    result=b''
    while result.count(b'\r\n\r\nOK')<count:
        chunk=client.recv(65536)
        assert chunk
        result+=chunk
    return result


def test_fragmented_headers_and_keepalive_get_fresh_stamp(server):
    address,records=server
    with socket.create_connection(address,timeout=5) as client:
        client.sendall(b'GET /first HTTP/1.1\r\nHost: local')
        time.sleep(.03)
        completed=time.perf_counter()
        client.sendall(b'\r\n\r\n')
        first=read_responses(client,1)
        client.sendall(b'GET /second HTTP/1.1\r\nHost: local\r\n\r\n')
        second=read_responses(client,1)
    assert len(records)==2
    assert records[0][1]>=completed
    assert records[1][1]>records[0][1]
    for response in [first,second]:
        assert b'app;dur=1' in response
        assert b'protocol_queue;dur=' in response and b'asgi_headers;dur=' in response


def test_pipeline_wait_is_measured_before_second_app_entry(server):
    address,records=server
    with socket.create_connection(address,timeout=5) as client:
        client.sendall(b'GET /slow HTTP/1.1\r\nHost: local\r\n\r\nGET /next HTTP/1.1\r\nHost: local\r\n\r\n')
        response=read_responses(client,2)
    assert response.count(b'protocol_queue;dur=')==2
    assert len(records)==2
    assert records[1][2]-records[1][1]>=.03


def test_missing_stamp_is_not_reported_as_zero():
    messages=[]
    async def app(scope,receive,send):
        await send({'type':'http.response.start','status':200,'headers':[]})
        await send({'type':'http.response.body','body':b''})
    async def send(message):
        messages.append(message)
    asyncio.run(ingress.IngressTiming(app)({'type':'http','state':{}},None,send))
    headers=dict(messages[0]['headers'])
    assert b'protocol_queue' not in headers[b'server-timing']
    assert b'asgi_headers' in headers[b'server-timing']


def test_server_keepalive_is_bounded(monkeypatch):
    monkeypatch.delenv("SERVER_KEEPALIVE_SECONDS", raising=False)
    assert ingress.server_keepalive_seconds() == 5
    monkeypatch.setenv("SERVER_KEEPALIVE_SECONDS", "10")
    assert ingress.server_keepalive_seconds() == 10
    monkeypatch.setenv("SERVER_KEEPALIVE_SECONDS", "0")
    with pytest.raises(RuntimeError, match="between 1 and 300"):
        ingress.server_keepalive_seconds()
    monkeypatch.setenv("SERVER_KEEPALIVE_SECONDS", "invalid")
    with pytest.raises(RuntimeError, match="must be an integer"):
        ingress.server_keepalive_seconds()

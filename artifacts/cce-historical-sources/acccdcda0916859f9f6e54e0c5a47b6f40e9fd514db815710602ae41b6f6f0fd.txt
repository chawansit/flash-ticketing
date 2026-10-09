import asyncio
import importlib.util
import socket
import time
from pathlib import Path
from threading import Thread

import httpx
import uvicorn


def test_real_idle_close_reuse_and_early_client_expiry(monkeypatch):
    scripts=Path(__file__).parents[2]/'scripts'
    monkeypatch.syspath_prepend(str(scripts))
    spec=importlib.util.spec_from_file_location('idle_probe',scripts/'idle_boundary_probe.py')
    probe=importlib.util.module_from_spec(spec)
    spec.loader.exec_module(probe)
    async def app(scope, receive, send):
        conditional=any(k==b'if-none-match' for k,v in scope['headers'])
        headers=[(b'etag',b'"v1"'),(b'x-request-id',b'local-test')]
        if not conditional:headers.append((b'content-length',b'2'))
        await send({'type':'http.response.start','status':304 if conditional else 200,'headers':headers})
        await send({'type':'http.response.body','body':b'' if conditional else b'OK'})
    listener=socket.socket()
    listener.bind(('127.0.0.1',0))
    listener.listen(16)
    server=uvicorn.Server(uvicorn.Config(app,http='httptools',timeout_keep_alive=1,lifespan='off',access_log=False,log_level='critical'))
    thread=Thread(target=server.run,kwargs={'sockets':[listener]},daemon=True)
    thread.start()
    deadline=time.monotonic()+10
    while not server.started:
        assert time.monotonic()<deadline
        time.sleep(.01)
    async def scenario():
        host,port=listener.getsockname()
        origin=f'http://{host}:{port}'
        async with httpx.AsyncClient(base_url=origin,trust_env=False,limits=httpx.Limits(max_connections=1,keepalive_expiry=5)) as client:
            reused=await probe.pair(client,'/',.05,True)
            assert reused['same_local_port']
            assert not reused['probe']['trace']['connect_attempted']
            assert reused['probe']['status']==304
            closed=await probe.pair(client,'/',1.15,False)
            assert closed['probe']['status']==200
            assert closed['probe']['trace']['connect_attempted']
            assert not closed['same_local_port']
        async with httpx.AsyncClient(base_url=origin,trust_env=False,limits=httpx.Limits(max_connections=1,keepalive_expiry=.01)) as client:
            early=await probe.pair(client,'/',.05,False)
            assert early['probe']['trace']['connect_attempted']
            assert early['actual_client_idle_seconds']>=.05
    try:
        asyncio.run(scenario())
    finally:
        server.should_exit=True
        thread.join(10)
        listener.close()
    assert not thread.is_alive()

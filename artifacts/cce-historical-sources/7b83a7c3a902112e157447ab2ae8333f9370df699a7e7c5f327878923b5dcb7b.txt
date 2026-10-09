import asyncio
import importlib.util
import json
import socket
import struct
import sys
from pathlib import Path
from threading import Thread

import httpx
import pytest

spec=importlib.util.spec_from_file_location('transport_generator',Path(__file__).parents[2]/'scripts/http_load_generator.py')
module=importlib.util.module_from_spec(spec)
spec.loader.exec_module(module)


def test_real_tcp_reset_records_phase_without_payloads():
    listener=socket.socket()
    listener.bind(('127.0.0.1',0))
    listener.listen()
    listener.settimeout(5)
    port=listener.getsockname()[1]
    def reset():
        conn,_=listener.accept()
        with conn:
            conn.settimeout(3)
            conn.recv(65536)
            conn.setsockopt(socket.SOL_SOCKET,socket.SO_LINGER,struct.pack('HH' if sys.platform=='win32' else 'ii',1,0))
    thread=Thread(target=reset,daemon=True)
    thread.start()
    async def scenario():
        trace=module.TransportTrace()
        async with httpx.AsyncClient(timeout=3,trust_env=False) as client:
            with pytest.raises(httpx.HTTPError) as error:
                await client.get(f'http://127.0.0.1:{port}',headers={'Authorization':'Bearer SECRET'},extensions={'trace':trace})
        result=trace.failure(error.value)
        assert result['connect_attempted']
        assert any(e['phase'].endswith('.failed') for e in result['events'])
        assert result['exception_chain']
        assert 'SECRET' not in json.dumps(result)
    try:
        asyncio.run(scenario())
    finally:
        listener.close()
        thread.join(5)
    assert not thread.is_alive()


def test_trace_and_exception_chain_are_bounded_and_redacted():
    trace=module.TransportTrace()
    async def events():
        for _ in range(100):await trace('http11.receive_response_headers.started',{'headers':'SECRET'})
    asyncio.run(events())
    inner=OSError(104,'SECRET')
    outer=httpx.ReadError('SECRET')
    outer.__cause__=inner
    result=trace.failure(outer)
    assert len(result['events'])==32
    assert result['exception_chain'][1]['errno']==104
    assert 'SECRET' not in json.dumps(result)

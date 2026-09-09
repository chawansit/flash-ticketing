import asyncio
from types import SimpleNamespace

import pytest

from ticketing.http import RequestInstrumentation
from ticketing.observability import HOLD_TRACE, REQUEST_ID


def scope():
    return {'type':'http','method':'POST','scheme':'http','path':'/v1/holds','root_path':'',
            'query_string':b'','headers':[],'server':('test',80),'state':{}}


async def receive():
    return {'type':'http.request','body':b'','more_body':False}


@pytest.mark.parametrize('after_headers', [False, True])
def test_exception_releases_once_and_restores_context(after_headers):
    async def scenario():
        owner=SimpleNamespace(state=SimpleNamespace(reserve_inflight=0))
        previous=REQUEST_ID.set('parent')
        async def downstream(s,r,send):
            assert owner.state.reserve_inflight==1
            if after_headers:
                await send({'type':'http.response.start','status':200,'headers':[]})
                assert owner.state.reserve_inflight==0
            raise ValueError('intentional')
        async def send(message):
            assert dict(message['headers'])[b'x-request-id'].decode()==REQUEST_ID.get()
        middleware=RequestInstrumentation(downstream,owner,SimpleNamespace(reserve_concurrency=2))
        try:
            with pytest.raises(ValueError,match='intentional'):
                await middleware(scope(),receive,send)
            assert owner.state.reserve_inflight==0
            assert REQUEST_ID.get()=='parent'
            assert HOLD_TRACE.get() is None
        finally:
            REQUEST_ID.reset(previous)
    asyncio.run(scenario())


def test_cancellation_releases_admission():
    async def scenario():
        entered=asyncio.Event()
        owner=SimpleNamespace(state=SimpleNamespace(reserve_inflight=0))
        async def downstream(s,r,send):
            entered.set()
            await asyncio.Event().wait()
        async def send(message):
            raise AssertionError('No response expected')
        middleware=RequestInstrumentation(downstream,owner,SimpleNamespace(reserve_concurrency=2))
        task=asyncio.create_task(middleware(scope(),receive,send))
        await entered.wait()
        assert owner.state.reserve_inflight==1
        task.cancel()
        with pytest.raises(asyncio.CancelledError):await task
        assert owner.state.reserve_inflight==0
        assert REQUEST_ID.get() is None
    asyncio.run(scenario())


def test_streaming_releases_at_headers_and_concurrent_context_is_isolated():
    async def scenario():
        owner=SimpleNamespace(state=SimpleNamespace(reserve_inflight=0))
        both=asyncio.Event()
        started=[]
        async def downstream(s,r,send):
            identifier=REQUEST_ID.get()
            HOLD_TRACE.get()['test']=identifier
            started.append(identifier)
            if len(started)==2:both.set()
            await both.wait()
            assert HOLD_TRACE.get()['test']==identifier
            # A numeric trace is required by production instrumentation.
            HOLD_TRACE.get().clear()
            await send({'type':'http.response.start','status':200,'headers':[]})
            await asyncio.sleep(0)
            assert owner.state.reserve_inflight==0
            assert REQUEST_ID.get()==identifier
            await send({'type':'http.response.body','body':b'OK'})
        responses=[]
        async def send(message):responses.append(message)
        middleware=RequestInstrumentation(downstream,owner,SimpleNamespace(reserve_concurrency=2))
        await asyncio.gather(*(middleware(scope(),receive,send) for _ in range(2)))
        assert len(set(started))==2
        assert owner.state.reserve_inflight==0
        assert REQUEST_ID.get() is None
        assert len(responses)==4
    asyncio.run(scenario())

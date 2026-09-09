import asyncio
import importlib.util
import logging
from pathlib import Path
from threading import Event

import anyio
import pytest
from starlette.concurrency import run_in_threadpool

from ticketing.observability import HOLD_TRACE, REQUEST_ID

spec = importlib.util.spec_from_file_location('dispatch_probe_test', Path(__file__).parents[2]/'scripts/thread_dispatch_probe.py')
probe = importlib.util.module_from_spec(spec)
spec.loader.exec_module(probe)


def test_real_dispatch_queue_results_and_exception(caplog):
    caplog.set_level(logging.INFO, logger='ticketing.dispatch_probe')
    async def scenario():
        limiter = anyio.to_thread.current_default_thread_limiter()
        previous = limiter.total_tokens
        limiter.total_tokens = 1
        entered, release = Event(), Event()
        def blocker():
            entered.set()
            release.wait(3)
        task = asyncio.create_task(run_in_threadpool(blocker))
        trace, identifier = HOLD_TRACE.set({}), REQUEST_ID.set('probe-test')
        def actor(value):
            return value
        try:
            while not entered.is_set():
                await asyncio.sleep(.001)
            queued = asyncio.create_task(probe.dispatch_probe(run_in_threadpool)(actor, value='ok'))
            await asyncio.sleep(.03)
            assert not queued.done()
            release.set()
            assert await queued == 'ok'
            def fail():
                raise ValueError('expected')
            with pytest.raises(ValueError, match='expected'):
                await probe.dispatch_probe(run_in_threadpool)(fail)
        finally:
            release.set()
            await task
            HOLD_TRACE.reset(trace)
            REQUEST_ID.reset(identifier)
            limiter.total_tokens = previous
    asyncio.run(scenario())
    rows=[r.fields for r in caplog.records if r.message=='dispatch_probe']
    assert rows[0]['stage']=='authentication'
    assert rows[0]['timing_ms']['submit_to_entry']>=20
    assert rows[0]['borrowed_tokens_at_submit']==1
    assert rows[1]['outcome']=='error'
    assert all(r['request_id']=='probe-test' for r in rows)


def test_passthrough_and_nested_idempotency_do_not_change_trace(caplog):
    caplog.set_level(logging.INFO, logger='ticketing.dispatch_probe')
    marker=object()
    assert asyncio.run(probe.dispatch_probe(run_in_threadpool)(lambda: marker)) is marker
    assert not caplog.records
    trace={}
    token=HOLD_TRACE.set(trace)
    try:
        wrapped=probe.idempotency_probe(lambda *args: marker)
        assert wrapped(None,'secret-actor','hold','secret-key',{}) is marker
        assert trace=={}
        def fail(*args):
            raise ValueError('expected')
        with pytest.raises(ValueError):probe.idempotency_probe(fail)(None,'a','hold','k',{})
    finally:
        HOLD_TRACE.reset(token)
    assert len(caplog.records)==2
    assert 'secret' not in str([r.fields for r in caplog.records])


def test_install_is_reversible_and_rejects_double_install():
    import fastapi.dependencies.utils as dependencies
    from fastapi import routing

    from ticketing.infrastructure import reservations
    original, idem=routing.run_in_threadpool, reservations.idem
    restore=probe.install()
    try:
        assert routing.run_in_threadpool is dependencies.run_in_threadpool
        assert routing.run_in_threadpool is not original
        with pytest.raises(RuntimeError):probe.install()
    finally:
        restore()
    assert routing.run_in_threadpool is dependencies.run_in_threadpool is original
    assert reservations.idem is idem

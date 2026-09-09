import asyncio
import importlib.util
from contextlib import contextmanager
from dataclasses import replace
from pathlib import Path
from uuid import uuid4

import httpx
import pytest
from prometheus_client import REGISTRY
from starlette.requests import Request
from starlette.responses import Response

from ticketing.api import app, settings
from ticketing.http import RequestInstrumentation
from ticketing.observability import HOLD_TRACE, TimedHoldResource, hold_phase

spec = importlib.util.spec_from_file_location(
    "http_load_generator", Path(__file__).resolve().parents[2] / "scripts/http_load_generator.py"
)
generator = importlib.util.module_from_spec(spec)
spec.loader.exec_module(generator)
error_diagnostic = generator.error_diagnostic


async def instrumentation(request, call_next):
    """Exercise the ASGI adapter with the existing test handler contract."""
    from ticketing import api
    messages = []
    async def downstream(scope, receive, send):
        response = await call_next(Request(scope))
        await response(scope, receive, send)
    async def receive():
        return {'type': 'http.request', 'body': b'', 'more_body': False}
    async def send(message):
        messages.append(message)
    await RequestInstrumentation(downstream, app, api.settings)(request.scope, receive, send)
    start = messages[0]
    return Response(b''.join(m.get('body', b'') for m in messages[1:]),
                    status_code=start['status'])


def sample(name, labels=None):
    return REGISTRY.get_sample_value(name, labels or {}) or 0


def test_real_inflight_rejection_is_counted_and_slot_released(monkeypatch, caplog):
    monkeypatch.setattr('ticketing.api.settings', replace(settings, reserve_concurrency=1))
    before = sample('ticketing_outcomes_total', {'operation': 'request', 'outcome': 'ADMISSION_FULL'})

    async def scenario():
        entered, release = asyncio.Event(), asyncio.Event()

        def request():
            return Request({'type': 'http', 'method': 'POST', 'path': '/v1/holds',
                            'headers': [], 'app': app})

        async def slow(_):
            entered.set()
            await release.wait()
            return Response(status_code=201)

        first = asyncio.create_task(instrumentation(request(), slow))
        await entered.wait()
        try:
            assert sample('ticketing_hold_inflight') == 1
            rejected = await instrumentation(request(), slow)
            assert rejected.status_code == 503
            assert b'ADMISSION_FULL' in rejected.body
            assert app.state.reserve_inflight == 1
        finally:
            release.set()
            await first
        assert app.state.reserve_inflight == 0
        assert sample('ticketing_hold_inflight') == 0
        assert HOLD_TRACE.get() is None

    with caplog.at_level('INFO', logger='ticketing.api'):
        asyncio.run(scenario())
    assert sample('ticketing_outcomes_total', {'operation': 'request', 'outcome': 'ADMISSION_FULL'}) == before+1
    fields = [r.fields for r in caplog.records if getattr(r, 'fields', {}).get('status') == 503]
    assert fields[0]['error_code'] == 'ADMISSION_FULL'
    assert fields[0]['hold_arrival_occupancy'] == 1


@pytest.mark.parametrize('failure', [RuntimeError, asyncio.CancelledError])
def test_exception_and_cancellation_release_admission(failure):
    async def scenario():
        req = Request({'type': 'http', 'method': 'POST', 'path': '/v1/holds', 'headers': [], 'app': app})
        async def fail(_):
            raise failure()
        with pytest.raises(failure):
            await instrumentation(req, fail)
        assert app.state.reserve_inflight == 0
        assert sample('ticketing_hold_inflight') == 0
        assert HOLD_TRACE.get() is None
    asyncio.run(scenario())


def test_timed_resource_preserves_rollback_and_cleanup_order():
    events, trace = [], {}
    @contextmanager
    def resource(name):
        events.append(name+'_enter')
        try:
            yield name
        except ValueError:
            events.append(name+'_rollback')
            raise
        finally:
            events.append(name+'_exit')
    token = HOLD_TRACE.set(trace)
    try:
        with (
            pytest.raises(ValueError),
            TimedHoldResource('redis', resource('redis')),
            TimedHoldResource('db', resource('db')),
            hold_phase('database_body'),
        ):
            raise ValueError('rollback')
    finally:
        HOLD_TRACE.reset(token)
    assert events == ['redis_enter','db_enter','db_rollback','db_exit','redis_rollback','redis_exit']
    assert set(trace) == {'redis_enter','db_enter','database_body','db_exit','redis_exit'}


def test_failed_enter_does_not_exit_and_suppression_is_preserved():
    class Resource:
        def __enter__(self):
            raise ValueError()
        def __exit__(self, *exc):
            pytest.fail('Exit called after failed entry')
    with pytest.raises(ValueError), TimedHoldResource('db', Resource()):
        pytest.fail('Body entered')
    @contextmanager
    def suppress():
        try:
            yield
        except ValueError:
            pass
    with TimedHoldResource('db', suppress()):
        raise ValueError()


def test_phase_trace_crosses_worker_thread_without_cross_request_leak():
    async def request(phase):
        trace = {}
        token = HOLD_TRACE.set(trace)
        try:
            def work():
                with hold_phase(phase):
                    pass
            await asyncio.to_thread(work)
            return trace
        finally:
            HOLD_TRACE.reset(token)
    async def scenario():
        a, b = await asyncio.gather(request('db_enter'), request('redis_enter'))
        assert set(a) == {'db_enter'}
        assert set(b) == {'redis_enter'}
    asyncio.run(scenario())


def test_error_diagnostic_allowlists_code_and_request_id():
    identifier = str(uuid4())
    response = httpx.Response(503, json={'code':'ADMISSION_FULL','secret':'do not retain'},
                              headers={'X-Request-ID':identifier})
    assert error_diagnostic(response) == ('ADMISSION_FULL', identifier)
    for body in [{'code':['bad']}, {'code':'a-private-token'}, ['bad']]:
        response = httpx.Response(503, json=body, headers={'X-Request-ID':'private-token'})
        assert error_diagnostic(response) == ('OTHER', None)
    assert error_diagnostic(httpx.Response(503, text='private non-json body')) == ('OTHER', None)

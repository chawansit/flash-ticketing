"""HTTP instrumentation without an intermediate task or body stream."""
import logging
import time
from uuid import uuid4

from starlette.datastructures import MutableHeaders
from starlette.requests import Request
from starlette.responses import JSONResponse

from ticketing.observability import (
    HOLD_ADMISSION,
    HOLD_INFLIGHT,
    HOLD_LIMIT,
    HOLD_OCCUPANCY,
    HOLD_TRACE,
    LATENCY,
    OUTCOMES,
    REQUEST_ID,
    REQUESTS,
)

log = logging.getLogger('ticketing.api')


class RequestInstrumentation:
    def __init__(self, app, owner, config):
        self.app, self.owner, self.config = app, owner, config

    async def __call__(self, scope, receive, send):
        if scope['type'] != 'http':
            return await self.app(scope, receive, send)
        request = Request(scope)
        started = time.monotonic()
        request.state.request_id = str(uuid4())
        request.state.error_code = None
        context = REQUEST_ID.set(request.state.request_id)
        reservation = request.method == 'POST' and request.url.path == '/v1/holds'
        trace = {} if reservation else None
        trace_context = HOLD_TRACE.set(trace)
        arrival_occupancy = self.owner.state.reserve_inflight
        admitted = False

        def release():
            nonlocal admitted
            if admitted:
                admitted = False
                self.owner.state.reserve_inflight -= 1
                HOLD_INFLIGHT.set(self.owner.state.reserve_inflight)

        async def measured_send(message):
            if message['type'] == 'http.response.start':
                release()
                elapsed = time.monotonic() - started
                route = getattr(scope.get('route'), 'path', '/v1/holds' if reservation else 'unmatched')
                status = message['status']
                REQUESTS.labels(route, request.method, status).inc()
                LATENCY.labels(route).observe(elapsed)
                message = {**message, 'headers': list(message.get('headers', []))}
                headers = MutableHeaders(scope=message)
                headers['X-Request-ID'] = request.state.request_id
                headers['Server-Timing'] = f'app;dur={elapsed * 1000:.2f}'
                log.info('request', extra={'fields': {
                    'request_id': request.state.request_id, 'route': route, 'status': status,
                    'duration_ms': round(elapsed * 1000, 2), 'error_code': request.state.error_code,
                    **({'hold_arrival_occupancy': arrival_occupancy,
                        'hold_limit': self.config.reserve_concurrency,
                        'hold_phase_ms': {k: round(v, 3) for k, v in trace.items()}}
                       if reservation else {}),
                }})
            await send(message)

        try:
            if reservation:
                HOLD_OCCUPANCY.observe(arrival_occupancy)
                HOLD_LIMIT.set(self.config.reserve_concurrency)
            # One process/event loop: check and increment contain no await.
            if reservation and self.owner.state.reserve_inflight >= self.config.reserve_concurrency:
                HOLD_ADMISSION.labels('rejected').inc()
                OUTCOMES.labels('request', 'ADMISSION_FULL').inc()
                request.state.error_code = 'ADMISSION_FULL'
                response = JSONResponse(status_code=503,
                    content={'code': 'ADMISSION_FULL', 'request_id': request.state.request_id},
                    headers={'Retry-After': '1'})
                await response(scope, receive, measured_send)
            else:
                if reservation:
                    self.owner.state.reserve_inflight += 1
                    admitted = True
                    request.state.hold_admitted_at = time.monotonic()
                    HOLD_INFLIGHT.set(self.owner.state.reserve_inflight)
                    HOLD_ADMISSION.labels('admitted').inc()
                await self.app(scope, receive, measured_send)
        finally:
            release()
            HOLD_TRACE.reset(trace_context)
            REQUEST_ID.reset(context)

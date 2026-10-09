"""Opt-in probes for pinned FastAPI dispatcher references; no scheduling changes."""
import logging
from functools import wraps
from time import monotonic

import anyio
from prometheus_client import Histogram

from ticketing.observability import HOLD_TRACE, REQUEST_ID

log = logging.getLogger('ticketing.dispatch_probe')
SECONDS = Histogram('ticketing_dispatch_probe_seconds', 'Diagnostic dispatch boundary duration',
                    ['stage', 'phase', 'outcome'])


def dispatch_probe(original):
    @wraps(original)
    async def measured(func, *args, **kwargs):
        if HOLD_TRACE.get() is None:
            return await original(func, *args, **kwargs)
        stage = {'actor': 'authentication', 'service': 'service_dependency',
                 'hold': 'hold_handler'}.get(getattr(func, '__name__', ''), 'other')
        limiter = anyio.to_thread.current_default_thread_limiter().statistics()
        stamps = {}
        submitted = monotonic()

        def work():
            stamps['entered'] = monotonic()
            try:
                return func(*args, **kwargs)
            finally:
                stamps['finished'] = monotonic()

        outcome = 'ok'
        try:
            return await original(work)
        except BaseException:
            outcome = 'error'
            raise
        finally:
            resumed = monotonic()
            timings = {}
            if 'entered' in stamps:
                timings['submit_to_entry'] = stamps['entered'] - submitted
            if 'finished' in stamps:
                timings['execution'] = stamps['finished'] - stamps['entered']
                timings['finish_to_resume'] = resumed - stamps['finished']
            for phase, seconds in timings.items():
                SECONDS.labels(stage, phase, outcome).observe(seconds)
            log.info('dispatch_probe', extra={'fields': {
                'request_id': REQUEST_ID.get(), 'stage': stage, 'outcome': outcome,
                'borrowed_tokens_at_submit': limiter.borrowed_tokens,
                'total_tokens_at_submit': limiter.total_tokens,
                'tasks_waiting_at_submit': limiter.tasks_waiting,
                'timing_ms': {k: round(v*1000, 3) for k, v in timings.items()},
            }})
    return measured


def idempotency_probe(original):
    @wraps(original)
    def measured(conn, actor, operation, key, request):
        if HOLD_TRACE.get() is None or operation != 'hold':
            return original(conn, actor, operation, key, request)
        started, outcome = monotonic(), 'ok'
        try:
            return original(conn, actor, operation, key, request)
        except BaseException:
            outcome = 'error'
            raise
        finally:
            seconds = monotonic()-started
            SECONDS.labels('idempotency', 'execution', outcome).observe(seconds)
            log.info('idempotency_probe', extra={'fields': {
                'request_id': REQUEST_ID.get(), 'outcome': outcome,
                'duration_ms': round(seconds*1000, 3),
            }})
    return measured


def install():
    import fastapi.dependencies.utils as dependencies
    from fastapi import routing

    from ticketing.infrastructure import reservations

    original = routing.run_in_threadpool
    if dependencies.run_in_threadpool is not original or getattr(original, '_ticketing_probe', False):
        raise RuntimeError('Unexpected FastAPI dispatcher layout or probe already installed')
    measured = dispatch_probe(original)
    measured._ticketing_probe = True
    idem = reservations.idem
    routing.run_in_threadpool = dependencies.run_in_threadpool = measured
    reservations.idem = idempotency_probe(idem)

    def restore():
        routing.run_in_threadpool = dependencies.run_in_threadpool = original
        reservations.idem = idem
    return restore

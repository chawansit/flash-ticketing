import threading
from uuid import uuid4

from fastapi.testclient import TestClient

from ticketing.api import actor, app, service


def test_real_route_resolves_service_without_thread_dispatch(monkeypatch):
    import fastapi.dependencies.utils as dependencies
    original = dependencies.run_in_threadpool
    dispatched = []
    loop_threads = []
    handler_threads = []
    async def authenticate():
        loop_threads.append(threading.get_ident())
        return 'viewer'
    async def observed(func, *args, **kwargs):
        dispatched.append(func)
        return await original(func, *args, **kwargs)
    class Reservations:
        def get_hold(self, who, hold_id):
            handler_threads.append(threading.get_ident())
            return {'actor': who, 'hold_id': str(hold_id)}
    monkeypatch.setattr(dependencies, 'run_in_threadpool', observed)
    monkeypatch.setattr(app.state, 'reservations', Reservations(), raising=False)
    previous = dict(app.dependency_overrides)
    app.dependency_overrides[actor] = authenticate
    identifier = uuid4()
    try:
        response = TestClient(app).get(f'/v1/holds/{identifier}')
        assert response.status_code == 200
        assert response.json() == {'actor': 'viewer', 'hold_id': str(identifier)}
        assert service not in dispatched
        assert handler_threads[0] != loop_threads[0]
    finally:
        app.dependency_overrides.clear()
        app.dependency_overrides.update(previous)

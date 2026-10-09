"""Exercise the real service dependency and authenticated FastAPI dispatch."""

import time
from threading import get_ident
from uuid import uuid4

import jwt
import pytest
from fastapi.dependencies import utils
from fastapi.testclient import TestClient

from ticketing.api import app, settings
from ticketing.domain import Failure


def token(subject, audience="ticketing", expiry=None):
    return jwt.encode(
        {"sub": subject, "exp": expiry or int(time.time()) + 60, "aud": audience, "iss": "ticketing"},
        settings.jwt_secret,
        algorithm="HS256",
    )


def test_service_accessor_avoids_worker_dispatch_but_handler_stays_offloaded(monkeypatch):
    calls, dispatched = [], []
    order_id = uuid4()

    class Store:
        def get_order(self, actor, identifier):
            calls.append((actor, identifier, get_ident()))
            return {"id": str(identifier), "status": "PENDING", "tickets": []}

    store = Store()
    monkeypatch.setattr(app.state, "reservations", store, raising=False)
    original = utils.run_in_threadpool

    async def spy(function, *args, **kwargs):
        dispatched.append(function.__name__)
        return await original(function, *args, **kwargs)

    monkeypatch.setattr(utils, "run_in_threadpool", spy)
    event_loop_threads = []
    original_service = next(
        dependency.call
        for route in app.routes
        if getattr(route, "path", None) == "/v1/orders/{order_id}"
        for dependency in route.dependant.dependencies
        if dependency.call.__name__ == "service"
    )

    # Observe service resolution without replacing its implementation or threading contract.
    async def observe_service(request):
        event_loop_threads.append(get_ident())
        return await original_service(request)

    from starlette.requests import Request

    observe_service.__annotations__["request"] = Request
    app.dependency_overrides[original_service] = observe_service
    try:
        response = TestClient(app).get(
            f"/v1/orders/{order_id}", headers={"Authorization": "Bearer " + token("owner")}
        )
    finally:
        app.dependency_overrides.pop(original_service)
    assert response.status_code == 200
    assert response.json() == {"id": str(order_id), "status": "PENDING", "tickets": []}
    assert response.headers["cache-control"] == "private, no-store"
    assert response.headers["vary"] == "Authorization"
    assert dispatched == ["actor"]
    assert calls[0][:2] == ("owner", order_id)
    assert calls[0][2] != event_loop_threads[0]


@pytest.mark.parametrize("kind", ["invalid", "wrong_audience", "expired"])
def test_real_accessor_does_not_bypass_authentication(monkeypatch, kind):
    credential = {
        "invalid": "invalid",
        "wrong_audience": token("owner", audience="other"),
        "expired": token("owner", expiry=int(time.time()) - 60),
    }[kind]

    class Store:
        def get_order(self, *_args):
            pytest.fail("Unauthenticated call must not reach storage")

    monkeypatch.setattr(app.state, "reservations", Store(), raising=False)
    response = TestClient(app).get(f"/v1/orders/{uuid4()}", headers={"Authorization": "Bearer " + credential})
    assert response.status_code == 401


def test_real_accessor_preserves_actor_ownership(monkeypatch):
    seen = []

    class Store:
        def get_order(self, actor, _identifier):
            seen.append(actor)
            if actor != "owner":
                raise Failure("ORDER_NOT_FOUND", 404)
            return {"status": "PENDING", "tickets": []}

    monkeypatch.setattr(app.state, "reservations", Store(), raising=False)
    client = TestClient(app)
    response = client.get(f"/v1/orders/{uuid4()}", headers={"Authorization": "Bearer " + token("other")})
    assert response.status_code == 404
    assert response.json()["code"] == "ORDER_NOT_FOUND"
    assert seen == ["other"]

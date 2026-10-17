"""JWT correctness and dispatch isolation for the ADR0336 scheduling candidate."""
import asyncio
import time
from threading import Event

import anyio
import httpx
import jwt
import pytest
from fastapi import Depends, FastAPI

from ticketing.api import actor, settings
from ticketing.domain import Failure


def token(**changes):
    claims = dict(sub="customer", exp=int(time.time())+300, aud="ticketing", iss="ticketing")
    claims.update(changes)
    return jwt.encode(claims, settings.jwt_secret, algorithm="HS256")


def protected_app():
    app = FastAPI()

    @app.exception_handler(Failure)
    async def failed(request, exc):
        from fastapi.responses import JSONResponse
        return JSONResponse({"code": exc.code}, status_code=exc.status)

    @app.get("/protected")
    async def protected(who=Depends(actor)):
        return {"actor": who}

    return app


@pytest.mark.parametrize("encoded,expected_actor", [
    (token(), "customer"), (token(sub="a"*128), "a"*128),
    (token(exp=int(time.time())-300), None),
    (token(aud="other"), None), (token(iss="other"), None), (token(sub=""), None),
    (token(sub="a"*129), None), (token(sub=1), None), (token(exp=None), None), ("malformed", None),
    (jwt.encode(dict(sub="customer", exp=int(time.time())+300, aud="ticketing", iss="ticketing"),
                "incorrect-signing-key-for-this-test-only", algorithm="HS256"), None),
    (jwt.encode(dict(sub="customer", exp=int(time.time())+300, aud="ticketing", iss="ticketing"),
                settings.jwt_secret, algorithm="HS384"), None),
    (jwt.encode(dict(sub="customer", aud="ticketing", iss="ticketing"), settings.jwt_secret, algorithm="HS256"), None),
    (jwt.encode(dict(exp=int(time.time())+300, aud="ticketing", iss="ticketing"), settings.jwt_secret, algorithm="HS256"), None),
])
def test_authentication_contract(encoded, expected_actor):
    async def run():
        async with httpx.AsyncClient(transport=httpx.ASGITransport(app=protected_app()),
                                     base_url="http://offline") as client:
            return await client.get("/protected", headers={"Authorization": "Bearer "+encoded})
    response = asyncio.run(run())
    if expected_actor is not None:
        assert response.status_code == 200
        assert response.json() == {"actor": expected_actor}
    else:
        assert response.status_code == 401
        assert response.json() == {"code": "UNAUTHENTICATED"}


def test_authentication_progresses_when_worker_thread_capacity_is_exhausted():
    async def run():
        limiter = anyio.to_thread.current_default_thread_limiter()
        previous = limiter.total_tokens
        limiter.total_tokens = 1
        entered, release = Event(), Event()

        def blocker():
            entered.set()
            release.wait(5)

        blocked = asyncio.create_task(anyio.to_thread.run_sync(blocker))
        try:
            async with asyncio.timeout(2):
                while not entered.is_set():
                    await asyncio.sleep(.001)
            async with httpx.AsyncClient(transport=httpx.ASGITransport(app=protected_app()),
                                         base_url="http://offline") as client:
                response = await asyncio.wait_for(client.get("/protected", headers={
                    "Authorization": "Bearer "+token()}), timeout=1)
            assert response.status_code == 200
            assert limiter.borrowed_tokens == 1
        finally:
            release.set()
            await blocked
            limiter.total_tokens = previous
    asyncio.run(run())

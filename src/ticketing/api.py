import hashlib
import hmac
import logging
import time
from contextlib import asynccontextmanager
from typing import Annotated, Literal
from uuid import UUID, uuid4

import jwt
from fastapi import Depends, FastAPI, Header, Request, Response
from fastapi.exceptions import RequestValidationError
from fastapi.responses import JSONResponse
from fastapi.security import HTTPAuthorizationCredentials, HTTPBearer
from prometheus_client import CONTENT_TYPE_LATEST, generate_latest
from psycopg import OperationalError
from psycopg.errors import DeadlockDetected, LockNotAvailable, QueryCanceled
from psycopg_pool import PoolTimeout, TooManyRequests
from pydantic import BaseModel, Field

from ticketing.application.reservations import Reservations
from ticketing.config import Settings
from ticketing.domain import Failure
from ticketing.infrastructure.cache import RedisSeats
from ticketing.infrastructure.postgres import Postgres
from ticketing.infrastructure.reservations import PostgresReservations
from ticketing.observability import LATENCY, OUTCOMES, REQUEST_ID, REQUESTS, configure_logging

settings = Settings()
security = HTTPBearer()
log = logging.getLogger("ticketing.api")


@asynccontextmanager
async def lifespan(app):
    settings.validate()
    configure_logging()
    db = Postgres(settings.database_url, settings.pool_max)
    cache = RedisSeats(settings.redis_url)
    app.state.db, app.state.cache = db, cache
    app.state.reservations = Reservations(PostgresReservations(db, cache, settings.hold_seconds))
    yield
    db.close()
    cache.redis.close()


app = FastAPI(
    title="Flash-sale Ticketing",
    version="0.1.0",
    lifespan=lifespan,
    description="Assigned-seat reservations with database-enforced ownership. "
    "Holds create pending orders atomically. All amounts are integer minor units. "
    "Seat contention returns immediately; clients must not blindly retry. "
    "Cached availability is advisory. No waiting room or frontend.",
)
app.state.reserve_inflight = 0


class Error(BaseModel):
    code: str
    request_id: str


ERRORS = {code: {"model": Error} for code in (401, 403, 404, 409, 422, 429, 503)}


def service(request: Request):
    return request.app.state.reservations


def actor(credentials: Annotated[HTTPAuthorizationCredentials, Depends(security)]):
    try:
        claims = jwt.decode(
            credentials.credentials,
            settings.jwt_secret,
            algorithms=["HS256"],
            audience="ticketing",
            issuer="ticketing",
            options={"require": ["exp", "sub"]},
        )
        subject = claims["sub"]
        if not isinstance(subject, str) or not 1 <= len(subject) <= 128:
            raise ValueError("invalid subject")
        return subject
    except (jwt.PyJWTError, ValueError) as exc:
        raise Failure("UNAUTHENTICATED", 401) from exc


Actor = Annotated[str, Depends(actor)]
Service = Annotated[Reservations, Depends(service)]
Key = Annotated[str, Header(alias="Idempotency-Key", min_length=1, max_length=128)]


class HoldInput(BaseModel):
    event_id: UUID
    seat_ids: list[str] = Field(min_length=1, max_length=8, examples=[["A001", "A002"]])


class OrderInput(BaseModel):
    hold_id: UUID


class PaymentInput(BaseModel):
    outcome: Literal["SUCCEEDED", "FAILED"] = "SUCCEEDED"
    delay_seconds: int = Field(default=1, ge=0, le=600)
    duplicates: int = Field(default=3, ge=1, le=10)


class Callback(BaseModel):
    callback_id: UUID
    payment_id: UUID
    order_id: UUID
    amount: int = Field(ge=0)
    currency: str = Field(min_length=3, max_length=3)
    outcome: Literal["SUCCEEDED", "FAILED"]


@app.middleware("http")
async def instrumentation(request, call_next):
    started = time.monotonic()
    request.state.request_id = str(uuid4())
    context = REQUEST_ID.set(request.state.request_id)
    reservation = request.method == "POST" and request.url.path == "/v1/holds"
    admitted = False
    try:
        # This check/increment runs on the process's one event loop with no await between
        # them. Reject before entering Starlette's synchronous worker-thread queue.
        if reservation and app.state.reserve_inflight >= settings.reserve_concurrency:
            response = JSONResponse(
                status_code=503,
                content={"code": "ADMISSION_FULL", "request_id": request.state.request_id},
                headers={"Retry-After": "1"},
            )
        else:
            if reservation:
                app.state.reserve_inflight += 1
                admitted = True
            response = await call_next(request)
    finally:
        if admitted:
            app.state.reserve_inflight -= 1
        REQUEST_ID.reset(context)
    route = getattr(request.scope.get("route"), "path", "/v1/holds" if reservation else "unmatched")
    elapsed = time.monotonic() - started
    REQUESTS.labels(route, request.method, response.status_code).inc()
    LATENCY.labels(route).observe(elapsed)
    response.headers["X-Request-ID"] = request.state.request_id
    response.headers["Server-Timing"] = f"app;dur={elapsed * 1000:.2f}"
    log.info(
        "request",
        extra={
            "fields": {
                "request_id": request.state.request_id,
                "route": route,
                "status": response.status_code,
                "duration_ms": round(elapsed * 1000, 2),
            }
        },
    )
    return response


@app.exception_handler(Failure)
async def business_error(request, exc):
    OUTCOMES.labels("request", exc.code).inc()
    return JSONResponse(
        status_code=exc.status,
        content={"code": exc.code, "request_id": request.state.request_id},
        headers={"Retry-After": "1"} if exc.status in {429, 503} else None,
    )


@app.exception_handler(RequestValidationError)
async def validation_error(request, exc):
    return await business_error(request, Failure("INVALID_REQUEST", 422))


async def contention_error(request, exc):
    return await business_error(request, Failure("RESOURCE_BUSY", 409))


async def unavailable_error(request, exc):
    return await business_error(request, Failure("DATABASE_UNAVAILABLE", 503))


for error_type in (LockNotAvailable, DeadlockDetected):
    app.add_exception_handler(error_type, contention_error)
for error_type in (PoolTimeout, TooManyRequests, OperationalError, QueryCanceled):
    app.add_exception_handler(error_type, unavailable_error)


@app.get("/health/live", tags=["Operations"])
def live():
    return {"status": "alive"}


@app.get("/health/ready", tags=["Operations"], responses=ERRORS)
def ready(request: Request):
    with request.app.state.db.transaction() as conn:
        conn.execute("SELECT 1")
    try:
        request.app.state.cache.redis.ping()
    except Exception as exc:
        raise Failure("ADMISSION_UNAVAILABLE", 503) from exc
    return {"status": "ready"}


@app.get("/metrics", include_in_schema=False)
def metrics():
    return Response(generate_latest(), media_type=CONTENT_TYPE_LATEST)


@app.get("/v1/events", tags=["Browse"], responses=ERRORS)
def events(request: Request):
    with request.app.state.db.transaction() as conn:
        return conn.execute("SELECT * FROM events ORDER BY sale_starts LIMIT 100").fetchall()


@app.get("/v1/events/{event_id}/seats", tags=["Browse"], responses=ERRORS)
def seats(event_id: UUID, request: Request):
    """Pre-warmed snapshot. No database fallback on cache miss or outage."""
    return request.app.state.cache.read(str(event_id))


@app.get("/v1/events/{event_id}/seat-deltas", tags=["Browse"], responses=ERRORS)
def deltas(event_id: UUID, request: Request, since: int = 0):
    """Current states changed since a snapshot version; use version from the response next time."""
    snapshot = request.app.state.cache.read(str(event_id))
    if since < 0 or since > snapshot["version"]:
        raise Failure("INVALID_VERSION", 422)
    return {**snapshot, "seats": [s for s in snapshot["seats"] if s["version"] > since]}


@app.post("/v1/holds", tags=["Reservations"], responses=ERRORS, status_code=201)
def hold(body: HoldInput, who: Actor, svc: Service, key: Key, request: Request):
    """Atomically hold 1–8 seats and create a pending order. Replays retain the original deadline."""
    request.app.state.cache.rate_limit(who)
    return svc.reserve(who, body.event_id, body.seat_ids, key)


@app.get("/v1/holds/{hold_id}", tags=["Reservations"], responses=ERRORS)
def get_hold(hold_id: UUID, who: Actor, svc: Service):
    return svc.get_hold(who, hold_id)


@app.delete("/v1/holds/{hold_id}", tags=["Reservations"], responses=ERRORS)
def release(hold_id: UUID, who: Actor, svc: Service):
    return svc.release(who, hold_id)


@app.post("/v1/orders", tags=["Checkout"], responses=ERRORS, status_code=201)
def checkout(body: OrderInput, who: Actor, svc: Service, key: Key):
    """Idempotently return the order created with the hold. Changed key payloads return 409."""
    return svc.checkout(who, body.hold_id, key)


@app.get("/v1/orders/{order_id}", tags=["Checkout"], responses=ERRORS)
def get_order(order_id: UUID, who: Actor, svc: Service):
    return svc.get_order(who, order_id)


@app.post("/v1/orders/{order_id}/payments", tags=["Payment simulator"], responses=ERRORS, status_code=202)
def payment(order_id: UUID, body: PaymentInput, who: Actor, svc: Service, key: Key):
    """Development only. Durable dispatch; no network calls inside reservation transactions."""
    if settings.environment != "development":
        raise Failure("SIMULATOR_DISABLED", 403)
    return svc.initiate_payment(who, order_id, key, body.outcome, body.delay_seconds, body.duplicates)


@app.post("/v1/webhooks/payments", tags=["Payments"], responses=ERRORS)
async def callback(
    request: Request,
    body: Callback,
    svc: Service,
    signature: Annotated[str, Header(alias="X-Payment-Signature")],
    timestamp: Annotated[int, Header(alias="X-Payment-Timestamp")],
):
    """HMAC-SHA256 over timestamp + '.' + raw JSON body. Signature expires after 5 minutes.

    Payload schema: callback_id, payment_id, order_id (UUID), amount (minor units), currency,
    outcome (SUCCEEDED or FAILED). Duplicate callback IDs and semantic duplicates are safe.
    """
    raw = await request.body()
    if len(raw) > 16384:
        raise Failure("PAYLOAD_TOO_LARGE", 413)
    expected = hmac.new(
        settings.webhook_secret.encode(), str(timestamp).encode() + b"." + raw, hashlib.sha256
    ).hexdigest()
    if abs(time.time() - timestamp) > 300 or not hmac.compare_digest(signature, expected):
        raise Failure("INVALID_SIGNATURE", 401)
    # The database adapter is synchronous; do not block FastAPI's event loop.
    from starlette.concurrency import run_in_threadpool

    return await run_in_threadpool(svc.callback, body.model_dump(mode="json"))

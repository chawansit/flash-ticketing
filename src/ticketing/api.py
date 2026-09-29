import asyncio
import hashlib
import hmac
import logging
import time
from contextlib import asynccontextmanager, suppress
from typing import Annotated, Literal
from uuid import UUID

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
from ticketing.http import RequestInstrumentation
from ticketing.infrastructure.cache import RedisSeats
from ticketing.infrastructure.postgres import Postgres
from ticketing.infrastructure.redis_reservations import RedisReservationIntake
from ticketing.infrastructure.reservations import PostgresReservations, RedisFirstReservations
from ticketing.observability import (
    DB_UNAVAILABLE,
    EVENT_LOOP_LAG_CURRENT_SECONDS,
    EVENT_LOOP_LAG_SECONDS,
    OUTCOMES,
    configure_logging,
    hold_phase,
    observe_hold_phase,
)

settings = Settings()
security = HTTPBearer()
log = logging.getLogger("ticketing.api")


async def observe_event_loop_lag(interval_seconds: float = 0.05):
    loop = asyncio.get_running_loop()
    while True:
        due = loop.time() + interval_seconds
        await asyncio.sleep(interval_seconds)
        lag = max(0.0, loop.time() - due)
        EVENT_LOOP_LAG_CURRENT_SECONDS.set(lag)
        EVENT_LOOP_LAG_SECONDS.observe(lag)


@asynccontextmanager
async def lifespan(app):
    settings.validate()
    configure_logging()
    db = Postgres(settings.database_url, settings.pool_max, settings.pool_wait_ms, settings.pool_max_waiting)
    cache = RedisSeats(settings.redis_url, seatmap_ttl_seconds=settings.seatmap_ttl_seconds)
    app.state.db, app.state.cache = db, cache
    durable = PostgresReservations(db, cache, settings.hold_seconds)
    intake = RedisReservationIntake(
        cache,
        hold_seconds=settings.hold_seconds,
        replica_acks=settings.redis_reservation_replica_acks,
        wait_ms=settings.redis_reservation_wait_ms,
        max_backlog=settings.redis_reservation_max_backlog,
        max_command_age_seconds=settings.redis_reservation_max_command_age_seconds,
    )
    store = RedisFirstReservations(durable, intake) if settings.reservation_mode == "redis-first" else durable
    app.state.reservation_intake = intake
    app.state.reservations = Reservations(store)
    loop_observer = asyncio.create_task(observe_event_loop_lag())
    try:
        yield
    finally:
        loop_observer.cancel()
        with suppress(asyncio.CancelledError):
            await loop_observer
        db.close()
        cache.redis.close()


app = FastAPI(
    title="Flash-sale Ticketing",
    version="0.1.0",
    lifespan=lifespan,
    description="Assigned-seat reservations with database-enforced durable ownership. "
    "Default holds create pending orders synchronously; opt-in Redis-first intake returns "
    "an explicit provisional command until its PostgreSQL writer commits. All amounts are "
    "integer minor units. Seat contention returns immediately; ambiguous Redis-first "
    "outcomes may be replayed only with the same idempotency key. Cached availability is "
    "advisory. No waiting room or frontend.",
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


app.add_middleware(RequestInstrumentation, owner=app, config=settings)


@app.exception_handler(Failure)
async def business_error(request, exc):
    request.state.error_code = exc.code
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
    # Keep metric labels fixed even when a driver raises an exception subclass.
    if isinstance(exc, PoolTimeout):
        cause = "PoolTimeout"
    elif isinstance(exc, TooManyRequests):
        cause = "TooManyRequests"
    elif isinstance(exc, QueryCanceled):
        cause = "QueryCanceled"
    else:
        cause = "OperationalError"
    request.state.db_failure_type = cause
    DB_UNAVAILABLE.labels(cause).inc()
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


class LayoutSeat(BaseModel):
    seat_id: str
    price: int


class SeatLayout(BaseModel):
    event_id: UUID
    seats: list[LayoutSeat]


class AvailableSeat(BaseModel):
    seat_id: str
    status: Literal["AVAILABLE", "HELD", "SOLD"]
    reserved_until: str | None


class Availability(BaseModel):
    event_id: UUID
    version: int
    incarnation: str
    seats: list[AvailableSeat]


class AvailabilityDelta(BaseModel):
    event_id: UUID
    from_version: int
    version: int
    incarnation: str
    reset_required: bool
    seats: list[AvailableSeat]


Conditional = Annotated[str | None, Header(alias="If-None-Match", max_length=8192)]
BROWSE_RESPONSES = {**ERRORS, 304: {"description": "Unchanged representation; no response body"}}


def browse_response(request, event_id, kind, validator):
    status, etag, body = request.app.state.cache.browse_encoded(str(event_id), kind, validator)
    headers = {
        "ETag": "W/" + etag,
        "Cache-Control": "public, max-age=3600, must-revalidate" if kind == "layout" else "private, no-cache",
    }
    if status == 304:
        return Response(status_code=304, headers=headers)
    return Response(body, media_type="application/json", headers=headers)


@app.get(
    "/v1/events/{event_id}/layout", tags=["Browse"], response_model=SeatLayout, responses=BROWSE_RESPONSES
)
def layout(event_id: UUID, request: Request, if_none_match: Conditional = None):
    """Static seat identifiers and show prices. Geometry is not modeled yet.

    Cache for one hour, then revalidate with If-None-Match. Existing inventory is static.
    """
    return browse_response(request, event_id, "layout", if_none_match)


@app.get(
    "/v1/events/{event_id}/availability",
    tags=["Browse"],
    response_model=Availability,
    responses=BROWSE_RESPONSES,
)
def availability(event_id: UUID, request: Request, if_none_match: Conditional = None):
    """Advisory availability without layout/prices. Always revalidate cached responses.

    Send the previous ETag in If-None-Match; unchanged reads return 304 without loading
    seats. Back off polling when unchanged and pause hidden clients. HELD includes its
    expiry; reservations always recheck PostgreSQL. Cache loss returns warming, not 304.
    """
    return browse_response(request, event_id, "availability", if_none_match)


@app.get(
    "/v1/events/{event_id}/seat-deltas",
    tags=["Browse"],
    response_model=AvailabilityDelta,
    responses=ERRORS,
)
def deltas(event_id: UUID, request: Request, since: int = 0, incarnation: str | None = None):
    """Changes since a composite snapshot cursor; reuse incarnation and version."""
    return JSONResponse(content=request.app.state.cache.deltas(str(event_id), since, incarnation))


HOLD_RESPONSES = {
    **ERRORS,
    202: {"description": "Redis-first provisional hold accepted; poll the command status"},
}


@app.post("/v1/holds", tags=["Reservations"], responses=HOLD_RESPONSES, status_code=201)
def hold(body: HoldInput, who: Actor, svc: Service, key: Key, request: Request):
    """Hold 1-8 seats idempotently.

    Redis-first mode returns 202 until PostgreSQL is durable. A
    RESERVATION_DURABILITY_UNKNOWN response may be replayed only with the identical
    actor, payload and Idempotency-Key.
    """
    observe_hold_phase("dispatch", time.monotonic() - request.state.hold_admitted_at)
    with hold_phase("rate_limit"):
        request.app.state.cache.rate_limit(who)
    result = svc.reserve(who, body.event_id, body.seat_ids, key)
    if result.get("persistence_status") == "PENDING":
        return JSONResponse(status_code=202, content=result)
    return result


@app.get(
    "/v1/reservation-commands/{event_id}/{command_id}",
    tags=["Reservations"],
    responses=ERRORS,
)
def reservation_command(event_id: UUID, command_id: UUID, who: Actor, request: Request):
    """Poll provisional Redis-first intake until it becomes DURABLE or FAILED."""
    return request.app.state.reservation_intake.status(event_id, command_id, who)


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

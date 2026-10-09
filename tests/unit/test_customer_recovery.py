import asyncio
import sys
from pathlib import Path

import httpx
import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[2] / "scripts"))

from customer_recovery_client import CustomerRecoveryClient, RecoveryPolicy


class Clock:
    def __init__(self):
        self.now = 0.0
        self.delays = []

    def __call__(self):
        return self.now

    async def sleep(self, seconds):
        self.delays.append(seconds)
        self.now += seconds


def exercise(handler, *, deadline=10, attempts=3):
    async def run():
        clock = Clock()
        async with httpx.AsyncClient(base_url="http://test", transport=httpx.MockTransport(handler)) as client:
            wrapper = CustomerRecoveryClient(client, deadline, RecoveryPolicy(max_attempts=attempts),
                clock=clock, sleep=clock.sleep, uniform=lambda low, high: high)
            response = await wrapper.post("/v1/orders/order/payments", json={"outcome": "SUCCEEDED"},
                headers={"Authorization": "Bearer owner", "Idempotency-Key": "original"})
            return response, wrapper, clock
    return asyncio.run(run())


def operation(**changes):
    return {"order_id": "order", "payment_id": None, "state": "NOT_STARTED", "can_initiate": True, **changes}


@pytest.mark.parametrize("failure", ["lost_response", "503"])
def test_existing_committed_operation_is_checked_and_never_posted_again(failure):
    requests = []

    def handler(request):
        requests.append(request)
        if request.method == "POST":
            if failure == "lost_response":
                raise httpx.ReadError("response lost after commit", request=request)
            return httpx.Response(503)
        return httpx.Response(200, json=operation(payment_id="payment", state="PENDING"))

    response, wrapper, _ = exercise(handler)
    assert response.status_code == 202
    assert len(requests) == 2 and requests[-1].url.path.endswith("/payment-operation")
    assert requests[-1].headers["Idempotency-Key"] == "original"
    assert requests[-1].headers["Authorization"] == "Bearer owner"
    assert wrapper.operation_checks == 1 and not wrapper.retry_attempts
    assert wrapper.evidence("fulfilled")["recovered"]


def test_rejection_replays_identical_request_only_after_authoritative_absence():
    posts, methods = [], []

    def handler(request):
        methods.append(request.method)
        if request.method == "GET":
            return httpx.Response(200, json=operation())
        posts.append(request)
        return httpx.Response(503 if len(posts) == 1 else 202)

    response, wrapper, clock = exercise(handler)
    assert response.status_code == 202 and methods == ["POST", "GET", "POST"]
    assert posts[0].content == posts[1].content
    assert posts[0].headers == posts[1].headers
    assert dict(wrapper.retry_attempts) == {"payment": 1}
    assert dict(wrapper.first_attempt_errors) == {"payment:http_503": 1}
    assert clock.delays == [0.1]


@pytest.mark.parametrize("code", [401, 403, 404, 409, 422])
def test_permanent_failure_never_checks_or_retries(code):
    requests = []
    response, wrapper, clock = exercise(lambda request: (requests.append(request), httpx.Response(code))[1])
    assert response.status_code == code and len(requests) == 1
    assert wrapper.operation_checks == 0 and not wrapper.retry_attempts and clock.delays == []


@pytest.mark.parametrize("body", [operation(can_initiate=False), operation(order_id="someone-else"),
                                  operation(state="UNKNOWN")])
def test_expiry_or_invalid_identity_never_reposts(body):
    posts = []

    def handler(request):
        if request.method == "POST":
            posts.append(request)
            return httpx.Response(503)
        return httpx.Response(200, json=body)

    if body["order_id"] != "order":
        with pytest.raises(ValueError):
            exercise(handler)
    else:
        response, _, _ = exercise(handler)
        assert response.status_code == 503
    assert len(posts) == 1


def test_lookup_unavailable_never_blindly_reposts_payment():
    posts = []

    def handler(request):
        if request.method == "POST":
            posts.append(request)
        return httpx.Response(503)

    response, wrapper, _ = exercise(handler)
    assert response.status_code == 503 and len(posts) == 1
    assert wrapper.retry_attempts["payment_operation"] == 2
    assert wrapper.retry_attempts["payment"] == 0


def test_retry_after_exceeding_original_deadline_stops_without_replay():
    response, wrapper, clock = exercise(lambda request: httpx.Response(429, headers={"Retry-After": "20"}))
    assert response.status_code == 429 and wrapper.operation_checks == 0
    assert not wrapper.retry_attempts and clock.delays == []


def test_retry_after_minimum_is_respected():
    posts = []

    def handler(request):
        if request.method == "GET":
            return httpx.Response(200, json=operation())
        posts.append(request)
        return httpx.Response(429, headers={"Retry-After": "2"}) if len(posts) == 1 else httpx.Response(202)

    response, _, clock = exercise(handler)
    assert response.status_code == 202 and clock.delays == [2]


def test_sustained_rejection_is_bounded_and_preserves_first_failure():
    posts = []

    def handler(request):
        if request.method == "GET":
            return httpx.Response(200, json=operation())
        posts.append(request)
        return httpx.Response(503)

    response, wrapper, _ = exercise(handler)
    assert response.status_code == 503 and len(posts) == 3
    assert wrapper.retry_attempts["payment"] == 2 and wrapper.operation_checks == 3
    assert wrapper.evidence("payment_http_503")["recovered"] is False
    assert wrapper.first_attempt_errors["payment:http_503"] == 1


def test_confirmation_recovery_reads_only_and_reports_processing():
    async def run():
        clock = Clock()
        requests = []

        def handler(request):
            requests.append(request)
            return httpx.Response(503) if len(requests) == 1 else httpx.Response(200, json={"status": "FULFILLED"})

        async with httpx.AsyncClient(base_url="http://test", transport=httpx.MockTransport(handler)) as client:
            wrapper = CustomerRecoveryClient(client, 10, clock=clock, sleep=clock.sleep)
            response = await wrapper.get("/v1/orders/order", headers={"Authorization": "Bearer owner"})
        assert response.status_code == 200 and all(request.method == "GET" for request in requests)
        assert wrapper.processing and wrapper.retry_attempts["order_status"] == 1
        assert wrapper.first_attempt_errors["order_status:http_503"] == 1
    asyncio.run(run())


@pytest.mark.parametrize("attempts", [0, 4, True])
def test_invalid_policy_rejected(attempts):
    with pytest.raises(ValueError):
        RecoveryPolicy(max_attempts=attempts)


@pytest.mark.parametrize("method,url,expected,role", [
    ("post", "/v1/holds", 202, "hold"),
    ("get", "/v1/reservation-commands/show/command", 200, "reservation_command"),
])
def test_non_payment_failures_are_visible_but_not_retried(method, url, expected, role):
    async def run():
        calls = []
        async with httpx.AsyncClient(base_url="http://test", transport=httpx.MockTransport(
            lambda request: (calls.append(request), httpx.Response(503))[1])) as client:
            wrapper = CustomerRecoveryClient(client, 10, clock=Clock())
            response = await getattr(wrapper, method)(url)
            assert response.status_code == 503 and len(calls) == 1
            assert wrapper.first_attempt_errors == {role + ":http_503": 1}
            assert not wrapper.retry_attempts
    asyncio.run(run())

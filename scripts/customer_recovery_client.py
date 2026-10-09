"""Bounded customer recovery; no frontend or external gateway adapter."""

import asyncio
import math
import random
from collections import Counter
from dataclasses import dataclass
from datetime import UTC, datetime
from email.utils import parsedate_to_datetime
from time import perf_counter

import httpx


@dataclass(frozen=True)
class RecoveryPolicy:
    max_attempts: int = 3
    base_delay_seconds: float = 0.1
    max_delay_seconds: float = 1.0

    def __post_init__(self):
        if (type(self.max_attempts) is not int or not 1 <= self.max_attempts <= 3
                or not 0 < self.base_delay_seconds <= self.max_delay_seconds <= 1):
            raise ValueError("Bounded recovery policy required")


class CustomerRecoveryClient:
    """Wrap a caller's HTTP client with a fixed original journey deadline.

    Ordinary successful polling is not a retry. Operation checks and all physical
    requests remain separately observable by the caller's HTTP transport hooks.
    """

    def __init__(self, client, deadline, policy=None, *, clock=perf_counter,
                 sleep=asyncio.sleep, uniform=random.uniform):
        self.client, self.deadline = client, deadline
        self.policy = policy or RecoveryPolicy()
        self.clock, self.sleep, self.uniform = clock, sleep, uniform
        self.first_attempt_errors = Counter()
        self.retry_attempts = Counter()
        self.operation_checks = 0
        self.processing = False

    @staticmethod
    def reason(response, error):
        if error is not None:
            return "transport_" + type(error).__name__
        if response.status_code in {429, 503}:
            return "http_" + str(response.status_code)
        return None

    def remaining(self):
        return max(0.0, self.deadline - self.clock())

    async def send(self, method, url, kwargs):
        if self.remaining() <= 0:
            raise httpx.TimeoutException("Original journey deadline elapsed")
        try:
            response = await asyncio.wait_for(
                getattr(self.client, method)(url, **kwargs), timeout=self.remaining())
            return response, None
        except TimeoutError:
            return None, httpx.TimeoutException("Original journey deadline elapsed")
        except httpx.TransportError as error:
            return None, error

    async def pause(self, attempt, response):
        delay = self.uniform(0.5, 1.0) * min(
            self.policy.max_delay_seconds, self.policy.base_delay_seconds * 2 ** attempt)
        value = response.headers.get("Retry-After") if response is not None else None
        if value is not None:
            try:
                minimum = float(value)
                if minimum < 0 or not math.isfinite(minimum):
                    raise ValueError("Invalid Retry-After")
            except ValueError:
                try:
                    minimum = max(0.0, (parsedate_to_datetime(value) - datetime.now(UTC)).total_seconds())
                except (ValueError, TypeError, OverflowError):
                    minimum = 0.0
            delay = max(delay, minimum)
        if delay >= self.remaining():
            return False
        await self.sleep(delay)
        return self.remaining() > 0

    @staticmethod
    def finish(response, error):
        if error is not None:
            raise error
        return response

    async def read(self, url, kwargs, role):
        for attempt in range(self.policy.max_attempts):
            if attempt:
                self.retry_attempts[role] += 1
            response, error = await self.send("get", url, kwargs)
            reason = self.reason(response, error)
            if attempt == 0 and (reason is not None or response.status_code != 200):
                self.first_attempt_errors[role + ":" + (reason or "http_" + str(response.status_code))] += 1
            if reason is None:
                return response
            self.processing = True
            if attempt + 1 == self.policy.max_attempts or not await self.pause(attempt, response):
                return self.finish(response, error)
        raise AssertionError("Unreachable recovery loop")

    async def single(self, method, url, kwargs, role, expected):
        response, error = await self.send(method, url, kwargs)
        if error is not None or response.status_code != expected:
            reason = self.reason(response, error) or "http_" + str(response.status_code)
            self.first_attempt_errors[role + ":" + reason] += 1
        return self.finish(response, error)

    async def get(self, url, **kwargs):
        if url.startswith("/v1/orders/"):
            return await self.read(url, kwargs, "order_status")
        return await self.single("get", url, kwargs, "reservation_command", 200)

    async def post(self, url, **kwargs):
        if not url.endswith("/payments"):
            return await self.single("post", url, kwargs, "hold", 202)
        headers = kwargs.get("headers", {})
        if not headers.get("Idempotency-Key"):
            raise ValueError("Payment recovery requires the original idempotency key")
        order_id = url.split("/")[-2]
        for attempt in range(self.policy.max_attempts):
            if attempt:
                self.retry_attempts["payment"] += 1
            response, error = await self.send("post", url, kwargs)
            reason = self.reason(response, error)
            if attempt == 0 and (reason is not None or response.status_code != 202):
                self.first_attempt_errors["payment:" + (reason or "http_" + str(response.status_code))] += 1
            if reason is None:
                return response
            self.processing = True
            if not await self.pause(attempt, response):
                return self.finish(response, error)
            self.operation_checks += 1
            operation = await self.read(
                url.removesuffix("/payments") + "/payment-operation",
                {"headers": headers}, "payment_operation")
            if operation.status_code != 200:
                return self.finish(response, error)
            body = operation.json()
            if body.get("order_id") != order_id:
                raise ValueError("Operation identity mismatch")
            if body.get("payment_id") is not None:
                if body.get("state") not in {"PENDING", "SUCCEEDED", "FAILED"}:
                    raise ValueError("Unknown payment operation state")
                return httpx.Response(202, json={"payment_id": body["payment_id"], "order_id": order_id})
            if body.get("state") != "NOT_STARTED" or body.get("can_initiate") is not True:
                return self.finish(response, error)
            if attempt + 1 == self.policy.max_attempts:
                return self.finish(response, error)
        raise AssertionError("Unreachable recovery loop")

    def evidence(self, outcome):
        retries = sum(self.retry_attempts.values())
        return {
            "first_attempt_errors": dict(self.first_attempt_errors),
            "retry_attempts": retries,
            "retry_attempts_by_operation": dict(self.retry_attempts),
            "payment_operation_checks": self.operation_checks,
            "recovered": outcome == "fulfilled" and bool(self.first_attempt_errors),
            "confirmation_processing_observed": self.processing,
        }

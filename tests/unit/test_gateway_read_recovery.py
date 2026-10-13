"""Gateway read retries preserve authorization, deadlines, counters and write behavior."""
import asyncio
import hashlib
import json
import sys
from pathlib import Path
from time import perf_counter

import httpx
import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[2] / "scripts"))
from customer_gateway_read_recovery import GatewayReadRecoveryClient
from customer_recovery_client import CustomerRecoveryClient


async def no_sleep(_):
    return None


@pytest.mark.parametrize("code", [502, 504])
def test_gateway_read_recovers_with_original_authorization_and_exact_counts(code):
    async def scenario():
        calls = []
        def handler(request):
            calls.append((request.method, request.headers["Authorization"]))
            return httpx.Response(code if len(calls) == 1 else 200, json={"status": "FULFILLED"})
        async with httpx.AsyncClient(base_url="http://test", transport=httpx.MockTransport(handler)) as client:
            deadline = perf_counter() + 10
            recovery = GatewayReadRecoveryClient(client, deadline, sleep=no_sleep)
            result = await recovery.get("/v1/orders/existing", headers={"Authorization": "Bearer test-only"})
            assert result.status_code == 200 and recovery.deadline == deadline
            assert calls == [("GET", "Bearer test-only")] * 2
            assert recovery.first_attempt_errors == {"order_status:http_" + str(code): 1}
            assert recovery.retry_attempts == {"order_status": 1}
            assert recovery.processing
    asyncio.run(scenario())


def test_exhausted_gateway_read_keeps_real_failure_and_three_attempt_cap():
    async def scenario():
        calls = []
        def handler(request):
            calls.append(request.method)
            return httpx.Response(502)
        async with httpx.AsyncClient(base_url="http://test", transport=httpx.MockTransport(handler)) as client:
            recovery = GatewayReadRecoveryClient(client, perf_counter() + 10, sleep=no_sleep)
            response = await recovery.get("/v1/orders/existing")
            assert response.status_code == 502 and calls == ["GET"] * 3
            assert recovery.retry_attempts == {"order_status": 2}
            assert recovery.first_attempt_errors == {"order_status:http_502": 1}
    asyncio.run(scenario())


def test_read_does_not_extend_original_deadline():
    async def scenario():
        now, calls = [0.0], []
        async def sleep(_):
            now[0] = 2.0
        def handler(request):
            calls.append(request.method)
            return httpx.Response(502)
        async with httpx.AsyncClient(base_url="http://test", transport=httpx.MockTransport(handler)) as client:
            recovery = GatewayReadRecoveryClient(client, 1.0, clock=lambda: now[0], sleep=sleep)
            response = await recovery.get("/v1/orders/existing")
            assert response.status_code == 502 and calls == ["GET"] and recovery.deadline == 1.0
    asyncio.run(scenario())


def test_optional_read_client_does_not_change_payment_write_policy():
    async def once(kind):
        calls = []
        def handler(request):
            calls.append((request.method, request.url.path))
            return httpx.Response(502)
        async with httpx.AsyncClient(base_url="http://test", transport=httpx.MockTransport(handler)) as client:
            recovery = kind(client, perf_counter() + 10, sleep=no_sleep)
            response = await recovery.post("/v1/orders/existing/payments", headers={"Idempotency-Key": "same"})
            return response.status_code, calls
    assert asyncio.run(once(GatewayReadRecoveryClient)) == asyncio.run(once(CustomerRecoveryClient))


def test_old_sealed_cloud_bundle_is_unchanged_and_new_client_is_not_enabled():
    root = Path(__file__).resolve().parents[2]
    manifest = json.loads((root / "artifacts/customer-recovery/manifest.json").read_text(encoding="utf-8"))
    raw = (root / "scripts/customer_recovery_client.py").read_bytes().replace(b"\\r\\n", b"\\n")
    assert hashlib.sha256(raw).hexdigest() == manifest["overlays"]["scripts/customer_recovery_client.py"]
    assert "scripts/customer_gateway_read_recovery.py" not in manifest["overlays"]

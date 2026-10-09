import asyncio
import sys
from datetime import UTC, datetime, timedelta
from pathlib import Path
from types import SimpleNamespace

sys.path.insert(0, str(Path(__file__).resolve().parents[2] / "scripts"))

import checkout_journey_probe as probe
import httpx
import paid_ticket_load_generator as generator
import paid_ticket_sharded_generator as shards
from customer_recovery_client import RecoveryPolicy


def manifest():
    return {"schema_version": 1, "environment": "development", "origin": "http://test",
            "show_ids": ["show"], "viewer_tokens": ["owner"], "seat_offset": 0,
            "seats_per_show": 300, "expires_at": (datetime.now(UTC)+timedelta(hours=1)).isoformat()}


def recovering_handler():
    calls, payments, orders = [], [], []

    def handler(request):
        calls.append(request)
        path = request.url.path
        if path == "/health/ready":
            return httpx.Response(200)
        if path == "/v1/holds":
            return httpx.Response(202, json={"command_id": "command", "order_id": "order"})
        if path.startswith("/v1/reservation-commands/"):
            return httpx.Response(200, json={"persistence_status": "DURABLE"})
        if path.endswith("/payment-operation"):
            return httpx.Response(200, json={"order_id": "order", "payment_id": None,
                "state": "NOT_STARTED", "can_initiate": True})
        if path.endswith("/payments"):
            payments.append(request)
            return httpx.Response(503 if len(payments) == 1 else 202)
        orders.append(request)
        return (httpx.Response(503) if len(orders) == 1 else
                httpx.Response(200, json={"status": "FULFILLED", "tickets": [{"id": "ticket"}]}))
    return handler, calls, payments


def test_recovery_journey_reports_both_first_failures_and_same_key_replay():
    async def run():
        handler, calls, payments = recovering_handler()
        async with httpx.AsyncClient(base_url="http://test", transport=httpx.MockTransport(handler)) as client:
            row = await probe.journey(client, manifest(), 0, "run", 5, .05, 1,
                recovery_policy=RecoveryPolicy(base_delay_seconds=.001))
        assert row["outcome"] == "fulfilled" and row["recovered"]
        assert row["first_attempt_errors"] == {"payment:http_503": 1, "order_status:http_503": 1}
        assert row["retry_attempts"] == 2 and row["payment_operation_checks"] == 1
        assert row["total_journey_ms"] >= row["hold_to_ticket_ms"] > 0
        assert payments[0].content == payments[1].content
        assert payments[0].headers["Idempotency-Key"] == payments[1].headers["Idempotency-Key"]
        assert len(calls) == 7
    asyncio.run(run())


def test_generator_preserves_recovery_accounting_without_hiding_traffic(monkeypatch, tmp_path):
    handler, _, _ = recovering_handler()
    original = httpx.AsyncClient
    monkeypatch.setattr(generator.httpx, "AsyncClient",
        lambda **kwargs: original(**kwargs, transport=httpx.MockTransport(handler)))
    args = SimpleNamespace(origin="http://test", rate=1, seconds=1, concurrency=1, duplicates=1,
        http_max_connections=1, http_client_count=1, timeout_seconds=5, poll_seconds=.05,
        completion_deadline_seconds=5, start_at_epoch=None, output=tmp_path/"result.json",
        recovery_max_attempts=3)
    result = asyncio.run(generator.scheduled_journeys(args, manifest()))
    assert result["pass"] and result["generator_drops"] == 0
    assert result["first_attempt_error_journeys"] == result["recovered_journeys"] == 1
    assert result["retry_attempts"] == 2 and result["final_customer_failures"] == 0
    assert result["physical_http_attempts"] == {"holds": 1, "reservation_commands": 1,
        "payments": 2, "payment_operations": 1, "orders": 2}
    assert result["total_journey_p95_ms"] > 0
    both = shards.aggregate([result, result], 2, 1, 2, [0, 0], recovery_max_attempts=3)
    assert both["pass"] and both["retry_attempts"] == 4
    assert both["total_journey_p95_ms"] >= result["total_journey_p95_ms"]
    assert both["first_attempt_errors"] == {"payment:http_503": 2, "order_status:http_503": 2}
    assert not shards.aggregate([result, result], 2, 1, 2, [0, 0])["pass"]

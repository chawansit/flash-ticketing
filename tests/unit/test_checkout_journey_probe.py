import asyncio
from importlib.util import module_from_spec, spec_from_file_location
from pathlib import Path

import httpx

spec = spec_from_file_location(
    "checkout_probe", Path(__file__).resolve().parents[2] / "scripts/checkout_journey_probe.py"
)
probe = module_from_spec(spec)
spec.loader.exec_module(probe)


def manifest():
    return {
        "show_ids": ["11111111-1111-1111-1111-111111111111"],
        "viewer_tokens": ["private-test-token"],
        "seat_offset": 0,
        "seats_per_show": 300,
    }


def test_waits_for_durable_before_payment_and_counts_one_ticket():
    command_polls = 0
    payment_calls = 0
    order_polls = 0

    def response(request):
        nonlocal command_polls, payment_calls, order_polls
        if request.method == "POST" and request.url.path == "/v1/holds":
            assert request.headers["Idempotency-Key"].endswith("-hold")
            return httpx.Response(202, json={"command_id": "c1", "order_id": "o1"})
        if request.method == "GET" and request.url.path.endswith("/c1"):
            command_polls += 1
            return httpx.Response(
                200, json={"persistence_status": "PENDING" if command_polls == 1 else "DURABLE"}
            )
        if request.method == "POST" and request.url.path == "/v1/orders/o1/payments":
            assert command_polls == 2
            assert request.headers["Idempotency-Key"].endswith("-payment")
            payment_calls += 1
            return httpx.Response(202, json={"payment_id": "p1"})
        if request.method == "GET" and request.url.path == "/v1/orders/o1":
            order_polls += 1
            return httpx.Response(
                200,
                json={
                    "status": "PAID" if order_polls == 1 else "FULFILLED",
                    "tickets": [] if order_polls == 1 else [{"id": "t1", "seat_id": "S0"}],
                },
            )
        raise AssertionError(f"Unexpected {request.method} {request.url.path}")

    async def exercise():
        async with httpx.AsyncClient(
            base_url="http://unused.invalid", transport=httpx.MockTransport(response)
        ) as client:
            return await probe.journey(client, manifest(), 0, "run", 2, 0.01, 3)

    result = asyncio.run(exercise())
    assert result["outcome"] == "fulfilled"
    assert result["ticket_id"] == "t1"
    assert result["durable_ms"] > 0
    assert result["hold_to_ticket_ms"] >= result["durable_ms"]
    assert payment_calls == 1
    assert order_polls == 2


def test_failed_reservation_never_initiates_payment():
    paths = []

    def response(request):
        paths.append(request.url.path)
        if request.url.path == "/v1/holds":
            return httpx.Response(202, json={"command_id": "c1", "order_id": "o1"})
        if request.url.path.endswith("/c1"):
            return httpx.Response(200, json={"persistence_status": "FAILED"})
        raise AssertionError("Payment must not be initiated")

    async def exercise():
        async with httpx.AsyncClient(
            base_url="http://unused.invalid", transport=httpx.MockTransport(response)
        ) as client:
            return await probe.journey(client, manifest(), 0, "run", 2, 0.01, 3)

    result = asyncio.run(exercise())
    assert result == {"outcome": "command_failed"}
    assert paths == ["/v1/holds", "/v1/reservation-commands/11111111-1111-1111-1111-111111111111/c1"]


def test_duplicate_ticket_shape_is_not_counted_as_fulfilled():
    def response(request):
        if request.url.path == "/v1/holds":
            return httpx.Response(202, json={"command_id": "c1", "order_id": "o1"})
        if request.url.path.endswith("/c1"):
            return httpx.Response(200, json={"persistence_status": "DURABLE"})
        if request.method == "POST":
            return httpx.Response(202, json={"payment_id": "p1"})
        return httpx.Response(
            200, json={"status": "FULFILLED", "tickets": [{"id": "t1"}, {"id": "t2"}]}
        )

    async def exercise():
        async with httpx.AsyncClient(
            base_url="http://unused.invalid", transport=httpx.MockTransport(response)
        ) as client:
            return await probe.journey(client, manifest(), 0, "run", 2, 0.01, 3)

    result = asyncio.run(exercise())
    assert result["outcome"] == "ticket_count_mismatch"



def test_aggregate_result_omits_private_tokens_and_ids(monkeypatch, tmp_path):
    import json
    from datetime import UTC, datetime, timedelta
    from types import SimpleNamespace

    private = {
        **manifest(),
        "schema_version": 1,
        "environment": "development",
        "origin": "http://unused.invalid",
        "expires_at": (datetime.now(UTC) + timedelta(hours=1)).isoformat(),
    }
    manifest_path = tmp_path / "private.json"
    result_path = tmp_path / "result.json"
    manifest_path.write_text(json.dumps(private))
    client_type = httpx.AsyncClient

    def response(request):
        if request.url.path == "/health/ready":
            return httpx.Response(200, json={"ready": True})
        if request.url.path == "/v1/holds":
            return httpx.Response(202, json={"command_id": "private-command", "order_id": "private-order"})
        if request.url.path.endswith("/private-command"):
            return httpx.Response(200, json={"persistence_status": "DURABLE"})
        if request.method == "POST":
            return httpx.Response(202, json={"payment_id": "private-payment"})
        return httpx.Response(
            200, json={"status": "FULFILLED", "tickets": [{"id": "private-ticket"}]}
        )

    monkeypatch.setattr(
        probe.httpx, "AsyncClient",
        lambda **kwargs: client_type(transport=httpx.MockTransport(response), **kwargs),
    )
    args = SimpleNamespace(
        manifest=manifest_path, origin="http://unused.invalid", output=result_path,
        journeys=1, concurrency=1, timeout_seconds=2, poll_seconds=0.01, duplicates=3,
    )
    asyncio.run(probe.run(args))
    result = json.loads(result_path.read_text())
    assert result["pass"] is True
    assert result["distinct_tickets"] == 1
    assert result["outcomes"] == {"fulfilled": 1}
    assert not any(
        value in result_path.read_text()
        for value in ("private-test-token", "private-command", "private-order", "private-ticket")
    )

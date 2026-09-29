import asyncio
import sys
from datetime import UTC, datetime, timedelta
from pathlib import Path
from types import SimpleNamespace

sys.path.insert(0, str(Path(__file__).resolve().parents[2] / "scripts"))
import paid_ticket_load_generator as generator


class ReadyResponse:
    def raise_for_status(self):
        return None


class FakeClient:
    def __init__(self, **kwargs):
        self.kwargs = kwargs

    async def __aenter__(self):
        return self

    async def __aexit__(self, *_):
        return None

    async def get(self, url):
        assert url == "/health/ready"
        return ReadyResponse()


def args(tmp_path, rate=5, concurrency=10):
    return SimpleNamespace(
        origin="http://127.0.0.1:8000",
        output=tmp_path / "result.json",
        rate=rate,
        seconds=1,
        completion_deadline_seconds=3,
        concurrency=concurrency,
        duplicates=3,
        timeout_seconds=2,
        poll_seconds=0.1,
    )


def manifest():
    return {
        "schema_version": 1,
        "environment": "development",
        "origin": "http://127.0.0.1:8000",
        "expires_at": (datetime.now(UTC) + timedelta(hours=1)).isoformat(),
        "show_ids": ["11111111-1111-1111-1111-111111111111"],
        "viewer_tokens": ["private-token"],
        "seat_offset": 0,
        "seats_per_show": 300,
    }


def fulfilled(index):
    return {
        "outcome": "fulfilled",
        "order_id": f"order-{index}",
        "ticket_id": f"ticket-{index}",
        "durable_ms": 10,
        "payment_to_ticket_ms": 20,
        "hold_to_ticket_ms": 30,
    }


def test_open_loop_accounts_for_every_scheduled_journey(monkeypatch, tmp_path):
    monkeypatch.setattr(generator.httpx, "AsyncClient", FakeClient)

    async def fake_journey(client, manifest, index, *_):
        await asyncio.sleep(0.01)
        return fulfilled(index)

    result = asyncio.run(generator.scheduled_journeys(args(tmp_path), manifest(), fake_journey))
    assert result["scheduled"] == result["dispatched"] == result["fulfilled"] == 5
    assert result["fulfilled_by_deadline"] == 5
    assert result["generator_drops"] == 0
    assert result["distinct_tickets"] == 5
    assert result["pass"]


def test_saturated_generator_reports_drops_without_queuing(monkeypatch, tmp_path):
    monkeypatch.setattr(generator.httpx, "AsyncClient", FakeClient)

    async def slow_journey(client, manifest, index, *_):
        await asyncio.sleep(0.4)
        return fulfilled(index)

    result = asyncio.run(generator.scheduled_journeys(args(tmp_path, 10, 1), manifest(), slow_journey))
    assert result["scheduled"] == 10
    assert result["generator_drops"] > 0
    assert result["dispatched"] + result["generator_drops"] == 10
    assert result["completed"] == result["dispatched"]
    assert not result["pass"]


def test_insufficient_seats_rejected_before_dispatch(tmp_path):
    fixture = manifest()
    fixture["seats_per_show"] = 4
    try:
        generator.validate(args(tmp_path), fixture)
    except ValueError as exc:
        assert "insufficient distinct seats" in str(exc)
    else:
        raise AssertionError("Expected fixture-capacity rejection")

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
        http_max_connections=concurrency,
        start_at_epoch=None,
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
        "hold_http_ms": 3,
        "command_durable_wait_ms": 7,
        "durable_ms": 10,
        "payment_http_ms": 4,
        "ticket_wait_ms": 16,
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
    assert result["http_max_connections"] == 10
    assert result["distinct_tickets"] == 5
    assert result["hold_http_p95_ms"] == 3
    assert result["hold_app_p95_ms"] is None
    assert result["hold_app_timing_samples"] == 0
    assert result["transport_phase_samples"]["holds"]["pre_send_ms"] == 0
    assert result["command_durable_wait_p95_ms"] == 7
    assert result["payment_http_p95_ms"] == 4
    assert result["ticket_wait_p95_ms"] == 16
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


def test_generator_allows_pool_headroom_without_raising_journey_cap(tmp_path):
    candidate = args(tmp_path, concurrency=10)
    candidate.http_max_connections = 20
    assert generator.validate(candidate, manifest()) == 5
    candidate.http_max_connections = 9
    try:
        generator.validate(candidate, manifest())
    except ValueError as exc:
        assert "HTTP connection limit" in str(exc)
    else:
        raise AssertionError("Expected bounded pool rejection")


def test_partitioned_clients_keep_total_budget_and_close(monkeypatch, tmp_path):
    created, closed, used = [], [], set()

    class TrackingClient(FakeClient):
        def __init__(self, **kwargs):
            super().__init__(**kwargs)
            created.append(self)

        async def __aexit__(self, *_):
            closed.append(self)

    async def fake_journey(client, _manifest, index, *_):
        used.add(client)
        return fulfilled(index)

    monkeypatch.setattr(generator.httpx, "AsyncClient", TrackingClient)
    candidate = args(tmp_path)
    candidate.http_client_count = 3
    result = asyncio.run(generator.scheduled_journeys(candidate, manifest(), fake_journey))
    assert result["pass"]
    assert result["http_connection_budgets"] == [4, 3, 3]
    assert sum(c.kwargs["limits"].max_connections for c in created) == candidate.http_max_connections
    assert used == set(created) == set(closed)


def test_partitioned_clients_close_on_readiness_failure(monkeypatch, tmp_path):
    closed = []

    class UnreadyClient(FakeClient):
        async def get(self, _):
            raise RuntimeError("unready")

        async def __aexit__(self, *_):
            closed.append(self)

    monkeypatch.setattr(generator.httpx, "AsyncClient", UnreadyClient)
    candidate = args(tmp_path)
    candidate.http_client_count = 3
    try:
        asyncio.run(generator.scheduled_journeys(candidate, manifest()))
    except RuntimeError as exc:
        assert str(exc) == "unready"
    else:
        raise AssertionError("Expected readiness failure")
    assert len(closed) == 3


def test_invalid_client_partition_rejected_before_any_requests(tmp_path):
    candidate = args(tmp_path)
    candidate.http_client_count = candidate.http_max_connections + 1
    try:
        generator.validate(candidate, manifest())
    except ValueError as exc:
        assert "client count" in str(exc)
    else:
        raise AssertionError("Expected client budget rejection")

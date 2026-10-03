import asyncio
import sys
from pathlib import Path

import httpx

sys.path.insert(0, str(Path(__file__).resolve().parents[2] / "scripts"))

from checkout_journey_probe import journey

from scripts.diagnose_paid_generator import Responder, manifest


def test_real_journey_protocol_and_keepalive():
    async def check():
        async with Responder(response_ms=1, command_seconds=0.08, ticket_seconds=0.1) as server:
            fixture = manifest(server.origin, 2)
            async with httpx.AsyncClient(base_url=server.origin) as client:
                row = await journey(client, fixture, 0, "synthetic-test", 5, 0.05, 1)
                assert row["outcome"] == "fulfilled"
                repeated = await client.post("/v1/holds", json={"event_id": fixture["show_ids"][0],
                                            "seat_ids": ["S0"]},
                                            headers={"Idempotency-Key": "synthetic-test-0-hold"})
                assert repeated.json()["order_id"] == row["order_id"]
                assert (await client.get("/unknown")).status_code == 404
            assert len(server.orders) == 1
            assert server.connections == 1
            assert server.requests["orders"] >= 2
            assert server.requests["reservation_commands"] >= 2
        assert server.closed_connections == 1
        assert server.protocol_errors == 0
    asyncio.run(check())


def test_two_shard_fixture_has_sufficient_disjoint_seats():
    from scripts.paid_ticket_sharded_generator import split_manifest

    parts = split_manifest(manifest("http://127.0.0.1:1234", 6000), 2, 3000)
    assert set(parts[0]["show_ids"]).isdisjoint(parts[1]["show_ids"])
    assert all(len(part["show_ids"]) * part["seats_per_show"] >= 3000 for part in parts)


def test_generator_real_transport_lifecycle_samples_and_cleanup(tmp_path):
    from types import SimpleNamespace

    from scripts.paid_ticket_load_generator import scheduled_journeys

    async def check():
        async with Responder(response_ms=2, command_seconds=0.03, ticket_seconds=0.05) as server:
            fixture = manifest(server.origin, 2)
            args = SimpleNamespace(origin=server.origin, output=tmp_path / "unused.json",
                rate=2, seconds=1, concurrency=4, http_max_connections=4,
                http_client_count=2, lifecycle_diagnostics=True, completion_deadline_seconds=5,
                start_at_epoch=None, duplicates=1, timeout_seconds=3, poll_seconds=0.05)
            result = await scheduled_journeys(args, fixture)
            assert result["fulfilled"] == result["dispatched"] == 2
            for route, attempts in result["physical_http_attempts"].items():
                samples = result["transport_phase_samples"][route]
                assert samples["response_body_ms"] == samples["stream_close_ms"] == attempts
                assert samples["request_to_pool_release_ms"] == attempts
            assert result["lifecycle"]["stream_close_errors"] == {}
            assert result["lifecycle"]["loop_samples"] > 0
            assert result["lifecycle"]["journey_duration_by_outcome_ms"]["fulfilled"]["count"] == 2
        assert server.closed_connections == server.connections
        assert server.protocol_errors == 0

    asyncio.run(check())

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

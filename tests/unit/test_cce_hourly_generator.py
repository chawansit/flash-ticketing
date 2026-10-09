"""ADR0232 hourly allocation and bounded delegation checks; no cloud traffic."""

import asyncio
import sys
from pathlib import Path
from types import SimpleNamespace
from uuid import UUID

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[2] / "scripts"))
import cce_hourly_paid_generator as parent
import cce_hourly_paid_leaf as leaf
import paid_ticket_sharded_generator as frozen


@pytest.fixture(scope="module")
def cohort():
    return {
        "schema_version": 1,
        "environment": "development",
        "fixture_layout": "distributed",
        "origin": "http://10.1.137.69:8000",
        "show_ids": [str(UUID(int=i + 1)) for i in range(1008)],
        "viewer_tokens": ["actor-" + str(i) for i in range(302400)],
        "seats_per_show": 300,
        "seat_offset": 0,
    }


def arguments():
    return SimpleNamespace(
        rate=84,
        seconds=3600,
        completion_deadline_seconds=3720,
        concurrency=500,
        http_client_count=8,
        poll_seconds=1,
        duplicates=1,
        origin="http://10.1.137.69:8000",
        start_at_epoch=1030,
    )


def test_hourly_allocation_has_302400_disjoint_seats_and_actors(cohort):
    parts = frozen.split_manifest(cohort, 2, 151200)
    seats, actors, keys = set(), set(), set()
    for shard, part in enumerate(parts):
        views = {}
        for index in range(151200):
            view, local, block = leaf.allocation(part, index, views)
            seat = (view["show_ids"][local % 42], local // 42)
            actor = view["viewer_tokens"][local]
            key = (shard, block, local)
            assert seat not in seats and actor not in actors and key not in keys
            assert 0 <= seat[1] < 300
            seats.add(seat)
            actors.add(actor)
            keys.add(key)
        assert len(views) == 12
        assert all(len(view["show_ids"]) == 42 for view in views.values())
    assert len(seats) == len(actors) == len(keys) == 302400


@pytest.mark.parametrize(
    "field,value",
    [
        ("seconds", 300),
        ("seconds", 3601),
        ("rate", 86),
        ("concurrency", 1000),
        ("http_client_count", 16),
        ("duplicates", 3),
        ("poll_seconds", 0.2),
        ("completion_deadline_seconds", 420),
        ("start_at_epoch", 1000),
        ("origin", "http://127.0.0.1:8000"),
    ],
)
def test_parent_rejects_non_hourly_or_changed_settings(cohort, monkeypatch, field, value):
    monkeypatch.setattr(parent, "time", lambda: 1000)
    args = arguments()
    setattr(args, field, value)
    with pytest.raises(ValueError):
        parent.validate(args, cohort)


def test_parent_accepts_only_exact_cohort(cohort, monkeypatch):
    monkeypatch.setattr(parent, "time", lambda: 1000)
    parent.validate(arguments(), cohort)
    data = {**cohort, "show_ids": [cohort["show_ids"][0]] * 1008}
    with pytest.raises(ValueError):
        parent.validate(arguments(), data)
    data = {**cohort, "viewer_tokens": [cohort["viewer_tokens"][0]] * 302400}
    with pytest.raises(ValueError):
        parent.validate(arguments(), data)


@pytest.mark.parametrize("index", [-1, 151200, True, 1.1])
def test_leaf_rejects_invalid_indices(cohort, index):
    part = frozen.split_manifest(cohort, 2, 151200)[0]
    with pytest.raises(ValueError):
        leaf.allocation(part, index, {})


def test_leaf_delegates_schedule_and_transaction_without_retry(cohort):
    part = frozen.split_manifest(cohort, 2, 151200)[0]
    calls = []

    async def journey(client, view, index, run_id, timeout, poll, duplicates):
        calls.append(
            (
                view["show_ids"][index % 42],
                view["viewer_tokens"][index],
                index,
                run_id,
                timeout,
                poll,
                duplicates,
            )
        )
        return {"outcome": "fulfilled"}

    async def schedule(args, manifest, journey_fn):
        await journey_fn(None, manifest, 12599, "run", 90, 1, 1)
        await journey_fn(None, manifest, 12600, "run", 90, 1, 1)
        return {"pass": True}

    module = SimpleNamespace(scheduled_journeys=schedule, journey=journey)
    original = leaf.install(module)
    args = arguments()
    args.rate = 42
    args.concurrency = 250
    args.http_max_connections = 250
    assert asyncio.run(module.scheduled_journeys(args, part)) == {"pass": True}
    assert original is schedule and len(calls) == 2
    assert calls[0][2:] == (12599, "run-block-0", 90, 1, 1)
    assert calls[1][2:] == (0, "run-block-1", 90, 1, 1)
    assert calls[0][0] != calls[1][0] and calls[0][1] != calls[1][1]

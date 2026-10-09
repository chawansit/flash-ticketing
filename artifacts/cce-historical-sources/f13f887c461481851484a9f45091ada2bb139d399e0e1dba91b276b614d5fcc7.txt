from __future__ import annotations

import asyncio
import importlib.util
from collections import Counter
from pathlib import Path

import httpx

spec = importlib.util.spec_from_file_location(
    "bootstrap_generator", Path(__file__).resolve().parents[2] / "scripts/http_load_generator.py"
)
assert spec and spec.loader
generator = importlib.util.module_from_spec(spec)
spec.loader.exec_module(generator)


class Response:
    def __init__(self, status: int, show: str):
        self.status_code = status
        self.headers = {"etag": f'"{show}"'}
        self._show = show

    def json(self):
        return {"code": "SEATMAP_WARMING"} if self.status_code == 503 else {}

    def raise_for_status(self):
        if self.status_code >= 400:
            request = httpx.Request("GET", f"http://test/{self._show}")
            raise httpx.HTTPStatusError("bootstrap failed", request=request, response=None)


class Client:
    def __init__(self):
        self.calls = Counter()
        self.active = 0
        self.maximum_active = 0

    async def get(self, path):
        show = path.rsplit("/", 2)[-2]
        self.calls[show] += 1
        self.active += 1
        self.maximum_active = max(self.maximum_active, self.active)
        await asyncio.sleep(0.005)
        self.active -= 1
        if show == "retry" and self.calls[show] == 1:
            return Response(503, show)
        return Response(200, show)


def test_bootstrap_is_bounded_concurrent_and_records_warming_retry():
    client = Client()
    shows = ["retry"] + [f"show-{index}" for index in range(12)]

    validators, evidence = asyncio.run(
        generator.fetch_initial_validators(client, shows, concurrency=4, max_attempts=3)
    )

    assert len(validators) == len(shows)
    assert validators["retry"] == '"retry"'
    assert 1 < client.maximum_active <= 4
    assert evidence == {
        "concurrency": 4,
        "max_attempts": 3,
        "attempt_statuses": {"503:SEATMAP_WARMING": 1, "200:OTHER": 13},
        "retry_count": 1,
    }


def test_bootstrap_rejects_nonpositive_bounds():
    async def exercise():
        await generator.fetch_initial_validators(Client(), ["one"], concurrency=0)

    try:
        asyncio.run(exercise())
    except ValueError as exc:
        assert str(exc) == "Positive bootstrap bounds required"
    else:
        raise AssertionError("Expected ValueError")

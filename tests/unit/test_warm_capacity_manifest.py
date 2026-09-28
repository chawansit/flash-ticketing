import asyncio
import importlib.util
import json
from argparse import Namespace
from pathlib import Path

SPEC = importlib.util.spec_from_file_location(
    "warm_capacity_manifest",
    Path(__file__).resolve().parents[2] / "scripts/warm_capacity_manifest.py",
)
MODULE = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(MODULE)


class FakeResponse:
    def __init__(self, status_code):
        self.status_code = status_code

    def json(self):
        return {"code": "SEATMAP_WARMING"}


class FakeClient:
    def __init__(self):
        self.calls = []
        self.counts = {}

    async def __aenter__(self):
        return self

    async def __aexit__(self, *_):
        return None

    async def get(self, path):
        show = path.rsplit("/", 2)[-2]
        self.calls.append(show)
        self.counts[show] = self.counts.get(show, 0) + 1
        if show == "show-b" and self.counts[show] == 1:
            return FakeResponse(503)
        return FakeResponse(200)


def test_warmup_rechecks_ready_shows_until_one_full_pass_is_ready(tmp_path, monkeypatch):
    manifest = tmp_path / "manifest.json"
    output = tmp_path / "warmup.json"
    manifest.write_text(
        json.dumps({"origin": "http://api", "show_ids": ["show-a", "show-b"]}),
        encoding="utf-8",
    )
    client = FakeClient()

    monkeypatch.setattr(MODULE.httpx, "AsyncClient", lambda **_: client)

    async def no_sleep(_):
        return None

    monkeypatch.setattr(MODULE.asyncio, "sleep", no_sleep)
    args = Namespace(
        manifest=manifest,
        output=output,
        attempts=2,
        concurrency=2,
        timeout=1,
    )

    assert asyncio.run(MODULE.run(args)) == 0
    assert client.calls == ["show-a", "show-b", "show-a", "show-b"]
    result = json.loads(output.read_text(encoding="utf-8"))
    assert [row["remaining"] for row in result["history"]] == [1, 0]
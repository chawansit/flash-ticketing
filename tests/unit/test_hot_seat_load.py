"""Verify contention correctness never hides an admission failure."""
import asyncio
import importlib.util
import json
import sys
from datetime import UTC, datetime, timedelta
from pathlib import Path
from types import SimpleNamespace

import httpx
import pytest

scripts = Path(__file__).resolve().parents[2] / "scripts"
sys.path.insert(0, str(scripts))
spec = importlib.util.spec_from_file_location("hot_seat_load", scripts / "hot_seat_load.py")
hot = importlib.util.module_from_spec(spec)
spec.loader.exec_module(hot)
sys.path.remove(str(scripts))


@pytest.mark.parametrize("rejected_status,code,available", [(409, "SEAT_BUSY", True), (503, "ADMISSION_FULL", False)])
def test_single_winner_and_availability_are_independent(tmp_path, monkeypatch, rejected_status, code, available):
    manifest = tmp_path / "private.json"
    manifest.write_text(json.dumps({"schema_version": 1, "environment": "development",
                                   "expires_at": (datetime.now(UTC)+timedelta(hours=1)).isoformat(),
                                   "origin": "http://test.invalid", "show_ids": ["test-show"],
                                   "viewer_tokens": ["fake"]*100, "seats_per_show": 300}))
    writes = 0
    def handler(request):
        nonlocal writes
        if request.method == "GET":
            return httpx.Response(200)
        writes += 1
        return httpx.Response(201, json={"hold_id": "winner"}) if writes == 1 else httpx.Response(rejected_status, json={"code": code})
    client = httpx.AsyncClient
    monkeypatch.setattr(hot.httpx, "AsyncClient", lambda **kw: client(transport=httpx.MockTransport(handler), **kw))
    output = tmp_path / "result.json"
    asyncio.run(hot.run(SimpleNamespace(manifest=manifest, output=output, contenders=100, seat=200)))
    result = json.loads(output.read_text())
    assert result["http_one_winner_pass"]
    assert result["availability_gate_pass"] is available
    assert result["error_codes"] == {code: 99}
    assert sum(result["statuses"]["hold"].values()) == 100

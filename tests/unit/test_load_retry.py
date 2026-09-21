import asyncio
import importlib.util
import json
from datetime import UTC, datetime, timedelta
from pathlib import Path
from types import SimpleNamespace

import httpx
import pytest

SPEC = importlib.util.spec_from_file_location(
    "retry_generator", Path(__file__).parents[2] / "scripts/http_load_generator.py"
)
generator = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(generator)


def arguments(tmp_path, *, rate=20, late_window=5000):
    manifest = tmp_path / "manifest.json"
    manifest.write_text(json.dumps({
        "schema_version": 1, "environment": "development", "id": "retry-test",
        "expires_at": (datetime.now(UTC) + timedelta(minutes=5)).isoformat(),
        "origin": "http://test", "show_ids": ["show"], "viewer_tokens": ["token"],
        "seat_offset": 0, "seats_per_show": 300,
    }))
    return SimpleNamespace(
        manifest=manifest, origin="http://test", output=tmp_path / "result.json",
        rate=rate, seconds=1, inflight=64, burst=False, start_at=None,
        topology="same-host", transport_diagnostics=True, keepalive_expiry=5,
        mixed_hot_holds=False, max_attempts=2, retry_base_delay_ms=0,
        late_delivery_window_ms=late_window,
    )


def install_client(monkeypatch, handler):
    original = httpx.AsyncClient
    monkeypatch.setattr(
        generator.httpx, "AsyncClient",
        lambda **kwargs: original(**kwargs, transport=httpx.MockTransport(handler)),
    )


def test_uncertain_hold_retry_reuses_idempotency_key_and_preserves_first_failure(tmp_path, monkeypatch):
    post_keys = []

    def response(request):
        if request.method == "GET":
            return httpx.Response(200, json={}, headers={"etag": '"v1"'})
        post_keys.append(request.headers["Idempotency-Key"])
        if len(post_keys) == 1:
            raise httpx.ReadError("uncertain response", request=request)
        return httpx.Response(201, json={})

    install_client(monkeypatch, response)
    args = arguments(tmp_path)
    asyncio.run(generator.run(args))
    result = json.loads(args.output.read_text())

    assert len(post_keys) == 2
    assert post_keys[0] == post_keys[1]
    assert result["physical_http_attempts"] == 21
    assert result["first_attempt_failures"] == {"hold:transport:ReadError": 1}
    assert result["retry_attempts"] == {"hold:transport:ReadError": 1}
    assert result["retry_successes"] == {"hold": 1}
    assert result["retry_exhausted"] == {}
    assert result["attempt_transport_error_types"] == {"ReadError": 1}
    assert result["transport_error_types"] == {}
    assert result["statuses"]["hold"] == {"201": 1}
    assert result["workload_gate_pass"]


def test_permanent_seat_conflict_is_not_retried(tmp_path, monkeypatch):
    post_keys = []

    def response(request):
        if request.method == "GET":
            return httpx.Response(200, json={}, headers={"etag": '"v1"'})
        post_keys.append(request.headers["Idempotency-Key"])
        return httpx.Response(409, json={"code": "SEAT_UNAVAILABLE"})

    install_client(monkeypatch, response)
    args = arguments(tmp_path)
    with pytest.raises(SystemExit):
        asyncio.run(generator.run(args))
    result = json.loads(args.output.read_text())

    assert len(post_keys) == 1
    assert result["retry_attempts"] == {}
    assert result["retry_successes"] == {}
    assert result["statuses"]["hold"] == {"409": 1}
    assert not result["workload_gate_pass"]


def test_late_scheduled_requests_are_delivered_once_and_remain_observable(tmp_path, monkeypatch):
    calls = 0

    def response(request):
        nonlocal calls
        calls += 1
        if request.method == "GET":
            return httpx.Response(200, json={}, headers={"etag": '"v1"'})
        return httpx.Response(201, json={})

    install_client(monkeypatch, response)
    monkeypatch.setattr(
        generator, "arrival_plan", lambda rate, seconds, burst=False: [(-0.060, 0), (-0.059, 0)]
    )
    args = arguments(tmp_path, rate=20, late_window=250)
    asyncio.run(generator.run(args))
    result = json.loads(args.output.read_text())

    assert calls == 4  # readiness, bootstrap, and two logical requests
    assert result["late_deliveries"] == 2
    assert result["generator_drops"] == 0
    assert result["scheduling_lag_over_50ms"] == 2
    assert result["physical_http_attempts"] == 2
    assert result["workload_gate_pass"]


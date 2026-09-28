import asyncio
import importlib.util
import json
from contextlib import nullcontext
from datetime import UTC, datetime, timedelta
from pathlib import Path
from types import SimpleNamespace
from uuid import uuid4

import httpx
import pytest

spec=importlib.util.spec_from_file_location('mixed_generator',Path(__file__).parents[2]/'scripts/http_load_generator.py')
generator=importlib.util.module_from_spec(spec)
spec.loader.exec_module(generator)


@pytest.mark.parametrize('code,passes',[('SEAT_BUSY',True),('IDEMPOTENCY_MISMATCH',False)])
def test_mixed_accounting_only_accepts_known_seat_conflicts(tmp_path,monkeypatch,code,passes):
    hot_calls=0
    def response(request):
        nonlocal hot_calls
        if request.method=='GET':return httpx.Response(200,json={},headers={'etag':'"v1"'})
        body=json.loads(request.content)
        if body['seat_ids']==['S299']:
            hot_calls+=1
            if hot_calls>1:
                return httpx.Response(409,json={'code':code},headers={'x-request-id':str(uuid4())})
        return httpx.Response(201,json={})
    original=httpx.AsyncClient
    monkeypatch.setattr(generator.httpx,'AsyncClient',lambda **kwargs: original(**kwargs,transport=httpx.MockTransport(response)))
    manifest=tmp_path/'manifest.json'
    manifest.write_text(json.dumps({'schema_version':1,'environment':'development','id':'test',
        'expires_at':(datetime.now(UTC)+timedelta(minutes=5)).isoformat(),'origin':'http://test',
        'show_ids':['one'],'viewer_tokens':['test'],'seat_offset':0,'seats_per_show':300,
        'hot_hold_show_id':'one','hot_hold_seat':299}))
    args=SimpleNamespace(manifest=manifest,origin='http://test',output=tmp_path/'result.json',
        rate=200,seconds=1,inflight=64,burst=False,start_at=None,topology='same-host',
        transport_diagnostics=True,keepalive_expiry=5,mixed_hot_holds=True)
    with nullcontext() if passes else pytest.raises(SystemExit):asyncio.run(generator.run(args))
    result=json.loads(args.output.read_text())
    assert result['statuses']=={'read':{'200':190},'hold':{'201':8},'hot_hold':{'201':1,'409':1}}
    assert result['accounting_pass']
    assert result['workload_gate_pass'] is passes
    assert result['expected_hot_conflicts']==int(passes)
    assert hot_calls==2

def test_redis_first_counts_202_as_provisional_success(tmp_path, monkeypatch):
    def response(request):
        if request.method == 'GET':
            return httpx.Response(200,json={},headers={'etag':'"v1"'})
        return httpx.Response(202,json={'command_id':'command','hold_id':'hold','order_id':'order',
                                       'persistence_status':'PENDING'})
    original=httpx.AsyncClient
    monkeypatch.setattr(generator.httpx,'AsyncClient',lambda **kwargs: original(**kwargs,transport=httpx.MockTransport(response)))
    manifest=tmp_path/'manifest.json'
    manifest.write_text(json.dumps({'schema_version':1,'environment':'development','id':'test',
        'expires_at':(datetime.now(UTC)+timedelta(minutes=5)).isoformat(),'origin':'http://test',
        'show_ids':['one'],'viewer_tokens':['test'],'seat_offset':0,'seats_per_show':300}))
    args=SimpleNamespace(manifest=manifest,origin='http://test',output=tmp_path/'result.json',
        rate=20,seconds=1,inflight=64,burst=False,start_at=None,topology='same-host',
        transport_diagnostics=False,keepalive_expiry=5,mixed_hot_holds=False,
        reservation_mode='redis-first')
    asyncio.run(generator.run(args))
    result=json.loads(args.output.read_text())
    assert result['reservation_mode']=='redis-first'
    assert result['statuses']=={'hold':{'202':1},'read':{'200':19},'hot_hold':{}}
    assert result['error_codes']=={}
    assert result['workload_gate_pass']

def test_delta_read_mode_bootstraps_snapshot_then_advances_versions(tmp_path, monkeypatch):
    delta_since = []

    def response(request):
        if request.url.path == "/health/ready":
            return httpx.Response(200, json={})
        if request.url.path.endswith("/availability"):
            return httpx.Response(200, json={"version": 5, "seats": []}, headers={"etag": '"v5"'})
        if request.url.path.endswith("/seat-deltas"):
            since = int(request.url.params["since"])
            delta_since.append(since)
            reset = len(delta_since) == 1
            return httpx.Response(
                200,
                json={
                    "event_id": "one",
                    "from_version": since,
                    "version": 3 if reset else since + 1,
                    "reset_required": reset,
                    "seats": [],
                },
            )
        return httpx.Response(202, json={"persistence_status": "PENDING"})

    original = httpx.AsyncClient
    monkeypatch.setattr(
        generator.httpx,
        "AsyncClient",
        lambda **kwargs: original(**kwargs, transport=httpx.MockTransport(response)),
    )
    manifest = tmp_path / "manifest.json"
    manifest.write_text(json.dumps({
        "schema_version": 1,
        "environment": "development",
        "id": "delta-test",
        "expires_at": (datetime.now(UTC) + timedelta(minutes=5)).isoformat(),
        "origin": "http://test",
        "show_ids": ["one"],
        "viewer_tokens": ["test"],
        "seat_offset": 0,
        "seats_per_show": 300,
    }))
    args = SimpleNamespace(
        manifest=manifest,
        origin="http://test",
        output=tmp_path / "result.json",
        rate=20,
        seconds=1,
        inflight=64,
        burst=False,
        start_at=None,
        topology="same-host",
        transport_diagnostics=False,
        keepalive_expiry=5,
        mixed_hot_holds=False,
        reservation_mode="redis-first",
        read_mode="delta",
    )
    asyncio.run(generator.run(args))
    result = json.loads(args.output.read_text())
    assert result["read_mode"] == "delta"
    assert result["workload_gate_pass"] is True
    assert result["time_windows"]["0"]["delta_observations"]["responses"] > 0
    assert delta_since[:2] == [5, 3]
    assert max(delta_since[1:]) > 3

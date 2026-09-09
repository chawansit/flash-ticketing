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

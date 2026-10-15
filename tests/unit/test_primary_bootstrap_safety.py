"""Exercise real safety probe across two primary API ports and preserve legacy host checks."""
import asyncio
from uuid import uuid4

import httpx
import probe_two_host_safety as probe
import pytest

ORIGINS = ['http://10.0.0.1:8101', 'http://10.0.0.1:8102']


def test_primary_mode_requires_explicit_opt_in():
    with pytest.raises(ValueError, match='Distinct API hosts'):
        probe.validate_origins(ORIGINS)
    probe.validate_origins(ORIGINS, primary_only=True)
    probe.validate_origins([ORIGINS[0], 'http://10.0.0.2:8101'])


@pytest.mark.parametrize('origins', [
    [ORIGINS[0], ORIGINS[0]],
    [ORIGINS[0], 'http://10.0.0.2:8102'],
    ['http://127.0.0.1:8101', 'http://127.0.0.1:8102'],
    [ORIGINS[0], 'http://user:secret@10.0.0.1:8102'],
])
def test_primary_mode_rejects_duplicate_endpoint_or_wrong_topology(origins):
    with pytest.raises(ValueError):
        probe.validate_origins(origins, primary_only=True)


@pytest.mark.parametrize('duplicate_payment', [False, True])
def test_complete_primary_probe_preserves_safety_and_honest_receipt(monkeypatch, duplicate_payment):
    body={k:str(uuid4()) for k in ('command_id','hold_id','order_id')}
    payment=str(uuid4());calls=[];audits=[]
    def audit(*args):audits.append(args);return True
    monkeypatch.setattr(probe,'audit_one_owner',audit)
    def respond(request):
        calls.append(request)
        actor=request.headers['Authorization']
        if request.url.path=='/v1/holds':
            return httpx.Response(202,json=body) if actor=='Bearer 0' else httpx.Response(409,json={'code':'SEAT_UNAVAILABLE'})
        if '/reservation-commands/' in request.url.path:
            return httpx.Response(200,json={'persistence_status':'DURABLE'})
        if request.url.path.endswith('/payments'):
            value=str(uuid4()) if duplicate_payment and request.url.port==8102 else payment
            return httpx.Response(202,json={'payment_id':value})
        return httpx.Response(200,json={'status':'FULFILLED','tickets':[{}]}) if actor=='Bearer 0' else httpx.Response(403,json={})
    async def exercise():
        async with httpx.AsyncClient(transport=httpx.MockTransport(respond)) as client:
            return await probe.probe(str(uuid4()),ORIGINS,[str(i) for i in range(100)],'adr0147-'+uuid4().hex,client=client,primary_only=True)
    if duplicate_payment:
        with pytest.raises(ValueError,match='created another payment'):asyncio.run(exercise())
    else:
        result=asyncio.run(exercise())
        assert result['accepted_holds']==1 and result['requests_per_api']==[50,50]
        assert result['cross_api_hold_replay'] and result['cross_api_payment_replay']
        assert result['cross_host_hold_replay'] is False and result['cross_host_payment_replay'] is False
        assert result['other_actor_denied'] and result['customer_ticket_confirmed']
        assert len([r for r in calls if r.url.path=='/v1/holds'])==101
        assert {r.url.port for r in calls}=={8101,8102}
    assert len(audits)==1

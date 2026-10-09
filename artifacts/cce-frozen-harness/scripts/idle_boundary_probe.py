"""Bounded read-only HTTP/1.1 idle-boundary diagnosis; no retries."""
import argparse
import asyncio
import json
from collections import Counter
from datetime import UTC, datetime
from pathlib import Path
from time import perf_counter
from urllib.parse import urlsplit
from uuid import UUID

import httpx
from http_load_generator import TransportTrace


async def read(client, path, headers=None):
    trace=TransportTrace()
    started=datetime.now(UTC).isoformat()
    try:
        response=await client.get(path,headers=headers or {},extensions={'trace':trace})
        stream=response.extensions.get('network_stream')
        address=stream.get_extra_info('client_addr') if stream else None
        return {'started_utc':started,'status':response.status_code,'request_id':response.headers.get('x-request-id'),
                'local_port':address[1] if address else None,'etag':response.headers.get('etag'),
                'duration_ms':round((perf_counter()-trace.started)*1000,3),'trace':trace.failure(None)}
    except httpx.HTTPError as exc:
        return {'started_utc':started,'status':'transport_error','duration_ms':round((perf_counter()-trace.started)*1000,3),
                'trace':trace.failure(exc)}


async def pair(client, path, delay, conditional):
    seed=await read(client,path)
    result={'requested_idle_seconds':delay,'conditional':conditional,'seed':seed}
    if seed['status'] not in (200,304):
        return result
    finished=perf_counter()
    await asyncio.sleep(delay)
    result['actual_client_idle_seconds']=perf_counter()-finished
    headers={'If-None-Match':seed['etag']} if conditional and seed.get('etag') else {}
    result['conditional_header_sent']=bool(headers)
    probe=await read(client,path,headers)
    result['probe']=probe
    result['same_local_port']=seed.get('local_port') is not None and seed.get('local_port')==probe.get('local_port')
    return result


async def run(args):
    args.output.mkdir(parents=True,exist_ok=False)
    path=f'/v1/events/{args.event}/availability'
    results=[]
    async with httpx.AsyncClient(base_url=args.origin,timeout=10,trust_env=False) as client:
        (await client.get('/health/ready')).raise_for_status()
    for expiry in args.expiries:
        async def lane(index, expiry=expiry):
            rows=[]
            limits=httpx.Limits(max_connections=1,max_keepalive_connections=1,keepalive_expiry=expiry)
            async with httpx.AsyncClient(base_url=args.origin,timeout=10,limits=limits,trust_env=False,http2=False) as client:
                for repetition in range(args.repeats):
                    for j in range(len(args.delays)):
                        delay=args.delays[(j+index)%len(args.delays)]
                        row=await pair(client,path,delay,(j+repetition)%2==0)
                        rows.append({'lane':index,'repetition':repetition,**row})
            return rows
        started=datetime.now(UTC).isoformat()
        rows=[r for group in await asyncio.gather(*(lane(i) for i in range(args.lanes))) for r in group]
        statuses=Counter(str(r[k]['status']) for r in rows for k in ('seed','probe') if k in r)
        failures=[r for r in rows if 'probe' not in r or any(r[k]['status'] not in (200,304) for k in ('seed','probe'))]
        result={'client_expiry_seconds':expiry,'declared_server_timeout_seconds':args.server_timeout,'started_utc':started,
                'finished_utc':datetime.now(UTC).isoformat(),'pairs':rows,'statuses':dict(statuses),'failed_pairs':len(failures),
                'same_port_probes':sum(r.get('same_local_port',False) for r in rows),
                'new_connect_probes':sum(r.get('probe',{}).get('trace',{}).get('connect_attempted',False) for r in rows)}
        (args.output/f'expiry-{expiry:g}.json').write_text(json.dumps(result,indent=2)+'\n',encoding='utf-8')
        summary={k:v for k,v in result.items() if k!='pairs'}
        results.append(summary)
        print(json.dumps(summary),flush=True)
    (args.output/'summary.json').write_text(json.dumps(results,indent=2)+'\n',encoding='utf-8')
    if any(r['failed_pairs'] for r in results):
        raise SystemExit(1)


if __name__=='__main__':
    p=argparse.ArgumentParser(description=__doc__)
    p.add_argument('--origin',required=True)
    p.add_argument('--event',type=UUID,required=True)
    p.add_argument('--output',type=Path,required=True)
    p.add_argument('--lanes',type=int,default=16)
    p.add_argument('--repeats',type=int,default=3)
    p.add_argument('--delays',type=float,nargs='+',default=[4.95,4.995,5,5.005,5.05])
    p.add_argument('--expiries',type=float,nargs='+',default=[5,30,2])
    p.add_argument('--server-timeout',type=float,default=5)
    a=p.parse_args()
    origin=urlsplit(a.origin)
    if origin.scheme not in ('http','https') or not origin.hostname or origin.username or origin.password or origin.query or origin.fragment or origin.path not in ('','/'):
        p.error('Use an explicit HTTP(S) origin without credentials/path/query')
    if not 1<=a.lanes<=32 or not 1<=a.repeats<=5 or not 1<=len(a.delays)<=8 or not 1<=len(a.expiries)<=3 or any(not 0<x<=60 for x in a.delays+a.expiries+[a.server_timeout]) or len(set(a.expiries))!=len(a.expiries):
        p.error('Use bounded lanes, repeats, distinct expiry policies and positive delays <=60s')
    asyncio.run(run(a))

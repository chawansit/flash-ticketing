"""Correlate app and opt-in ingress logs without retaining request payloads."""
import argparse
import json
import math
from collections import defaultdict
from pathlib import Path


def stats(values):
    values=sorted(values)
    return {'count':len(values),'p95_ms':values[math.ceil(len(values)*.95)-1],
            'max_ms':values[-1]}


def summarize(lines):
    app,ingress={},{}
    for line in lines:
        try:r=json.loads(line.split('|',1)[-1])
        except ValueError:continue
        identifier=r.get('request_id')
        if not identifier:continue
        if r.get('message')=='request':
            app[identifier]=(r.get('route'),r.get('status'),r.get('duration_ms'))
        elif r.get('message')=='ingress':
            ingress[identifier]=(r.get('status'),r.get('protocol_queue_ms'),r.get('asgi_headers_ms'))
    groups=defaultdict(lambda:defaultdict(list))
    mismatches,missing_stamp=0,0
    for identifier,(route,status,duration) in app.items():
        if identifier not in ingress:continue
        ingress_status,queue,total=ingress[identifier]
        mismatches+=status != ingress_status
        missing_stamp+=queue is None
        rows=groups[f'{route}|{status}']
        for key,value in [('app',duration),('protocol_queue',queue),('asgi_headers',total)]:
            if value is not None:rows[key].append(value)
    return {'application_records':len(app),'ingress_records':len(ingress),
            'missing_ingress':len(app.keys()-ingress.keys()),'status_mismatches':mismatches,
            'missing_protocol_stamps':missing_stamp,
            'groups':{k:{name:stats(values) for name,values in rows.items()} for k,rows in groups.items()},
            'note':'Protocol callback to ASGI excludes network/kernel waiting before headers-complete. Percentiles describe separate distributions.'}


if __name__=='__main__':
    p=argparse.ArgumentParser(description=__doc__)
    p.add_argument('--logs',type=Path,required=True);p.add_argument('--output',type=Path,required=True)
    args=p.parse_args()
    with args.logs.open(encoding='utf-8-sig') as lines:result=summarize(lines)
    args.output.write_text(json.dumps(result,indent=2)+'\n',encoding='utf-8')
    print(json.dumps({k:v for k,v in result.items() if k!='groups'}))

"""Summarize bounded cloud observations; absent measurements remain absent."""
import argparse
import json
import math
from collections import defaultdict
from datetime import datetime
from itertools import pairwise
from pathlib import Path


def epoch(value):
    return datetime.fromisoformat(value).timestamp()


def describe(values):
    return {'samples': len(values), 'mean': sum(values)/len(values), 'max': max(values)} if values else {'samples': 0}


def main():
    p=argparse.ArgumentParser(description=__doc__)
    p.add_argument('--metrics', type=Path, required=True)
    p.add_argument('--database', type=Path, required=True)
    p.add_argument('--cpu', type=Path, required=True)
    p.add_argument('--start', required=True)
    p.add_argument('--end', required=True)
    p.add_argument('--output', type=Path, required=True)
    a=p.parse_args()
    start,end=epoch(a.start),epoch(a.end)
    if start>=end:p.error('Start must precede end')
    report={'start':a.start,'end':a.end,'metrics':{}}
    for name,group in json.loads(a.metrics.read_text())['series'].items():
        rows=[]
        for series in group['data']['result']:
            values=[float(v) for t,v in series['values'] if start<=t<=end and math.isfinite(float(v))]
            rows.append({'labels':series['metric'],**describe(values)})
        report['metrics'][name]=rows
    db=[r for r in json.loads(a.database.read_text()) if start<=epoch(r['utc'])<=end]
    report['database']={'samples':len(db),'errors':[r['error'] for r in db if 'error' in r]}
    for key in ('database_connections','lock_waiters','active_connections','missing_maps','overdue_active_holds','oldest_overdue_seconds','reconciliation_age_seconds','redis_used_bytes'):
        report['database'][key]=describe([r[key] for r in db if key in r])
    cpu=json.loads(a.cpu.read_text())
    containers=defaultdict(list)
    for r in cpu:
        if start<=epoch(r['utc'])<=end:
            for c in r.get('containers',[]):
                containers[c['Name']].append(float(c['CPUPerc'].rstrip('%')))
    report['container_cpu_percent_one_core']={k:describe(v) for k,v in containers.items()}
    busy,steal=[],[]
    for before,after in pairwise(cpu):
        if not start<=epoch(before['utc'])<=epoch(after['utc'])<=end:continue
        b=[int(x) for x in before['host_cpu_ticks'].split()[1:9]]
        c=[int(x) for x in after['host_cpu_ticks'].split()[1:9]]
        d=[y-x for x,y in zip(b,c)]
        total=sum(d)
        if total>0:
            busy.append(100*(total-d[3]-d[4])/total)
            steal.append(100*d[7]/total)
    report['host_busy_percent']=describe(busy)
    report['host_steal_percent']=describe(steal)
    report['note']='Prometheus histogram p95 values are bucket estimates over trailing 1-minute windows, not exact query percentiles. Worker busy rate includes I/O and overlapping operations. CPU Docker percent is relative to one core; host percent spans all cores. Zero sampled lock waiters does not exclude short waits. Absent samples are not zero.'
    a.output.write_text(json.dumps(report,indent=2)+'\n')


if __name__=='__main__':main()

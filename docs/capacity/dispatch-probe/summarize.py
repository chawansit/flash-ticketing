"""Summarize retained diagnostic records against exact measured request IDs."""
import json
import math
from collections import Counter, defaultdict
from pathlib import Path

ROOT=Path(__file__).parent
clients=[r for f in sorted(ROOT.glob('dispatch-hot-*/worker-*.json')) for r in json.loads(f.read_text())['requests']]
ids={r['request_id'] for r in clients}
assert len(ids)==len(clients)==2000
rows=[json.loads(l) for l in (ROOT/'dispatch-records.jsonl').read_text().splitlines()]
rows=[r for r in rows if r.get('request_id') in ids]
requests={r['request_id']:r for r in rows if r['message']=='request'}
assert len(requests)==2000
for c in clients:
 assert str(requests[c['request_id']]['status'])==c['status']
 assert requests[c['request_id']].get('error_code')==c.get('code')
admitted={c['request_id'] for c in clients if c['code']!='ADMISSION_FULL'}
def stats(values):
 values=sorted(values)
 return {'count':len(values),'p95_ms':values[math.ceil(len(values)*.95)-1],'max_ms':values[-1]}
groups=defaultdict(list)
for r in rows:
 if r['message']=='dispatch_probe':groups[r['stage']].append(r)
summary={}
for stage,rs in groups.items():
 assert Counter(r['request_id'] for r in rs)==Counter({i:1 for i in admitted})
 summary[stage]={'count':len(rs),'timing':{k:stats([r['timing_ms'][k] for r in rs]) for k in ('submit_to_entry','execution','finish_to_resume')},'max_borrowed_tokens':max(r['borrowed_tokens_at_submit'] for r in rs),'total_tokens':sorted({r['total_tokens_at_submit'] for r in rs}),'max_tasks_waiting':max(r['tasks_waiting_at_submit'] for r in rs)}
idem=[r for r in rows if r['message']=='idempotency_probe']
result={'matched_requests':len(ids),'admitted_requests':len(admitted),'dispatch':summary,'idempotency':stats([r['duration_ms'] for r in idem]),'note':'Each admitted request has exactly one authentication, service dependency and hold-handler dispatch record. Stage distributions overlap existing dispatch/DB timings: do not sum percentiles. Limiter occupancy sampled at submission only.'}
(ROOT/'summary.json').write_text(json.dumps(result,indent=2)+'\n',encoding='utf-8')
print(json.dumps(result,indent=2))

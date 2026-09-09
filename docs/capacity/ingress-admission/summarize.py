"""Regenerate bounded-window operational evidence."""
import json
from itertools import pairwise
from pathlib import Path

ROOT=Path(__file__).parent
def read(path):
 return json.loads((ROOT/path).read_text(encoding='utf-8'))
def delta(s):
 v=[float(x) for _,x in s['values']]
 assert all(b>=a for a,b in pairwise(v)), 'Counter reset'
 return v[-1]-v[0]
result={}
for n in (8,16):
 ws=[json.loads(f.read_text()) for f in (ROOT/f'generator/ingress-{n}-uniform').glob('worker-*.json')]
 start=min(w['measured_started_utc'] for w in ws);end=max(w['utc'] for w in ws)
 obs=[o for o in read(f'backend/ingress-{n}-observations.json') if start<=o['utc']<=end]
 series=read(f'backend/ingress-{n}-prometheus.json')['data']['result']
 api=[s for s in series if s['metric'].get('job')=='api']
 means={}
 for prefix in ('ticketing_db_pool_acquire_seconds','ticketing_db_query_seconds','ticketing_db_transaction_seconds'):
  count=sum(delta(s) for s in api if s['metric']['__name__']==prefix+'_count')
  total=sum(delta(s) for s in api if s['metric']['__name__']==prefix+'_sum')
  means[prefix]={'sample_count':count,'mean_ms':1000*total/count if count else None}
 busy={}
 for s in series:
  if s['metric']['__name__']=='ticketing_worker_busy_seconds_total':
   seconds=s['values'][-1][0]-s['values'][0][0]
   busy[s['metric']['instance']+'|'+s['metric']['operation']]=delta(s)/seconds
 result[str(n)]={'start_utc':start,'end_utc':end,'observer_samples':len(obs),'observer_max':{k:max(o[k] for o in obs) for k in obs[0] if k!='utc'},'minimum_map_ttl':min(o['minimum_ttl'] for o in obs),'api_means_from_counter_deltas':means,'worker_busy_seconds_per_wall_second':busy,'note':'Observer filtered to measured interval. Prometheus deltas are scrape-aligned approximations; means are not p95. Worker operation intervals can overlap: do not sum as CPU utilization. Short locks can escape 2-second sampling.'}
(ROOT/'operational-summary.json').write_text(json.dumps(result,indent=2)+'\n',encoding='utf-8')
print(json.dumps({n:r['api_means_from_counter_deltas'] for n,r in result.items()},indent=2))

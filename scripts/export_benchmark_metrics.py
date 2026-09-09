"""Export bounded historical Prometheus series from the private benchmark network."""
import argparse
import json
import urllib.parse
import urllib.request
from pathlib import Path

p=argparse.ArgumentParser(description=__doc__)
p.add_argument('--start',required=True)
p.add_argument('--end',required=True)
p.add_argument('--output',type=Path,required=True)
a=p.parse_args()
queries={
 'transaction_p95':'histogram_quantile(0.95, sum by (le,instance) (rate(ticketing_db_transaction_seconds_bucket[1m])))',
 'dispatch_p95':'histogram_quantile(0.95, sum by (le,instance,stage,phase,outcome) (rate(ticketing_dispatch_probe_seconds_bucket[1m])))',
 'admission_rejected_increase':'increase(ticketing_hold_admission_total{outcome="rejected"}[1m])',
 'query_p95':'histogram_quantile(0.95, sum by (le,instance,command) (rate(ticketing_db_query_seconds_bucket[1m])))',
 'pool_p95':'histogram_quantile(0.95, sum by (le,instance) (rate(ticketing_db_pool_acquire_seconds_bucket[1m])))',
 'pool_in_use':'ticketing_db_pool_in_use',
 'pool_acquiring':'ticketing_db_pool_acquiring',
 'pool_state':'ticketing_db_pool_state',
 'worker_active':'ticketing_worker_active',
 'worker_busy_rate':'rate(ticketing_worker_busy_seconds_total[1m])',
 'worker_error_increase':'increase(ticketing_worker_errors_total[1m])',
}
report={'start':a.start,'end':a.end,'step_seconds':10,'series':{}}
for name,query in queries.items():
 url='http://prometheus:9090/api/v1/query_range?'+urllib.parse.urlencode({'query':query,'start':a.start,'end':a.end,'step':10})
 with urllib.request.urlopen(url,timeout=30) as r: result=json.load(r)
 if result['status']!='success':raise ValueError(result['status'])
 report['series'][name]={'query':query,'data':result['data']}
a.output.write_text(json.dumps(report,indent=2)+'\n')
print(json.dumps({name:len(value['data']['result']) for name,value in report['series'].items()}))

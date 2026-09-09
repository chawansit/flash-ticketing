"""Export existing Prometheus samples for an explicitly bounded benchmark window."""
import argparse
import json
from pathlib import Path
from urllib.parse import urlencode
from urllib.request import urlopen

p = argparse.ArgumentParser(description=__doc__)
p.add_argument('--start', required=True)
p.add_argument('--end', required=True)
p.add_argument('--output', type=Path, required=True)
p.add_argument('--origin', default='http://127.0.0.1:9090')
a = p.parse_args()
query = '{__name__=~"ticketing_ingress_.*|ticketing_browse_body_.*|ticketing_hold_.*|ticketing_db_.*|ticketing_worker_.*|ticketing_reconciliation_.*|ticketing_cache_refresh_.*|up"}'
params = urlencode({'query': query, 'start': a.start, 'end': a.end, 'step': '15s'})
with urlopen(a.origin+'/api/v1/query_range?'+params, timeout=60) as response:
    result = json.load(response)
if result.get('status') != 'success':
    raise SystemExit('Prometheus query failed')
a.output.write_text(json.dumps(result)+'\n')
print(json.dumps({'series': len(result['data']['result']), 'start': a.start, 'end': a.end}))

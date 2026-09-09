"""Continue the documented ABBA comparison after the separately launched A1."""
import json
import subprocess
import sys
import time
from pathlib import Path

for _ in range(90):
    if Path('expiry-a1/summary.json').exists():
        break
    time.sleep(5)
else:
    raise SystemExit('A1 did not finish within the bounded wait')

stages = []
for name, expiry, offset in [('a1', 5, 140), ('b1', 2, 180), ('b2', 2, 220), ('a2', 5, 260)]:
    if name != 'a1':
        with Path(f'expiry-{name}.log').open('x') as log:
            result = subprocess.run([
                sys.executable, 'parallel_cloud_load.py', '--manifest', 'expiry-manifest.json',
                '--rate', '400', '--seconds', '300', '--seat-offset', str(offset),
                '--output', f'expiry-{name}', '--transport-diagnostics', '--keepalive-expiry', str(expiry)
            ], stdout=log, stderr=subprocess.STDOUT, timeout=420, check=False)
    summary = json.loads(Path(f'expiry-{name}/summary.json').read_text())
    workers = [json.loads(Path(f'expiry-{name}/worker-{i}.json').read_text()) for i in range(4)]
    if any(w['task_error_types'] for w in workers):
        raise SystemExit('Unexpected worker task failure; inspect before continuing')
    stages.append({'name': name, 'expiry': expiry, 'offset': offset, 'summary': summary})
    Path('expiry-stages.json').write_text(json.dumps(stages, indent=2) + '\n')
    print(json.dumps(stages[-1]), flush=True)
if any(not stage['summary']['gate_pass'] for stage in stages):
    raise SystemExit(1)

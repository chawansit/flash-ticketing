"""Verify each matrix wave's live winning seat before the hold expires."""
import argparse
import json
import subprocess
import sys
from pathlib import Path

p=argparse.ArgumentParser(description=__doc__)
p.add_argument('--results',type=Path,required=True)
p.add_argument('--verifier',type=Path,required=True)
p.add_argument('--output',type=Path,required=True)
a=p.parse_args()
a.output.mkdir(parents=True,exist_ok=True)
for path in sorted(a.results.rglob('summary.json')):
    result=json.loads(path.read_text())
    if len(result['winners']) != 1:
        raise ValueError(f'Expected one winner: {path}')
    winner=result['winners'][0]
    subprocess.run([sys.executable,str(a.verifier),'--run-id',winner['run_id'],
                    '--event-id',result['event_id'],'--seat-id',result['seat_id'],
                    '--hold-id',winner['hold_id'],'--output',str(a.output/(path.parent.name+'.json'))],check=True)

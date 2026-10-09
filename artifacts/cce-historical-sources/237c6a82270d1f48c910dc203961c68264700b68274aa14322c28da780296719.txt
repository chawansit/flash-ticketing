"""Bounded mixed-load staircase; stop escalation on the first failed stage."""
import argparse
import json
import subprocess
import sys
from datetime import UTC, datetime
from pathlib import Path

p=argparse.ArgumentParser(description=__doc__)
p.add_argument('--manifest',type=Path,required=True)
p.add_argument('--output',type=Path,required=True)
a=p.parse_args()
if a.output.exists():p.error('Use a fresh output directory')
a.output.mkdir(parents=True)
report={'started_utc':datetime.now(UTC).isoformat(),'stages':[],'selected_soak_rps':None}
def run(rate):
 offset=len(report['stages'])*40
 output=a.output/f'rate-{rate}'
 with (a.output/f'rate-{rate}.log').open('w') as log:
  result=subprocess.run([sys.executable,'parallel_cloud_load.py','--manifest',str(a.manifest),'--rate',str(rate),'--seconds','300','--workers','4','--seat-offset',str(offset),'--mixed-hot-holds','--transport-diagnostics','--output',str(output)],stdout=log,stderr=subprocess.STDOUT,timeout=420,check=False)
 path=output/'summary.json'
 summary=json.loads(path.read_text()) if path.exists() else {}
 passed=result.returncode==0 and summary.get('gate_pass') is True
 report['stages'].append({'rate':rate,'seat_offset':offset,'exit_code':result.returncode,'pass':passed,'summary':summary})
 if passed:report['selected_soak_rps']=rate
 (a.output/'stages.json').write_text(json.dumps(report,indent=2)+'\n')
 print(json.dumps({'rate':rate,'pass':passed,'selected_soak_rps':report['selected_soak_rps']}),flush=True)
 return passed
for rate in (400,500,600,800):
 if not run(rate):break
if report['selected_soak_rps'] is None:
 for rate in (200,100):
  if run(rate):break
report['completed_utc']=datetime.now(UTC).isoformat()
(a.output/'stages.json').write_text(json.dumps(report,indent=2)+'\n')
if report['selected_soak_rps'] is None:raise SystemExit(1)

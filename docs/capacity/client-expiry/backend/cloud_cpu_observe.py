"""Bounded Docker CPU samples for the isolated benchmark project."""
import argparse
import json
import subprocess
import time
from datetime import UTC, datetime
from pathlib import Path

p=argparse.ArgumentParser(description=__doc__)
p.add_argument('--seconds',type=int,required=True)
p.add_argument('--output',type=Path,required=True)
a=p.parse_args()
if not 1<=a.seconds<=2400 or a.output.exists():p.error('Use a fresh output and 1..2400 seconds')
ids=subprocess.check_output(['docker','ps','-q','--filter','label=com.docker.compose.project=flash-cloud-bench'],text=True).split()
if not ids:raise SystemExit('No benchmark containers')
end=time.monotonic()+a.seconds
rows=[]
while time.monotonic()<end:
 start=time.monotonic()
 row={'utc':datetime.now(UTC).isoformat(),'host_cpu_ticks':Path('/proc/stat').read_text().splitlines()[0]}
 try:
  output=subprocess.check_output(['docker','stats','--no-stream','--format','{{json .}}',*ids],text=True,timeout=15)
  row['containers']=[json.loads(line) for line in output.splitlines()]
 except (subprocess.SubprocessError,ValueError) as exc:
  row['error']=type(exc).__name__
 rows.append(row)
 a.output.write_text(json.dumps(rows,indent=2)+'\n')
 time.sleep(max(0,10-(time.monotonic()-start)))

"""Staircase must stop escalation, retain failures, and use bounded fallback."""
import json
import subprocess
import sys
from pathlib import Path

import pytest


@pytest.mark.parametrize('passing,attempted,selected', [([400],[400,500],400),([100],[400,200,100],100),([],[400,200,100],None)])
def test_stages_stop_and_select_only_passed_rate(tmp_path,passing,attempted,selected):
 script=Path(__file__).parents[2]/'scripts/cloud_mixed_stages.py'
 (tmp_path/'parallel_cloud_load.py').write_text("import argparse,json\nfrom pathlib import Path\np=argparse.ArgumentParser()\np.add_argument('--rate',type=int);p.add_argument('--output');a,_=p.parse_known_args()\np=Path(a.output);p.mkdir(parents=True)\npassed=a.rate in "+repr(passing)+"\n(p/'summary.json').write_text(json.dumps({'gate_pass':passed}))\nraise SystemExit(0 if passed else 1)\n")
 result=subprocess.run([sys.executable,str(script),'--manifest','unused','--output',str(tmp_path/'results')],cwd=tmp_path,capture_output=True,text=True,timeout=20,check=False)
 report=json.loads((tmp_path/'results/stages.json').read_text())
 assert [s['rate'] for s in report['stages']]==attempted
 assert report['selected_soak_rps']==selected
 assert result.returncode==int(selected is None)
 assert all((tmp_path/f'results/rate-{rate}/summary.json').exists() for rate in attempted)

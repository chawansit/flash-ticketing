"""Real owned local Docker probe lifecycle; dependency client outcomes are synthetic here."""
import json
import os
import subprocess
import sys
from pathlib import Path

import pytest

ROOT=Path(__file__).resolve().parents[2]
sys.path.insert(0,str(ROOT/'scripts'))
import worker_separation_readiness as readiness
from stage_status_refresh_images import new_stage_output

pytestmark=pytest.mark.integration


def probe(context):
    if context['force_failure']:
        raise ValueError('Synthetic dependency failure')
    return {k:True for k in ('verified_rds_tls','redis_ready','pooler_ready','private_kafka_ready','all_four_apis_reachable')}


@pytest.mark.parametrize('outcome',['pass','failure'])
def test_real_owned_probe_container_is_removed_on_success_and_failure(monkeypatch,outcome):
    image=os.getenv('WORKER_CONFIG_TEST_IMAGE')
    if not image:pytest.skip('Set cached immutable local Python image for owned probe test')
    monkeypatch.setattr(readiness,'probe',probe)
    ids=subprocess.check_output(['docker','ps','-aq','--no-trunc'],text=True,timeout=10).split()
    rows=json.loads(subprocess.check_output(['docker','inspect',*ids],text=True,timeout=15)) if ids else []
    before={'rows':rows,'volumes':[],'bind_sha256':{}}
    saved={'broker_volume':{'Name':'unused-local-probe-volume'},'bind_sha256':{}}
    name=new_stage_output().name+'-dependency-secondary'
    code=readiness.probe_program(saved,before,{'synthetic':True,'force_failure':outcome=='failure'},image,name,'a'*64,45)
    result=subprocess.run([sys.executable,'-'],input=code,text=True,capture_output=True,timeout=60,check=False)
    remaining=subprocess.check_output(['docker','ps','-aq','--no-trunc','--filter','name=^/'+name+'$'],text=True,timeout=10).split()
    assert remaining==[],result.stderr[-1200:]
    if outcome=='pass':
        assert result.returncode==0,result.stderr[-1200:]
        receipt=json.loads(result.stdout)
        assert receipt['checks']=={k:True for k in readiness.CHECKS}
        assert receipt['probe_removed'] and receipt['runtime_unchanged']
    else:
        assert result.returncode!=0
        assert 'Dependency readiness failed' in result.stderr

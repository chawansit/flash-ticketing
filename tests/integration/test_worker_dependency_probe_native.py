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
from worker_separation_retained import eligible, row_digest, verify_retained

pytestmark=pytest.mark.integration


def probe(context, progress=None):
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
    retained={r['Id']:row_digest(r) for r in rows if eligible(r)}
    owner=new_stage_output().name
    name=owner+'-dependency-primary'
    volume_name=owner+'-probe-volume'
    subprocess.run(['docker','volume','create',volume_name],check=True,capture_output=True,timeout=10)
    try:
        volume=json.loads(subprocess.check_output(['docker','volume','inspect',volume_name],text=True,timeout=10))[0]
        before={'rows':verify_retained(rows,retained),'volumes':[volume],'bind_sha256':{}}
        saved={'broker_volume':volume,'bind_sha256':{},'retained_inactive_containers':retained}
        code=readiness.probe_program(saved,before,{'synthetic':True,'force_failure':outcome=='failure'},image,name,'a'*64,45)
        result=subprocess.run([sys.executable,'-'],input=code,text=True,capture_output=True,timeout=60,check=False)
    finally:
        subprocess.run(['docker','volume','rm',volume_name],check=True,capture_output=True,timeout=10)
    remaining=subprocess.check_output(['docker','ps','-aq','--no-trunc','--filter','name=^/'+name+'$'],text=True,timeout=10).split()
    assert remaining==[],result.stderr[-1200:]
    if outcome=='pass':
        assert result.returncode==0,result.stderr[-1200:]
        receipt=json.loads(result.stdout)
        assert receipt['checks']=={k:True for k in readiness.CHECKS}
        assert receipt['probe_removed'] and receipt['runtime_unchanged']
    else:
        assert result.returncode==0,result.stderr[-1200:]
        receipt=json.loads(result.stdout)
        assert receipt['failure']=={'phase':'imports','exception_category':'ValueError','category':'check_failed'}
        assert receipt['probe_removed'] and receipt['runtime_unchanged']

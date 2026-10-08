"""Execute startup attestation in the exact local image with networking disabled."""
import json
import os
import subprocess
import sys
from pathlib import Path
from uuid import uuid4

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[2] / 'scripts'))
import cce_api_adapter as cce
import work_envelope as policy

pytestmark = pytest.mark.skipif(os.getenv('TEST_CCE_FROZEN_IMAGE') != '1', reason='Opt-in exact local Docker image test')


@pytest.mark.parametrize('fault', ['none', 'environment', 'source', 'bytecode'])
def test_exact_image_startup_proof_without_network_or_api_dispatch(fault):
    command = ['uvicorn','ticketing.api:app','--host','0.0.0.0','--port','8000','--limit-concurrency','256','--timeout-keep-alive','10']
    environment = {**cce.contract()['api_settings'], 'DATABASE_URL':'postgresql://test:test@10.1.137.69:6432/test',
        'REDIS_URL':'redis://:test@10.1.245.61:6379/0','JWT_SECRET':'offline-test','WEBHOOK_SECRET':'offline-test'}
    program = 'environment_keys=' + repr(sorted(environment)) + '\n' + cce.startup_program(cce.contract()['api_sources'],policy.digest(environment),command)
    prelude = 'import os,json\nos.environ.update(' + repr(environment) + ')\nos.environ.update(CCE_RUN_ID="adr0151-'+'a'*12+'",CCE_POD_UID="offline-uid")\n'
    if fault == 'environment': prelude += 'os.environ["DB_POOL_MAX"]="5"\n'
    elif fault == 'source': program = program.replace(repr(cce.contract()['api_sources']['src/ticketing/api.py']),repr('0'*64))
    elif fault == 'bytecode':
        prelude += 'import ticketing.api,importlib.machinery\nloader=importlib.machinery.SourceFileLoader\nold=loader.get_code\nloader.get_code=lambda self,name:compile("pass","wrong.py","exec") if name=="ticketing.api" else old(self,name)\n'
    script = prelude + 'from unittest.mock import patch\nwith patch("os.execvp") as start:\n exec(' + repr(program) + ')\n assert start.call_args.args==(' + repr(command[0]) + ',' + repr(command) + ')\n print(json.dumps({"offline_startup_verified":True}))\n'
    name = 'codex-cce-local-'+uuid4().hex[:12]
    try:
        result = subprocess.run(['docker','run','--rm','--pull','never','--network','none','--read-only',
            '--cap-drop','ALL','--security-opt','no-new-privileges','--name',name,'--label','codex-owner='+name,
            '--memory','512m','--cpus','1','--entrypoint','python','-i',cce.dependency.INDEX,'-'],
            input=script,text=True,capture_output=True,timeout=60,check=False)
    finally:
        ids = subprocess.run(['docker','ps','-aq','--no-trunc','--filter','name=^/'+name+'$'],text=True,capture_output=True,check=True,timeout=10).stdout.split()
        if ids:
            assert len(ids)==1
            row=json.loads(subprocess.run(['docker','inspect',ids[0]],text=True,capture_output=True,check=True,timeout=10).stdout)[0]
            assert row['Name']=='/'+name and row['Config']['Labels']['codex-owner']==name and row['Image']==cce.dependency.INDEX
            subprocess.run(['docker','rm','-f',ids[0]],text=True,capture_output=True,check=True,timeout=10)
    if fault == 'none':
        assert result.returncode == 0, result.stderr[-2000:]
        proof = cce.parse_startup(result.stdout,policy.digest(environment),command)
        assert proof['sources']==cce.contract()['api_sources']
        assert json.loads(result.stdout.splitlines()[-1])['offline_startup_verified'] is True
    else:
        assert result.returncode != 0
        assert {'environment':'API environment drift','source':'Copied API source drift','bytecode':'Loaded API bytecode drift'}[fault] in result.stderr
    # --rm must have removed exactly this owned disposable test container.
    remaining = subprocess.run(['docker','ps','-aq','--filter','name=^/'+name+'$'],text=True,capture_output=True,check=True,timeout=10)
    assert remaining.stdout.strip()==''

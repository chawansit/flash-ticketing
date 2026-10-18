"""ADR0360 exact200 full preparation caller, provenance and actual-image dry rehearsal."""
import json
import os
import subprocess
import sys
from datetime import UTC, datetime, timedelta
from pathlib import Path
from types import SimpleNamespace
from uuid import uuid4

import pytest

sys.path.insert(0,str(Path(__file__).resolve().parents[2]/'scripts'))
import cce_paid_inputs as inputs
import cce_transaction_profile as transaction
import fixture_identity_evidence as provenance
import work_envelope as policy
from cce_paid_profiles import RATE_PUSH_200


def fixture():
    now=datetime.now(UTC)
    return {'schema_version':1,'environment':'development','fixture_id':str(uuid4()),
            'created_at':now.isoformat(),'sale_ends':(now+timedelta(hours=1)).isoformat(),
            'shows':200,'seats_per_show':300,'fixture_layout':'distributed',
            'show_ids':[str(uuid4()) for _ in range(200)]}


@pytest.mark.parametrize('fault',[None,'duplicate','count','seats','environment','expired','producer_count'])
def test_200_provenance_retains_ownership_and_freshness(fault):
    row=fixture()
    if fault=='duplicate':row['show_ids'][-1]=row['show_ids'][0]
    elif fault=='count':row['show_ids'].pop()
    elif fault=='seats':row['seats_per_show']=299
    elif fault=='environment':row['environment']='production'
    elif fault=='expired':row['created_at']=(datetime.now(UTC)-timedelta(minutes=3)).isoformat()
    elif fault=='producer_count':row['shows']=199
    if fault is None:assert provenance.fixture_identity(row,200)['shows']==200
    else:
        with pytest.raises(ValueError):provenance.fixture_identity(row,200)
    with pytest.raises(ValueError):inputs.fixture_program('/owned',201,'a'*64)
    with pytest.raises(ValueError):provenance.fixture_identity(row,201)


def test_complete_inputs_paid_200_uses_actual_builder_and_provenance(monkeypatch,tmp_path):
    row=fixture();calls=[]
    monkeypatch.setattr(transaction,'active',lambda:None)
    monkeypatch.setattr(policy,'ROOT',tmp_path)
    out=tmp_path/'candidate';out.mkdir()
    stage=SimpleNamespace(run='adr0151-036000000000',cid='cid',profile=RATE_PUSH_200,
           origin='http://qualified',bundle={'scripts/prepare_capacity_fixture.py':b'producer'},
           record={},output=out,events=[],checkpoint=lambda:None)
    def api(cid,program,timeout):
        compile(program,'actual200remoteprogram','exec');calls.append(program)
        if 'creation-intent' in program:
            assert "str(200)" in program and "'300'" in program and timeout==115
            return row
        assert 'range(60000)' in program
        return {'viewers':60000}
    session=SimpleNamespace(api=api);stage.session=session
    paid=inputs.Inputs(stage,'/owned','audit');paid.prepare=lambda:None
    manifest={'show_ids':row['show_ids'],'viewer_tokens':['synthetic']*60000,'seat_offset':0,'seats_per_show':300,'fixture_layout':'distributed'}
    monkeypatch.setattr(inputs,'copy_out',lambda *args:json.dumps(manifest).encode())
    actual,returned=paid.paid()
    assert actual==row and len(returned['viewer_tokens'])==60000
    assert stage.events==row['show_ids'] and stage.record['fixture_creation_attempted']
    assert paid.record['creation_attempted']==['paid'] and paid.record['fixtures']['paid']['shows']==200
    assert len(calls)==2


def test_optional_actual_candidate_producer_and_builder(monkeypatch,tmp_path):
    target=os.getenv('QUALIFY_200_FIXTURE_RECEIPT')
    if not target:pytest.skip('Explicit actual-image dry preparation not requested')
    import cce_simulator_roundtrip_profile as profile
    program=inputs.fixture_program('/tmp/qualified200',200,'PRODUCERHASHPLACEHOLDER')
    (tmp_path/'program.txt').write_text(program,encoding='utf-8')
    (tmp_path/'fixture_identity_evidence.py').write_bytes(Path(provenance.__file__).read_bytes())
    harness=r"""import contextlib,hashlib,io,json,os,runpy,subprocess,sys
from pathlib import Path
from types import SimpleNamespace
from datetime import UTC,datetime,timedelta
from uuid import uuid4
sys.path[:0]=['/qualified','/app/scripts']
from fixture_identity_evidence import fixture_identity
f=Path('/app/scripts/prepare_capacity_fixture.py')
module=runpy.run_path(str(f),run_name='qualification')
args=SimpleNamespace(output=Path('/tmp/nonexistent-qualified-fixture'),shows=200,seats=300,sale_hours=1,fixture_layout='distributed')
module['validate'](args,SimpleNamespace(environment='development'))
try:module['validate'](args,SimpleNamespace(environment='production'));raise AssertionError('production accepted')
except RuntimeError:pass
now=datetime.now(UTC)
row={'schema_version':1,'environment':'development','fixture_id':str(uuid4()),'created_at':now.isoformat(),'sale_ends':(now+timedelta(hours=1)).isoformat(),'shows':200,'seats_per_show':300,'fixture_layout':'distributed','show_ids':[str(uuid4()) for _ in range(200)]}
calls=[]
def dry_run(command,**kwargs):
 assert command[command.index('--shows')+1]=='200' and command[command.index('--seats')+1]=='300'
 assert kwargs['timeout']==90 and kwargs['check'] is True
 calls.append(command)
 Path(command[command.index('--output')+1]).write_text(json.dumps(row))
 return SimpleNamespace(returncode=0)
subprocess.run=dry_run
os.environ['DATABASE_URL']='postgresql://dry/unused';os.environ['REDIS_URL']='redis://dry/unused'
source=Path('/qualified/program.txt').read_text().replace('PRODUCERHASHPLACEHOLDER',hashlib.sha256(f.read_bytes().replace(b'\r\n',b'\n')).hexdigest())
output=io.StringIO()
with contextlib.redirect_stdout(output):exec(compile(source,'actual200fixturebuilder','exec'),{})
value=json.loads(output.getvalue());assert len(calls)==1 and fixture_identity(value,200)['shows']==200
print(json.dumps({'pass':True,'actual_image_producer_validated':True,'actual_generated_builder_executed':True,'fixture_rows_synthetic':True,'database_calls':0,'network_disabled':True,'shows':200,'seats':60000}))
"""
    image=profile.LOCAL_IMAGE
    result=subprocess.run(['docker','run','--rm','--network=none','-i','--entrypoint','python','--mount',
         'type=bind,source='+str(tmp_path.resolve())+',target=/qualified,readonly',image,'-c',harness],capture_output=True,text=True,timeout=90,check=False)
    Path('tmp/adr0360-actual-fixture-image.log').write_text(result.stderr,encoding='utf-8')
    assert result.returncode==0,'Inspect owned actual fixture image log'
    receipt=json.loads(result.stdout);assert receipt['pass']
    path=Path(target);assert not path.exists() and path.resolve().is_relative_to((policy.ROOT/'tmp').resolve())
    path.write_bytes((json.dumps(receipt,indent=2)+'\n').encode())

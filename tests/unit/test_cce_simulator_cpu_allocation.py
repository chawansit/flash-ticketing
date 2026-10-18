"""ADR0359 preserve six partition owners within unchanged aggregate CPU."""
import copy
import hashlib
import json
import os
import subprocess
import sys
from pathlib import Path
from types import SimpleNamespace

import pytest

sys.path.insert(0,str(Path(__file__).resolve().parents[2]/'scripts'))
import cce_fixed_worker_allocation as allocation
import cce_simulator_roundtrip_profile as profile
import cce_worker_identity as identity
import work_envelope as policy
from test_cce_fixed_worker_allocation import allocated


def test_exact_local_and_transferred_resources(monkeypatch):
    goal,bundle=allocated(monkeypatch,'ADR0359')
    spec=identity.specification('ADR0359',goal=goal)
    assert spec['counts']['consumer']==6 and spec['counts']['reservation-writer']==4
    assert len(identity.validate_bundle(bundle))==len(identity.validate_transferred_bundle(bundle))==15
    for kind in ('requests','limits'):
        assert sum(int(r['resources'][kind]['cpu'][:-1]) for r in bundle['receipts'])==3750
        assert sum(int(r['resources'][kind]['memory'][:-2]) for r in bundle['receipts'])==7680
    assert spec['resources']['simulator']['limits']['cpu']=='500m'
    assert allocation.native_drain_options(SimpleNamespace(cleanup_mode=False))=={}
    monkeypatch.setattr(profile,'image',lambda *args:(_ for _ in ()).throw(AssertionError('local authority')))
    assert len(identity.validate_transferred_bundle(bundle))==15


@pytest.mark.parametrize('fault',['old_count','extra_writer','sim_cpu','maintenance_cpu','manifest','source','root_resources'])
def test_rejects_previous_allocation_and_budget_drift(monkeypatch,fault):
    goal,bundle=allocated(monkeypatch,'ADR0359')
    if fault=='old_count':
        bundle['receipts'][-1]['role']='reservation-writer'
    elif fault=='extra_writer':bundle['receipts'].append(copy.deepcopy(bundle['receipts'][0]))
    elif fault=='sim_cpu':next(r for r in bundle['receipts'] if r['role']=='simulator')['resources']['limits']['cpu']='250m'
    elif fault=='maintenance_cpu':next(r for r in bundle['receipts'] if r['role']=='maintenance')['resources']['limits']['cpu']='250m'
    elif fault=='manifest':bundle['manifest_digest']=profile.parent.image()['registry_manifest_digest']
    elif fault=='source':bundle['sources']={}
    else:bundle['resources']['simulator']['limits']['cpu']='750m'
    with pytest.raises(ValueError):identity.validate_transferred_bundle(bundle)
    with pytest.raises(ValueError):identity.specification('ADR0357',goal=goal)


def test_actual_parent_startup_six_partition_contract_and_transport(monkeypatch):
    import cce_api_adapter as adapter
    import cce_writer_scale_verification as verification
    import run_cce_paid_comparison as core
    from observe_paid_pipeline import pipeline_startup_view
    _,bundle=allocated(monkeypatch,'ADR0359')
    contract=core.worker_contract()
    assert "result['kafka_members']==6" in contract.global_audit
    assert "result['kafka_members'] in (5,6)" not in contract.global_audit
    assert "projection['assigned_partitions']==6" in contract.global_audit
    row=policy.read(policy.ROOT/'tmp/adr0151-ccb85bad6425/candidate/pipeline-startup-row.private.json')
    contract.verify_pipeline_startup(row)
    assert verification.startup(row,bundle,pipeline_startup_view)['pass']
    monkeypatch.setattr(adapter.dependency,'kube_material',lambda _: {'client.key':'test'})
    calls=[]
    def call(*args,**kwargs):
        calls.append(args)
        return {'value':{'observed':True}}
    transport=adapter.KubernetesTransport(SimpleNamespace(call=call),Path('unused'))
    root='/api/v1/namespaces/flash-cce-'+'a'*12+'/pods/'
    for name in ['consumer-5','reservation-writer-3']:
        assert transport.request('GET',root+name)['observed']
    for name in ['consumer-6','reservation-writer-4']:
        with pytest.raises(ValueError):transport.request('GET',root+name)
    assert len(calls)==2


def test_200_requires_same_cpu_allocation_and_real_clean_180(monkeypatch):
    import cce_rate_qualification as qualification
    goal,_=allocated(monkeypatch,'ADR0359')
    goal['rate_qualification_rate']=200
    goal['rate_qualification_reference']['rate']=180
    value=allocation.contract(goal)
    before=copy.deepcopy(policy.read(policy.STATE)[goal['rate_qualification_reference']['ledger']]['binding'])
    wanted=copy.deepcopy(before)
    wanted['cce_worker_placement_factor'].update(worker_counts=value['counts'],worker_resources=value['resources'])
    wanted['cce_writer_scale_contract'].update(decision='ADR0359',native_counts=value['counts'],worker_resources=value['resources'])
    with pytest.raises(ValueError):qualification.require_previous(goal)
    with pytest.raises(ValueError):allocation.validate_binding(goal,wanted,before)
    before['cce_simulator_roundtrip_contract']['worker_allocation']=value
    assert allocation.validate_binding(goal,wanted,before)


def test_optional_actual_image_cpu_allocation(monkeypatch,tmp_path):
    target=os.getenv('QUALIFY_CPU_ALLOCATION_RECEIPT')
    if not target:pytest.skip('Explicit immutable-image rehearsal not requested')
    from test_cce_transferred_combined_workers import PROGRAM, capture_helpers
    image,helpers=capture_helpers(monkeypatch,tmp_path,'candidate')
    _,bundle=allocated(monkeypatch,'ADR0359')
    (tmp_path/'workers.json').write_bytes(json.dumps(bundle).encode())
    inventory=policy.read(policy.ROOT/'tmp/adr0151-ccb85bad6425/candidate/pre-transition-inventory.private.json')
    from datetime import UTC, datetime
    inventory['captured_at']=datetime.now(UTC).isoformat()
    (tmp_path/'inventory.json').write_bytes(json.dumps(inventory).encode())
    source=PROGRAM.replace("metadata=json.load(sys.stdin)","metadata=json.load(sys.stdin)\nfor name,want in metadata['runtime_sources'].items():assert hashlib.sha256((Path('/app')/name).read_bytes()).hexdigest()==want")
    metadata={'helpers':{n:hashlib.sha256(v).hexdigest() for n,v in helpers.items()},'runtime_sources':image['runtime_source_sha256']}
    command=['docker','run','--rm','--network=none','-i','--entrypoint','python','--mount',
             'type=bind,source='+str(tmp_path.resolve())+',target=/qualified,readonly',image['local_image_id'],'-c',source]
    result=subprocess.run(command,input=json.dumps(metadata),text=True,capture_output=True,timeout=90,check=False)
    (policy.ROOT/'tmp/adr0359-actual-image-helper.log').write_text(result.stderr,encoding='utf-8')
    assert result.returncode==0,'Inspect owned actual-image helper log'
    output=Path(target);assert output.resolve().is_relative_to((policy.ROOT/'tmp').resolve()) and not output.exists()
    output.write_bytes((json.dumps({'decision':'ADR0359',**json.loads(result.stdout),'runtime_modules_verified':22,'synthetic_inventory_timestamp':True},indent=2)+'\n').encode())

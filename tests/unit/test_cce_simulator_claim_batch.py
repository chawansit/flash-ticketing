"""Exact new-image, observer, allocation and prior-result authority boundaries."""
import copy
import hashlib
import json
import os
import subprocess
from datetime import UTC, datetime
from pathlib import Path

import cce_image_qualification as modes
import cce_rate_qualification as qualification
import cce_simulator_claim_batch as batch
import cce_simulator_roundtrip_profile as profile
import prepare_rate_qualification as preparer
import pytest
import work_envelope as policy
from test_cce_persistent_uuid import goal as parent_goal


def goal(rate=180):
    value=parent_goal(rate)
    value.update(guarded_issuance_decision='ADR0365',terminal_admission_decision='ADR0367',
                 simulator_claim_batch_decision='ADR0368')
    return preparer.goal_for(value,rate,value['rate_qualification_reference'],value['simulator_roundtrip_pair'])


def test_actual_candidate_and_clean_parent_authority_are_source_bound():
    value=goal()
    assert modes.for_goal(value) is batch and profile.selected(value)
    assert qualification.require_authority(value)[0]['run']=='adr0151-6668a7e46ed3'
    assert profile.image(value)['registry_manifest_digest']==batch.MANIFEST
    assert batch.contract(value)['configuration']==batch.CONFIG
    assert batch.image()['source_identity_sha256']==batch.SOURCE
    with pytest.raises(ValueError):qualification.require_previous(goal(200))


@pytest.mark.parametrize('fault',['decision','image_parent','observer','guards','allocation','claim_mode'])
def test_no_implicit_or_mixed_image_mode(fault):
    value=goal()
    if fault=='decision':value[batch.KEY]='unknown'
    elif fault=='image_parent':value.pop('persistent_uuid_decision')
    elif fault=='observer':value.pop('terminal_admission_decision')
    elif fault=='guards':value.pop('guarded_issuance_decision')
    elif fault=='allocation':value['fixed_worker_allocation_decision']='ADR0357'
    else:value['reservation_claim_lookup_decision']='ADR0362'
    with pytest.raises(ValueError):modes.for_goal(value).contract(value)


def test_feature_scope_and_resource_budget_are_exact():
    value=goal()
    for role in ['api','simulator','reservation-writer','consumer','projection-consumer','publisher','maintenance','reconciler']:
        env=batch.environment(role,{'kept':'value'},value)
        assert env['PERSISTENT_UUID_V7']=='1'
        assert env.get('SIMULATOR_BATCH_CLAIM')==('1' if role=='simulator' else None)
    with pytest.raises(ValueError):batch.environment('simulator',{'SIMULATOR_BATCH_CLAIM':'0'},value)
    with pytest.raises(ValueError):batch.normalize_api_environment({'PERSISTENT_UUID_V7':'1','SIMULATOR_BATCH_CLAIM':'1'},value)
    assert batch.normalize_api_environment({'PERSISTENT_UUID_V7':'1','kept':'value'},value)=={'kept':'value'}


def test_200_requires_exact_new_image_and_terminal_observer():
    value=goal(200)
    previous=profile.binding_contract(goal())
    batch.validate_previous(value,previous)
    for key in ['source','simulator_claim_batch','terminal_admission','worker_allocation']:
        broken=copy.deepcopy(previous);broken.pop(key)
        with pytest.raises(ValueError):batch.validate_previous(value,broken)


def test_transferred_bundle_pins_only_the_six_consumer_allocation(monkeypatch):
    import cce_worker_identity as identity
    from test_cce_fixed_worker_allocation import allocated
    value,bundle=allocated(monkeypatch,'ADR0359',goal=goal())
    assert len(identity.validate_transferred_bundle(bundle))==15
    for decision in ['ADR0345','ADR0357']:
        broken=copy.deepcopy(bundle);broken['decision']=decision
        with pytest.raises(ValueError):identity.validate_transferred_bundle(broken)
    assert value['fixed_worker_allocation_decision']=='ADR0359'


def test_extended_terminal_transport_requires_the_explicit_new_goal(monkeypatch):
    from types import SimpleNamespace

    import cce_api_adapter as api
    import cce_transaction_profile as transaction
    from cce_recovery_headroom import runtime_view
    value=goal()
    monkeypatch.setattr(transaction,'active',lambda:runtime_view(value))
    monkeypatch.setattr(api.dependency,'kube_material',lambda path:{'ca.crt':'local-test-only'})
    calls=[]
    transport=api.KubernetesTransport(SimpleNamespace(call=lambda *a,**k:calls.append(a) or {'value':{'schema_version':1}}),Path('unused'))
    route='/api/v1/namespaces/flash-cce-aaaaaaaaaaaa/pods/api-0/log?container=api&sinceSeconds=600&limitBytes=67108864'
    assert transport.request('GET',route)=={'schema_version':1}
    assert len(calls)==1
    value.pop('terminal_admission_decision')
    with pytest.raises(ValueError):transport.request('GET',route)


def test_actual_published_image_helper_rehearsal(monkeypatch,tmp_path):
    target=os.getenv('QUALIFY_CLAIM_BATCH_HELPERS_RECEIPT')
    if not target:pytest.skip('Explicit published-image helper rehearsal not requested')
    import cce_transaction_profile as transaction
    import test_cce_transferred_combined_workers as retained
    from cce_recovery_headroom import runtime_view
    from test_cce_fixed_worker_allocation import allocated
    value,bundle=allocated(monkeypatch,'ADR0359',goal=goal())
    monkeypatch.setattr(transaction,'active',lambda:runtime_view(value))
    monkeypatch.setattr(retained,'arguments',lambda *args:(value,{}, {'run':bundle['run'],'workers_before':bundle},[]))
    image,helpers=retained.capture_helpers(monkeypatch,tmp_path,'candidate')
    inventory=policy.read(policy.ROOT/'tmp/adr0151-ccb85bad6425/candidate/pre-transition-inventory.private.json')
    inventory['captured_at']=datetime.now(UTC).isoformat()
    (tmp_path/'inventory.json').write_bytes(json.dumps(inventory).encode())
    program=retained.PROGRAM.replace('metadata=json.load(sys.stdin)',"metadata=json.load(sys.stdin)\nfor name,want in metadata['runtime_sources'].items():assert hashlib.sha256((Path('/app')/name).read_bytes()).hexdigest()==want")
    metadata={'helpers':{n:hashlib.sha256(v).hexdigest() for n,v in helpers.items()},'runtime_sources':image['runtime_source_sha256']}
    result=subprocess.run(['docker','run','--rm','--network=none','-i','--entrypoint','python',
        '--mount','type=bind,source='+str(tmp_path.resolve())+',target=/qualified,readonly',
        image['local_image_id'],'-c',program],input=json.dumps(metadata),text=True,capture_output=True,timeout=90,check=False)
    (policy.ROOT/'tmp/adr0370-actual-helper.log').write_text(result.stderr,encoding='utf-8')
    assert result.returncode==0,'Inspect owned helper log'
    path=Path(target);assert not path.exists() and path.resolve().is_relative_to((policy.ROOT/'tmp').resolve())
    path.write_bytes((json.dumps({'decision':'ADR0368',**json.loads(result.stdout),'runtime_modules_verified':22,
                                 'synthetic_inventory_timestamp':True},indent=2)+'\n').encode())

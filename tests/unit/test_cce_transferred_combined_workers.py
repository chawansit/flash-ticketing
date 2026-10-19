"""Portable observer identity and exact transferred-package rehearsal; no cloud calls."""
import copy
import hashlib
import json
import os
import subprocess
import sys
from datetime import UTC, datetime
from pathlib import Path
from types import SimpleNamespace

import pytest

sys.path.insert(0,str(Path(__file__).resolve().parents[2]/'scripts'))
import cce_combined_worker_identity as pins
import cce_paid_observers as observations
import cce_simulator_roundtrip_profile as profile
import cce_transaction_profile as transaction
import cce_worker_identity as identity
import diagnostic_runner_connection as diagnostics
import work_envelope as policy
from cce_paid_profiles import RATE_PUSH_160
from test_cce_combined_inventory_renewal import arguments
from test_observe_cce_paid_pipeline import bundle as legacy_api_bundle


@pytest.fixture(autouse=True)
def inactive_authorization(monkeypatch):
    data=copy.deepcopy(policy.envelope())
    data['spending']['temporary_cce_pilot_exception']['goal_bounded_authorization']={}
    monkeypatch.setattr(policy,'envelope',lambda:data)


@pytest.mark.parametrize('arm',['control','candidate'])
def test_transferred_verification_needs_no_local_authority(monkeypatch,arm):
    _,_,kwargs,_=arguments(monkeypatch,arm)
    value=kwargs['workers_before']
    def unavailable(*args,**kwargs):
        raise AssertionError('Local authority must not be used by transferred verification')
    monkeypatch.setattr(transaction,'active',unavailable)
    monkeypatch.setattr(policy,'read',unavailable)
    assert len(identity.validate_transferred_bundle(value))==15


def test_static_pins_match_real_registry_and_control_receipts():
    import cce_persistent_uuid as identifiers
    import cce_projection_refresh_pipeline as projection
    import cce_reservation_claim_lookup as claim
    assert pins.PINS=={image['registry_manifest_digest']:image['source_identity_sha256']
                       for image in (profile.parent.image(),profile.registry_image(),claim.image(),projection.image(),identifiers.image())}


@pytest.mark.parametrize('fault',['manifest','source','image','resources','missing','duplicate_uid',
                                  'public_ip','process','command','run'])
def test_transferred_verification_rejects_changed_runtime_evidence(monkeypatch,fault):
    _,_,kwargs,_=arguments(monkeypatch)
    value=copy.deepcopy(kwargs['workers_before']);row=value['receipts'][0]
    if fault=='manifest':value['manifest_digest']='sha256:'+'0'*64
    elif fault=='source':value['sources']['src/ticketing/api.py']='0'*64
    elif fault=='image':row['image_id']='sha256:'+'0'*64
    elif fault=='resources':row['resources']={}
    elif fault=='missing':value['receipts'].pop()
    elif fault=='duplicate_uid':value['receipts'][1]['pod_uid']=row['pod_uid']
    elif fault=='public_ip':row['private_ipv4']='8.8.8.8'
    elif fault=='process':row['process_start_time_seconds']=float('nan')
    elif fault=='command':row['startup_proof']['command_sha256']='0'*64
    else:row['startup_proof']['run']='foreign'
    with pytest.raises(ValueError):identity.validate_transferred_bundle(value)


def test_default_local_verification_still_requires_active_arm(monkeypatch):
    control,_,_,_=arguments(monkeypatch,'control')
    _,_,kwargs,_=arguments(monkeypatch,'candidate')
    from cce_recovery_headroom import runtime_view
    monkeypatch.setattr(transaction,'active',lambda:runtime_view(control))
    with pytest.raises(ValueError):identity.validate_bundle(kwargs['workers_before'])
    assert len(identity.validate_transferred_bundle(kwargs['workers_before']))==15


def capture_helpers(monkeypatch,folder,arm):
    actual,_,kwargs,_=arguments(monkeypatch,arm)
    image=profile.image(actual)
    api=copy.deepcopy(legacy_api_bundle())
    settings=observations.cce.contract()
    api.update(run=kwargs['run'],api_sources=image['runtime_source_sha256'],
               manifest_digest=image['registry_manifest_digest'],resources=settings['resources'])
    for row in api['receipts']:
        row['image_id']=image['registry_image'];row['resources']=settings['resources']
        row['startup_proof'].update(run=api['run'],sources=api['api_sources'])
    captured={}
    def upload(session,cid,owner,directory,name,content):
        raw=content.encode()
        if name in captured:assert captured[name]==raw
        captured[name]=raw;(folder/name).write_bytes(raw)
        return directory+'/'+name
    monkeypatch.setattr(observations,'api_upload',upload)
    class Captured(Exception):pass
    def diagnostic_prepare(*args,**kwargs):raise Captured()
    monkeypatch.setattr(diagnostics,'prepare',diagnostic_prepare)
    def api_call(cid,program,timeout):
        compile(program,'offline-observer-command','exec')
        return {'native_helpers_verified':True} if 'native_helpers_verified' in program else {'fresh':True}
    session=SimpleNamespace(api=api_call,call=lambda *args:{'fresh':True})
    stage=SimpleNamespace(profile=RATE_PUSH_160,session=session,run=api['run'],cid='c'*64,
                          manifest={'environment':'development','show_ids':['synthetic'],'viewer_tokens':['synthetic']},
                          output=folder,record={},check=lambda *args:None,checkpoint=lambda:None,
                          guard=SimpleNamespace(binding={}))
    observer=observations.Observers(stage,{},None,None,'/offline')
    observer.native_workers=kwargs['workers_before']
    with pytest.raises(Captured):observer.start(api['receipts'])
    assert 'cce_combined_worker_identity.py' in captured
    assert 'cce_simulator_roundtrip_profile.py' not in captured
    assert 'work_envelope.py' not in captured
    (folder/'api.json').write_bytes(json.dumps(api).encode())
    (folder/'workers.json').write_bytes(json.dumps(kwargs['workers_before']).encode())
    return image,captured


@pytest.mark.parametrize('arm',['control','candidate'])
def test_actual_observer_start_selects_complete_portable_helpers(monkeypatch,tmp_path,arm):
    _,helpers=capture_helpers(monkeypatch,tmp_path,arm)
    assert {'observe_cce_paid_pipeline.py','observe_cce_workers.py','cce_worker_identity.py',
            'cce_combined_worker_identity.py','cce_fulfillment_batch_identity.py'}<=helpers.keys()


PROGRAM = r"""
import copy,hashlib,json,sys
from pathlib import Path
from datetime import UTC, datetime
from types import SimpleNamespace
sys.path.insert(0,'/qualified')
metadata=json.load(sys.stdin)
for name,want in metadata['helpers'].items():
 assert hashlib.sha256((Path('/qualified')/name).read_bytes()).hexdigest()==want
import observe_cce_paid_pipeline as api
import observe_slot_paid_pipeline as entry
import observe_cce_workers as workers
import cce_worker_identity as identity
import cce_combined_worker_identity as pins
for module in (api,entry,workers,identity,pins):assert str(Path(module.__file__).resolve()).startswith('/qualified/')
a=json.load(open('/qualified/api.json'));w=json.load(open('/qualified/workers.json'))
api.validate_receipts(a)
inventory=json.load(open('/qualified/inventory.json'))
view,sealed=entry.correction_inventory(inventory,identity.digest(inventory))
assert view==inventory and sealed==identity.digest(inventory)
assert len(identity.validate_transferred_bundle(w))==15
assert 'cce_simulator_roundtrip_profile' not in sys.modules
called=[]
api.observer.main=lambda args:called.append(True)
args=['--native-receipts','/qualified/api.json','--native-receipts-sha256',identity.digest(a),
      '--native-workers','/qualified/workers.json','--native-workers-sha256',identity.digest(w)]
entry.main(args)
assert called==[True]
try:entry.main(args[:-1]+['0'*64])
except ValueError:pass
else:raise AssertionError('Changed worker transfer hash accepted')
module=SimpleNamespace(METRICS={},api_replicas=lambda host,port:[],worker_counters=lambda *args:{},urlopen=None,
                       PAID_COHORT_QUERY='FROM payment_attempts p JOIN cohort o ON o.id=p.order_id; FROM tickets t JOIN bookings b ON b.id=t.booking_id',
                       parse_api_metrics=lambda value:{})
entry.install(module,a)
workers.install(module,w)
assert len(module.api_replicas('reservation-writer',9101))==4
assert len(module.api_replicas('consumer',9101))==6
assert 'cce_simulator_roundtrip_profile' not in sys.modules
print(json.dumps({'pass':True,'helper_files_verified':len(metadata['helpers']),'api_receipts':4,
                  'worker_receipts':15,'actual_cli_validation_passed':True,'outer_entry_installation_passed':True,
                  'retained_inventory_structure_validated':True,'inventory_timestamp_scope':'synthetic local replay',
                  'changed_transfer_hash_rejected':True,'worker_installation_passed':True,
                  'network_disabled':True,'local_authority_profile_imported':False,'cloud_calls':0}))
"""


def test_optional_actual_images_with_exact_captured_helper_package(monkeypatch,tmp_path):
    target=os.getenv('QUALIFY_COMBINED_OBSERVER_HELPERS_RECEIPT')
    if not target:pytest.skip('Explicit bounded Docker helper rehearsal not requested')
    output=Path(target).resolve()
    assert output.is_relative_to((policy.ROOT/'tmp').resolve()) and not output.exists()
    inventory_path=Path(os.environ['QUALIFY_COMBINED_OBSERVER_INVENTORY']).resolve()
    assert inventory_path.is_relative_to(policy.ROOT.resolve()) and inventory_path.is_file()
    inventory=json.loads(inventory_path.read_text(encoding='utf-8'))
    inventory['captured_at']=datetime.now(UTC).isoformat()
    results={}
    for arm in ('control','candidate'):
        folder=tmp_path/arm;folder.mkdir()
        with pytest.MonkeyPatch.context() as patch:
            image,helpers=capture_helpers(patch,folder,arm)
        (folder/'inventory.json').write_bytes(json.dumps(inventory).encode())
        command=['docker','run','--rm','--network=none','-i','--entrypoint','python','--mount',
                 'type=bind,source='+str(folder.resolve())+',target=/qualified,readonly',image['registry_image'],'-c',PROGRAM]
        result=subprocess.run(command,input=json.dumps({'helpers':{name:hashlib.sha256(raw).hexdigest() for name,raw in helpers.items()}}),text=True,capture_output=True,timeout=60,check=False)
        (policy.ROOT/'tmp'/('adr0349-'+arm+'-helper-rehearsal.private.log')).write_text(result.stderr,encoding='utf-8')
        assert result.returncode==0,'Actual-image helper rehearsal failed; inspect owned log'
        results[arm]={**json.loads(result.stdout),'registry_image':image['registry_image'],
                      'source_identity_sha256':image['source_identity_sha256']}
    output.write_bytes((json.dumps({'decision':'ADR0349','arms':results,'capacity_improvement_measured':False},indent=2)+'\n').encode())

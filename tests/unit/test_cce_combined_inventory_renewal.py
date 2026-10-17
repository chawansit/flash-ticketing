"""Actual renewal/observer-start path; synthetic runtime proofs, no cloud writes."""
import copy
import json
import sys
from datetime import UTC, datetime, timedelta
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[2]/'scripts'))
import cce_inventory_renewal as renewal
import cce_paid_observers as observations
import cce_recovery_headroom as headroom
import cce_simulator_roundtrip_profile as profile
import cce_transaction_profile as transaction
import cce_worker_identity as identity
import work_envelope as policy
from test_cce_simulator_roundtrip_profile import goal


@pytest.fixture(autouse=True)
def inactive_authorization(monkeypatch):
    data=copy.deepcopy(policy.envelope())
    data['spending']['temporary_cce_pilot_exception']['goal_bounded_authorization']={}
    monkeypatch.setattr(policy,'envelope',lambda:data)


def arguments(monkeypatch, arm='control'):
    actual=goal(arm, preview=False)
    monkeypatch.setattr(transaction,'active',lambda:headroom.runtime_view(actual))
    image=profile.image(actual)
    selected=identity.specification('ADR0345',goal=actual)
    run='adr0151-034800000000'
    rows=[]
    for role,count in selected['counts'].items():
        for i in range(count):
            index=len(rows)+1
            uid=f'synthetic-{index}'
            rows.append({'role':role,'pod_name':f'{role}-{i}','pod_uid':uid,
                         'private_ipv4':f'10.2.0.{index}','image_id':image['registry_image'],
                         'resources':identity.RESOURCES,'process_start_time_seconds':100+index,
                         'startup_proof':{'run':run,'pod_uid':uid,'sources':image['runtime_source_sha256'],
                                          'environment_sha256':'a'*64,
                                          'command_sha256':policy.digest(['python','-m','ticketing.workers',role])}})
    workers={'decision':'ADR0345','run':run,'sources':image['runtime_source_sha256'],
             'manifest_digest':selected['manifest'],'resources':identity.RESOURCES,'receipts':rows}
    native={'run':run,'receipts':[{'pod_uid':f'api-{i}'} for i in range(4)]}
    now=datetime.now(UTC)
    inventory={'captured_at':(now-timedelta(seconds=600)).isoformat(),'hosts':{'primary':'same'}}
    binding={'cce_inventory_renewal_contract':renewal.contract(),
             'cce_writer_scale_contract':headroom.writer_scale_contract(),
             'cce_simulator_roundtrip_contract':profile.binding_contract(actual)}
    calls=[]
    kwargs={'run':run,'binding':binding,'native_before':native,'workers_before':workers,
            'observe_native':lambda:(calls.append('api') or native),
            'observe_workers':lambda:(calls.append('workers') or workers),
            'verify_background':lambda:calls.append('background'),'now':now}
    return actual,inventory,kwargs,calls


@pytest.mark.parametrize('arm',['control','candidate'])
def test_complete_combined_worker_proofs_reach_live_renewal(monkeypatch,arm):
    _,inventory,kwargs,calls=arguments(monkeypatch,arm)
    original=copy.deepcopy(inventory)
    assert len(identity.validate_bundle(kwargs['workers_before']))==15
    value,evidence=renewal.renew(inventory,**kwargs)
    assert calls==['api','workers','background']
    assert inventory==original
    assert evidence['native_background_workers']==15 and evidence['background_unchanged']
    assert value['captured_at']==kwargs['now'].isoformat()


@pytest.mark.parametrize('fault',['missing_pair','different_pair','preview','source','image',
                                  'missing_worker','duplicate_uid','live_change'])
def test_combined_renewal_never_accepts_partial_or_unbound_evidence(monkeypatch,fault):
    actual,inventory,kwargs,_=arguments(monkeypatch)
    workers=kwargs['workers_before']
    if fault=='missing_pair':kwargs['binding'].pop('cce_simulator_roundtrip_contract')
    elif fault=='different_pair':kwargs['binding']['cce_simulator_roundtrip_contract']['pair']['id']='f'*32
    elif fault=='preview':
        actual['simulator_roundtrip_preview']=True
        kwargs['binding']['cce_simulator_roundtrip_contract']=profile.binding_contract(actual)
    elif fault=='source':workers['sources']={**workers['sources'],'src/ticketing/api.py':'0'*64}
    elif fault=='image':workers['manifest_digest']='sha256:'+'0'*64
    elif fault=='missing_worker':workers['receipts'].pop()
    elif fault=='duplicate_uid':workers['receipts'][1]['pod_uid']=workers['receipts'][0]['pod_uid']
    else:
        changed=copy.deepcopy(workers);changed['receipts'][0]['process_start_time_seconds']+=1
        kwargs['observe_workers']=lambda:changed
    original=copy.deepcopy(inventory)
    with pytest.raises(ValueError):renewal.renew(inventory,**kwargs)
    assert inventory==original


def test_actual_observer_start_retains_worker_proof_before_renewal_failure(monkeypatch,tmp_path):
    from test_cce_paid_observers import (
        test_observer_start_transfers_all_helpers_once_and_blocks_bad_startup as scenario,
    )
    original=observations.Observers.__init__
    proof={'decision':'synthetic-retention-only','receipts':[]}
    def setup(observer,*args,**kwargs):
        original(observer,*args,**kwargs)
        observer.native_workers=proof
        def reject(*args):
            raise ValueError('intentional renewal rejection')
        observer.renew_inventory=reject
    monkeypatch.setattr(observations.Observers,'__init__',setup)
    with pytest.raises(ValueError,match='intentional renewal rejection'):
        scenario(monkeypatch,tmp_path,None)
    retained=json.loads((tmp_path/'native-workers.private.json').read_text(encoding='utf-8'))
    assert retained==proof

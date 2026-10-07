"""ADR0204 predeployment recovery rejection and append-only closure tests."""
import copy
import json
import sys
from pathlib import Path

import pytest

ROOT=Path(__file__).resolve().parents[2]
sys.path.insert(0,str(ROOT/'scripts'))
import work_envelope as policy
import worker_separation_staging_recovery as recovery


@pytest.fixture
def case(monkeypatch):
    config={'synthetic':'config'};saved={'synthetic':'runtime'};helpers={'synthetic':'helpers'}
    package={'saved':saved,'helpers':helpers,'inputs':{'synthetic':'inputs'}}
    expected={'worker_saved_runtime_sha256':policy.digest(saved),'worker_staging_contract_sha256':'a'*64}
    monkeypatch.setattr(recovery,'binding_for',lambda inputs:expected)
    monkeypatch.setattr(recovery,'audit_contract',lambda helpers:{'synthetic':'audit'})
    binding={**expected,'configuration_sha256':policy.digest(config),'worker_audit_contract_sha256':policy.digest({'synthetic':'audit'})}
    staging={'status':'FAILED_CLEANED','pass':False,'runtime_unchanged':True,'service_deployments':0,'customer_dispatches':0,
             'contract_sha256':'a'*64,'hosts':{'primary':{'creation_attempted':True,'archive_removed':True,'owner':'/synthetic/owner','seal':{'owner':'/synthetic/owner'}}}}
    report={'run':'adr0153-parents-'+'c'*12,'binding_sha256':policy.digest(binding),'status':'RECOVERY_REQUIRED','pass':False,
            'comparison_attempted':True,'arms':{},'staging':staging}
    scope={'binding':binding,'worker_run':report['run'],'active_run':report['run'],'worker_result_sha256':policy.digest(report),
           'paid_runs_started':0,'paid_protocols_started':0,'qualification_protocols_started':0,'safety_protocols_started':0}
    state={'current_run':report['run'],'ledger':scope}
    entry={'ledger':'ledger','profile':'worker_separation','status':'RECOVERY_REQUIRED','binding_sha256':policy.digest(binding),
           'result_sha256':policy.digest(report),'actual_elapsed_seconds':35.0,'paid_runs_started':0,'reserved_seconds':3600}
    return state,{'experiments':[entry]},report,package,config


def test_exact_failed_staging_is_eligible(case):
    state,journal,report,package,config=case
    assert recovery.validate(state,journal,'ledger',report,package,config)==state['ledger']['binding']


@pytest.mark.parametrize('target,key,value',[
    ('scope','paid_runs_started',1),('scope','paid_runs_started',False),('scope','paid_protocols_started',1),
    ('scope','qualification_protocols_started',1),('scope','safety_protocols_started',1),('scope','attempted_paid_arms',['control']),
    ('scope','active_run','foreign'),('scope','worker_result_sha256','f'*64),('scope','worker_run','foreign'),
    ('state','current_run',None),('entry','status','ACTIVE'),('entry','profile','other'),('entry','result_sha256','f'*64),
    ('report','arms',{'control':{}}),('report','comparison_attempted',False),('report','status','FAILED_RESTORED'),
    ('report','pass',True),('staging','status','RECOVERY_REQUIRED'),('staging','runtime_unchanged',False),
    ('staging','service_deployments',1),('staging','customer_dispatches',1),('staging','service_deployments',False),
    ('staging','hosts',{}),('host','archive_removed',False),('host','creation_attempted',False),('host','seal',None)])
def test_uncertain_or_dispatched_failure_cannot_close(case,target,key,value):
    state,journal,report,package,config=case
    objects={'scope':state['ledger'],'state':state,'entry':journal['experiments'][0],'report':report,
             'staging':report['staging'],'host':report['staging']['hosts']['primary']}
    objects[target][key]=value
    # Keep original-result authentication intact when testing other rejected conditions.
    if target in {'report','staging','host'}:
        state['ledger']['worker_result_sha256']=policy.digest(report);journal['experiments'][0]['result_sha256']=policy.digest(report)
    with pytest.raises(ValueError):recovery.validate(state,journal,'ledger',report,package,config)


def setup_close(case,tmp_path,monkeypatch):
    state,journal,report,_package,_config=case;records={'state':copy.deepcopy(state),'journal':copy.deepcopy(journal)}
    state_path=tmp_path/'state.json';journal_path=tmp_path/'journal.json'
    monkeypatch.setattr(policy,'ROOT',tmp_path);monkeypatch.setattr(policy,'STATE',state_path);monkeypatch.setattr(policy,'JOURNAL',journal_path)
    monkeypatch.setattr(policy,'envelope',lambda:{'synthetic':True});monkeypatch.setattr(policy,'journal',lambda _:copy.deepcopy(records['journal']))
    def read(path):
        return copy.deepcopy(records['state']) if path==state_path else json.loads(Path(path).read_text())
    writes=[]
    def write(path,value):
        writes.append(path);records['state' if path==state_path else 'journal']=copy.deepcopy(value)
    monkeypatch.setattr(policy,'read',read);monkeypatch.setattr(policy,'write',write)
    folder=tmp_path/'tmp'/'adr0153-parents-aaaaaaaaaaaa';folder.mkdir(parents=True);evidence=folder/'recovery.json'
    receipt={'decision':'ADR0204','ledger':'ledger','original_result_sha256':policy.digest(report),
             'binding_sha256':policy.digest(state['ledger']['binding']),'actual_elapsed_seconds':20.0,
             'financial_cohort':'not_created','queue_counts':{**{k:0 for k in recovery.queue_checks.__globals__['QUEUE_ZERO']},'kafka_members':1,'pass':True}}
    for key in ('pass','runtime_unchanged','all_staging_owners_absent','zero_dispatch','zero_double_booking','all_queues_zero','kafka_drained','generator_idle'):receipt[key]=True
    evidence.write_text(json.dumps(receipt))
    return records,writes,evidence,receipt


def test_close_keeps_original_result_and_consumption(case,tmp_path,monkeypatch):
    records,writes,evidence,_=setup_close(case,tmp_path,monkeypatch)
    state,journal,report,package,config=case
    result=recovery.close('ledger',report,package,config,evidence)
    assert result['status']=='FAILED_RESTORED' and result['original_result_preserved']
    entry=records['journal']['experiments'][0]
    assert entry['result_sha256']==journal['experiments'][0]['result_sha256'] and entry['initial_status']=='RECOVERY_REQUIRED'
    assert entry['actual_elapsed_seconds']==55.0 and entry['initial_actual_elapsed_seconds']==35.0
    assert entry['reserved_seconds']==3600 and entry['paid_runs_started']==0
    assert records['state']['ledger']['worker_result_sha256']==state['ledger']['worker_result_sha256']
    assert records['state']['current_run'] is None and records['state']['ledger']['active_run'] is None
    assert writes==[policy.JOURNAL,policy.STATE]
    with pytest.raises(ValueError):recovery.close('ledger',report,package,config,evidence)


@pytest.mark.parametrize('key', ['pass','runtime_unchanged','all_staging_owners_absent','zero_dispatch','zero_double_booking','all_queues_zero','kafka_drained','generator_idle'])
def test_missing_recovery_proof_leaves_ownership_blocking(case,tmp_path,monkeypatch,key):
    records,writes,evidence,receipt=setup_close(case,tmp_path,monkeypatch);receipt[key]=False;evidence.write_text(json.dumps(receipt))
    with pytest.raises(ValueError):recovery.close('ledger',case[2],case[3],case[4],evidence)
    assert not writes and records['state']['current_run'] is not None


def test_journal_failure_never_clears_ownership(case,tmp_path,monkeypatch):
    records,writes,evidence,_=setup_close(case,tmp_path,monkeypatch)
    monkeypatch.setattr(policy,'write',lambda *args:(_ for _ in ()).throw(OSError('Synthetic disk full')))
    with pytest.raises(OSError):recovery.close('ledger',case[2],case[3],case[4],evidence)
    assert records['state']['current_run'] is not None and not writes


def bootstrap_case(case):
    state,journal,report,_package,_config=case
    report['staging'].update(status='STAGED_VERIFIED',pass_=True)
    report['staging'].pop('pass_');report['staging']['pass']=True
    report['staging']['hosts']['secondary']=copy.deepcopy(report['staging']['hosts']['primary'])
    report['arms']={'control':{'arm':'control','decision':'ADR0200','status':'RECOVERY_REQUIRED','pass':False,
        'customers_dispatched':False,'zero_dispatch_proven':True,'restoration_complete':False,'failures':[],
        'bootstrap_recovery':{'bootstrap_zero_dispatch':{'financial_cohort':'not_created','zero_dispatch':True}},
        'events':['install_primary','verify_original_runtime','verify_generator_idle','initial_queue_drain',
                  'stop_original_workers','verify_all_workers_absent','configure_common_infrastructure']}}
    state['ledger']['worker_result_sha256']=policy.digest(report);journal['experiments'][0]['result_sha256']=policy.digest(report)
    return case


def test_bootstrap_failure_closes_only_after_original_runtime_and_cleanup_proof(case,tmp_path,monkeypatch):
    case=bootstrap_case(case);records,_writes,evidence,receipt=setup_close(case,tmp_path,monkeypatch)
    receipt.update(decision='ADR0206',original_runtime_restored=True,private_configuration_cleaned=True,
                   restore_checks={'runtime_restored':True,'broker_volume_retained':True,'bind_files_restored':True,'secondary_empty':True})
    evidence.write_text(json.dumps(receipt))
    result=recovery.close('ledger',case[2],case[3],case[4],evidence)
    assert result['status']=='FAILED_RESTORED' and records['journal']['experiments'][0]['recovery']['decision']=='ADR0206'


@pytest.mark.parametrize('field,value',[('stage',{}),('customers_dispatched',True),('zero_dispatch_proven',False),('restoration_complete',True)])
def test_bootstrap_customer_or_unknown_stage_cannot_close(case,field,value):
    state,journal,report,package,config=bootstrap_case(case);report['arms']['control'][field]=value
    state['ledger']['worker_result_sha256']=policy.digest(report);journal['experiments'][0]['result_sha256']=policy.digest(report)
    with pytest.raises(ValueError):recovery.validate(state,journal,'ledger',report,package,config)


@pytest.mark.parametrize('field',['original_runtime_restored','private_configuration_cleaned','restore_checks'])
def test_bootstrap_missing_restore_proof_blocks_closure(case,tmp_path,monkeypatch,field):
    case=bootstrap_case(case);_records,writes,evidence,receipt=setup_close(case,tmp_path,monkeypatch)
    receipt.update(decision='ADR0206',original_runtime_restored=True,private_configuration_cleaned=True,
                   restore_checks={'runtime_restored':True,'broker_volume_retained':True,'bind_files_restored':True,'secondary_empty':True})
    receipt.pop(field);evidence.write_text(json.dumps(receipt))
    with pytest.raises(ValueError):recovery.close('ledger',case[2],case[3],case[4],evidence)
    assert not writes


def restored_case():
    import importlib.util
    spec=importlib.util.spec_from_file_location('lifecycle_recovery_fixture',ROOT/'tests/unit/test_worker_separation_lifecycle.py')
    module=importlib.util.module_from_spec(spec);spec.loader.exec_module(module)
    model,original,volumes,hashes=module.original()
    for row in original:row['Config']['Entrypoint']=None
    saved=module.snapshot.capture_runtime(model,original,volumes,hashes)
    for service in saved['model']['services'].values():service['entrypoint']=[]  # Historical sealed restore.
    observed={'primary':copy.deepcopy(original),'secondary':[]}
    for row in observed['primary']:row['Config']['Entrypoint']=[]
    return saved,original,observed,volumes,hashes


def test_authenticated_historical_null_to_empty_entrypoint_is_only_recovery_equivalence():
    saved,original,observed,volumes,hashes=restored_case();before=copy.deepcopy(observed)
    assert recovery.verify_restored_runtime(saved,original,observed,volumes,hashes)['runtime_restored']
    assert observed==before


@pytest.mark.parametrize('change',['original_command','observed_command','nonempty_entrypoint','image','user','unsealed','missing_original'])
def test_empty_entrypoint_equivalence_cannot_hide_other_runtime_changes(change):
    saved,original,observed,volumes,hashes=restored_case();row=observed['primary'][0]
    if change=='original_command':original[0]['Config']['Cmd']=['foreign']
    elif change=='observed_command':row['Config']['Cmd']=['foreign']
    elif change=='nonempty_entrypoint':row['Config']['Entrypoint']=['foreign']
    elif change=='image':row['Image']='sha256:'+'f'*64
    elif change=='user':row['Config']['User']='root'
    elif change=='unsealed':saved['model']['services'][row['Config']['Labels']['com.docker.compose.service']]['entrypoint']=None
    else:original.pop()
    with pytest.raises(ValueError):recovery.verify_restored_runtime(saved,original,observed,volumes,hashes)

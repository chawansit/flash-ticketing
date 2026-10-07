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


def test_bootstrap_automatic_restoration_with_failed_audits_still_requires_independent_receipt(case,tmp_path,monkeypatch):
    case=bootstrap_case(case);state,journal,report,_package,_config=case
    arm=report['arms']['control'];arm['restoration_complete']=True
    checks={'runtime_restored':True,'broker_volume_retained':True,'bind_files_restored':True,'secondary_empty':True}
    for name in ('restore_bootstrap_original','verify_bootstrap_restoration'):arm['bootstrap_recovery'][name]=checks.copy()
    state['ledger']['worker_result_sha256']=policy.digest(report);journal['experiments'][0]['result_sha256']=policy.digest(report)
    records,_writes,evidence,receipt=setup_close(case,tmp_path,monkeypatch)
    receipt.update(decision='ADR0206',original_runtime_restored=True,private_configuration_cleaned=True,restore_checks=checks)
    evidence.write_text(json.dumps(receipt))
    assert recovery.close('ledger',case[2],case[3],case[4],evidence)['status']=='FAILED_RESTORED'
    assert records['state']['current_run'] is None



def prepared_fixture_case(case):
    from datetime import UTC, datetime, timedelta
    from uuid import uuid4
    state,journal,report,package,config=copy.deepcopy(case)
    now=datetime.now(UTC)
    identity={'schema_version':1,'environment':'development','fixture_layout':'distributed',
              'fixture_id':str(uuid4()),'created_at':now.isoformat(),'sale_ends':(now+timedelta(hours=1)).isoformat(),
              'shows':60,'seats_per_show':300,'show_ids':[str(uuid4()) for _ in range(60)]}
    restored={'runtime_restored':True,'broker_volume_retained':True,'bind_files_restored':True,'secondary_empty':True}
    report['arms']={'control':{'status':'RECOVERY_REQUIRED','pass':False,'customers_dispatched':False,
        'restoration_complete':True,'verify_restoration':restored,'restored_idle':{'generator_idle':True},
        'restored_queues':{'dispatch_stopped':True,'all_queues_zero':True,'kafka_drained':True},
        'stage':{'customers_dispatched':False,'pre_dispatch_qualified':False,
            'duplicates':{'zero_double_booking':True},'job_cleanup':{'pass':True,'all_jobs_stopped':True,
                'dispatch_stopped':True,'journal_healthy':True,'failures':[]},
            'fixture_identity':{'decision':'ADR0185','arm':'control','fixture_identity':identity,
                'fixture_identity_sha256':policy.digest(identity)}}}}
    report['staging'].update(status='STAGED_VERIFIED',pass_=True)
    report['staging']['pass']=report['staging'].pop('pass_')
    report['staging']['hosts']['secondary']=copy.deepcopy(report['staging']['hosts']['primary'])
    state['ledger']['worker_result_sha256']=journal['experiments'][0]['result_sha256']=policy.digest(report)
    return state,journal,report,package,config


def zero_financial():
    names=('orders','fulfilled_orders','expired_orders','pending_orders','payment_attempts','pending_payment_attempts',
           'succeeded_payments','bookings','tickets','payment_callbacks','incomplete_callback_deliveries',
           'callback_delivery_attempts','callback_delivery_target','duplicate_booked_seats','multi_booking_orders',
           'unpublished_outbox','dead_letters')
    return {'counts':{**dict.fromkeys(names,0),'pass':True,'expected':0,'expected_paid':0,
               'expected_callback_deliveries_per_payment':3},'fixture_hold_rows':0,'pass':True,
        'checks':{'post_ttl_complete':True,'payments_durable':True,'ticket_relationships_valid':True}}


def test_exact_retained_zero_dispatch_fixture_scope(case):
    state,journal,report,package,config=prepared_fixture_case(case)
    assert recovery.zero_dispatch_fixture(report)
    assert recovery.validate(state,journal,'ledger',report,package,config)==state['ledger']['binding']


@pytest.mark.parametrize('change',['claimed','dispatched','dispatch_attempt','unqualified_unknown','job_unknown','fixture_hash','duplicate','restoration','queue','idle'])
def test_prepared_abort_cannot_hide_uncertain_customer_or_ownership(case,change):
    state,journal,report,package,config=prepared_fixture_case(case);arm=report['arms']['control'];stage=arm['stage']
    if change=='claimed':state['ledger']['paid_runs_started']=1
    elif change=='dispatched':stage['customers_dispatched']=True
    elif change=='dispatch_attempt':stage['dispatch_attempted']=True
    elif change=='unqualified_unknown':stage['pre_dispatch_qualified']=None
    elif change=='job_unknown':stage['job_cleanup']['failures']=['unknown']
    elif change=='fixture_hash':stage['fixture_identity']['fixture_identity_sha256']='f'*64
    elif change=='duplicate':stage['duplicates']['zero_double_booking']=False
    elif change=='restoration':arm['restoration_complete']=False
    elif change=='queue':arm['restored_queues']['all_queues_zero']=False
    else:arm['restored_idle']['generator_idle']=False
    state['ledger']['worker_result_sha256']=journal['experiments'][0]['result_sha256']=policy.digest(report)
    with pytest.raises(ValueError):recovery.validate(state,journal,'ledger',report,package,config)


@pytest.mark.parametrize('field',['fixture_identity_sha256','zero_fixture_financial_rows','private_artifacts_cleaned','fixture_financial_audit'])
def test_missing_zero_fixture_cleanup_proof_retains_block(case,tmp_path,monkeypatch,field):
    case=prepared_fixture_case(case);_records,writes,evidence,receipt=setup_close(case,tmp_path,monkeypatch)
    receipt.update(decision='ADR0213',original_runtime_restored=True,private_configuration_cleaned=True,
        restore_checks=case[2]['arms']['control']['verify_restoration'],financial_cohort='created_zero_financial_rows',
        fixture_identity_sha256=case[2]['arms']['control']['stage']['fixture_identity']['fixture_identity_sha256'],
        zero_fixture_financial_rows=True,private_artifacts_cleaned=True,fixture_financial_audit=zero_financial())
    receipt.pop(field);evidence.write_text(json.dumps(receipt))
    with pytest.raises(ValueError):recovery.close('ledger',case[2],case[3],case[4],evidence)
    assert not writes


def test_complete_zero_fixture_closure_preserves_original_failure(case,tmp_path,monkeypatch):
    case=prepared_fixture_case(case);records,_writes,evidence,receipt=setup_close(case,tmp_path,monkeypatch)
    receipt.update(decision='ADR0213',original_runtime_restored=True,private_configuration_cleaned=True,
        restore_checks=case[2]['arms']['control']['verify_restoration'],financial_cohort='created_zero_financial_rows',
        fixture_identity_sha256=case[2]['arms']['control']['stage']['fixture_identity']['fixture_identity_sha256'],
        zero_fixture_financial_rows=True,private_artifacts_cleaned=True,fixture_financial_audit=zero_financial())
    evidence.write_text(json.dumps(receipt));original=case[1]['experiments'][0]['result_sha256']
    assert recovery.close('ledger',case[2],case[3],case[4],evidence)['status']=='FAILED_RESTORED'
    assert records['journal']['experiments'][0]['result_sha256']==original
    assert records['journal']['experiments'][0]['paid_runs_started']==0


@pytest.mark.parametrize('change',['orders','payments','tickets','holds','missing','bool','ttl','expected'])
def test_zero_row_financial_audit_is_strict(change):
    value=zero_financial();assert recovery.zero_fixture_financial_audit(value)
    if change=='orders':value['counts']['orders']=1
    elif change=='payments':value['counts']['payment_attempts']=1
    elif change=='tickets':value['counts']['tickets']=1
    elif change=='holds':value['fixture_hold_rows']=1
    elif change=='missing':value['counts'].pop('payment_callbacks')
    elif change=='bool':value['counts']['orders']=False
    elif change=='ttl':value['checks']['post_ttl_complete']=False
    else:value['counts']['expected']=18000
    assert not recovery.zero_fixture_financial_audit(value)

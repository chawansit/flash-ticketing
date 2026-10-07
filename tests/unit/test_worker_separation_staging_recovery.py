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

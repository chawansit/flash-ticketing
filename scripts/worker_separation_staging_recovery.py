"""ADR0204/0206 append-only closure of independently verified zero-dispatch failures."""
import copy
import math
import time
from pathlib import Path

import work_envelope as policy
from fetch_status_refresh_parents import owner_path
from qualify_two_host_deployment import GENERATOR_IDLE
from worker_separation_audits import (
    audit_contract,
    duplicate_body,
    queue_body,
    queue_checks,
    transport_program,
)
from worker_separation_execution import observation_identity, row_identity
from worker_separation_preload import binding_for
from worker_separation_runtime import observation_program
from worker_separation_snapshot import verify_restored


def cleaned_predeployment(value):
    hosts=value.get('hosts') if isinstance(value,dict) else None
    return (isinstance(value,dict) and value.get('status')=='FAILED_CLEANED' and value.get('pass') is False
            and value.get('runtime_unchanged') is True
            and all(type(value.get(k)) is int and value[k]==0 for k in ('service_deployments','customer_dispatches'))
            and isinstance(hosts,dict) and bool(hosts) and set(hosts)<={'primary','secondary'}
            and all(isinstance(v,dict) and isinstance(v.get('owner'),str) and v['owner'].startswith('/')
                    and v.get('creation_attempted') is True and v.get('archive_removed') is True
                    and isinstance(v.get('seal'),dict) and v['seal'].get('owner')==v.get('owner') for v in hosts.values()))



def zero_dispatch_bootstrap(report):
    arms=report.get('arms')
    if not isinstance(arms,dict) or set(arms)!={'control'}:return False
    arm=arms['control']
    fields={'arm','bootstrap_recovery','customers_dispatched','decision','events','failures','pass',
            'restoration_complete','status','zero_dispatch_proven'}
    return (isinstance(arm,dict) and set(arm)==fields and arm.get('arm')=='control'
            and arm.get('status')=='RECOVERY_REQUIRED' and arm.get('pass') is False
            and arm.get('customers_dispatched') is False and arm.get('zero_dispatch_proven') is True
            and (arm.get('restoration_complete') is False or (arm.get('restoration_complete') is True
                and isinstance(arm.get('bootstrap_recovery'),dict)
                and all(arm['bootstrap_recovery'].get(name)=={'runtime_restored':True,'broker_volume_retained':True,'bind_files_restored':True,'secondary_empty':True}
                        for name in ('restore_bootstrap_original','verify_bootstrap_restoration'))))
            and isinstance(arm.get('bootstrap_recovery'),dict)
            and arm['bootstrap_recovery'].get('bootstrap_zero_dispatch')=={'financial_cohort':'not_created','zero_dispatch':True}
            and isinstance(arm.get('events'),list) and arm['events'][:7]==[
                'install_primary','verify_original_runtime','verify_generator_idle','initial_queue_drain',
                'stop_original_workers','verify_all_workers_absent','configure_common_infrastructure'])

def validate(state, journal, key, report, package, config):
    scope=state.get(key,{})
    matches=[entry for entry in journal['experiments'] if entry['ledger']==key]
    if len(matches)!=1:raise ValueError('One exact original reservation required')
    entry=matches[0];binding=scope.get('binding',{})
    counters=('paid_runs_started','paid_protocols_started','qualification_protocols_started','safety_protocols_started')
    staging=report.get('staging',{});hosts=staging.get('hosts');bootstrap=zero_dispatch_bootstrap(report)
    if (entry.get('profile')!='worker_separation' or entry.get('status')!='RECOVERY_REQUIRED'
            or entry.get('binding_sha256')!=policy.digest(binding)
            or entry.get('result_sha256')!=policy.digest(report) or scope.get('worker_result_sha256')!=policy.digest(report)
            or state.get('current_run')!=report.get('run') or scope.get('active_run')!=report.get('run')
            or scope.get('worker_run')!=report.get('run') or report.get('binding_sha256')!=policy.digest(binding)
            or report.get('status')!='RECOVERY_REQUIRED' or report.get('pass') is not False
            or report.get('comparison_attempted') is not True or (report.get('arms')!={} and not bootstrap)
            or any(type(scope.get(k)) is not int or scope[k]!=0 for k in counters)
            or scope.get('attempted_paid_arms',[])!=[]
            or staging.get('status')!=('STAGED_VERIFIED' if bootstrap else 'FAILED_CLEANED')
            or staging.get('pass') is not bootstrap
            or (bootstrap and set(hosts or {})!={'primary','secondary'})
            or staging.get('runtime_unchanged') is not True
            or any(type(staging.get(k)) is not int or staging[k]!=0 for k in ('service_deployments','customer_dispatches'))
            or not isinstance(hosts,dict) or not hosts or not set(hosts)<={'primary','secondary'}
            or any(v.get('creation_attempted') is not True or v.get('archive_removed') is not True
                   or not isinstance(v.get('seal'),dict) or v['seal'].get('owner')!=v.get('owner') for v in hosts.values())
            or any(binding.get(k)!=v for k,v in binding_for(package['inputs']).items())
            or binding.get('configuration_sha256')!=policy.digest(config)
            or binding.get('worker_saved_runtime_sha256')!=policy.digest(package['saved'])
            or binding.get('worker_audit_contract_sha256')!=policy.digest(audit_contract(package['helpers']))
            or staging.get('contract_sha256')!=binding.get('worker_staging_contract_sha256')):
        raise ValueError('Exact failed, cleaned, zero-dispatch predeployment scope required')
    return copy.deepcopy(binding)



def verify_restored_runtime(saved, original_rows, rows_by_host, volumes, bind_hashes):
    """Authenticate old hashes before one exact empty-entrypoint representation equivalence."""
    original_rows=[r for r in original_rows if r['Id'] in saved['original_containers']]
    if sorted(r['Id'] for r in original_rows)!=sorted(saved['original_containers']):
        raise ValueError('Exact original raw container identities required')
    verify_restored(saved, {'primary':original_rows, 'secondary':[]}, volumes, bind_hashes)
    expected={r['Config']['Labels']['com.docker.compose.service']:r['Config'].get('Entrypoint') for r in original_rows}
    observed=copy.deepcopy(rows_by_host)
    for row in observed.get('primary',[]):
        role=row['Config']['Labels']['com.docker.compose.service']
        if (role in expected and expected[role] is None and row['Config'].get('Entrypoint')==[]
                and saved['model']['services'][role].get('entrypoint')==[]):
            row['Config']['Entrypoint']=None
    return verify_restored(saved, observed, volumes, bind_hashes)

def verify(session, package, owners, expected_starts):
    """Existing read-only transport bodies; never stage, create, stop or deploy services."""
    saved=package['saved'];started=time.monotonic();previous=session.cleanup_mode
    session.cleanup_mode=True
    def observe():
        value={h:session.call(h,observation_program(saved,h=='primary'),30) for h in ('primary','secondary')}
        primary=value['primary']
        verify_restored(saved,{h:v['rows'] for h,v in value.items()},primary['volumes'],primary['bind_sha256'])
        if (sorted(r['Id'] for r in primary['rows'])!=saved['original_containers']
                or {r['Id']:r['State']['StartedAt'] for r in primary['rows']}!=expected_starts):
            raise ValueError('Exact original container identities required')
        return value
    try:
        before=observe()
        if session.call('generator',GENERATOR_IDLE,30)!={'generator_idle':True}:raise ValueError('Generator not idle')
        for host,owner in owners.items():
            owner_path('/qualification/repository',Path(owner).name)
            code='import json,os\nowner='+repr(owner)+"\nif os.path.lexists(owner):raise ValueError('Staging owner still exists')\nprint(json.dumps({'owner_absent':True}))\n"
            if session.call(host,code,10)!={'owner_absent':True}:raise ValueError('Unknown owner absence acknowledgement')
        apis=[r for r in before['primary']['rows'] if r['Config']['Labels']['com.docker.compose.service']=='api']
        if len(apis)!=4:raise ValueError('Four original APIs required')
        target=row_identity(min(apis,key=lambda r:r['Id']))
        duplicates=session.call('primary',transport_program(saved,before['primary'],target,duplicate_body(),60),60)
        if duplicates!={'duplicate_booked_seats':0,'zero_double_booking':True}:raise ValueError('Global duplication audit failed')
        deadline=time.monotonic()+120
        while True:
            remaining=math.floor(deadline-time.monotonic())
            if remaining<1:raise TimeoutError('Complete queue drain deadline exceeded')
            queues=session.call('primary',transport_program(saved,before['primary'],target,queue_body(package['helpers'],1),min(30,remaining)),min(30,remaining))
            if queue_checks(queues,1):break
            time.sleep(min(2,max(0,deadline-time.monotonic())))
        after=observe()
        if any(observation_identity(before[h])!=observation_identity(after[h]) for h in before):raise ValueError('Runtime changed during recovery')
        if session.call('generator',GENERATOR_IDLE,30)!={'generator_idle':True}:raise ValueError('Generator started during recovery')
        return {'decision':'ADR0204','pass':True,'runtime_unchanged':True,'all_staging_owners_absent':True,
                'zero_dispatch':True,'financial_cohort':'not_created','zero_double_booking':True,
                'all_queues_zero':True,'kafka_drained':True,'generator_idle':True,
                'queue_counts':queues,'actual_elapsed_seconds':time.monotonic()-started}
    finally:session.cleanup_mode=previous


def close(key, report, package, config, evidence):
    """Append recovery evidence first; clear blocking ownership last, never reset counters."""
    state=policy.read(policy.STATE);journal=policy.journal(policy.envelope())
    binding=validate(state,journal,key,report,package,config)
    path=Path(evidence).absolute();root=(policy.ROOT/'tmp').resolve()
    if not path.is_relative_to(root) or path.is_symlink() or path.resolve()!=path:raise ValueError('Owned durable recovery evidence required')
    receipt=policy.read(path)
    required=('pass','runtime_unchanged','all_staging_owners_absent','zero_dispatch','zero_double_booking',
              'all_queues_zero','kafka_drained','generator_idle')
    if (receipt.get('ledger')!=key or receipt.get('original_result_sha256')!=policy.digest(report)
            or receipt.get('binding_sha256')!=policy.digest(binding) or receipt.get('decision')!=('ADR0206' if zero_dispatch_bootstrap(report) else 'ADR0204')
            or (zero_dispatch_bootstrap(report) and (receipt.get('original_runtime_restored') is not True
                or receipt.get('private_configuration_cleaned') is not True
                or receipt.get('restore_checks')!={'runtime_restored':True,'broker_volume_retained':True,'bind_files_restored':True,'secondary_empty':True}))
            or any(receipt.get(k) is not True for k in required)
            or receipt.get('financial_cohort')!='not_created' or not queue_checks(receipt.get('queue_counts'),1)
            or type(receipt.get('actual_elapsed_seconds')) not in (int,float)
            or not math.isfinite(receipt['actual_elapsed_seconds']) or receipt['actual_elapsed_seconds']<0):
        raise ValueError('Complete independently verified recovery receipt required')
    entry=next(r for r in journal['experiments'] if r['ledger']==key)
    entry['initial_status']=entry['status'];entry['initial_actual_elapsed_seconds']=entry['actual_elapsed_seconds']
    entry['recovery']={'decision':receipt['decision'],'evidence':str(path.relative_to(policy.ROOT)),
                       'sha256':policy.digest(receipt),'actual_elapsed_seconds':receipt['actual_elapsed_seconds']}
    entry['actual_elapsed_seconds']+=receipt['actual_elapsed_seconds'];entry['status']='FAILED_RESTORED'
    policy.write(policy.JOURNAL,journal)
    scope=state[key];scope['worker_recovery']=copy.deepcopy(entry['recovery']);scope['active_run']=None
    state['current_run']=None
    policy.write(policy.STATE,state)
    return {'ledger':key,'status':'FAILED_RESTORED','original_result_preserved':True,'paid_runs_started':0,
            'recovery_elapsed_seconds':receipt['actual_elapsed_seconds']}

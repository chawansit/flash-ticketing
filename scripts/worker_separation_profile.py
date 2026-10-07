"""ADR0201 worker entry point and independent accounting; registered execution only."""
import copy
import hashlib
import json
import os
import stat
import time
from pathlib import Path
from types import SimpleNamespace

import work_envelope as policy
from diagnostic_runner_connection import ProtectedContext, validate_target
from fetch_status_refresh_parents import owner_path
from qualify_two_host_deployment import Session
from run_status_refresh_comparison import RunLock
from run_two_host_paid_comparison import frozen_bundle, validate_config
from runtime_source_identity import source_identity_program
from stage_status_refresh_images import new_stage_output
from two_host_topology import NORMAL_COUNTS
from worker_separation_audits import audit_contract
from worker_separation_observer_bundle import prepare as observers
from worker_separation_paid_stage import stage_sources
from worker_separation_preload import binding_for
from worker_separation_readiness import dependency_context
from worker_separation_recovery import FINANCIAL, RESTORED, checks
from worker_separation_runner import WorkerComparison
from worker_separation_staging import _receipt, validate_archive
from worker_separation_staging_recovery import cleaned_predeployment
from worker_separation_topology import WORKERS

PROFILE='worker_separation'
ENTRY_FILES=('worker_separation_profile.py','work_envelope.py','run_work_envelope.py',
             'worker_separation_retained.py','worker_separation_snapshot.py','worker_separation_runtime.py',
             'worker_separation_staging.py','worker_separation_runner.py','worker_separation_topology.py',
             'worker_separation_execution.py','worker_separation_readiness.py','worker_separation_staging_recovery.py',
             'two_host_topology.py','worker_separation_readiness.py')


def entry_identity():
    return policy.digest({name:hashlib.sha256((policy.ROOT/'scripts'/name).read_bytes().replace(b'\r\n',b'\n')).hexdigest()
                          for name in ENTRY_FILES})


def plan():
    return {'decision':'ADR0201','arms':['control','candidate'],
            'factor':'worker_placement_primary_to_secondary',
            'common':{'buyer_journeys_per_second':60,'duration_seconds':300},
            'api_replicas':4,'workers':dict(WORKERS),
            'allowance':{'qualification_runs_authorized':0,'paid_runs_authorized':2,'safety_tickets_authorized':0}}


def bounded_text(path, limit):
    path=Path(path).absolute()
    before=path.stat(follow_symlinks=False)
    if not stat.S_ISREG(before.st_mode) or not 0<before.st_size<=limit:
        raise ValueError('Bounded regular protected input required')
    with path.open('rb') as stream:
        opened=os.fstat(stream.fileno());value=stream.read(limit+1);after=os.fstat(stream.fileno())
    current=path.stat(follow_symlinks=False)
    # Reuse ADR0199 portable identity semantics; read access-time updates are expected.
    identity=lambda s:(s.st_dev,s.st_ino,s.st_size,s.st_mtime_ns)
    if (any(identity(v)!=identity(before) for v in (opened,after,current))
            or before.st_ctime_ns!=current.st_ctime_ns or opened.st_ctime_ns!=after.st_ctime_ns
            or not stat.S_ISREG(current.st_mode) or len(value)!=before.st_size):
        raise ValueError('Protected input changed during read')
    return value.decode('utf-8')


class Prepared:
    """Validated private package; repr and public accounting never contain credentials."""
    def __init__(self, config, package, archive, ca_pem, target):
        if not isinstance(package,dict) or set(package)!={'schema','decision','inputs','saved','inventory_sources','helpers'} or type(package['schema']) is not int or package['schema']!=1 or package['decision']!='ADR0201':
            raise ValueError('Exact protected prepared worker package required')
        validate_config(config)
        self.config,self.package=copy.deepcopy(config),copy.deepcopy(package)
        self.used=False
        self.inputs,self.saved=self.package['inputs'],self.package['saved']
        self.archive=Path(archive).absolute();self.ca_pem=ca_pem;self.target=validate_target(target)
        self.output=new_stage_output();self.observer_sources=observers();self.frozen=frozen_bundle()
        expected=binding_for(self.inputs)
        if (self.inputs['saved_runtime_sha256']!=policy.digest(self.saved)
                or self.saved.get('decision')!='ADR0186' or self.saved.get('schema')!=1
                or self.saved.get('counts')!=NORMAL_COUNTS
                or len(set(self.saved.get('original_containers',[])))!=sum(NORMAL_COUNTS.values())
                or set(package['inventory_sources'])!={'api',*WORKERS}
                or {r:package['inventory_sources'][r] for r in WORKERS}!=self.inputs['role_sources']):
            raise ValueError('Exact original snapshot and every role source required')
        for value in package['inventory_sources'].values():source_identity_program(value)
        context=dependency_context(SimpleNamespace(pair=self.inputs['pair'],saved=self.saved),ca_pem)
        if (context['primary']!=config['primary']['private_ipv4']
                or any(self.target[k]!=context['rds'][k] for k in ('host','port','dbname'))
                or self.target['ca_sha256']!=hashlib.sha256(ca_pem.encode()).hexdigest()
                or self.saved['bind_sha256'].get(self.target['ca_source_path'])!=self.target['ca_sha256']):
            raise ValueError('Prepared dependency authority differs from configured target')
        owner=self.archive.parent
        owner_path('/qualification/repository',owner.name)
        if (self.archive.name!='images.tar' or self.archive.resolve()!=self.archive or self.archive.is_symlink()
                or owner.parent!=(policy.ROOT/'tmp').absolute()
                or any((owner/name).exists() for name in ('staging-summary.json','primary-owner-intent.json','secondary-owner-intent.json'))):
            raise ValueError('Fresh canonical owned image package required')
        for name,expected_receipt in (('contract.json',self.inputs['staging_contract']),
                                     ('source-expectations.json',self.inputs['role_sources']),
                                     ('archive.json',self.inputs['archive_receipt'])):
            if json.loads(bounded_text(owner/name,16*1024*1024))!=expected_receipt:
                raise ValueError('Durable prepared package receipt differs')
        validate_archive(self.archive,set(self.inputs['staging_contract']['role_images'].values()))
        receipt=self.inputs['archive_receipt']
        h=hashlib.sha256()
        with self.archive.open('rb') as stream:
            for block in iter(lambda:stream.read(1024*1024),b''):h.update(block)
        if self.archive.stat().st_size!=receipt['bytes'] or h.hexdigest()!=receipt['sha256']:
            raise ValueError('Archive differs from exact prepared receipt')
        self.binding={'configuration_sha256':policy.digest(config),**expected,
            'worker_dependency_context_sha256':policy.digest(context),
            'worker_inventory_sources_sha256':policy.digest(package['inventory_sources']),
            'worker_observer_manifest_sha256':self.observer_sources['manifest_sha256'],
            'worker_observer_owner_name':self.output.name,
            'worker_diagnostic_target_sha256':policy.digest(self.target),
            'worker_paid_stage_contract_sha256':policy.digest(stage_sources(self.frozen)[1]),
            'worker_audit_contract_sha256':policy.digest(audit_contract(package['helpers'])),
            'worker_audit_expectations':{'expected_orders':18000,'expected_paid':18000,'callbacks':3,'shows':60},
            'worker_entrypoint_sources_sha256':entry_identity(),
            'worker_archive_path_sha256':policy.digest(str(self.archive))}
        self.artifact_identity=self._artifacts()
        self.identity=policy.digest([self.config,self.package,self.ca_pem,self.target,self.binding,str(self.archive)])

    def _artifacts(self):
        return policy.digest([self.observer_sources,{k:hashlib.sha256(v).hexdigest() for k,v in self.frozen.items()},str(self.output)])

    def check(self):
        if (self.identity!=policy.digest([self.config,self.package,self.ca_pem,self.target,self.binding,str(self.archive)])
                or self.binding['worker_entrypoint_sources_sha256']!=entry_identity()
                or self.artifact_identity!=self._artifacts()):
            raise ValueError('Prepared entry-point binding changed')

    def components(self, context):
        self.check()
        return {'helpers':self.package['helpers'],'ca_pem':self.ca_pem,
            'inventory_sources':self.package['inventory_sources'],'observer_sources':self.observer_sources,
            'diagnostic_context':context,'frozen_bundle':self.frozen,'artifact_output':self.output}


def prepare_files(config_path, inputs_path, archive, ca_path, target_path):
    if any(v is None for v in (config_path,inputs_path,archive,ca_path,target_path)):
        raise ValueError('Protected configuration, worker package, image archive, CA and diagnostic target required')
    return Prepared(json.loads(bounded_text(config_path,1024*1024)),
                    json.loads(bounded_text(inputs_path,16*1024*1024)),archive,
                    bounded_text(ca_path,1024*1024),json.loads(bounded_text(target_path,65536)))


def flags(value, *keys):
    return isinstance(value,dict) and all(value.get(k) is True for k in keys)


def outcome(report, scope, binding):
    """Do not translate passing booleans into missing financial or restoration evidence."""
    if (not isinstance(report,dict) or report.get('binding_sha256')!=policy.digest(binding)
            or report.get('run')!=scope.get('worker_run') or type(scope.get('paid_runs_started')) is not int
            or scope.get('binding')!=binding
            or report.get('status') not in {'PASSED_RESTORED','FAILED_RESTORED'}):
        return False,False,False
    count=scope['paid_runs_started'];attempts=scope.get('attempted_paid_arms',[])
    if not 0<=count<=2 or attempts!=['control','candidate'][:count]:return False,False,False
    if report.get('comparison_attempted') is False:
        safe=(count==0 and report.get('zero_mutation') is True and report.get('arms')=={})
        return safe,safe,False
    staging=report.get('staging',{})
    if not isinstance(staging,dict):return False,False,False
    hosts=staging.get('hosts')
    failed_staging=cleaned_predeployment(staging)
    if failed_staging and (count!=0 or list(report.get('arms',{}))!=['control']):return False,False,False
    if (not failed_staging and (not isinstance(hosts,dict) or staging.get('pass') is not True or staging.get('runtime_unchanged') is not True
            or set(hosts)!={'primary','secondary'}
            or any(not flags(v,'archive_removed') for v in hosts.values()))):
        return False,False,False
    arms=report.get('arms',{})
    if not isinstance(arms,dict) or list(arms) not in [['control'],['control','candidate']]:return False,False,False
    dispatched=0
    for arm,value in arms.items():
        if not isinstance(value,dict) or value.get('status') not in {'PASSED_RESTORED','FAILED_RESTORED'} or value.get('restoration_complete') is not True or type(value.get('pass')) is not bool:
            return False,False,False
        stage=value.get('stage')
        if stage is None:
            recovery=value.get('bootstrap_recovery',{})
            if (not isinstance(recovery,dict) or value.get('zero_dispatch_proven') is not True
                    or recovery.get('bootstrap_zero_dispatch')!={'zero_dispatch':True,'financial_cohort':'not_created'}
                    or not checks(recovery.get('verify_bootstrap_restoration'),RESTORED)
                    or not checks(recovery.get('bootstrap_zero_double_booking'),{'zero_double_booking':True})
                    or not flags(recovery.get('bootstrap_restored_queues',{}),'dispatch_stopped','all_queues_zero','kafka_drained')
                    or recovery.get('bootstrap_restored_idle')!={'generator_idle':True}
                    or count!=(0 if arm=='control' else 1)):
                return False,False,False
        elif (not isinstance(stage,dict) or type(stage.get('customers_dispatched')) is not bool
                or not checks(stage.get('financial'),FINANCIAL)
                or not checks(stage.get('duplicates'),{'zero_double_booking':True})
                or not flags(stage.get('queues',{}),'dispatch_stopped','all_queues_zero','kafka_drained')
                or not flags(stage.get('job_cleanup'),'pass','all_jobs_stopped')
                or value.get('private_artifact_cleanup_complete') is not True
                or not checks(value.get('verify_restoration'),RESTORED)
                or not flags(value.get('restored_queues',{}),'dispatch_stopped','all_queues_zero','kafka_drained')
                or value.get('restored_idle')!={'generator_idle':True}):
            return False,False,False
        if isinstance(stage,dict) and stage.get('customers_dispatched') is True:dispatched+=1
    if count!=dispatched:return False,False,False
    if 'candidate' in arms and arms['control'].get('pass') is not True:return False,False,False
    passed=(report.get('pass') is True and count==2 and list(arms)==['control','candidate']
            and all(v.get('pass') is True and v.get('stage',{}).get('stage_pass') is True
                    and v['stage'].get('customers_dispatched') is True for v in arms.values()))
    return True,True,passed


def execute(prepared, ssh_password, diagnostic_password):
    if type(prepared) is not Prepared:raise TypeError('Validated worker preparation required')
    if prepared.used:raise ValueError('Single-use worker preparation cannot be replayed')
    prepared.check()
    prepared.used=True  # Any uncertain reservation/action requires a new preparation identity.
    policy.LOCK.parent.mkdir(exist_ok=True)
    # Same existing exclusive lock serializes reservation, ownership and closure.
    from uuid import uuid4
    lock=RunLock('adr0151-'+uuid4().hex[:12])
    session=None;context=None;entry=None;guard=None;started=time.monotonic()
    report={'decision':'ADR0201','run':prepared.output.name,'binding_sha256':policy.digest(prepared.binding),
            'pass':False,'status':'FAILED_RESTORED','arms':{},'comparison_attempted':False,'zero_mutation':True}
    try:
        entry=policy.reserve(prepared.binding,plan(),profile=PROFILE)
        guard=policy.ActionGuard(entry['ledger'],prepared.binding)
        state=policy.read(policy.STATE);scope=state[guard.key]
        scope.update(worker_run=prepared.output.name,active_run=prepared.output.name)
        state['current_run']=prepared.output.name;policy.write(policy.STATE,state)
        prepared.output.mkdir()
        context=ProtectedContext(prepared.target,diagnostic_password)
        guard.check(45)
        session=Session(prepared.config,prepared.output,ssh_password,action_guard=guard)
        prepared.check()
        comparison=WorkerComparison(session,guard,prepared.inputs,prepared.saved,prepared.archive,**prepared.components(context))
        report.update(comparison_attempted=True,zero_mutation=False,status='RECOVERY_REQUIRED')
        report.update(comparison.run())
    except BaseException as exc:
        if entry is None:raise
        report['failure_type']=type(exc).__name__
    finally:
        if context is not None:context.clear()
        ssh_password=diagnostic_password=None
        if session is not None:
            try:
                session.close()
                if session.state.get('ssh_close_errors'):report['status']='RECOVERY_REQUIRED'
            except BaseException as exc:  # noqa: BLE001 - never omit accounting after close failure.
                report.update(status='RECOVERY_REQUIRED',close_failure_type=type(exc).__name__)
        try:
            if entry is not None:
                report['actual_elapsed_seconds']=time.monotonic()-started
                try:_receipt(prepared.output/'worker-envelope-result.json',report)
                except BaseException as exc:  # noqa: BLE001 - missing durable evidence blocks certification.
                    report.update(status='RECOVERY_REQUIRED',receipt_failure_type=type(exc).__name__)
                state=policy.read(policy.STATE);scope=state[guard.key]
                if state.get('current_run')!=prepared.output.name or scope.get('active_run')!=prepared.output.name:
                    report['status']='RECOVERY_REQUIRED'
                restored,integrity,_passed=outcome(report,scope,prepared.binding)
                scope['worker_result_sha256']=policy.digest(report)
                if restored and integrity:
                    scope['active_run']=None;state['current_run']=None
                policy.write(policy.STATE,state)
                receipt=guard.finish([report],time.monotonic()-started)
        finally:lock.release()
    return receipt

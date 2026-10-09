"""ADR0200 concrete guarded bootstrap and comparison; ADR0201 supplies the registered entry point."""
import copy
from pathlib import Path

import work_envelope as policy
from diagnostic_runner_connection import ProtectedContext
from qualify_two_host_deployment import GENERATOR_IDLE
from stage_status_refresh_images import new_stage_output
from worker_separation_audits import connect_audits
from worker_separation_configuration import ConfigurationActions
from worker_separation_diagnostics import DiagnosticActions
from worker_separation_execution import ExecutionActions, verify_phase
from worker_separation_handover import CandidateHandover, issue
from worker_separation_inventory import collect_roles
from worker_separation_observer_bundle import validate as observer_identity
from worker_separation_paid_stage import PaidStage, stage_sources
from worker_separation_readiness import ReadinessActions
from worker_separation_recovery import RESTORED, RestoredPaidArm, checks
from worker_separation_runtime import LIMITS, ORDER
from worker_separation_snapshot import verify_restored
from worker_separation_staging import stage_package
from worker_separation_staging_recovery import cleaned_predeployment
from worker_separation_topology import WORKERS


def control_authorization(proof):
    if type(proof) is not CandidateHandover:raise TypeError('Actual passing-control handover required')
    proof._validate()
    return {'decision':'ADR0200','control_summary_sha256':proof.summary_sha,
            'handover_sha256':proof.sha256,'scope_binding_sha256':proof.control.runtime.scope_binding_sha256}


def authorize_candidate(proof):
    """Durable original-scope marker; never infer authority from a returned summary dictionary."""
    marker=control_authorization(proof)
    if proof.used:raise ValueError('Unclaimed original candidate continuation required')
    original=proof.control.runtime
    original._authorize(5)
    state=policy.read(policy.STATE);scope=state.get(original.guard.key,{})
    if (scope.get('binding')!=original.guard.binding or scope.get('paid_runs_authorized')!=2
            or type(scope.get('paid_runs_started')) is not int or scope['paid_runs_started']!=1
            or scope.get('attempted_paid_arms')!=['control']
            or 'worker_control_authorization' in scope or 'worker_control_pass' in scope):
        raise ValueError('Original consumed control and unused candidate authorization required')
    original._write('candidate-authorization-intent',marker)
    scope.update(worker_control_pass=True,worker_control_authorization=marker)
    policy.write(policy.STATE,state)
    original._write('candidate-authorization-ack',marker)
    return marker


class WorkerArm:
    """Concrete existing primitives; no customer work before verified bootstrap."""
    def __init__(self, session, guard, inputs, saved, arm, *, helpers, ca_pem, inventory_sources,
                 observer_sources, diagnostic_context, frozen_bundle, artifact_output, handover=None):
        if (not isinstance(diagnostic_context,ProtectedContext)
                or set(inventory_sources)!={'api',*WORKERS}
                or {k:inventory_sources[k] for k in WORKERS}!=inputs['role_sources']):
            raise ValueError('Protected diagnostics and explicit source expectations for every role required')
        self.sources=copy.deepcopy(inventory_sources)
        self.observer_sources=copy.deepcopy(observer_sources)
        self.frozen_bundle=copy.deepcopy(frozen_bundle)
        self.context,self.artifact_output=diagnostic_context,Path(artifact_output).absolute()
        self.configurations=ConfigurationActions(session,guard,inputs,saved,arm,new_stage_output(),handover=handover)
        self.runtime=self.configurations.runtime
        self.execution=ExecutionActions(self.configurations,drain_provider=lambda _:(_ for _ in ()).throw(ValueError('Unconnected audit provider')))
        self.audits=connect_audits(self.execution,helpers)
        self.readiness=ReadinessActions(self.execution,ca_pem)
        self.session,self.arm=session,arm
        self.used,self.journal_healthy=False,True
        self.stage,self.lifecycle=None,None
        self.configuration_attempts=[]
        self.result={'decision':'ADR0200','arm':arm,'pass':False,'status':'RECOVERY_REQUIRED',
                     'restoration_complete':False,'customers_dispatched':False,'events':[],'failures':[]}
        self._guard(5)

    def _guard(self, timeout):
        self.execution._guard(timeout)
        binding=self.runtime.guard.binding
        if (binding.get('worker_inventory_sources_sha256')!=policy.digest(self.sources)
                or binding.get('worker_observer_manifest_sha256')!=observer_identity(self.observer_sources,check_closure=False)
                or binding.get('worker_observer_owner_name')!=self.artifact_output.name
                or binding.get('worker_diagnostic_target_sha256')!=policy.digest(dict(self.context.target))
                or binding.get('worker_paid_stage_contract_sha256')!=policy.digest(stage_sources(self.frozen_bundle)[1])):
            raise ValueError('Exact scope-bound bootstrap inputs and sources required')
        self.readiness._guard(timeout)

    def _write(self, kind, value):
        try:return self.runtime._write(kind,value)
        except BaseException:
            self.journal_healthy=self.execution.journal_healthy=False
            raise

    def _forward(self, name, action, timeout):
        self.result['events'].append(name)
        self._guard(timeout)
        self._write('bootstrap-intent',{'action':name,'arm':self.arm})
        value=action()
        self._write('bootstrap-ack',{'action':name,'arm':self.arm})
        return value

    def _install(self, host):
        self.configuration_attempts.append(host)  # Unknown installation cannot be adopted on failure.
        self.configurations.install(host)
        self.configurations.verify(host)

    def _original_step(self, action):
        return self.runtime.perform({**self.runtime.binding,'action':action,'cleanup':False,'timeout_seconds':LIMITS[action]})

    def prepare(self):
        if self.used:raise ValueError('Single-use worker bootstrap; no ambiguous replay')
        self.used=True
        for host in ('primary','secondary'):
            if self.execution.pair[self.arm][host] is not None:
                self._forward('install_'+host,lambda h=host:self._install(h),45)
        for action in ORDER[:2]:
            self._forward(action,lambda a=action:self._original_step(a),LIMITS[action])
        self._forward('initial_queue_drain',self.execution._drain,120)
        for action in ORDER[2:]:
            self._forward(action,lambda a=action:self._original_step(a),LIMITS[action])
        self._forward('configure_common_infrastructure',self.execution.configure_common,150)
        self._forward('verify_private_dependencies',self.readiness.verify,90)
        self._forward('start_arm_workers',self.execution.start_workers,150)
        inventory=self._forward('verify_host_aware_inventory',
                                lambda:collect_roles(self.session,self.execution.pair,self.arm,self.sources,
                                                     retained=self.execution.saved.get("retained_inactive_containers",{})),180)
        self.runtime._write('bootstrap-inventory',inventory)
        cid=min(r['container_id'] for r in inventory['containers'] if r['role']=='api' and r['host_role']=='primary')
        diagnostic=DiagnosticActions(self.execution,inventory,self.observer_sources,self.context,cid,
                                     artifact_output=self.artifact_output)
        # Readiness and exact inventory verify all four private APIs and unchanged callback budgets.
        stage=PaidStage(self.execution,diagnostic,self.audits,self.frozen_bundle)
        lifecycle=RestoredPaidArm(stage)
        self.stage,self.lifecycle=stage,lifecycle
        return self.lifecycle

    def _restored(self):
        observed=self.execution._observe(cleanup=True);primary=observed['primary']
        verify_restored(self.execution.saved,{h:v['rows'] for h,v in observed.items()},primary['volumes'],primary['bind_sha256'])
        return observed

    def _recover_bootstrap(self, *, read_only=False):
        """No paid-stage attempt exists: retain explicit zero-dispatch evidence and owned cleanup."""
        previous=self.session.cleanup_mode;self.session.cleanup_mode=True
        results={}
        def attempt(name, action):
            self.result['events'].append(name)
            try:
                self.execution._guard(180,cleanup=True)
                try:self._write('bootstrap-recovery-intent',{'action':name})
                except BaseException as exc:  # noqa: BLE001 - known cleanup survives journal loss.
                    self.result['failures'].append({'phase':'recovery_journal','exception_type':type(exc).__name__})
                results[name]=action()
                try:self._write('bootstrap-recovery-ack',{'action':name})
                except BaseException as exc:  # noqa: BLE001 - known cleanup survives journal loss.
                    self.result['failures'].append({'phase':'recovery_journal','exception_type':type(exc).__name__})
                return True
            except BaseException as exc:  # noqa: BLE001 - independently attempt owned recovery.
                self.result['failures'].append({'phase':name,'exception_type':type(exc).__name__})
                return False
        try:
            # Independently distinguish original restoration from a known partial arm deployment.
            try:self._restored();already_original=True
            except BaseException:already_original=False  # noqa: BLE001 - unknown runtime is not restored.
            stopped=applied=True
            if not already_original and read_only:
                stopped=applied=False
            elif not already_original:
                stopped=attempt('stop_bootstrap_workers',self.execution.stop_workers)
                def absent():
                    observed=self.execution._observe(cleanup=True)
                    verify_phase(self.execution.pair,self.execution.saved,self.arm,observed,'restorable')
                    return {'arm_workers_absent':True}
                absent_ok=attempt('verify_bootstrap_worker_absence',absent)
                applied=attempt('restore_bootstrap_original',self.execution.restore) if absent_ok else False
            def restored_check():
                self._restored()
                return copy.deepcopy(RESTORED)
            restored=attempt('verify_bootstrap_restoration',restored_check)
            duplicate=attempt('bootstrap_zero_double_booking',self.audits.zero_double_booking)
            drained=attempt('bootstrap_restored_queues',lambda:self.audits.queue_drain(self.runtime.audit_binding,cleanup=True))
            def idle():
                if self.session.call('generator',GENERATOR_IDLE,30)!={'generator_idle':True}:raise ValueError('Generator not idle')
                return {'generator_idle':True}
            idle_ok=attempt('bootstrap_restored_idle',idle)
            def zero_dispatch():
                state=policy.read(policy.STATE);scope=state.get(self.runtime.guard.key,{})
                expected=[] if self.arm=='control' else ['control']
                if (self.stage is not None or scope.get('binding')!=self.runtime.guard.binding
                        or type(scope.get('paid_runs_started')) is not int or scope['paid_runs_started']!=len(expected)
                        or scope.get('attempted_paid_arms',[])!=expected):
                    raise ValueError('Unchanged exact paid allowance and no stage required')
                return {'zero_dispatch':True,'financial_cohort':'not_created'}
            no_dispatch=attempt('bootstrap_zero_dispatch',zero_dispatch)
            safe=(stopped and applied and restored and duplicate and drained and idle_ok and no_dispatch
                  and checks(results.get('verify_bootstrap_restoration'),RESTORED)
                  and checks(results.get('bootstrap_zero_double_booking'),{'zero_double_booking':True})
                  and all(results.get('bootstrap_restored_queues',{}).get(k) is True for k in ('dispatch_stopped','all_queues_zero','kafka_drained'))
                  and self.journal_healthy and self.execution.journal_healthy
                  and set(self.configuration_attempts)==set(self.configurations.seals))
            removed=True
            if safe:
                for host in self.configurations.seals:
                    removed=attempt('cleanup_bootstrap_'+host,lambda h=host:self.configurations.cleanup(h)) and removed
            self.result.update(restoration_complete=restored and checks(results.get('verify_bootstrap_restoration'),RESTORED),
                               zero_dispatch_proven=no_dispatch,bootstrap_recovery=results,
                               status='FAILED_RESTORED' if safe and removed and self.journal_healthy else 'RECOVERY_REQUIRED')
        finally:self.session.cleanup_mode=previous

    def run(self):
        if self.used:raise ValueError('Worker arm is single-use; no ambiguous replay')
        try:
            self.prepare()
            report=self.lifecycle.run()
            self.result.update(report)
            self.result['customers_dispatched']=report.get('stage',{}).get('customers_dispatched') is True
        except BaseException as exc:  # noqa: BLE001 - independently attempt owned recovery.
            self.result['failures'].append({'phase':'bootstrap_or_paid','exception_type':type(exc).__name__})
            if self.stage is None:self._recover_bootstrap()
            else:
                # Paid execution already owns recovery; never replay an ambiguous lifecycle.
                self.result['status']='RECOVERY_REQUIRED'
        try:self._write('worker-runner-arm-summary',self.result)
        except BaseException:  # noqa: BLE001 - failed receipt cannot certify the arm.
            self.result.update({'pass':False,'status':'RECOVERY_REQUIRED'})
        return copy.deepcopy(self.result)


class WorkerComparison:
    """One exact staged package, passing restored control, then a proven candidate."""
    def __init__(self, session, guard, inputs, saved, archive, **components):
        self.session,self.guard=session,guard
        self.inputs,self.saved=copy.deepcopy(inputs),copy.deepcopy(saved)
        self.archive=Path(archive).absolute();self.components=components
        self.used=False
        self.control=None;self.candidate=None
        self.result={'decision':'ADR0200','pass':False,'status':'RECOVERY_REQUIRED','arms':{},'failures':[],
                     'capacity_improvement_measured':False}

    def run(self):
        if self.used:raise ValueError('Comparison is single-use; no ambiguous replay')
        self.used=True
        try:
            # Construct connected, validated primitives before any staging mutation.
            self.control=WorkerArm(self.session,self.guard,self.inputs,self.saved,'control',**self.components)
            self.result['staging']=stage_package(self.session,self.inputs['staging_contract'],self.inputs['role_sources'],
                                                  self.archive,self.inputs['archive_receipt'],self.guard,saved=self.saved)
            if self.result['staging'].get('pass') is not True:
                if cleaned_predeployment(self.result['staging']):
                    self.control._recover_bootstrap(read_only=True)
                    self.result['arms']['control']=copy.deepcopy(self.control.result)
                    self.result['status']=self.control.result['status']
                raise ValueError('Verified unchanged staged package required')
            self.result['arms']['control']=self.control.run()
            if self.result['arms']['control']['pass'] is not True:
                self.result['status']=self.result['arms']['control']['status']
            else:
                proof=issue(self.control.lifecycle)
                authorize_candidate(proof)
                self.candidate=WorkerArm(self.session,self.guard,self.inputs,self.saved,'candidate',handover=proof,**self.components)
                self.result['arms']['candidate']=self.candidate.run()
                self.result['pass']=all(v.get('pass') is True for v in self.result['arms'].values())
                self.result['status']='PASSED_RESTORED' if self.result['pass'] else self.result['arms']['candidate']['status']
        except BaseException as exc:  # noqa: BLE001 - independently attempt owned recovery.
            self.result['failures'].append({'phase':'worker_comparison','exception_type':type(exc).__name__})
        if self.control is not None:
            try:self.control.runtime._write('worker-comparison-summary',self.result)
            except BaseException as exc:  # noqa: BLE001 - lost final evidence blocks certification.
                self.result.update({'pass':False,'status':'RECOVERY_REQUIRED'})
                self.result['failures'].append({'phase':'comparison_summary','exception_type':type(exc).__name__})
        return copy.deepcopy(self.result)

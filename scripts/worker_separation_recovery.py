"""ADR0198 single-use paid-arm restoration; no bootstrap, CLI or registration bypass."""
import copy

from fetch_status_refresh_parents import owner_path
from qualify_two_host_deployment import GENERATOR_IDLE
from worker_separation_artifacts import cleanup_program
from worker_separation_configuration import bundle, host_repository
from worker_separation_configuration import validate_seal as validate_configuration_seal
from worker_separation_execution import ExecutionActions, verify_phase
from worker_separation_snapshot import verify_restored

FINANCIAL = {'post_ttl_complete':True,'payments_durable':True,'ticket_relationships_valid':True}
RESTORED = {'runtime_restored':True,'broker_volume_retained':True,'bind_files_restored':True,'secondary_empty':True}


def checks(value, expected):
    return isinstance(value,dict) and set(value)==set(expected) and all(value[k] is True for k in expected)


def artifacts(stage, site):
    """Explicit files emitted by this fixed stage; no glob or guessed ownership."""
    if site not in stage.locations:raise ValueError('Exact artifact site required')
    if site=='generator':
        from run_two_host_paid_comparison import CORE
        from worker_separation_paid_stage import SCHEDULER
        names={*CORE,SCHEDULER,'manifest.private.json','customer.json'}
    else:
        names=set(stage.diagnostics.sources['files'])
        if site in {'primary','container'}:
            names.update({'diagnostic-ca.pem','diagnostic.private.json',
                          'diagnostic-preflight-inventory.private.json','inventory.private.json'})
        if site=='container':names.update({'fixture.json','manifest.private.json','pipeline.jsonl','kafka.jsonl'})
        else:names.update({'cpu-spec.json','cpu.json'})
    for attempt in stage.jobs.attempts:
        if attempt['site']==site:
            name=attempt['expected']['name']
            names.update({'job-'+name+'.log','job-'+name+'-identity.json','job-'+name+'-exit.json'})
    return names


class RestoredPaidArm:
    """Run an already prepared worker deployment, then independently verify restoration."""
    def __init__(self, stage):
        from worker_separation_paid_stage import PaidStage
        if (not isinstance(stage,PaidStage) or not isinstance(stage.execution,ExecutionActions)
                or stage.used or stage.jobs.cleanup_used
                or not {'configure_common_infrastructure','start_arm_workers'}<=stage.execution.used):
            raise ValueError('Original single-use paid stage and started guarded worker deployment required')
        self.stage,self.execution,self.runtime,self.session=stage,stage.execution,stage.runtime,stage.session
        self.used,self.journal_healthy=False,True
        self.summary_path = None
        self.record={'decision':'ADR0198','arm':self.execution.arm,'pass':False,'restoration_complete':False,
                     'status':'RECOVERY_REQUIRED','failures':[],'events':[]}
        stage._guard(5)

    def _journal(self, kind, value):
        try:
            path = self.runtime._write(kind,value)
            if kind == 'worker-arm-summary':self.summary_path = path
        except BaseException:  # noqa: BLE001 - journal loss cannot stop mandatory recovery.
            self.journal_healthy=self.execution.journal_healthy=False
            return False
        return True

    def _step(self, name, action):
        self.record['events'].append(name)
        self._journal('worker-arm-intent',{'action':name,'arm':self.execution.arm})
        try:
            self.execution._guard(180,cleanup=True)
            result=action()
            self.record[name]=copy.deepcopy(result)
            self._journal('worker-arm-result',{'action':name,'result':result})
            return True
        except BaseException as exc:  # noqa: BLE001 - never skip independent recovery checks.
            self.record['failures'].append({'phase':name,'exception_type':type(exc).__name__})
            return False

    def _restored(self):
        observed=self.execution._observe(cleanup=True)
        return verify_restored(self.execution.saved,{h:o['rows'] for h,o in observed.items()},
                               observed['primary']['volumes'],observed['primary']['bind_sha256'])

    def _absent(self):
        observed=self.execution._observe(cleanup=True)
        verify_phase(self.execution.pair,self.execution.saved,self.execution.arm,observed,'restorable')
        return {'arm_workers_absent':True}

    def _idle(self):
        self.execution._guard(30,cleanup=True)
        if self.session.call('generator',GENERATOR_IDLE,30)!={'generator_idle':True}:
            raise ValueError('Generator still active after restoration')
        return {'generator_idle':True}

    def _files(self):
        stage=self.stage
        if (set(stage.artifact_attempts)!=set(stage.locations)
                or len(stage.artifact_attempts)!=len(stage.locations)
                or set(stage.artifact_seals)!=set(stage.locations)):
            raise ValueError('Every attempted artifact directory requires its original ownership seal')
        expected_hosts={h for h in ('primary','secondary') if self.execution.pair[self.execution.arm][h] is not None}
        if set(self.execution.configurations.seals)!=expected_hosts:
            raise ValueError('Every configured host requires its original configuration seal')
        for host,seal in self.execution.configurations.seals.items():
            prepared=bundle(self.execution.pair,self.execution.saved,self.execution.arm,host,self.execution.scope_sha)
            validate_configuration_seal(seal,owner_path(host_repository(self.session.config, host),self.runtime.output.name),prepared['manifest'])
        # Validate every site before deleting anything; unexpected files retain evidence.
        retired=stage.diagnostics.container_retired()
        sites=[s for s in stage.locations if s!='container' or not retired]
        for site in sites:
            stage._guard(45,cleanup=True)
            code=cleanup_program(stage.locations[site],stage.artifact_seals[site],artifacts(stage,site),remove=False)
            value=(self.session.api(stage.diagnostics.cid,code,45) if site=='container'
                   else self.session.call(site,code,45))
            if value!={'owned_artifacts_verified':True}:raise ValueError('Artifact preflight failed')
        if stage.diagnostics.attempted:
            stage.diagnostics.cleanup(restored=True)
        # All independent sites are attempted, even if one acknowledgement is lost.
        failures=[]
        for site in sites:
            try:
                stage._guard(45,cleanup=True)
                code=cleanup_program(stage.locations[site],stage.artifact_seals[site],artifacts(stage,site))
                value=(self.session.api(stage.diagnostics.cid,code,45) if site=='container'
                       else self.session.call(site,code,45))
                if value!={'owned_artifacts_removed':True}:raise ValueError('Exact artifact cleanup acknowledgement required')
            except BaseException as exc:  # noqa: BLE001 - attempt every known independent artifact site.
                failures.append(type(exc).__name__)
        for host in self.execution.configurations.seals:
            try:self.execution.configurations.cleanup(host)
            except BaseException as exc:failures.append(type(exc).__name__)  # noqa: BLE001
        if failures:raise RuntimeError('Owned artifact/configuration cleanup requires recovery')
        return {'owned_private_files_removed':True,'diagnostic_container_retired':retired}

    def run(self):
        if self.used:raise ValueError('Completed or ambiguous paid lifecycle cannot be replayed')
        self.used=True
        try:
            if not self._journal('worker-arm-start',{'arm':self.execution.arm}):
                raise RuntimeError('Journal failure blocks paid dispatch')
            self.record['stage']=self.stage.run()
        except BaseException as exc:  # noqa: BLE001 - recovery is required even if the stage unexpectedly aborts.
            self.record['failures'].append({'phase':'paid_stage','exception_type':type(exc).__name__})
        previous=self.session.cleanup_mode
        self.session.cleanup_mode=True
        try:
            # Fallback only for an unattempted independent check; never replay an ambiguous action.
            if not self.stage.jobs.cleanup_used:
                self._step('fallback_stop_jobs',self.stage.jobs.stop_all)
            for key,action in (('financial',self.stage.audits.payment_durability),
                               ('duplicates',self.stage.audits.zero_double_booking)):
                if key not in self.stage.audits.used:self._step('fallback_'+key,action)
            if 'queues' not in self.record.get('stage',{}):
                self._step('fallback_queues',lambda:self.stage.audits.queue_drain(self.runtime.audit_binding,cleanup=True))
            stopped=self._step('stop_workers',self.execution.stop_workers)
            absent=self._step('verify_worker_absence',self._absent)
            applied=self._step('restore_original',self.execution.restore) if absent else False
            verified=self._step('verify_restoration',self._restored)
            drained=self._step('restored_queues',lambda:self.stage.audits.queue_drain(self.runtime.audit_binding,cleanup=True))
            idle=self._step('restored_idle',self._idle)
            stage=self.record.get('stage',{})
            financial=checks(stage.get('financial',self.record.get('fallback_financial')),FINANCIAL)
            duplicates=checks(stage.get('duplicates',self.record.get('fallback_duplicates')),{'zero_double_booking'})
            queues=stage.get('queues',self.record.get('fallback_queues',{}))
            jobs=stage.get('job_cleanup',self.record.get('fallback_stop_jobs',{}))
            safe=(financial and duplicates and all(queues.get(k) is True for k in ('dispatch_stopped','all_queues_zero','kafka_drained'))
                  and jobs.get('pass') is True and jobs.get('all_jobs_stopped') is True
                  and stopped and absent and applied and verified and checks(self.record.get('verify_restoration'),RESTORED)
                  and drained and all(self.record.get('restored_queues',{}).get(k) is True for k in ('dispatch_stopped','all_queues_zero','kafka_drained'))
                  and idle and self.record.get('restored_idle')=={'generator_idle':True}
                  and self.journal_healthy and self.stage.journal_healthy and self.execution.journal_healthy)
            files=self._step('cleanup_files',self._files) if safe else False
            self.record['restoration_complete']=verified and checks(self.record.get('verify_restoration'),RESTORED)
            complete=safe and files and self.journal_healthy and self.execution.journal_healthy
            self.record['pass']=complete and stage.get('stage_pass') is True
            self.record['status']=('PASSED_RESTORED' if self.record['pass'] else 'FAILED_RESTORED') if complete else 'RECOVERY_REQUIRED'
            self.record['private_artifact_cleanup_complete']=files
            self.record['private_artifact_retention_status']='removed_verified' if files else 'retained_or_cleanup_unverified'
        finally:self.session.cleanup_mode=previous
        self.record['journal_healthy']=self.journal_healthy and self.execution.journal_healthy
        if not self._journal('worker-arm-summary',self.record):
            self.record.update({'pass':False,'status':'RECOVERY_REQUIRED','journal_healthy':False})
        return copy.deepcopy(self.record)

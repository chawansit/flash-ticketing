"""ADR0198 fixed paid-stage adapter; no CLI, registration or lifecycle bypass."""
import copy
import hashlib
import ipaddress
import json
import math
import time
from datetime import UTC, datetime

import work_envelope as policy
from fetch_status_refresh_parents import owner_path
from fixture_identity_evidence import retain_fixture_identity
from observe_paid_pipeline import kafka_startup_view
from observe_two_host_cpu import compare_windows
from observe_two_host_cpu import summarize as summarize_cpu
from observe_worker_pipeline import startup
from observe_worker_pipeline import summarize as summarize_worker
from run_two_host_paid_comparison import CORE, PLAN, REVISION, kafka_pass
from stage_status_refresh_images import ROOT
from summarize_paid_kafka_lag import summarize as summarize_kafka
from worker_separation_artifacts import directories_program, validate_seal
from worker_separation_inventory import cpu_spec
from worker_separation_paid_jobs import PaidJobs

FIXED = {'rate':60,'seconds':300,'expected_tickets':18000,'shows':60,'seats_per_show':300,
         'completion_seconds':420,'concurrency':500,'http_clients':8,'poll_seconds':1,
         'payment_start_duplicates':1,'callback_deliveries':3,'observer_seconds':480,'start_delay_seconds':30}
IMAGE_HELPERS = ('observe_paid_pipeline.py','kafka_lag_observe.py','prepare_capacity_fixture.py')
SCHEDULER = 'run_synchronized_paid_generator.py'
ADAPTER_FILES = ('worker_separation_paid_stage.py','worker_separation_paid_jobs.py',
                 'worker_separation_artifacts.py','worker_separation_recovery.py',
                 'worker_separation_handover.py','worker_separation_runtime.py',
                 'worker_separation_configuration.py','worker_separation_execution.py',
                 'worker_separation_runner.py','worker_separation_readiness.py',
                 'worker_separation_inventory.py','worker_separation_audits.py',
                 'worker_separation_diagnostics.py','worker_separation_snapshot.py',
                 'worker_separation_preload.py','worker_separation_staging.py',
                 'worker_separation_topology.py','worker_separation_observer_bundle.py')


def adapter_identity():
    return {name:hashlib.sha256((ROOT/'scripts'/name).read_bytes().replace(b'\r\n',b'\n')).hexdigest()
            for name in ADAPTER_FILES}


def stage_sources(bundle):
    """Verify the entire existing frozen manifest, not a hand-picked passing subset."""
    expected = policy.read(PLAN)['harness_source_sha256']
    if len(expected)!=78 or set(bundle)!=set(expected):raise ValueError('Exact frozen 78-file bundle required')
    for name,source in bundle.items():
        if not isinstance(source,bytes) or hashlib.sha256(source.replace(b'\r\n',b'\n')).hexdigest()!=expected[name]:
            raise ValueError('Frozen workload source differs')
    names = (*CORE,*IMAGE_HELPERS)
    files = {name:bundle['scripts/'+name].replace(b'\r\n',b'\n').decode() for name in names}
    files[SCHEDULER] = (ROOT/'scripts'/SCHEDULER).read_bytes().replace(b'\r\n',b'\n').decode()
    contract = {'decision':'ADR0198','frozen_revision':REVISION,'frozen_manifest_sha256':policy.digest(expected),
                'fixed':copy.deepcopy(FIXED),'adapter_sources':adapter_identity(),'files':{name:hashlib.sha256(source.encode()).hexdigest()
                                                     for name,source in sorted(files.items())}}
    return files,contract


def verify_files_program(directory, sources):
    expected = {name:hashlib.sha256(content.encode()).hexdigest() for name,content in sources.items()}
    return ("import hashlib,json,os,stat;from pathlib import Path\np=Path("+repr(directory)+")\n"
            "assert p.is_dir() and not p.is_symlink() and stat.S_IMODE(p.stat().st_mode)==0o700 and p.stat().st_uid==os.geteuid()\n"
            "expected="+repr(expected)+"\n"
            "for name,wanted in expected.items():\n"
            " fd=os.open(p/name,os.O_RDONLY|os.O_NOFOLLOW|os.O_NONBLOCK)\n"
            " with os.fdopen(fd,'rb') as stream:\n"
            "  s=os.fstat(stream.fileno())\n"
            "  assert stat.S_ISREG(s.st_mode) and stat.S_IMODE(s.st_mode)==0o600 and s.st_uid==os.geteuid() and s.st_nlink==1 and s.st_size<=1048576\n"
            "  raw=stream.read(1048577)\n"
            "  assert len(raw)==s.st_size and hashlib.sha256(raw).hexdigest()==wanted\n"
            "print(json.dumps({'transferred_sources_verified':True,'files':len(expected)}))")


def mint_program(directory, origin, fixture):
    """Tokens stay in protected files; never transfer or print the signing key."""
    return (r'''import json,os,jwt,time
from pathlib import Path
from uuid import uuid4
from datetime import UTC,datetime
p=Path(DIRECTORY)
fixture=json.loads((p/'fixture.json').read_text())
assert fixture==EXPECTED_FIXTURE,'Retained fixture changed before token creation'
identity=str(uuid4());expiry=time.time()+3600
manifest={'schema_version':1,'environment':'development','id':identity,'origin':ORIGIN,
'expires_at':datetime.fromtimestamp(expiry,UTC).isoformat(),'show_ids':fixture['show_ids'],
'viewer_tokens':[jwt.encode({'sub':'load-'+identity+'-'+str(i),'aud':'ticketing','iss':'ticketing','exp':expiry},os.environ['JWT_SECRET'],algorithm='HS256') for i in range(18000)],
'seat_offset':0,'seats_per_show':300,'fixture_layout':'distributed'}
with (p/'manifest.private.json').open('x') as stream:
 os.chmod(p/'manifest.private.json',0o600);stream.write(json.dumps(manifest));stream.flush();os.fsync(stream.fileno())
print(json.dumps({'viewers':18000,'shows':len(manifest['show_ids'])}))
'''.replace('DIRECTORY',repr(directory)).replace('ORIGIN',repr(origin)).replace('EXPECTED_FIXTURE',repr(fixture)))


def generator_arguments(directory, origin, start):
    if type(start) not in {float,int} or not math.isfinite(start):raise ValueError('Finite offered start required')
    return ['/root/http-load-venv/bin/python',directory+'/'+SCHEDULER,
            '--frozen-generator',directory+'/paid_ticket_sharded_generator.py','--start-at-epoch',str(start),
            '--manifest',directory+'/manifest.private.json','--origin',origin,'--output',directory+'/customer.json',
            '--rate','60','--seconds','300','--completion-deadline-seconds','420','--concurrency','500',
            '--http-client-count','8','--poll-seconds','1','--duplicates','1','--lifecycle-diagnostics']


class PaidStage:
    """Single-use actual transport adapter; outer lifecycle owns restoration and file retention."""
    def __init__(self, execution, diagnostics, audits, bundle, *, clock=time.monotonic, wall=time.time, sleep=time.sleep):
        if diagnostics.execution is not execution or audits.execution is not execution:
            raise ValueError('Original connected diagnostic/audit execution required')
        self.execution,self.diagnostics,self.audits = execution,diagnostics,audits
        self.runtime,self.session = execution.runtime,execution.session
        self.sources,self.contract = stage_sources(bundle)
        self.clock,self.wall,self.sleep = clock,wall,sleep
        self.locations = {'container':diagnostics.directory, **{host:owner_path(self.session.config[host]['repo'],
                          diagnostics.artifact_owner_name)+'/'+execution.arm for host in ('primary','secondary','generator')}}
        self.jobs = PaidJobs(execution,diagnostics,self.locations,clock=clock,sleep=sleep)
        self.local = self.runtime.output/execution.arm
        self.used,self.prepared,self.journal_healthy = False,False,True
        self.record = {'decision':'ADR0198','arm':execution.arm,'customers_dispatched':False,'pass':False,
                       'pre_dispatch_qualified':False,'restoration_complete':False,'failures':[]}
        self.inventory = None
        self.artifact_seals,self.artifact_attempts = {},[]
        self.artifact_seals_sha256 = None
        self.inventory_sha256,self.fixture_receipt_bytes = None,None
        self._guard(5)

    def _guard(self, timeout, *, cleanup=False):
        self.diagnostics._guard(timeout,cleanup=cleanup)
        if not cleanup and (not self.journal_healthy or not self.execution.journal_healthy):
            raise ValueError('Failed journal blocks new paid actions')
        if not cleanup and adapter_identity()!=self.contract['adapter_sources']:
            raise ValueError('Paid adapter source changed after scope binding')
        if self.artifact_seals_sha256 is not None and policy.digest(self.artifact_seals)!=self.artifact_seals_sha256:
            raise ValueError('Original artifact ownership seals changed')
        if self.inventory_sha256 is not None and policy.digest(self.inventory)!=self.inventory_sha256:
            raise ValueError('Qualified inventory changed')
        if self.fixture_receipt_bytes is not None and (self.local/'fixture-identity.json').read_bytes()!=self.fixture_receipt_bytes:
            raise ValueError('Retained fixture changed')
        if (self.runtime.guard.binding.get('worker_paid_stage_contract_sha256')!=policy.digest(self.contract)
                or self.contract['fixed']!=FIXED
                or self.runtime.guard.binding.get('worker_audit_expectations')!={
                    'expected_orders':18000,'expected_paid':18000,'callbacks':3,'shows':60}
                or {name:hashlib.sha256(source.encode()).hexdigest() for name,source in sorted(self.sources.items())}
                    !=self.contract['files']):
            raise ValueError('Exact original frozen paid stage and financial expectations required')

    def _api(self, code, timeout, *, cleanup=False):
        self._guard(timeout,cleanup=cleanup)
        observed=self.execution._observe(cleanup=cleanup)
        self.diagnostics._stable(observed,observed)
        return self.session.api(self.diagnostics.cid,code,timeout)

    def _call(self, host, code, timeout, *, cleanup=False):
        self._guard(timeout,cleanup=cleanup)
        return self.session.call(host,code,timeout)

    def _write(self, kind, value, *, cleanup=False):
        try:self.runtime._write(kind,value)
        except BaseException:
            self.journal_healthy=self.execution.journal_healthy=False
            if not cleanup:raise

    def _read(self, site, name, limit, *, cleanup=False):
        """Bounded protected read without anonymous host temporary files."""
        if '/' in name or limit>16777216:raise ValueError('Bounded owned artifact name required')
        code=("import json,os,stat;from pathlib import Path\np=Path("+repr(self.locations[site]+'/'+name)+")\n"
              "fd=os.open(p,os.O_RDONLY|os.O_NOFOLLOW|os.O_NONBLOCK)\n"
              "with os.fdopen(fd,'rb') as stream:\n"
              " s=os.fstat(stream.fileno())\n"
              " assert stat.S_ISREG(s.st_mode) and s.st_uid==os.geteuid() and s.st_nlink==1 and s.st_size<="+str(limit)+"\n"
              " raw=stream.read("+str(limit+1)+")\n assert len(raw)==s.st_size\n"
              "print(json.dumps({'text':raw.decode()}))")
        value=(self._api(code,45,cleanup=cleanup) if site=='container'
               else self._call(site,code,45,cleanup=cleanup))
        if not isinstance(value,dict) or set(value)!={'text'} or not isinstance(value['text'],str):
            raise ValueError('Exact owned artifact response required')
        return value['text']

    def prepare(self):
        if self.used or self.prepared:raise ValueError('Paid preparation cannot be replayed')
        self.used=True
        self._guard(115)
        self.local.mkdir(mode=0o700)
        for site in ('primary','secondary','generator','container'):
            root=self.locations[site].rsplit('/',1)[0]
            code=directories_program(root,self.execution.arm)
            self._write('paid-artifact-intent',{'site':site,'directory':self.locations[site]})
            self.artifact_attempts.append(site)
            result=(self._api(code,45) if site=='container' else self._call(site,code,45))
            self.artifact_seals[site]=validate_seal(result,self.locations[site])
            self.artifact_seals_sha256=policy.digest(self.artifact_seals)
            self._write('paid-artifact-seal',{'site':site,'seal':self.artifact_seals[site]})
        expected={name:self.contract['files'][name] for name in IMAGE_HELPERS}
        code=("import hashlib,json;from pathlib import Path;expected="+repr(expected)+
              ";actual={name:hashlib.sha256((Path('/app/scripts')/name).read_bytes().replace(b'\\r\\n',b'\\n')).hexdigest() for name in expected};assert actual==expected;print(json.dumps({'frozen_helpers_verified':True}))")
        if self._api(code,45)!={'frozen_helpers_verified':True}:raise ValueError('Frozen image helpers differ')
        diagnostic=self.diagnostics.prepare()
        self.inventory=copy.deepcopy(diagnostic['inventory'])
        self.inventory_sha256=policy.digest(self.inventory)
        self.record['diagnostic_receipt']=diagnostic['receipt']
        directory=self.locations['container']
        fixture_code=("import json,os,subprocess;from pathlib import Path;os.umask(0o077);env=dict(os.environ,PYTHONDONTWRITEBYTECODE='1',TEST_DATABASE_URL=os.environ['DATABASE_URL'],TEST_REDIS_URL=os.environ['REDIS_URL']);"
                      "r=subprocess.run(['python','/app/scripts/prepare_capacity_fixture.py','--output',"+
                      repr(directory+'/fixture.json')+",'--shows','60','--seats','300','--sale-hours','1'],env=env,stdin=subprocess.DEVNULL,capture_output=True,text=True,timeout=90);r.check_returncode();print(Path("+
                      repr(directory+'/fixture.json')+").read_text())")
        self._write('paid-fixture-intent',{'arm':self.execution.arm,'shows':60,'producer_sha256':expected['prepare_capacity_fixture.py']})
        fixture=self._api(fixture_code,115)
        retain_fixture_identity(self.local,self.record,fixture,60,producer_sha256=expected['prepare_capacity_fixture.py'])
        self.audits.bind_fixture(self.local/'fixture-identity.json',18000,18000,3)
        self.fixture_receipt_bytes=(self.local/'fixture-identity.json').read_bytes()
        sale_end=datetime.fromisoformat(self.record['fixture_identity']['fixture_identity']['sale_ends']).timestamp()
        if sale_end-self.wall()<FIXED['start_delay_seconds']+FIXED['completion_seconds']+180:
            raise ValueError('Sale window does not cover completion and financial audit')
        address=str(ipaddress.IPv4Address(self.session.config['primary']['private_ipv4']))
        if not ipaddress.IPv4Address(address).is_private:raise ValueError('Private primary ingress required')
        self.origin='http://'+address+':8000'
        if self._api(mint_program(directory,self.origin,fixture),45)!={'viewers':18000,'shows':60}:
            raise ValueError('Fixed token allocation differs')
        manifest=self._read('container','manifest.private.json',16777216)
        decoded=json.loads(manifest)
        expires=datetime.fromisoformat(decoded['expires_at'])
        if (decoded.get('schema_version')!=1 or decoded.get('environment')!='development'
                or decoded.get('origin')!=self.origin or decoded.get('show_ids')!=fixture['show_ids']
                or decoded.get('fixture_layout')!='distributed' or decoded.get('seat_offset')!=0
                or decoded.get('seats_per_show')!=300 or expires.tzinfo is None
                or expires.timestamp()-self.wall()<630
                or not isinstance(decoded.get('viewer_tokens'),list) or len(decoded['viewer_tokens'])!=18000
                or any(not isinstance(t,str) or not t for t in decoded['viewer_tokens'])):
            raise ValueError('Protected manifest differs from retained fixture and workload')
        self._guard(180)
        self.session.put('generator',self.locations['generator']+'/manifest.private.json',manifest,True)
        if self._call('generator',verify_files_program(self.locations['generator'],{'manifest.private.json':manifest}),45)!={'transferred_sources_verified':True,'files':1}:
            raise ValueError('Transferred protected manifest differs')
        self.diagnostics._upload('inventory.private.json',json.dumps(self.inventory))
        for site in ('primary','secondary','generator'):
            sources=({name:self.sources[name] for name in (*CORE,SCHEDULER)} if site=='generator'
                     else self.diagnostics.sources['files'])
            # Diagnostic upload already wrote the primary host's sealed sources.
            for name,content in (sources.items() if site!='primary' else ()):
                self._guard(180);self.session.put(site,self.locations[site]+'/'+name,content,True)
            proof=self._call(site,verify_files_program(self.locations[site],sources),45)
            if proof!={'transferred_sources_verified':True,'files':len(sources)}:
                raise ValueError('Transferred helper sources differ')
        self.record.update(transferred_generator_identity=True,frozen_helpers_verified=True,pinned_harness_files=78)
        self.prepared=True
        self._write('paid-prepared',{'arm':self.execution.arm,'inventory_sha256':policy.digest(self.inventory),
                                    'fixture_identity_sha256':self.record['fixture_identity']['fixture_identity_sha256'],
                                    'contract_sha256':policy.digest(self.contract)})
        return copy.deepcopy(self.record)

    def _claim(self):
        self._guard(480)
        state=policy.read(policy.STATE)
        scope=state.get(self.runtime.guard.key,{})
        expected=[] if self.execution.arm=='control' else ['control']
        if (scope.get('binding')!=self.runtime.guard.binding or scope.get('paid_runs_authorized')!=2
                or type(scope.get('paid_runs_started')) is not int or scope['paid_runs_started']!=len(expected)
                or scope.get('attempted_paid_arms',[])!=expected
                or (self.execution.arm=='candidate' and scope.get('worker_control_pass') is not True)):
            raise ValueError('Fresh unused paid allowance and restored passing control required')
        if self.execution.arm=='candidate':
            from worker_separation_handover import CandidateHandover
            from worker_separation_runner import control_authorization
            proof=getattr(self.runtime,'handover',None)
            if (type(proof) is not CandidateHandover or not proof.used or proof.runtime is not self.runtime
                    or scope.get('worker_control_authorization')!=control_authorization(proof)):
                raise ValueError('Exact claimed passing-control handover authorization required')
        self._write('paid-dispatch-intent',{'arm':self.execution.arm,'inventory_sha256':policy.digest(self.inventory),
                                          'scheduled_offered_start_utc':self.record['scheduled_offered_start_utc'],
                                          'fixture_identity_sha256':self.record['fixture_identity']['fixture_identity_sha256']})
        scope['paid_runs_started']+=1
        scope['attempted_paid_arms']=[*expected,self.execution.arm]
        policy.write(policy.STATE,state)
        self.record['customers_dispatched']=True
        self._write('paid-allowance-consumed',{'arm':self.execution.arm,'paid_runs_started':scope['paid_runs_started']})

    def dispatch(self):
        if not self.prepared or self.record.get('dispatch_attempted'):raise ValueError('Prepared single-use dispatch required')
        self.record['dispatch_attempted']=True
        self._guard(480)
        directory=self.locations['container']
        arguments=['python',directory+'/observe_worker_pipeline.py','--inventory',directory+'/inventory.private.json',
                   '--frozen-observer','/app/scripts/observe_paid_pipeline.py','--approved-inventory-sha256',policy.digest(self.inventory),
                   '--diagnostic-connection-bundle',directory+'/diagnostic.private.json','--manifest',directory+'/manifest.private.json',
                   '--output',directory+'/pipeline.jsonl','--seconds','480']
        self.jobs.launch('container',arguments,database=True)
        self.jobs.launch('container',['python','/app/scripts/kafka_lag_observe.py','--backend','python',
                                     '--output',directory+'/kafka.jsonl','--seconds','480'],database=True)
        deadline=self.clock()+30
        while True:
            code=("import json;from pathlib import Path;p=Path("+repr(directory)+");print(json.dumps({name:json.loads((p/(name+'.jsonl')).read_text().splitlines()[0]) if (p/(name+'.jsonl')).exists() and (p/(name+'.jsonl')).stat().st_size else None for name in ('pipeline','kafka')}))")
            first=self._api(code,45)
            if first.get('pipeline') and first.get('kafka'):break
            if self.clock()>=deadline:raise TimeoutError('Worker observer startup deadline exceeded')
            self.sleep(1)
        self.record['pipeline_startup']=startup(first['pipeline'],self.inventory)
        self.record['kafka_startup']=kafka_startup_view(first['kafka'],6)
        if not self.record['pipeline_startup']['pass'] or not self.record['kafka_startup']['pass']:
            raise ValueError('Worker observer startup gates failed before dispatch')
        self.record['pre_dispatch_qualified']=True
        start=self.wall()+30
        self.record['scheduled_offered_start_utc']=datetime.fromtimestamp(start,UTC).isoformat()
        cpu_jobs={}
        for host in ('primary','secondary'):
            self._guard(180)
            self.session.put(host,self.locations[host]+'/cpu-spec.json',json.dumps(cpu_spec(self.inventory,host)),True)
            cpu_jobs[host]=self.jobs.launch(host,['python3',self.locations[host]+'/observe_two_host_cpu.py',
                '--spec',self.locations[host]+'/cpu-spec.json','--output',self.locations[host]+'/cpu.json',
                '--start-at',self.record['scheduled_offered_start_utc'],'--seconds','300'])
        if not 3<=start-self.wall()<=60:raise ValueError('Scheduled offered start expired before dispatch')
        self._claim()
        customer=self.jobs.launch('generator',generator_arguments(self.locations['generator'],self.origin,start))
        self.jobs.wait(customer,480,allowed_returncodes=(0,1))
        self.record['customer']=json.loads(self._read('generator','customer.json',16777216))
        for host,job in cpu_jobs.items():self.jobs.wait(job,365)
        self.record['cpu']={host:summarize_cpu(json.loads(self._read(host,'cpu.json',16777216))) for host in cpu_jobs}
        starts=[datetime.fromisoformat(shard['started_at_utc']) for shard in self.record['customer'].get('shards',[])]
        if (len(starts)!=2 or any(t.tzinfo is None for t in starts)
                or (max(starts)-min(starts)).total_seconds()>1 or abs(min(t.timestamp() for t in starts)-start)>2):
            raise ValueError('Actual offered window differs from matched scheduled window')
        first=min(starts)
        self.record.update(offered_start_utc=first.isoformat(),offered_end_utc=datetime.fromtimestamp(first.timestamp()+300,UTC).isoformat())
        self.record['cpu_window']=compare_windows(self.record['cpu']['primary'],self.record['cpu']['secondary'],
                                                 offered_start_utc=self.record['offered_start_utc'],offered_end_utc=self.record['offered_end_utc'])

    def run(self):
        """Always attempt job stop and independent audits; restoration remains external."""
        if self.used:raise ValueError('Completed or ambiguous paid stage cannot be replayed')
        try:
            self.prepare();self.dispatch()
        except BaseException as exc:  # noqa: BLE001 - stop/audits must survive interruption.
            self.record['failures'].append({'phase':'paid_stage','exception_type':type(exc).__name__})
        previous=self.session.cleanup_mode
        self.session.cleanup_mode=True
        try:
            try:self.record['job_cleanup']=self.jobs.stop_all()
            except BaseException as exc:  # noqa: BLE001 - all independent audits remain mandatory.
                self.record['failures'].append({'phase':'stop_jobs','exception_type':type(exc).__name__})
            for name in ('pipeline','kafka'):
                try:
                    raw=self._read('container',name+'.jsonl',16777216,cleanup=True)
                    (self.local/(name+'.jsonl')).write_text(raw,encoding='utf-8')
                    rows=[json.loads(line) for line in raw.splitlines() if line.strip()]
                    self.record[name+'_summary']=(summarize_worker(rows,self.inventory,
                        offered_start_utc=self.record['offered_start_utc'],offered_end_utc=self.record['offered_end_utc'])
                        if name=='pipeline' else summarize_kafka(rows))
                except BaseException as exc:  # noqa: BLE001 - trace failure cannot skip financial checks.
                    self.record['failures'].append({'phase':name+'_evidence','exception_type':type(exc).__name__})
            for name,action in (('financial',self.audits.payment_durability),('duplicates',self.audits.zero_double_booking),
                                ('queues',lambda:self.audits.queue_drain(self.runtime.audit_binding,cleanup=True))):
                try:self.record[name]=action()
                except BaseException as exc:  # noqa: BLE001 - never short circuit independent correctness gates.
                    self.record['failures'].append({'phase':name,'exception_type':type(exc).__name__})
        finally:self.session.cleanup_mode=previous
        customer=self.record.get('customer',{})
        self.record['stage_pass']=(not self.record['failures'] and self.record.get('job_cleanup',{}).get('pass') is True
            and customer.get('pass') is True and all(type(customer.get(k)) is int and customer[k]==18000
                for k in ('scheduled','dispatched','fulfilled_by_deadline'))
            and self.record.get('financial')=={'post_ttl_complete':True,'payments_durable':True,'ticket_relationships_valid':True}
            and self.record.get('duplicates')=={'zero_double_booking':True}
            and all(self.record.get('queues',{}).get(k) is True for k in ('dispatch_stopped','all_queues_zero','kafka_drained'))
            and self.record.get('pipeline_summary',{}).get('pass') is True
            and bool(self.record.get('kafka_summary')) and kafka_pass(self.record['kafka_summary'])
            and self.record.get('cpu_window',{}).get('both_hosts_cpu_observed') is True
            and self.record.get('cpu_window',{}).get('same_offered_measurement_window') is True
            and self.journal_healthy and self.execution.journal_healthy)
        self._write('paid-stage-summary',self.record,cleanup=True)
        if not self.journal_healthy:self.record['stage_pass']=False
        return copy.deepcopy(self.record)

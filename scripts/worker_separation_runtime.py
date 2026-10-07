"""ADR0188 guarded runtime primitives; entry-point authority is enforced by ADR0201."""
import copy
import inspect
import re
from datetime import UTC, datetime
from pathlib import Path

import work_envelope as policy
from fetch_status_refresh_parents import owner_path
from prepare_status_refresh_artifacts import owned_output
from qualify_two_host_deployment import GENERATOR_IDLE
from two_host_topology import NORMAL_COUNTS, bindings, environment, semantic
from worker_separation_preload import binding_for, check_authority
from worker_separation_snapshot import runtime_semantic, verify_restored
from worker_separation_staging import PROFILE, _receipt
from worker_separation_topology import WORKERS

LIMITS = {'verify_original_runtime':45, 'verify_generator_idle':30,
          'stop_original_workers':60, 'verify_all_workers_absent':45}
ORDER = tuple(LIMITS)
ID = re.compile(r'[0-9a-f]{64}$')


def observation_program(saved, primary):
    """Bounded all-container/broker/bind observation; private data is never printed locally."""
    if type(primary) is not bool: raise TypeError('Explicit observation host required')
    volume = saved['broker_volume']['Name'] if primary else None
    files = sorted(saved['bind_sha256']) if primary else []
    if (len(files) > 64 or any(not isinstance(p, str) or not p.startswith('/') or '..' in p.split('/') for p in files)
            or (primary and (not isinstance(volume, str) or not re.fullmatch(r'[A-Za-z0-9][A-Za-z0-9_.-]*', volume)))):
        raise ValueError('Bounded exact original volume and bind paths required')
    return "import hashlib,json,os,stat,subprocess,time\nvolume=" + repr(volume) + "\nfiles=" + repr(files) + r"""
deadline=time.monotonic()+25
def budget(limit):
 remaining=deadline-time.monotonic()
 if remaining<=0:raise TimeoutError('Runtime action deadline expired')
 return min(limit,remaining)
def observe():
 ids=subprocess.check_output(['docker','ps','-aq','--no-trunc'],text=True,timeout=budget(5)).split()
 if len(ids)>64 or len(set(ids))!=len(ids):raise ValueError('Bounded distinct inventory required')
 rows=json.loads(subprocess.check_output(['docker','inspect',*ids],text=True,timeout=budget(10))) if ids else []
 if len(rows)!=len(ids) or {r['Id'] for r in rows}!=set(ids):raise ValueError('Complete inventory required')
 volumes=json.loads(subprocess.check_output(['docker','volume','inspect',volume],text=True,timeout=budget(5))) if volume else []
 hashes={}
 for path in files:
  fd=os.open(path,os.O_RDONLY|os.O_NOFOLLOW|os.O_NONBLOCK)
  with os.fdopen(fd,'rb') as stream:
   before=os.fstat(stream.fileno())
   if not stat.S_ISREG(before.st_mode) or not 0<=before.st_size<=4194304:raise ValueError('Bounded regular bind file required')
   content=stream.read(4194305)
   after=os.fstat(stream.fileno());current=os.stat(path,follow_symlinks=False)
   if len(content)!=before.st_size or (before.st_dev,before.st_ino,before.st_size,before.st_mtime_ns,before.st_ctime_ns)!=(after.st_dev,after.st_ino,after.st_size,after.st_mtime_ns,after.st_ctime_ns) or (current.st_dev,current.st_ino)!=(before.st_dev,before.st_ino):raise ValueError('Bind file changed during observation')
   hashes[path]=hashlib.sha256(content).hexdigest()
 return {'rows':rows,'volumes':volumes,'bind_sha256':hashes}
print(json.dumps(observe()))
"""


def stop_program(saved, rows):
    """One exact-ID removal pass; recheck the whole runtime then each original worker."""
    if sorted(r['Id'] for r in rows) != saved['original_containers']:
        raise ValueError('Exact original container identities required')
    targets = [{ 'container_id':r['Id'], 'started_at':r['State']['StartedAt'],
                 'semantic_sha256':policy.digest(runtime_semantic(r)),
                 'role':r['Config']['Labels']['com.docker.compose.service']}
               for r in rows if r['Config']['Labels']['com.docker.compose.service'] in WORKERS]
    if len(targets) != 5 or any(not ID.fullmatch(t['container_id']) for t in targets):
        raise ValueError('Exactly five immutable original worker targets required')
    expected = {r['Id']:policy.digest(runtime_semantic(r)) for r in rows}
    prelude = observation_program(saved, True)
    prelude = prelude[:prelude.rindex('print(json.dumps(observe()))')].replace('time.monotonic()+25','time.monotonic()+55')
    helpers = '\n'.join(inspect.getsource(f) for f in (environment, bindings, semantic, runtime_semantic))
    code = prelude + '\n' + helpers + '\n' + inspect.getsource(policy.digest)
    code += '\nexpected=' + repr(expected) + '\ntargets=' + repr(targets)
    code += '\nstarts_expected=' + repr({r['Id']:r['State']['StartedAt'] for r in rows})
    code += '\nvolume_expected=' + repr(saved['broker_volume']) + '\nfiles_expected=' + repr(saved['bind_sha256'])
    code += r"""
observed=observe()
if observed['volumes']!=[volume_expected] or observed['bind_sha256']!=files_expected or {r['Id'] for r in observed['rows']}!=set(expected):raise ValueError('Original runtime changed before stop')
for row in observed['rows']:
 if row['State']['Running'] is not True or row['State']['StartedAt']!=starts_expected[row['Id']] or row['Config']['Labels'].get('com.docker.compose.project')!='flash-ticketing' or digest(runtime_semantic(row))!=expected[row['Id']]:raise ValueError('Original runtime semantics changed')
for target in targets:
 row=json.loads(subprocess.check_output(['docker','inspect',target['container_id']],text=True,timeout=budget(5)))[0]
 if row['Id']!=target['container_id'] or row['State']['Running'] is not True or row['State']['StartedAt']!=target['started_at'] or digest(runtime_semantic(row))!=target['semantic_sha256'] or row['Config']['Labels'].get('com.docker.compose.project')!='flash-ticketing' or row['Config']['Labels'].get('com.docker.compose.service')!=target['role']:raise ValueError('Worker identity changed before removal')
 subprocess.run(['docker','rm','-f',target['container_id']],check=True,stdout=subprocess.DEVNULL,stderr=subprocess.PIPE,timeout=budget(5))
remaining=observe()
if any(r['Id'] in {t['container_id'] for t in targets} for r in remaining['rows']):raise ValueError('Owned worker removal incomplete')
print(json.dumps({'owned_workers_stopped':True,'target_count':len(targets)}))
"""
    return code, targets


class RuntimeActions:
    """Partial transport-bound adapter; incomplete actions cannot authorize a lifecycle."""
    offline_only = False

    def __init__(self, session, guard, inputs, saved, arm, output, *, drain_provider=None, now=None, handover=None):
        if (arm not in {'control','candidate'} or not isinstance(guard,policy.ActionGuard)
                or getattr(session,'action_guard',None) is not guard or getattr(session,'cleanup_mode',False)):
            raise ValueError('Same active guarded session and exact arm required')
        self.inputs,self.saved = copy.deepcopy(inputs),copy.deepcopy(saved)
        if (saved.get('decision')!='ADR0186' or saved.get('schema')!=1 or saved.get('counts')!=NORMAL_COUNTS
                or sorted(saved.get('semantics',{}))!=sorted(NORMAL_COUNTS)
                or inputs['saved_runtime_sha256']!=policy.digest(saved)
                or len(saved['original_containers'])!=sum(NORMAL_COUNTS.values())
                or len(set(saved['original_containers']))!=len(saved['original_containers'])
                or any(not ID.fullmatch(i) for i in saved['original_containers'])):
            raise ValueError('Exact full original snapshot required')
        observation_program(saved,True)
        self.expected = binding_for(inputs)
        self.session,self.guard,self.drain_provider = session,guard,drain_provider
        self.now = now or (lambda:datetime.now(UTC))
        self.binding = {'decision':'ADR0186','arm':arm,'pair_sha256':self.expected['worker_pair_sha256'],
                        'saved_runtime_sha256':self.expected['worker_saved_runtime_sha256']}
        self.scope_binding_sha256=policy.digest(guard.binding)
        self.audit_binding={**self.expected,'scope':guard.key,'scope_binding_sha256':self.scope_binding_sha256,
                            'lifecycle_binding_sha256':policy.digest(self.binding)}
        self.used,self.failed = [],False
        self.original_starts = None
        self.handover = handover
        if handover is not None:
            from worker_separation_handover import CandidateHandover
            if type(handover) is not CandidateHandover or arm != 'candidate':
                raise ValueError('Proven candidate continuation required')
            handover.claim(self)
            self.binding['handover_sha256'] = handover.sha256
            self.audit_binding['lifecycle_binding_sha256'] = policy.digest(self.binding)
        self._authorize(5)
        output = Path(output).absolute()
        owner_path('/qualification/repository',output.name)
        if output.parent!=(policy.ROOT/'tmp').absolute() or output.resolve()!=output:
            raise ValueError('Fresh shared staging output required')
        self.output = owned_output(output)
        self.sequence = 0
        self._write('binding',{'decision':'ADR0188','binding_sha256':policy.digest(self.binding),
                               'scope_binding_sha256':policy.digest(guard.binding)})

    def _authorize(self,timeout):
        if (policy.digest(self.guard.binding)!=self.scope_binding_sha256
                or getattr(self.session,'action_guard',None) is not self.guard or getattr(self.session,'cleanup_mode',False)
                or PROFILE not in policy.PROFILES or any(self.guard.binding.get(k)!=v for k,v in self.expected.items())
                or check_authority(self.expected,self.guard.key)['status']!='PASS'):
            raise ValueError('Registered fresh exact worker scope required; recovery or pause blocks runtime actions')
        self.guard.check(timeout)
        if self.handover is not None:self.handover._validate()

    def _write(self,kind,value):
        self.sequence += 1
        path = self.output/f'{self.sequence:04d}-{kind}.json'
        _receipt(path,value)
        return path

    def _call(self,host,code,timeout):
        self._authorize(timeout)
        return self.session.call(host,code,timeout)

    def _observe(self):
        return {h:self._call(h,observation_program(self.saved,h=='primary'),30) for h in ('primary','secondary')}

    def _original(self):
        observed=self._observe()
        primary=observed['primary']
        verify_restored(self.saved,{h:o['rows'] for h,o in observed.items()},primary['volumes'],primary['bind_sha256'])
        expected_ids = self.saved['original_containers']
        if self.handover is not None:
            self.handover.verify(self, observed)
            expected_ids = self.handover.container_ids
        if sorted(r['Id'] for r in primary['rows'])!=expected_ids:
            raise ValueError('Original containers replaced before handover')
        starts={r['Id']:r['State']['StartedAt'] for r in primary['rows']}
        if self.original_starts is not None and starts!=self.original_starts:
            raise ValueError('Original container restarted before handover')
        self.original_starts=starts
        return observed

    def _idle(self):
        if self._call('generator',GENERATOR_IDLE,30)!={'generator_idle':True}:
            raise ValueError('Generator must remain idle')
        return {'generator_idle':True}

    def _drain(self):
        if not callable(self.drain_provider):raise TypeError('Live bound drain provider required before stopping workers')
        self._authorize(120)
        receipt=self.drain_provider(copy.deepcopy(self.audit_binding))
        if not isinstance(receipt,dict) or set(receipt)!={'binding_sha256','checked_at','dispatch_stopped','all_queues_zero','kafka_drained'}:
            raise ValueError('Exact drain receipt required')
        stamp=datetime.fromisoformat(receipt['checked_at'])
        if (receipt['binding_sha256']!=policy.digest(self.audit_binding) or stamp.tzinfo is None
                or not 0<=(self.now()-stamp).total_seconds()<=30
                or any(receipt[k] is not True for k in ('dispatch_stopped','all_queues_zero','kafka_drained'))):
            raise ValueError('Fresh successful bound drain receipt required')
        self._write('drain-receipt',receipt)
        return stamp

    def perform(self,request):
        name=request.get('action')
        expected={**self.binding,'action':name,'cleanup':False,'timeout_seconds':LIMITS.get(name)}
        if request!=expected or name not in LIMITS:
            raise ValueError('Unsupported or mismatched partial lifecycle action')
        if self.failed or len(self.used)>=len(ORDER) or self.used!=list(ORDER[:len(self.used)]) or name!=ORDER[len(self.used)]:
            raise ValueError('Single-use ordered runtime actions; no replay after failure')
        self.used.append(name)
        record={'action':name,'input_sha256':policy.digest(request),'binding_sha256':policy.digest(self.binding)}
        try:
            self._authorize(LIMITS[name])
            self._write('intent',record)
            if name=='verify_original_runtime':
                self._original();checks={'exact_original_runtime':True,'broker_volume_retained':True}
            elif name=='verify_generator_idle':checks=self._idle()
            elif name=='stop_original_workers':
                self._idle();observed=self._original();drained_at=self._drain()
                targets_snapshot = copy.deepcopy(self.saved)
                if self.handover is not None:
                    targets_snapshot['original_containers'] = self.handover.container_ids
                program,targets=stop_program(targets_snapshot,observed['primary']['rows'])
                self._write('stop-targets',{'targets':targets,'input_sha256':policy.digest(request)})
                if not 0<=(self.now()-drained_at).total_seconds()<=30:
                    raise ValueError('Drain receipt expired before worker stop')
                if self._call('primary',program,60)!={'owned_workers_stopped':True,'target_count':5}:
                    raise ValueError('Exact worker stop acknowledgement required')
                checks={'owned_workers_stopped':True}
            else:
                observed=self._observe()
                for host,value in observed.items():
                    if any(r['Config']['Labels'].get('com.docker.compose.service') in WORKERS for r in value['rows']):
                        raise ValueError('Workers remain, stopped or in another project')
                    if host=='secondary' and value['rows']:raise ValueError('Secondary must remain empty before handover')
                primary=observed['primary']
                rows=primary['rows']
                roles=[r['Config']['Labels'].get('com.docker.compose.service') for r in rows]
                if (len(rows)!=sum(v for k,v in NORMAL_COUNTS.items() if k not in WORKERS)
                        or any(r['Config']['Labels'].get('com.docker.compose.project')!='flash-ticketing'
                                   or r['State']['Running'] is not True
                                   or policy.digest(runtime_semantic(r))!=self.saved['semantics'].get(role)
                                   for role,r in zip(roles,rows,strict=True))
                        or any(roles.count(k)!=v for k,v in NORMAL_COUNTS.items() if k not in WORKERS)
                        or primary['volumes']!=[self.saved['broker_volume']] or primary['bind_sha256']!=self.saved['bind_sha256']):
                    raise ValueError('Infrastructure changed while proving absence')
                checks={'primary_workers_absent':True,'secondary_workers_absent':True}
            receipt={'action':name,'input_sha256':policy.digest(request),'pass':True,'checks':checks}
            self._write('ack',receipt)
            return receipt
        except BaseException:
            self.failed=True
            raise

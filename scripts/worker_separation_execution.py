"""ADR0195 execution primitives; no SSH factory, customer dispatch or registered CLI."""
import copy
import hashlib
import inspect
import json
import re
from collections import Counter
from datetime import UTC, datetime
from pathlib import PurePosixPath

import work_envelope as policy
from two_host_topology import NORMAL_COUNTS, bindings, environment, literal_model, semantic
from worker_separation_configuration import POSIX, bundle, validate_seal
from worker_separation_inventory import _port_matches
from worker_separation_preload import binding_for
from worker_separation_runtime import ORDER, observation_program
from worker_separation_snapshot import broker_records, runtime_semantic, verify_restored
from worker_separation_topology import INFRA, PROJECT, WORKERS, validate_pair

HASH = re.compile(r'[0-9a-f]{64}$')
LIMITS = {'configure_common_infrastructure':150, 'start_arm_workers':150,
          'stop_arm_workers':60, 'restore_original_runtime':180}


def row_identity(row):
    return {'id':row['Id'], 'started_at':row['State']['StartedAt'],
            'running':row['State']['Running'], 'semantic_sha256':policy.digest(runtime_semantic(row))}


def observation_identity(value):
    return {'containers':sorted((row_identity(r) for r in value['rows']), key=lambda r:r['id']),
            'volumes':value['volumes'], 'bind_sha256':value['bind_sha256']}


def planned_row(row, service, model, ports, *, stopped=False):
    """Exact executable settings, private ports, mounts and restrictive host options."""
    if (not HASH.fullmatch(row['Id']) or type(row['State']['Running']) is not bool
            or row['State']['Running'] is not True and not stopped
            or environment(row) != service['environment'] or row['Image'] != service['image']
            or row['Config']['Cmd'] != service.get('command', [])
            or row['Config'].get('Entrypoint') != service.get('entrypoint', [])
            or row['Config'].get('User', '') != service.get('user', '')
            or row['Config'].get('WorkingDir', '') != service.get('working_dir', '')):
        raise ValueError('Exact planned runtime settings required')
    stamp = datetime.fromisoformat(row['State']['StartedAt'])
    if stamp.tzinfo is None: raise ValueError('Aware runtime start identity required')
    _port_matches(row, service, ports)
    expected = sorted((m['type'], model.get('volumes', {}).get(m['source'], {}).get('name')
                       if m['type'] == 'volume' else m['source'], m['target'], not m.get('read_only', False))
                      for m in service.get('volumes', []))
    actual = sorted((m['Type'], m.get('Name') if m['Type'] == 'volume' else m['Source'], m['Destination'], m['RW'])
                    for m in row.get('Mounts', []))
    if actual != expected: raise ValueError('Exact runtime mounts required')
    for m in row.get('Mounts', []):
        if m.get('Mode', '') not in {'', 'ro', 'rw'} or m.get('Propagation', '') not in {'', 'rprivate'}:
            raise ValueError('Unsupported mount options')
    host = row['HostConfig']
    if (host.get('NetworkMode') != model['name'] + '_default'
            or any(host.get(k) for k in ('ReadonlyRootfs', 'Privileged', 'SecurityOpt', 'CapAdd', 'CapDrop',
                                        'Tmpfs', 'Memory', 'NanoCpus'))
            or host.get('PidsLimit') not in (None, 0)):
        raise ValueError('Unsupported runtime execution options')
    limits=service.get('ulimits')
    if limits not in (None,{'nofile':{'soft':65535,'hard':65535}}) or (limits is not None and row['Config']['Labels'].get('com.docker.compose.service')!='load-balancer'):
        raise ValueError('Exact existing load-balancer nofile setting required')
    expected_limits=[{'Name':'nofile','Soft':65535,'Hard':65535}] if limits else None
    observed_limits=host.get('Ulimits')
    if (expected_limits is None and observed_limits not in (None,[])) or (expected_limits is not None and observed_limits!=expected_limits):
        raise ValueError('Open-file resource limits changed')
    restart = service.get('restart', 'no').split(':')
    if (host['RestartPolicy']['Name'] != restart[0]
            or host['RestartPolicy'].get('MaximumRetryCount', 0) != (int(restart[1]) if len(restart) == 2 else 0)):
        raise ValueError('Restart policy changed')
    logging = service.get('logging', {'driver':'json-file', 'options':{}})
    if host.get('LogConfig') != {'Type':logging['driver'], 'Config':logging.get('options', {})}:
        raise ValueError('Logging policy changed')
    health = service.get('healthcheck')
    expected_health = None
    if health is not None:
        expected_health = {}
        for field, key in {'test':'Test', 'interval':'Interval', 'timeout':'Timeout', 'retries':'Retries',
                           'start_period':'StartPeriod', 'start_interval':'StartInterval'}.items():
            if field not in health: continue
            value = health[field]
            if field in {'interval', 'timeout', 'start_period', 'start_interval'}:
                if not isinstance(value, str) or not re.fullmatch(r'[0-9]+ns', value):
                    raise ValueError('Snapshot nanosecond health durations required')
                value = int(value[:-2])
            expected_health[key] = value
    if row['Config'].get('Healthcheck') != expected_health: raise ValueError('Health policy changed')


def verify_phase(pair, saved, arm, observed, phase):
    """Only canonical full/infra/partial/restore phases; no caller-supplied budgets."""
    validate_pair(pair)
    if arm not in {'control','candidate'} or set(observed) != {'primary','secondary'}:
        raise ValueError('Complete bound two-host observations required')
    if phase not in {'infrastructure','workers','partial_workers','restorable','original_infrastructure'}:
        raise ValueError('Exact lifecycle phase required')
    if observed['primary']['volumes'] != broker_records(saved) or observed['primary']['bind_sha256'] != saved['bind_sha256']:
        raise ValueError('Original broker volume and bind files must be retained')
    if observed['secondary']['volumes'] or observed['secondary']['bind_sha256']:
        raise ValueError('Secondary persistent resources are forbidden')
    seen, ports = set(), set()
    for host in ('primary','secondary'):
        counts = Counter()
        model = pair[arm][host]
        for row in observed[host]['rows']:
            labels = row['Config']['Labels']; role = labels.get('com.docker.compose.service')
            project = 'flash-ticketing' if host == 'primary' else PROJECT
            if row['Id'] in seen or labels.get('com.docker.compose.project') != project:
                raise ValueError('Foreign, duplicate or overlapping resource')
            seen.add(row['Id']); counts[role] += 1
            if phase in {'restorable','original_infrastructure'} or phase == 'partial_workers' and role in INFRA:
                if host != 'primary' or role not in INFRA: raise ValueError('Workers must be absent before restoration')
                if (row['State']['Running'] is True and HASH.fullmatch(row['Id'])
                        and policy.digest(runtime_semantic(row)) == saved['semantics'][role]):
                    continue
                if phase == 'original_infrastructure': raise ValueError('Original infrastructure changed')
            if model is None or role not in model['services']:
                raise ValueError('Resource outside selected arm')
            planned_row(row, model['services'][role], model, ports, stopped=phase in {'partial_workers','restorable'})
        if phase in {'infrastructure','original_infrastructure'}:
            required = INFRA if host == 'primary' else {}
        elif phase in {'workers','partial_workers'}:
            required = pair[arm]['counts'][host]
        else:
            required = INFRA if host == 'primary' else {}
        if phase in {'partial_workers','restorable'}:
            if any(role not in required or count > required[role] for role,count in counts.items()):
                raise ValueError('Owned partial layout exceeds fixed counts')
            # A partial worker start may omit workers; infrastructure must remain complete.
            if phase == 'partial_workers' and host == 'primary' and any(counts[r] != n for r,n in INFRA.items()):
                raise ValueError('Infrastructure missing during worker cleanup')
        elif dict(counts) != required:
            raise ValueError('Exact lifecycle role counts required')
    return True


def compose_arguments(repo, model, counts):
    path = PurePosixPath(repo)
    if (not path.is_absolute() or str(path) != repo or '..' in path.parts or len(path.parts) < 3
            or model['name'] not in {'flash-ticketing', PROJECT}
            or counts not in (INFRA, WORKERS, NORMAL_COUNTS)
            or any(type(n) is not int or n <= 0 for n in counts.values())
            or not set(counts) <= set(model['services'])):
        raise ValueError('Exact existing repository, project and roles required')
    base = ['docker','compose','--project-directory',repo,'--project-name',model['name'],'-f','-']
    up = [*base,'up','-d','--no-deps','--no-build','--pull','never']
    for role,count in sorted(counts.items()): up += ['--scale',f'{role}={count}']
    return base, [*up,*sorted(counts)]


def verify_composed(composed, model, counts):
    """Check literal identity after real Compose interpolation, before container mutation."""
    serialized = literal_model(model)  # Compose config retains dollar escaping for safe serialization.
    for role in counts:
        actual, expected = composed['services'][role], serialized['services'][role]
        if (actual['image'] != expected['image'] or actual.get('environment', {}) != expected['environment']
                or actual.get('command', []) != expected.get('command', [])
                or actual.get('entrypoint', []) != expected.get('entrypoint', [])
                or actual.get('user', '') != expected.get('user', '')
                or actual.get('working_dir', '') != expected.get('working_dir', '')):
            raise ValueError('Compose changed literal executable settings')


def program(saved, before, *, seal=None, repo=None, model=None, counts=None, filename=None, targets=None):
    """One remote action, rechecked observation and seal; stdout is private transport data."""
    if (targets is None and any(value is None for value in (seal, repo, model, counts, filename))
            or targets is not None and any(value is not None for value in (seal, repo, model, counts, filename))):
        raise ValueError('Exactly one complete application or owned-stop action required')
    prefix = observation_program(saved, seal is not None and seal['manifest']['host'] == 'primary' or
                                 (targets is not None and bool(before['volumes'])))
    prefix = prefix[:prefix.rindex('print(json.dumps(observe()))')].replace('time.monotonic()+25','time.monotonic()+135')
    helpers = '\n'.join(inspect.getsource(f) for f in (environment, bindings, semantic, runtime_semantic))
    code = prefix + '\n' + helpers + '\n' + inspect.getsource(policy.digest)
    code += '\n' + inspect.getsource(row_identity).replace('policy.digest','digest')
    code += '\n' + inspect.getsource(observation_identity)
    code += '\nexpected=' + repr(policy.digest(observation_identity(before)))
    code += "\nif digest(observation_identity(observe()))!=expected:raise ValueError('Target runtime changed before action')\n"
    if targets is not None:
        if (not isinstance(targets,list) or not targets or any(not HASH.fullmatch(t['id']) for t in targets)
                or len({t['id'] for t in targets}) != len(targets)
                or any(t not in [row_identity(r) for r in before['rows']
                                if r['Config']['Labels'].get('com.docker.compose.service') in WORKERS
                                and r['Config']['Labels'].get('com.docker.compose.project') in {'flash-ticketing', PROJECT}]
                       for t in targets)):
            raise ValueError('Exact observed immutable worker targets required')
        code = code.replace('time.monotonic()+135','time.monotonic()+55')
        code += '\ntargets=' + repr(targets) + r'''
for target in targets:
 row=json.loads(subprocess.check_output(['docker','inspect',target['id']],text=True,timeout=budget(5)))[0]
 if row_identity(row)!=target:raise ValueError('Worker replaced before exact removal')
 subprocess.run(['docker','rm','-f',target['id']],check=True,stdout=subprocess.DEVNULL,stderr=subprocess.PIPE,timeout=budget(5))
print(json.dumps({'applied':True,'after':observe()}))
'''
        return code
    validate_seal(seal, seal['owner'], seal['manifest'])
    if filename not in seal['files']: raise ValueError('Exact sealed document required')
    document = json.dumps(literal_model(model),sort_keys=True,separators=(',', ':'),allow_nan=False) + '\n'
    if seal['files'][filename] != {**{k:seal['files'][filename][k] for k in ('device','inode','uid','mode')},
                                  'size':len(document.encode()),'sha256':hashlib.sha256(document.encode()).hexdigest()}:
        raise ValueError('Complete intended model must match sealed document')
    base, up = compose_arguments(repo, model, counts)
    code += '\n' + POSIX + '\nsealed=' + repr(seal) + '\nfilename=' + repr(filename)
    code += '\nmodel=' + repr(model) + '\ncounts=' + repr(counts)
    code += '\n' + inspect.getsource(literal_model) + '\n' + inspect.getsource(verify_composed)
    code += '\nbase=' + repr(base) + '\nup=' + repr(up) + r'''
parent,name=open_parent(sealed['owner'])
try:
 if observe_configuration(parent,name,sealed['manifest'])!={'directory':sealed['directory'],'files':sealed['files']}:raise ValueError('Sealed document changed')
 fd=os.open(name,os.O_RDONLY|os.O_DIRECTORY|os.O_NOFOLLOW,dir_fd=parent)
 try:
  child=os.open(filename,os.O_RDONLY|os.O_NOFOLLOW|os.O_NONBLOCK,dir_fd=fd)
  try:
   content=b'';expected_file=sealed['files'][filename]
   while len(content)<=expected_file['size']:
    block=os.read(child,min(65536,expected_file['size']+1-len(content)))
    if not block:break
    content+=block
   if len(content)!=expected_file['size'] or hashlib.sha256(content).hexdigest()!=expected_file['sha256'] or identity(os.fstat(child))!={k:expected_file[k] for k in ('device','inode','uid','mode')}:raise ValueError('Document changed before use')
  finally:os.close(child)
 finally:os.close(fd)
finally:os.close(parent)
resolved=subprocess.run([*base,'config','--format','json'],input=content.decode(),text=True,capture_output=True,timeout=budget(20),check=False)
if resolved.returncode!=0:raise RuntimeError('Bound configuration validation failed')
verify_composed(json.loads(resolved.stdout),model,counts)
result=subprocess.run(up,input=content.decode(),text=True,stdout=subprocess.DEVNULL,stderr=subprocess.PIPE,timeout=budget(110),check=False)
if result.returncode!=0:raise RuntimeError('Owned Compose application failed')
print(json.dumps({'applied':True,'after':observe()}))
'''
    # Configuration and runtime observers share a name; keep the runtime observer intact.
    code = code.replace('def observe(parent,name,manifest):','def observe_configuration(parent,name,manifest):')
    return code


class ExecutionActions:
    """Ordered guarded primitives; full lifecycle audits and registration remain external."""
    def __init__(self, configurations, *, drain_provider, now=None):
        self.configurations, self.runtime = configurations, configurations.runtime
        self.session = configurations.session
        self.pair, self.saved = copy.deepcopy(self.runtime.inputs['pair']), copy.deepcopy(self.runtime.saved)
        self.arm = configurations.arm
        self.scope_key = self.runtime.guard.key
        self.scope_sha = self.runtime.scope_binding_sha256
        self.drain_provider, self.now = drain_provider, now or (lambda:datetime.now(UTC))
        self.used, self.failed, self.journal_healthy = set(), False, True
        self.dependency_receipt = None
        if not callable(drain_provider): raise TypeError('Bound live drain provider required')
        self._guard(5)
        # Kafka image/data identity is a hard invariant across common configuration and restoration.
        broker = self.pair[self.arm]['primary']['services']['kafka']
        mounts = broker['volumes']
        definitions=self.pair[self.arm]['primary']['volumes']
        original=self.saved['model']
        mount_identity=lambda model,values:sorted((model['volumes'][m['source']].get('name'),m['target'],m.get('read_only',False)) for m in values)
        if (mount_identity(self.pair[self.arm]['primary'],mounts)!=mount_identity(original,original['services']['kafka']['volumes'])
                or {definitions[m['source']].get('name') for m in mounts}!={r['Name'] for r in broker_records(self.saved)}
                or any(definitions[m['source']].get('external') is not True for m in mounts)
                or broker['image'] != self.saved['model']['services']['kafka']['image']):
            raise ValueError('Every original external broker volume and immutable image required')

    def _guard(self, timeout, *, cleanup=False):
        if (self.session.action_guard is not self.runtime.guard or self.runtime.guard.key != self.scope_key
                or policy.digest(self.runtime.guard.binding) != self.scope_sha
                or binding_for(self.runtime.inputs) != self.runtime.expected
                or policy.digest(self.saved) != self.runtime.expected['worker_saved_runtime_sha256']
                or policy.digest(self.pair) != self.runtime.expected['worker_pair_sha256']):
            raise ValueError('Exact original execution binding and transport required')
        if not cleanup: self.runtime._authorize(timeout)

    def _call(self, host, code, timeout, *, cleanup=False):
        self._guard(timeout, cleanup=cleanup)
        return self.session.call(host, code, timeout)

    def _observe(self, *, cleanup=False):
        return {host:self._call(host,observation_program(self.saved,host=='primary'),30,cleanup=cleanup)
                for host in ('primary','secondary')}

    def _drain(self, *, cleanup=False):
        from qualify_two_host_deployment import GENERATOR_IDLE
        if self._call('generator',GENERATOR_IDLE,30,cleanup=cleanup) != {'generator_idle':True}:
            raise ValueError('Generator must be idle before mutation')
        self._guard(120, cleanup=cleanup)
        value = self.drain_provider(copy.deepcopy(self.runtime.audit_binding))
        if (not isinstance(value,dict) or set(value) != {'binding_sha256','checked_at','dispatch_stopped','all_queues_zero','kafka_drained'}
                or value['binding_sha256'] != policy.digest(self.runtime.audit_binding)
                or any(value[k] is not True for k in ('dispatch_stopped','all_queues_zero','kafka_drained'))):
            raise ValueError('Exact successful bound drain receipt required')
        stamp = datetime.fromisoformat(value['checked_at'])
        if stamp.tzinfo is None or not 0 <= (self.now()-stamp).total_seconds() <= 30:
            raise ValueError('Fresh drain receipt required')
        try:self.runtime._write('execution-drain',value)
        except BaseException:
            self.journal_healthy=False
            if not cleanup:raise
        return stamp

    def _seal(self, host):
        value = bundle(self.pair,self.saved,self.arm,host,self.scope_sha)
        seal = self.configurations.seals[host]
        return validate_seal(seal,seal['owner'],value['manifest'])

    def _operation(self, name, operation, *, cleanup=False):
        if name in self.used or (not cleanup and (self.failed or self.configurations.failed or self.runtime.failed)):
            raise ValueError('Single-use execution; ambiguous actions cannot be replayed')
        self.used.add(name)
        previous = self.session.cleanup_mode
        if cleanup: self.session.cleanup_mode = True
        record = {'decision':'ADR0195','action':name,'cleanup':cleanup,'scope_binding_sha256':self.scope_sha}
        try:
            self._guard(LIMITS[name],cleanup=cleanup)
            try:self.runtime._write('execution-intent',record)
            except BaseException:
                self.journal_healthy=False
                if not cleanup:raise
            result = operation()
            self.runtime._write('execution-ack',record)
            return result
        except BaseException:
            self.failed=True
            raise
        finally:self.session.cleanup_mode=previous

    def _apply(self, host, before, model, counts, filename, *, cleanup=False):
        code = program(self.saved,before[host],seal=self._seal(host),repo=self.session.config[host]['repo'],
                       model=model,counts=counts,filename=filename)
        receipt = self._call(host,code, LIMITS['restore_original_runtime'] if cleanup else 150,cleanup=cleanup)
        if not isinstance(receipt,dict) or set(receipt) != {'applied','after'} or receipt['applied'] is not True:
            raise ValueError('Exact private application acknowledgement required')
        after = self._observe(cleanup=cleanup)
        if observation_identity(after[host]) != observation_identity(receipt['after']):
            raise ValueError('Runtime changed after application')
        other = 'secondary' if host == 'primary' else 'primary'
        if observation_identity(before[other]) != observation_identity(after[other]):
            raise ValueError('Other host changed during application')
        return after

    def configure_common(self):
        def action():
            if self.runtime.used != list(ORDER): raise ValueError('Original stop and absence acknowledgements required')
            before=self._observe();verify_phase(self.pair,self.saved,self.arm,before,'original_infrastructure')
            stamp=self._drain()
            if not 0 <= (self.now()-stamp).total_seconds() <= 30:raise ValueError('Drain receipt expired before application')
            after=self._apply('primary',before,self.pair[self.arm]['primary'],INFRA,'arm.compose.json')
            verify_phase(self.pair,self.saved,self.arm,after,'infrastructure')
            return {'broker_volume_retained':True,'common_config_applied':True}
        return self._operation('configure_common_infrastructure',action)

    def start_workers(self):
        def action():
            if 'configure_common_infrastructure' not in self.used:raise ValueError('Common infrastructure required')
            from worker_separation_readiness import CHECKS
            receipt = self.dependency_receipt
            if (not isinstance(receipt, dict) or set(receipt) != {'binding_sha256','checked_at','context_sha256','checks','runtime_unchanged','probes_removed'}
                    or receipt['binding_sha256'] != policy.digest(self.runtime.audit_binding)
                    or not HASH.fullmatch(receipt['context_sha256'])
                    or receipt['context_sha256'] != self.runtime.guard.binding.get('worker_dependency_context_sha256')
                    or receipt['runtime_unchanged'] is not True or receipt['probes_removed'] is not True
                    or not isinstance(receipt['checks'],dict) or set(receipt['checks']) != set(CHECKS)
                    or any(receipt['checks'][k] is not True for k in CHECKS)):
                raise ValueError('Exact complete dependency readiness required before worker start')
            ready_at = datetime.fromisoformat(receipt['checked_at'])
            if ready_at.tzinfo is None or not 0 <= (self.now()-ready_at).total_seconds() <= 30:
                raise ValueError('Fresh dependency readiness required before worker start')
            before=self._observe();verify_phase(self.pair,self.saved,self.arm,before,'infrastructure')
            stamp=self._drain()
            host='primary' if self.arm == 'control' else 'secondary'
            if not 0 <= (self.now()-stamp).total_seconds() <= 30:raise ValueError('Drain receipt expired before start')
            if not 0 <= (self.now()-ready_at).total_seconds() <= 30:
                raise ValueError('Dependency readiness expired before worker start')
            after=self._apply(host,before,self.pair[self.arm][host],WORKERS,'arm.compose.json')
            verify_phase(self.pair,self.saved,self.arm,after,'workers')
            return {'exact_arm_workers_started':True}
        return self._operation('start_arm_workers',action)

    def stop_workers(self):
        def action():
            before=self._observe(cleanup=True);verify_phase(self.pair,self.saved,self.arm,before,'partial_workers')
            self._drain(cleanup=True)
            for host in ('primary','secondary'):
                targets=[row_identity(r) for r in before[host]['rows']
                         if r['Config']['Labels']['com.docker.compose.service'] in WORKERS]
                if not targets:continue
                try:self.runtime._write('execution-stop-targets',{'host':host,'targets':targets})
                except BaseException:  # noqa: BLE001 - owned cleanup must survive local journal failure.
                    self.journal_healthy=False
                result=self._call(host,program(self.saved,before[host],targets=targets),60,cleanup=True)
                if not isinstance(result,dict) or set(result) != {'applied','after'} or result['applied'] is not True:
                    raise ValueError('Exact owned worker stop acknowledgement required')
            after=self._observe(cleanup=True);verify_phase(self.pair,self.saved,self.arm,after,'restorable')
            return {'owned_workers_stopped':True,'primary_workers_absent':True,'secondary_workers_absent':True}
        return self._operation('stop_arm_workers',action,cleanup=True)

    def restore(self):
        def action():
            before=self._observe(cleanup=True);verify_phase(self.pair,self.saved,self.arm,before,'restorable')
            stamp=self._drain(cleanup=True)
            if not 0 <= (self.now()-stamp).total_seconds() <= 30:raise ValueError('Drain receipt expired before restore')
            after=self._apply('primary',before,self.saved['model'],NORMAL_COUNTS,'restore.compose.json',cleanup=True)
            return verify_restored(self.saved,{h:o['rows'] for h,o in after.items()},after['primary']['volumes'],after['primary']['bind_sha256'])
        return self._operation('restore_original_runtime',action,cleanup=True)

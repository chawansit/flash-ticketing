"""ADR0198 exact-identity paid/observer jobs on the original guarded session."""
import ast
import copy
import hashlib
import math
import re
import time
from pathlib import PurePosixPath

from fetch_status_refresh_parents import owner_path
from run_two_host_paid_comparison import job_program, process_program

HASH = re.compile(r'[0-9a-f]{64}$')
KEYS = {'pid', 'start_ticks', 'identity_path', 'name', 'status_path', 'command_sha256'}


def launch_spec(arguments, directory, *, database=False):
    """Derive the identity from the existing launcher, without another command template."""
    path = PurePosixPath(directory)
    if (not path.is_absolute() or '..' in path.parts or str(path) != directory
            or not isinstance(arguments, list) or len(arguments) < 2
            or any(not isinstance(a, str) or not a or '\x00' in a for a in arguments)
            or not re.fullmatch(r'[a-zA-Z0-9_]+\.py', PurePosixPath(arguments[1]).name)
            or type(database) is not bool):
        raise ValueError('Exact bounded script launch required')
    code = job_program(arguments, directory, database=database)
    marker='env=dict(os.environ)'
    if code.count(marker)!=1:raise ValueError('Historical job environment changed')
    code=code.replace(marker,marker+";env['PYTHONDONTWRITEBYTECODE']='1';os.umask(0o077)")
    marker='file.write(json.dumps(job))'
    if code.count(marker)!=1:raise ValueError('Historical identity persistence changed')
    code=code.replace(marker,marker+';file.flush();os.fsync(file.fileno())')
    tree = ast.parse(code)
    calls = [node for node in ast.walk(tree) if isinstance(node, ast.Call)
             and isinstance(node.func, ast.Attribute) and node.func.attr == 'Popen']
    if len(calls) != 1:raise ValueError('Historical single-process launcher changed')
    command = ast.literal_eval(calls[0].args[0])
    name = PurePosixPath(arguments[1]).stem
    expected = {'identity_path':directory, 'name':name, 'status_path':directory+'/job-'+name+'-exit.json',
                'command_sha256':hashlib.sha256(b'\0'.join(a.encode() for a in command)+b'\0').hexdigest()}
    return code, expected


def validate_job(job, expected):
    if (not isinstance(job, dict) or set(job) != KEYS
            or type(job['pid']) is not int or job['pid'] <= 0
            or type(job['start_ticks']) is not int or job['start_ticks'] <= 0
            or any(job[k] != v for k,v in expected.items())
            or not HASH.fullmatch(job['command_sha256'])):
        raise ValueError('Exact launch identity required; foreign PID cannot be adopted')
    return copy.deepcopy(job)


class PaidJobs:
    """Single-use launch; uncertain outcomes remain failed even after identity recovery."""
    def __init__(self, execution, diagnostics, locations, *, clock=time.monotonic, sleep=time.sleep):
        if diagnostics.execution is not execution or execution.session is not diagnostics.session:
            raise ValueError('Original execution and diagnostic session required')
        if set(locations) != {'container', 'primary', 'secondary', 'generator'}:
            raise ValueError('Exact observer and generator locations required')
        for value in locations.values():
            p = PurePosixPath(value)
            if not p.is_absolute() or '..' in p.parts or str(p) != value:
                raise ValueError('Canonical owned job location required')
        expected_locations = {'container':diagnostics.directory, **{host:owner_path(execution.session.config[host]['repo'], diagnostics.artifact_owner_name)+'/'+execution.arm for host in ('primary','secondary','generator')}}
        if locations != expected_locations:
            raise ValueError('Scope-bound observer owner required on every site')
        self.execution, self.diagnostics = execution, diagnostics
        self.session, self.runtime = execution.session, execution.runtime
        self.locations = copy.deepcopy(locations)
        self.locations_sha256 = __import__('work_envelope').digest(locations)
        self.clock, self.sleep = clock, sleep
        self.attempts, self.journal_healthy = [], True
        self.cleanup_used = False

    def _guard(self, timeout, *, cleanup=False):
        self.diagnostics._guard(timeout, cleanup=cleanup)
        if not cleanup and not self.journal_healthy:
            raise ValueError('Failed journal blocks new job launches')
        from work_envelope import digest
        if digest(self.locations) != self.locations_sha256:
            raise ValueError('Original owned job locations changed')

    def _journal(self, kind, record, *, cleanup=False):
        try:self.runtime._write(kind, record)
        except BaseException:
            self.journal_healthy = self.execution.journal_healthy = False
            if not cleanup:raise

    def _remote(self, attempt, code, timeout, *, cleanup=False):
        self._guard(timeout, cleanup=cleanup)
        if attempt['site']=='container':
            # PID ownership is meaningless after the containing runtime is replaced.
            observed = self.execution._observe(cleanup=cleanup)
            self.diagnostics._stable(observed, observed)
            return self.session.api(self.diagnostics.cid, code, timeout)
        return self.session.call(attempt['site'], code, timeout)

    def _recover(self, attempt):
        code = ("import json,os,stat;from pathlib import Path\np=Path("+
                repr(attempt['expected']['identity_path']+'/job-'+attempt['expected']['name']+'-identity.json')+
                ")\nfd=os.open(p,os.O_RDONLY|os.O_NOFOLLOW|os.O_NONBLOCK)\n"
                "with os.fdopen(fd,'rb') as stream:\n"
                " s=os.fstat(stream.fileno())\n"
                " assert stat.S_ISREG(s.st_mode) and stat.S_IMODE(s.st_mode)==0o600 and s.st_uid==os.geteuid() and s.st_nlink==1 and 0<s.st_size<=4096\n"
                " raw=stream.read(4097)\n"
                " assert len(raw)==s.st_size\n"
                "print(json.dumps(json.loads(raw)))")
        value = self._remote(attempt, code, 45, cleanup=True)
        attempt['job'] = validate_job(value, attempt['expected'])
        self._journal('paid-job-recovered', self._public(attempt), cleanup=True)

    @staticmethod
    def _public(attempt):
        return {k:copy.deepcopy(attempt[k]) for k in ('site','expected','acknowledged','stopped','job')}

    def launch(self, site, arguments, *, database=False):
        if self.cleanup_used or site not in self.locations:
            raise ValueError('Known forward job location required')
        self._guard(45)
        code, expected = launch_spec(arguments, self.locations[site], database=database)
        if any(a['site']==site and a['expected']['name']==expected['name'] for a in self.attempts):
            raise ValueError('A completed or ambiguous launch cannot be replayed')
        attempt = {'site':site, 'expected':expected, 'job':None, 'acknowledged':False, 'stopped':False}
        # Persist the attempted identity before a call that might dispatch a customer.
        self._journal('paid-job-intent', self._public(attempt))
        self.attempts.append(attempt)
        try:
            value = self._remote(attempt, code, 45)
            attempt['job'] = validate_job(value, expected)
            attempt['acknowledged'] = True
            self._journal('paid-job-ack', self._public(attempt))
            return attempt
        except BaseException:
            previous = self.session.cleanup_mode
            self.session.cleanup_mode = True
            try:
                if attempt['job'] is None:
                    try:self._recover(attempt)
                    except BaseException:  # noqa: BLE001 - retain failed launch while recovering cleanup identity.
                        self._journal('paid-job-unknown', self._public(attempt), cleanup=True)
            finally:self.session.cleanup_mode = previous
            raise

    def wait(self, attempt, timeout, *, allowed_returncodes=(0,)):
        if (not any(a is attempt for a in self.attempts) or not attempt['acknowledged']
                or attempt['job'] is None or attempt['stopped'] or self.cleanup_used
                or type(timeout) is not int or not 1<=timeout<=480):
            raise ValueError('Known acknowledged bounded job required')
        deadline = self.clock()+timeout
        while True:
            remaining = math.floor(deadline-self.clock())
            if remaining<1:raise TimeoutError('Owned paid/observer job exceeded deadline')
            state = self._remote(attempt, process_program(attempt['job']), min(45,remaining))
            if not isinstance(state,dict) or set(state)!={'running'} or type(state['running']) is not bool:
                raise ValueError('Exact job state required')
            if state['running'] is False:
                code = "import json;from pathlib import Path;print(json.dumps(json.loads(Path("+repr(attempt['job']['status_path'])+").read_text())))"
                receipt = self._remote(attempt, code, min(45,remaining))
                if (not isinstance(receipt,dict) or set(receipt)!={'returncode'}
                        or type(receipt['returncode']) is not int or receipt['returncode'] not in allowed_returncodes):
                    raise ValueError('Owned job exited outside its allowed result codes')
                return receipt
            self.sleep(min(5,max(0,deadline-self.clock())))

    def stop_all(self):
        if self.cleanup_used:raise ValueError('Job cleanup cannot be replayed')
        self.cleanup_used = True
        previous = self.session.cleanup_mode
        self.session.cleanup_mode = True
        failures = []
        try:
            # Customer dispatch stops first; all other jobs are attempted independently.
            for attempt in sorted(self.attempts,key=lambda a:a['site']!='generator'):
                try:
                    if attempt['job'] is None:self._recover(attempt)
                    self._journal('paid-job-stop-intent', self._public(attempt),cleanup=True)
                    value = self._remote(attempt,process_program(attempt['job'],stop=True),45,cleanup=True)
                    if (not isinstance(value,dict) or set(value)!={'running'} or value['running'] is not False):
                        raise ValueError('Owned job remains running or stop evidence is incomplete')
                    attempt['stopped']=True
                    self._journal('paid-job-stopped', self._public(attempt),cleanup=True)
                except BaseException as exc:  # noqa: BLE001 - remaining owned jobs must still be stopped.
                    failures.append({'site':attempt['site'],'name':attempt['expected']['name'],
                                     'exception_type':type(exc).__name__})
        finally:self.session.cleanup_mode = previous
        return {'dispatch_stopped':not any(a['site']=='generator' and not a['stopped'] for a in self.attempts),
                'all_jobs_stopped':all(a['stopped'] for a in self.attempts),
                'journal_healthy':self.journal_healthy, 'failures':failures,
                'pass':not failures and self.journal_healthy}

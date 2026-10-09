"""ADR0199 same-session, single-use continuation from a restored passing control."""
import copy
import hashlib
import json
import os
import stat
from datetime import datetime

import work_envelope as policy
from worker_separation_execution import observation_identity
from worker_separation_paid_stage import adapter_identity
from worker_separation_snapshot import verify_restored

_TOKEN = object()


def evidence(path):
    """Read bounded, stable regular evidence; missing/replaced receipts never authorize."""
    before = path.stat(follow_symlinks=False)
    if not stat.S_ISREG(before.st_mode) or not 0 < before.st_size <= 4194304:
        raise ValueError('Bounded regular handover evidence required')
    with path.open('rb') as stream:
        opened = os.fstat(stream.fileno())
        raw = stream.read(4194305)
        after = os.fstat(stream.fileno())
    current = path.stat(follow_symlinks=False)
    # Windows path stat and descriptor fstat can expose different ctime semantics.
    identity = lambda s:(s.st_dev,s.st_ino,s.st_size,s.st_mtime_ns)
    if (any(identity(s) != identity(before) for s in (opened,after,current))
            or before.st_ctime_ns != current.st_ctime_ns or opened.st_ctime_ns != after.st_ctime_ns
            or len(raw) != before.st_size):
        raise ValueError('Handover evidence changed during observation')
    return json.loads(raw), hashlib.sha256(raw).hexdigest()


def identities(observed):
    return {host:observation_identity(value) for host,value in observed.items()}


class CandidateHandover:
    """Issued only by the completed control; not reconstructable from an outcome dictionary."""
    def __init__(self, token, control, receipt, path, summary_sha):
        if token is not _TOKEN:raise TypeError('Use issue() with the actual restored control')
        self.control = control
        self._receipt = copy.deepcopy(receipt)
        self.path = path
        self.sha256 = evidence(path)[1]
        self.summary_sha = summary_sha
        self.used = False
        self.runtime = None

    @property
    def container_ids(self):
        return sorted(c['id'] for c in self._receipt['runtime']['primary']['containers'])

    def _validate(self):
        control = self.control
        if (not control.used or control.execution.arm != 'control'
                or control.record.get('pass') is not True or control.record.get('status') != 'PASSED_RESTORED'
                or any(control.record.get(k) is not True for k in
                       ('restoration_complete','private_artifact_cleanup_complete','journal_healthy'))
                or not control.journal_healthy or not control.execution.journal_healthy
                or not control.stage.journal_healthy or control.summary_path is None):
            raise ValueError('Durably passed and fully restored original control required')
        if control.summary_path.parent != control.runtime.output or self.path.parent != control.runtime.output:
            raise ValueError('Original control evidence owner required')
        summary, summary_sha = evidence(control.summary_path)
        receipt, receipt_sha = evidence(self.path)
        if (summary != control.record or summary_sha != self.summary_sha
                or receipt != self._receipt or receipt_sha != self.sha256
                or receipt['adapter_sources'] != adapter_identity()
                or receipt['scope'] != control.runtime.guard.key
                or receipt['scope_binding_sha256'] != policy.digest(control.runtime.guard.binding)
                or receipt['expected'] != control.runtime.expected
                or receipt['saved_runtime_sha256'] != policy.digest(control.runtime.saved)):
            raise ValueError('Original immutable control, source and handover evidence required')
        control.stage._guard(5)
        control.runtime._authorize(5)

    def claim(self, runtime):
        if self.used:raise ValueError('Candidate handover is single-use; no ambiguous replay')
        self._validate()
        original = self.control.runtime
        stamp = datetime.fromisoformat(self._receipt['checked_at'])
        if (runtime.binding['arm'] != 'candidate' or runtime.session is not original.session
                or runtime.guard is not original.guard or runtime.expected != original.expected
                or policy.digest(runtime.saved) != policy.digest(original.saved)
                or runtime.scope_binding_sha256 != original.scope_binding_sha256
                or stamp.tzinfo is None or not 0 <= (runtime.now()-stamp).total_seconds() <= 30):
            raise ValueError('Fresh original-session candidate handover required')
        runtime._authorize(5)
        self.used = True  # Consume before persistence; an unknown acknowledgement cannot be replayed.
        original._write('candidate-handover-claim',{'handover_sha256':self.sha256,'scope':original.guard.key})
        self.runtime = runtime

    def verify(self, runtime, observed):
        if not self.used or self.runtime is not runtime:
            raise ValueError('Original claimed candidate runtime required')
        self._validate()
        if identities(observed) != self._receipt['runtime']:
            raise ValueError('Restored handover container identity changed')


def issue(control):
    from worker_separation_recovery import RestoredPaidArm
    if type(control) is not RestoredPaidArm or getattr(control,'handover_attempted',False):
        raise ValueError('Original single-use restored control required')
    control.handover_attempted = True
    if (not control.used or control.execution.arm != 'control' or control.summary_path is None
            or control.record.get('pass') is not True or control.record.get('status') != 'PASSED_RESTORED'
            or any(control.record.get(k) is not True for k in
                   ('restoration_complete','private_artifact_cleanup_complete','journal_healthy'))):
        raise ValueError('Passing persisted control required before candidate handover')
    summary, summary_sha = evidence(control.summary_path)
    if summary != control.record:raise ValueError('Persisted control summary changed')
    control.stage._guard(5)
    control.runtime._authorize(5)
    first = control.runtime._observe()
    primary = first['primary']
    verify_restored(control.runtime.saved,{h:v['rows'] for h,v in first.items()},primary['volumes'],primary['bind_sha256'])
    control.execution._drain()  # Exact fresh global queues, Kafka and generator-idle receipt.
    second = control.runtime._observe()
    primary = second['primary']
    verify_restored(control.runtime.saved,{h:v['rows'] for h,v in second.items()},primary['volumes'],primary['bind_sha256'])
    if identities(first) != identities(second):raise ValueError('Restored runtime changed while proving handover')
    receipt = {'decision':'ADR0199','scope':control.runtime.guard.key,
               'scope_binding_sha256':control.runtime.scope_binding_sha256,
               'expected':copy.deepcopy(control.runtime.expected),'saved_runtime_sha256':policy.digest(control.runtime.saved),
               'control_summary_sha256':summary_sha,'adapter_sources':adapter_identity(),
               'checked_at':control.runtime.now().isoformat(),'runtime':identities(second)}
    path = control.runtime._write('candidate-handover',receipt)
    proof = CandidateHandover(_TOKEN,control,receipt,path,summary_sha)
    proof._validate()
    return proof

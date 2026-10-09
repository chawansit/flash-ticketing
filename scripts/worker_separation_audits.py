"""ADR0197 read-only financial checks and scope-bound audit transport; no load CLI."""
import copy
import inspect
import json
import math
import time
from datetime import UTC, datetime
from pathlib import Path
from uuid import UUID

import audit_checkout_smoke
import work_envelope as policy
from async_confirmation_contract import GLOBAL_RECEIPT_AUDIT
from fixture_identity_evidence import fixture_identity
from two_host_topology import NORMAL_COUNTS, environment
from worker_separation_execution import observation_identity, planned_row, row_identity, verify_phase
from worker_separation_runtime import observation_program
from worker_separation_snapshot import runtime_semantic

QUEUE_ZERO = ('unpublished_outbox', 'pending_refresh', 'dead_letters', 'pending_callback_deliveries',
              'pending_refunds', 'reservation_stream_entries', 'reservation_stream_pending',
              'kafka_total_lag', 'pending_confirmation_receipts', 'review_confirmation_receipts',
              'confirmation_capacity_outstanding', 'confirmation_capacity_mismatches')
HELPERS = ('capacity_queue_state.py', 'kafka_lag_observe.py')
RELATIONSHIP_SQL = """WITH cohort AS (SELECT * FROM orders WHERE event_id=ANY(%s::uuid[])),
 scoped_bookings AS (SELECT b.* FROM bookings b LEFT JOIN orders o ON o.id=b.order_id
                    WHERE b.event_id=ANY(%s::uuid[]) OR o.event_id=ANY(%s::uuid[]))
 SELECT
 (SELECT count(*) FROM cohort o LEFT JOIN holds h ON h.id=o.hold_id
   WHERE h.id IS NULL OR h.event_id<>o.event_id OR h.actor<>o.actor
     OR (o.status='FULFILLED' AND h.status<>'CONSUMED')),
 (SELECT count(*) FROM scoped_bookings b LEFT JOIN cohort o ON o.id=b.order_id
    LEFT JOIN order_items i ON i.order_id=b.order_id AND i.event_id=b.event_id AND i.seat_id=b.seat_id
    LEFT JOIN event_seats s ON s.event_id=b.event_id AND s.seat_id=b.seat_id
    LEFT JOIN payment_attempts p ON p.order_id=b.order_id
    LEFT JOIN tickets t ON t.booking_id=b.id
   WHERE o.id IS NULL OR o.status<>'FULFILLED' OR o.event_id<>b.event_id OR i.order_id IS NULL
      OR s.booked_order_id IS DISTINCT FROM b.order_id OR s.hold_id IS NOT NULL
      OR p.id IS NULL OR p.status<>'SUCCEEDED' OR p.outcome<>'SUCCEEDED' OR t.id IS NULL),
 (SELECT count(*) FROM cohort o WHERE
   (o.status='FULFILLED' AND (SELECT count(*) FROM order_items i WHERE i.order_id=o.id)<>1)
   OR o.total<>coalesce((SELECT sum(i.price) FROM order_items i WHERE i.order_id=o.id),0)
   OR o.currency<>(SELECT e.currency FROM events e WHERE e.id=o.event_id)
   OR EXISTS(SELECT 1 FROM order_items i WHERE i.order_id=o.id AND i.event_id<>o.event_id)
   OR (o.status='EXPIRED' AND EXISTS(SELECT 1 FROM scoped_bookings b WHERE b.order_id=o.id))),
 (SELECT count(*) FROM event_seats s LEFT JOIN orders o ON o.id=s.booked_order_id
   WHERE (s.event_id=ANY(%s::uuid[]) OR o.event_id=ANY(%s::uuid[])) AND s.booked_order_id IS NOT NULL
     AND NOT EXISTS(SELECT 1 FROM scoped_bookings b
                    WHERE b.order_id=s.booked_order_id AND b.event_id=s.event_id AND b.seat_id=s.seat_id)),
 (SELECT count(*) FROM cohort o WHERE o.status='FULFILLED'
   AND (SELECT count(*) FROM scoped_bookings b JOIN tickets t ON t.booking_id=b.id WHERE b.order_id=o.id)<>1),
 (SELECT count(DISTINCT t.id) FROM tickets t JOIN scoped_bookings b ON b.id=t.booking_id),
 (SELECT count(*) FROM holds WHERE event_id=ANY(%s::uuid[]) AND expires_at>=clock_timestamp())"""
RELATIONSHIP_NAMES = ('hold_relationship_errors', 'booking_relationship_errors', 'order_item_relationship_errors',
                      'inventory_relationship_errors', 'fulfilled_ticket_relationship_errors',
                      'unique_issued_tickets', 'holds_not_past_ttl')


def expectations(show_ids, expected_orders, expected_paid, callbacks):
    if (not isinstance(show_ids, list) or not show_ids or len(show_ids) not in {1, 60, 84}
            or len(set(show_ids)) != len(show_ids)
            or any(not isinstance(v, str) or str(UUID(v)) != v for v in show_ids)
            or type(expected_orders) is not int or not 0 <= expected_orders <= len(show_ids) * 300
            or type(expected_paid) is not int or not 0 <= expected_paid <= expected_orders
            or type(callbacks) is not int or not 1 <= callbacks <= 10):
        raise ValueError('Exact bounded one-ticket workload expectations required')


def financial_snapshot(conn, show_ids, expected_orders, expected_paid, callbacks):
    """Committed cardinalities and relationships in the same read-only snapshot."""
    expectations(show_ids, expected_orders, expected_paid, callbacks)
    with conn.transaction():
        conn.execute('SET TRANSACTION ISOLATION LEVEL REPEATABLE READ READ ONLY')
        conn.execute("SET LOCAL statement_timeout='20s'")
        conn.execute("SET LOCAL lock_timeout='2s'")
        aggregate = audit_checkout_smoke.audit(conn, show_ids, expected_orders, expected_paid, callbacks)
        row = conn.execute(RELATIONSHIP_SQL, (show_ids,) * 6).fetchone()
        relationships = dict(zip(RELATIONSHIP_NAMES, row, strict=True))
    checks = {
        'post_ttl_complete': relationships['holds_not_past_ttl'] == 0,
        'payments_durable': aggregate['pass'] is True,
        'ticket_relationships_valid': relationships['unique_issued_tickets'] == expected_paid
            and all(relationships[k] == 0 for k in RELATIONSHIP_NAMES[:5]),
    }
    return {'counts': aggregate, 'relationships': relationships, 'checks': checks,
            'pass': all(checks.values())}


def duplicate_snapshot(conn):
    """Independent global seat duplication audit, not just the fixture's counts."""
    with conn.transaction():
        conn.execute('SET TRANSACTION READ ONLY')
        conn.execute("SET LOCAL statement_timeout='20s'")
        row = conn.execute('''SELECT count(*) FROM
          (SELECT event_id,seat_id FROM bookings GROUP BY event_id,seat_id HAVING count(*)>1) duplicates''').fetchone()
    return {'duplicate_booked_seats': row[0], 'zero_double_booking': row[0] == 0}


def financial_body(cohort):
    body = 'import json,os,time,psycopg\n' + inspect.getsource(expectations)
    body = 'from uuid import UUID\n' + body
    body += '\nRELATIONSHIP_SQL=' + repr(RELATIONSHIP_SQL) + '\nRELATIONSHIP_NAMES=' + repr(RELATIONSHIP_NAMES)
    body += '\n' + inspect.getsource(audit_checkout_smoke.audit)
    body += '\n' + inspect.getsource(financial_snapshot).replace('audit_checkout_smoke.audit', 'audit')
    body += '\ncohort=' + repr(cohort) + '''
with psycopg.connect(os.environ['DATABASE_URL'],autocommit=True,connect_timeout=5) as conn:
 with conn.transaction():
  conn.execute('SET TRANSACTION READ ONLY');conn.execute("SET LOCAL statement_timeout='10s'")
  delay=conn.execute("SELECT greatest(coalesce(extract(epoch FROM max(expires_at)-clock_timestamp()),0),0) FROM holds WHERE event_id=ANY(%s::uuid[])",(cohort['show_ids'],)).fetchone()[0]
 if delay>150:raise TimeoutError('Cohort TTL exceeds bounded audit window')
 time.sleep(float(delay)+0.05)
 result=financial_snapshot(conn,cohort['show_ids'],cohort['expected_orders'],cohort['expected_paid'],cohort['callbacks'])
print(json.dumps(result))
'''
    return body


def duplicate_body():
    return ('import json,os,psycopg\n' + inspect.getsource(duplicate_snapshot)
            + "\nwith psycopg.connect(os.environ['DATABASE_URL'],autocommit=True,connect_timeout=5) as conn:result=duplicate_snapshot(conn)\nprint(json.dumps(result))\n")


def dormant_kafka_sample(observer, group):
    """Never infer a drained queue from missing assignments during handover."""
    from kafka import TopicPartition
    deadline = time.monotonic() + 10
    while True:
        descriptions = observer.admin.describe_consumer_groups([group])
        if len(descriptions) != 1:raise ValueError('Exact dormant group description required')
        state = descriptions[0]
        if not state.error_code and state.state == 'Empty' and not state.members:break
        if time.monotonic() >= deadline:raise TimeoutError('Consumer-free group did not settle')
        time.sleep(.5)
    metadata = observer.end_reader.partitions_for_topic(observer.topic)
    if not metadata or any(type(p) is not int or p < 0 for p in metadata):
        raise ValueError('Full dormant topic metadata required')
    partitions = [TopicPartition(observer.topic, p) for p in sorted(metadata)]
    offsets = observer.admin.list_consumer_group_offsets(group, partitions=partitions)
    ends = observer.end_reader.end_offsets(partitions)
    if set(offsets) != set(partitions) or set(ends) != set(partitions):
        raise ValueError('Full dormant partition offsets required')
    lag = 0
    for partition in partitions:
        offset, end = offsets[partition].offset, ends[partition]
        if type(offset) is not int or type(end) is not int or offset < 0 or end < offset:
            raise ValueError('Invalid dormant partition offsets')
        lag += end - offset
    after = observer.admin.describe_consumer_groups([group])
    metadata_after = observer.end_reader.partitions_for_topic(observer.topic)
    offsets_after = observer.admin.list_consumer_group_offsets(group, partitions=partitions)
    ends_after = observer.end_reader.end_offsets(partitions)
    if (len(after) != 1 or after[0].error_code or after[0].state != 'Empty' or after[0].members
            or metadata_after != metadata or set(offsets_after) != set(partitions) or ends_after != ends
            or any(type(offsets_after[p].offset) is not int or offsets_after[p].offset != offsets[p].offset for p in partitions)):
        raise ValueError('Dormant group changed during observation')
    return {'total_lag': lag, 'members': 0}


def queue_body(helpers, members=6):
    if (not isinstance(helpers, dict) or set(helpers) != set(HELPERS)
            or any(not isinstance(v, str) or len(v) != 64 or any(c not in '0123456789abcdef' for c in v)
                   for v in helpers.values())):
        raise ValueError('Exact pinned queue helper hashes required')
    if type(members) is not int or not 0 <= members <= 6:raise ValueError('Exact observed consumer count required')
    prefix = 'import hashlib,time\nfrom pathlib import Path\nexpected_helpers=' + repr(helpers) + '''
for name,expected in expected_helpers.items():
 path=Path('/app/scripts')/name
 if path.is_symlink() or not path.is_file() or path.stat().st_size>262144:raise ValueError('Pinned helper missing')
 if hashlib.sha256(path.read_bytes().replace(b'\\r\\n',b'\\n')).hexdigest()!=expected:raise ValueError('Pinned helper changed')
'''
    body = GLOBAL_RECEIPT_AUDIT
    if members == 0:
        prefix += '\n' + inspect.getsource(dormant_kafka_sample)
        body = body.replace("observer.sample('ticketing-fulfillment-v1')", "dormant_kafka_sample(observer,'ticketing-fulfillment-v1')")
    return prefix + body


def audit_contract(helpers):
    """Bind actual generated read-only logic and helpers into a fresh reservation."""
    queue_body(helpers)
    sources = [inspect.getsource(f) for f in (expectations, audit_checkout_smoke.audit, financial_snapshot,
                                             duplicate_snapshot, financial_body, duplicate_body, queue_body,
                                             transport_program, dormant_kafka_sample, queue_checks, AuditActions, connect_audits)]
    return {'decision': 'ADR0197', 'program_sha256': policy.digest([sources, RELATIONSHIP_SQL, RELATIONSHIP_NAMES,
                                                                  GLOBAL_RECEIPT_AUDIT]),
            'helpers': copy.deepcopy(helpers)}


def transport_program(saved, before, target, body, timeout):
    if type(timeout) is not int or not 1 <= timeout <= 180:
        raise ValueError('Bounded audit deadline required')
    if target not in [row_identity(r) for r in before['rows'] if r['Config']['Labels'].get('com.docker.compose.service') == 'api'
                     and r['Config']['Labels'].get('com.docker.compose.project') == 'flash-ticketing'
                     and r['State']['Running'] is True]:
        raise ValueError('Exact observed primary API target required')
    prefix = observation_program(saved, True)
    prefix = prefix[:prefix.rindex('print(json.dumps(observe()))')].replace('time.monotonic()+25', f'time.monotonic()+{timeout}')
    helpers = '\n'.join(inspect.getsource(f) for f in (environment, runtime_semantic))
    # runtime_semantic depends on the existing semantic/binding helpers.
    from two_host_topology import bindings, semantic
    helpers = '\n'.join(inspect.getsource(f) for f in (bindings, semantic)) + '\n' + helpers
    code = prefix + '\n' + helpers + '\n' + inspect.getsource(policy.digest)
    code += '\n' + inspect.getsource(row_identity).replace('policy.digest', 'digest')
    code += '\n' + inspect.getsource(observation_identity)
    code += '\nexpected=' + repr(policy.digest(observation_identity(before))) + '\ntarget=' + repr(target)
    code += '\naudit_body=' + repr(body) + '''
if digest(observation_identity(observe()))!=expected:raise ValueError('Runtime drift before audit')
row=json.loads(subprocess.check_output(['docker','inspect',target['id']],text=True,timeout=budget(5)))[0]
if row_identity(row)!=target:raise ValueError('Audit target replaced')
body_budget=int(budget(180))-6
if body_budget<1:raise TimeoutError('Insufficient remaining audit deadline')
audit_body='import signal\\nsignal.alarm('+str(body_budget)+')\\n'+audit_body
result=subprocess.run(['docker','exec','-i',target['id'],'python','-'],input=audit_body,text=True,capture_output=True,timeout=body_budget+1,check=False)
if result.returncode!=0:raise RuntimeError('Bound read-only audit failed')
if len(result.stdout.encode())>65536:raise ValueError('Oversized audit receipt')
value=json.loads(result.stdout)
if digest(observation_identity(observe()))!=expected:raise ValueError('Runtime drift after audit')
print(json.dumps(value))
'''
    return code


def queue_checks(value, members):
    if (not isinstance(value, dict) or set(value) != {*QUEUE_ZERO, 'kafka_members', 'pass'}
            or any(type(value[k]) is not int or value[k] < 0 for k in (*QUEUE_ZERO, 'kafka_members'))
            or type(value['pass']) is not bool):
        raise ValueError('Complete integer queue receipt required')
    drained = all(value[k] == 0 for k in QUEUE_ZERO)
    if value['pass'] != drained:
        raise ValueError('Queue acknowledgement disagrees with counts')
    return drained and value['kafka_members'] == members


class AuditActions:
    """Same-session concrete audit adapter. No SSH factory or dispatch authority."""
    def __init__(self, execution, helpers, *, now=None, clock=time.monotonic, sleep=time.sleep):
        self.execution, self.runtime = execution, execution.runtime
        self.helpers = copy.deepcopy(helpers)
        self.contract = audit_contract(helpers)
        self.now, self.clock, self.sleep = now or (lambda: datetime.now(UTC)), clock, sleep
        self.cohort, self.fixture_path, self.fixture_bytes = None, None, None
        self.used = set()
        self._guard(5)

    def _guard(self, timeout, *, cleanup=False):
        self.execution._guard(timeout, cleanup=cleanup)
        if (audit_contract(self.helpers) != self.contract
                or self.runtime.guard.binding.get('worker_audit_contract_sha256') != policy.digest(self.contract)):
            raise ValueError('Original scope must bind exact audit implementation')

    def bind_fixture(self, path, expected_orders, expected_paid, callbacks):
        if self.cohort is not None:
            raise ValueError('Fixture cannot be rebound or replayed')
        self._guard(5)
        path = Path(path).absolute()
        if (path.name != 'fixture-identity.json' or path.parent.name != self.execution.arm
                or path.parent.parent != self.runtime.output or path.resolve() != path
                or path.is_symlink() or not path.is_file() or path.stat().st_size > 65536):
            raise ValueError('Exact retained owned fixture path required')
        payload = path.read_bytes()
        receipt = json.loads(payload)
        if receipt.get('decision') != 'ADR0185' or receipt.get('arm') != self.execution.arm:
            raise ValueError('Exact fixture arm receipt required')
        identity = fixture_identity(receipt['fixture_identity'], receipt['fixture_identity']['shows'], now=self.now())
        if receipt.get('fixture_identity_sha256') != policy.digest(identity):
            raise ValueError('Fixture identity fingerprint mismatch')
        shows = identity['show_ids']
        expectations(shows, expected_orders, expected_paid, callbacks)
        expected = {'expected_orders': expected_orders, 'expected_paid': expected_paid, 'callbacks': callbacks,
                    'shows': len(shows)}
        if self.runtime.guard.binding.get('worker_audit_expectations') != expected:
            raise ValueError('Predetermined expectations must match fresh scope')
        cohort = {**expected, 'show_ids': shows, 'fixture_identity_sha256': policy.digest(identity)}
        self.runtime._write('audit-cohort', {'decision': 'ADR0197', 'arm': self.execution.arm,
                                           'binding_sha256': policy.digest(self.runtime.audit_binding), **cohort})
        self.cohort, self.fixture_path, self.fixture_bytes = cohort, path, payload

    def _observe_until(self, deadline, *, cleanup):
        values = {}
        for host in ('primary', 'secondary'):
            remaining = math.floor(deadline - self.clock())
            if remaining < 1:raise TimeoutError('Shared audit deadline exceeded')
            timeout = min(30, remaining)
            code = observation_program(self.execution.saved, host == 'primary')
            code = code.replace('time.monotonic()+25', 'time.monotonic()+' + str(timeout))
            values[host] = self.execution._call(host, code, timeout, cleanup=cleanup)
        return values

    def _query(self, body, timeout, *, cleanup):
        deadline = self.clock() + timeout
        self._guard(timeout, cleanup=cleanup)
        before = self._observe_until(deadline, cleanup=cleanup)
        apis = [r for r in before['primary']['rows'] if r['Config']['Labels'].get('com.docker.compose.service') == 'api']
        if len(apis) != 4 or any(r['Config']['Labels'].get('com.docker.compose.project') != 'flash-ticketing' for r in apis):
            raise ValueError('Four observed primary APIs required for audit')
        ports = set()
        for row in apis:
            if policy.digest(runtime_semantic(row)) == self.execution.saved['semantics']['api']:
                if row['State']['Running'] is not True:raise ValueError('Original API stopped')
            else:
                model = self.execution.pair[self.execution.arm]['primary']
                planned_row(row, model['services']['api'], model, ports)
        target = row_identity(min(apis, key=lambda r: r['Id']))
        remaining = math.floor(deadline - self.clock())
        if remaining <= 6:raise TimeoutError('Insufficient bounded audit execution time')
        value = self.execution._call('primary', transport_program(self.execution.saved, before['primary'], target, body, remaining),
                                     remaining, cleanup=cleanup)
        after = self._observe_until(deadline, cleanup=cleanup)
        if any(observation_identity(before[h]) != observation_identity(after[h]) for h in ('primary', 'secondary')):
            raise ValueError('Two-host runtime changed during audit')
        return value

    def _financial_action(self, name, body):
        if name in self.used:
            raise ValueError('Financial audit cannot be replayed after an unknown outcome')
        self.used.add(name)
        previous = self.execution.session.cleanup_mode
        self.execution.session.cleanup_mode = True
        record = {'decision': 'ADR0197', 'action': name, 'binding_sha256': policy.digest(self.runtime.audit_binding)}
        journal_failed = False
        try:
            self._guard(180 if name == 'financial' else 60, cleanup=True)
            try:self.runtime._write('audit-intent', record)
            except BaseException:  # noqa: BLE001 - mandatory audits survive journal failure.
                self.execution.journal_healthy = False
                journal_failed = True
            result = self._query(body(), 180 if name == 'financial' else 60, cleanup=True)
            try:self.runtime._write('audit-result', {**record, 'result': result})
            except BaseException:  # Evidence failure blocks certification.
                self.execution.journal_healthy = False
                raise
            if journal_failed:raise RuntimeError('Audit completed but journal persistence failed')
            return result
        finally:self.execution.session.cleanup_mode = previous

    def payment_durability(self):
        def body():
            if (self.cohort is None or self.fixture_path.is_symlink() or self.fixture_path.resolve() != self.fixture_path
                    or self.fixture_path.read_bytes() != self.fixture_bytes):
                raise ValueError('Original retained fixture required; no inferred or empty cohort')
            receipt = json.loads(self.fixture_bytes)
            expected = self.runtime.guard.binding.get('worker_audit_expectations')
            if ({k:self.cohort[k] for k in ('expected_orders','expected_paid','callbacks','shows')} != expected
                    or self.cohort['show_ids'] != receipt['fixture_identity']['show_ids']
                    or self.cohort['fixture_identity_sha256'] != receipt['fixture_identity_sha256']):
                raise ValueError('Retained cohort or expectations changed')
            return financial_body(self.cohort)
        result = self._financial_action('financial', body)
        checks = result.get('checks') if isinstance(result, dict) else None
        if (not isinstance(checks, dict) or set(checks) != {'post_ttl_complete', 'payments_durable', 'ticket_relationships_valid'}
                or any(checks[k] is not True for k in checks) or result.get('pass') is not True):
            raise ValueError('Financial durability, relationships and post-TTL must all pass')
        return checks

    def zero_double_booking(self):
        result = self._financial_action('duplicates', duplicate_body)
        if (not isinstance(result, dict) or set(result) != {'duplicate_booked_seats', 'zero_double_booking'}
                or type(result['duplicate_booked_seats']) is not int or result['duplicate_booked_seats'] != 0
                or result['zero_double_booking'] is not True):
            raise ValueError('Global zero double-booking audit required')
        return {'zero_double_booking': True}

    def _queue_journal(self, kind, value, *, cleanup):
        try:self.runtime._write(kind, value)
        except BaseException:
            self.execution.journal_healthy = False
            if not cleanup:raise

    def queue_drain(self, binding, *, cleanup=False):
        if binding != self.runtime.audit_binding:
            raise ValueError('Exact original drain binding required')
        previous = self.execution.session.cleanup_mode
        if cleanup:self.execution.session.cleanup_mode = True
        deadline = self.clock() + 120
        try:
            self._guard(120, cleanup=cleanup)
            self._queue_journal('queue-audit-intent', {'decision': 'ADR0197', 'binding_sha256': policy.digest(binding)}, cleanup=cleanup)
            while True:
                remaining = math.floor(deadline - self.clock())
                if remaining < 1:raise TimeoutError('Complete queue drain deadline exceeded')
                from qualify_two_host_deployment import GENERATOR_IDLE
                if self.execution._call('generator', GENERATOR_IDLE, min(30, remaining), cleanup=cleanup) != {'generator_idle': True}:
                    raise ValueError('Dispatch must be stopped and generator idle')
                observed = self._observe_until(deadline, cleanup=cleanup)
                try:
                    from worker_separation_snapshot import verify_restored
                    primary = observed['primary']
                    verify_restored(self.execution.saved, {h:o['rows'] for h,o in observed.items()}, primary['volumes'], primary['bind_sha256'])
                    members = NORMAL_COUNTS['consumer']
                except ValueError:
                    valid = False
                    for phase in ('original_infrastructure', 'infrastructure', 'partial_workers', 'restorable'):
                        try:verify_phase(self.execution.pair, self.execution.saved, self.execution.arm, observed, phase)
                        except ValueError:continue
                        valid = True
                        break
                    if not valid:raise ValueError('Unknown layout cannot establish queue membership')
                    members = sum(r['State']['Running'] is True and r['Config']['Labels'].get('com.docker.compose.service') == 'consumer'
                                  for host in ('primary','secondary') for r in observed[host]['rows'])
                remaining = math.floor(deadline - self.clock())
                if remaining < 1:raise TimeoutError('Complete queue drain deadline exceeded')
                value = self._query(queue_body(self.helpers, members), min(30, remaining), cleanup=cleanup)
                passed = queue_checks(value, members)
                self._queue_journal('queue-audit-sample', {'counts': value, 'binding_sha256': policy.digest(binding)}, cleanup=cleanup)
                if passed:
                    return {'binding_sha256': policy.digest(binding), 'checked_at': self.now().isoformat(),
                            'dispatch_stopped': True, 'all_queues_zero': True, 'kafka_drained': True}
                remaining = deadline - self.clock()
                if remaining <= 0:raise TimeoutError('Complete queue drain deadline exceeded')
                self.sleep(min(2, remaining))
        finally:self.execution.session.cleanup_mode = previous



def connect_audits(execution, helpers, **options):
    """Attach one concrete provider to original-stop, placement and restoration gates."""
    if execution.used or execution.failed or execution.configurations.failed or execution.runtime.failed:
        raise ValueError('Audit provider must be connected before placement execution')
    component = AuditActions(execution, helpers, **options)
    def drain(binding):
        return component.queue_drain(binding, cleanup=execution.session.cleanup_mode)
    execution.drain_provider = execution.runtime.drain_provider = drain
    return component

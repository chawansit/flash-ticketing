"""ADR0186 offline-only handover engine; no live adapter, SSH, CLI or load dispatch."""
import copy
import json
import os
from datetime import UTC, datetime

import stage_status_refresh_images as staging
from fetch_status_refresh_parents import owner_path
from worker_separation_inventory import digest
from worker_separation_topology import validate_pair

FORWARD = (
    ('verify_recovery_and_fresh_scope', 5, ('recovery_verified', 'fresh_scope', 'binding_matches')),
    ('verify_original_runtime', 45, ('exact_original_runtime', 'broker_volume_retained')),
    ('verify_generator_idle', 30, ('generator_idle',)),
    ('stop_dispatch', 30, ('dispatch_stopped',)),
    ('audit_queue_drain', 120, ('all_queues_zero', 'kafka_drained')),
    ('stage_immutable_images', 240, ('images_match', 'runtime_unchanged', 'owned_archive_cleanup')),
    ('stop_original_workers', 60, ('owned_workers_stopped',)),
    ('verify_all_workers_absent', 45, ('primary_workers_absent', 'secondary_workers_absent')),
    ('configure_common_infrastructure', 150, ('broker_volume_retained', 'common_config_applied')),
    ('verify_private_dependencies', 90, ('verified_rds_tls', 'redis_ready', 'pooler_ready', 'private_kafka_ready')),
    ('start_arm_workers', 150, ('exact_arm_workers_started',)),
    ('verify_host_aware_inventory', 180, ('exact_layout', 'sources_match', 'all_metrics_bound', 'budgets_match')),
    ('verify_callback_distribution', 60, ('all_four_apis_reachable', 'callback_budget_unchanged')),
)
CLEANUP = (
    ('stop_dispatch', 30, ('dispatch_stopped',)),
    ('audit_post_ttl_payment_durability', 180, ('post_ttl_complete', 'payments_durable', 'ticket_relationships_valid')),
    ('audit_zero_double_booking', 60, ('zero_double_booking',)),
    ('audit_full_queue_drain', 120, ('all_queues_zero', 'kafka_drained')),
)
STOP = ('stop_arm_workers', 60, ('owned_workers_stopped',))
ABSENT = ('verify_all_workers_absent', 45, ('primary_workers_absent', 'secondary_workers_absent'))
RESTORE = ('restore_original_runtime', 180, ('broker_volume_retained', 'original_config_applied'))
VERIFY = ('verify_exact_restoration', 90, ('runtime_restored', 'broker_volume_retained', 'bind_files_restored', 'secondary_empty'))
FILES = ('cleanup_owned_private_files', 60, ('owned_private_files_removed',))
DRAIN = ('verify_restored_queue_drain', 120, ('all_queues_zero', 'kafka_drained'))


class Journal:
    """New local owner only. Pending intents survive loss of acknowledgement."""
    def __init__(self, output, binding):
        output = output.absolute()
        owner_path('/qualification/repository', output.name)  # Shared ownership validator.
        parent = (staging.ROOT / 'tmp').absolute()
        if (output.parent != parent or output.resolve() != output or parent.is_symlink()
                or output.exists() or output.is_symlink()):
            raise ValueError('Fresh canonical staging output required')
        parent.mkdir(exist_ok=True)
        output.mkdir(mode=0o700)
        self.output, self.sequence = output, 0
        self.write('binding', {'decision': 'ADR0186', 'binding_sha256': digest(binding),
                               'offline_only': True, 'created_at': datetime.now(UTC).isoformat()})

    def write(self, kind, value):
        self.sequence += 1
        path = self.output / f'{self.sequence:04d}-{kind}.json'
        with path.open('x', encoding='utf-8') as stream:
            stream.write(json.dumps(value, sort_keys=True, allow_nan=False) + '\n')
            stream.flush()
            os.fsync(stream.fileno())
        return path


class Qualification:
    """Locally exercise ordering with simulated adapters; live use is prohibited."""
    def __init__(self, pair, arm, saved, adapter, output, *, journal_factory=Journal):
        validate_pair(pair)
        if (arm not in {'control', 'candidate'} or saved.get('decision') != 'ADR0186'
                or saved.get('schema') != 1 or getattr(adapter, 'offline_only', None) is not True):
            raise ValueError('Exact snapshot, arm and explicitly offline adapter required')
        self.binding = {'decision': 'ADR0186', 'arm': arm, 'pair_sha256': digest(pair),
                        'saved_runtime_sha256': digest(saved)}
        self.adapter, self.journal = adapter, journal_factory(output, self.binding)
        self.events, self.failures = [], []
        self.used, self.runtime_attempted, self.journal_healthy = False, False, True

    def _action(self, step, *, cleanup=False):
        name, timeout, checks = step
        request = {**self.binding, 'action': name, 'cleanup': cleanup, 'timeout_seconds': timeout}
        request_hash = digest(request)
        record = {'action': name, 'cleanup': cleanup, 'input_sha256': request_hash}
        self.events.append(record)
        try:
            if not cleanup:
                self.adapter.check_forward(timeout)  # Simulated pause/deadline/binding checks.
            try:
                self.journal.write('intent', record)
            except BaseException:
                self.journal_healthy = False
                if not cleanup:
                    raise
                # Lost local evidence must not prevent mandatory owned cleanup.
            if name == 'stop_original_workers' and not cleanup:
                self.runtime_attempted = True  # Set before a potentially ambiguous response.
            receipt = self.adapter.perform(copy.deepcopy(request))
            if (not isinstance(receipt, dict) or set(receipt) != {'action', 'input_sha256', 'pass', 'checks'}
                    or receipt['action'] != name or receipt['input_sha256'] != request_hash
                    or receipt['pass'] is not True or not isinstance(receipt['checks'], dict)
                    or set(receipt['checks']) != set(checks)
                    or any(receipt['checks'][key] is not True for key in checks)):
                raise ValueError('Exact successful action acknowledgement required')
            try:
                self.journal.write('ack', record)
            except BaseException:
                self.journal_healthy = False
                raise
            record['acknowledged'] = True
            return True
        except BaseException as exc:  # noqa: BLE001 - mandatory cleanup survives interruption and unknown outcome.
            record['acknowledged'] = False
            self.failures.append({'action': name, 'cleanup': cleanup, 'exception_type': type(exc).__name__})
            return False

    def run(self):
        if self.used:
            raise ValueError('An ambiguous or completed lifecycle cannot be replayed')
        self.used = True
        prepared = True
        for step in FORWARD:
            if not self._action(step):
                prepared = False
                break
        # Never short-circuit mandatory financial/correctness/queue audits.
        audits = [self._action(step, cleanup=True) for step in CLEANUP]
        absence, restored, stopped = True, False, True
        if self.runtime_attempted:
            stopped = self._action(STOP, cleanup=True)
            absence = self._action(ABSENT, cleanup=True)
            if absence:
                applied = self._action(RESTORE, cleanup=True)
                verified = self._action(VERIFY, cleanup=True)
                restored = applied and verified
        else:
            restored = self._action(VERIFY, cleanup=True)
        drain = self._action(DRAIN, cleanup=True)
        idle = self._action(FORWARD[2], cleanup=True)
        files = self._action(FILES, cleanup=True) if restored and absence else False
        safe = all(audits) and stopped and absence and restored and drain and idle and files and self.journal_healthy
        result = {'decision': 'ADR0186', 'offline_only': True, 'arm': self.binding['arm'],
                  'binding_sha256': digest(self.binding), 'deployment_preparation_pass': prepared,
                  'restoration_verified': restored, 'mandatory_audits_pass': all(audits),
                  'journal_healthy': self.journal_healthy,
                  'status': ('LOCALLY_QUALIFIED' if prepared else 'FAILED_RESTORED') if safe else 'RECOVERY_REQUIRED',
                  'pass': prepared and safe, 'events': self.events, 'failures': self.failures,
                  'cloud_deployments': 0, 'customer_dispatches': 0, 'capacity_improvement_measured': False}
        try:
            self.journal.write('summary', result)
        except BaseException as exc:  # noqa: BLE001 - preserve failed summary persistence as recovery required.
            self.journal_healthy = False
            result.update({'pass': False, 'status': 'RECOVERY_REQUIRED', 'journal_healthy': False})
            result['failures'].append({'action': 'persist_summary', 'cleanup': True, 'exception_type': type(exc).__name__})
        return result


def qualify_pair(pair, saved, adapter_factory):
    """Local comparison ordering only; failed control prevents even a simulated candidate."""
    validate_pair(pair)
    reports = {}
    for arm in ('control', 'candidate'):
        engine = Qualification(pair, arm, saved, adapter_factory(arm), staging.new_stage_output())
        reports[arm] = engine.run()
        if reports[arm]['pass'] is not True:
            break
    return {'decision': 'ADR0186', 'offline_only': True, 'arms': reports,
            'pass': len(reports) == 2 and all(r['pass'] is True for r in reports.values()),
            'cloud_deployments': 0, 'customer_dispatches': 0,
            'capacity_improvement_measured': False}

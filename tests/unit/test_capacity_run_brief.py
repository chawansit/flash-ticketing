import copy
import hashlib
import json
import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[2] / 'scripts'))
import capacity_run_brief as brief


def records():
    raw = {'run': 'adr0151-' + 'a' * 12, 'pass': True, 'restoration_complete': True,
           'integrity_verified': True, 'transport_credentials_cleared': True,
           'native': {'cleanup_complete': True}}
    report = {'run': raw['run'], 'status': 'PASSED_RESTORED', 'short_capacity_qualified': True,
              'customer': {'scheduled': 2, 'dispatched': 2, 'fulfilled': 2, 'generator_drops': 0},
              'financial': {'pass': True, 'hold_deadlines_elapsed': True,
                            'counts': {'tickets': 2, 'succeeded_payments': 2, 'bookings': 2, 'fulfilled_orders': 2,
                                       'duplicate_booked_seats': 0, 'multi_booking_orders': 0},
                            'relationships': {'unique_issued_tickets': 2}},
              'queues_after_restoration': {'pass': True, 'kafka_total_lag': 0, 'kafka_members': 1},
              'restoration': {k: True for k in ('original_runtime_restored', 'integrity_verified', 'generator_idle',
                                               'namespace_removed', 'cleanup_complete', 'transport_credentials_cleared')}}
    report['financial']['counts'].update({k: 0 for k in ('pending_orders', 'pending_payment_attempts', 'incomplete_callback_deliveries', 'unpublished_outbox', 'dead_letters')})
    report['financial']['relationships'].update({k: 0 for k in ('hold_relationship_errors', 'booking_relationship_errors', 'order_item_relationship_errors', 'inventory_relationship_errors', 'fulfilled_ticket_relationship_errors', 'holds_not_past_ttl')})
    raw['paid_stage'] = {'financial': copy.deepcopy(report['financial'])}
    report['raw_result_sha256'] = hashlib.sha256(json.dumps(raw, sort_keys=True, separators=(',', ':')).encode()).hexdigest()
    return report, raw


def test_compact_allowlist_never_emits_private_payloads():
    report, _ = records()
    report.update(password='secret-value', traceback='secret-value')
    report['customer']['arbitrary'] = 'secret-value'
    report['customer']['retry_attempts'] = 'secret-value'
    result = brief.compact(report, phases={'phases': ['secret-value !']})
    assert 'secret-value' not in json.dumps(result)
    assert result['phase'] is None and result['quality'] == 'PASS'
    assert result['customer']['retry_attempts'] is None
    assert len(json.dumps(result)) < 1500


def test_retained_success_audits_and_does_not_mutate_inputs():
    report, raw = records()
    before = copy.deepcopy((report, raw))
    assert brief.audit(report, raw)['pass']
    assert (report, raw) == before
    raw['pass'] = False
    assert not brief.audit(report, raw)['pass']


@pytest.mark.parametrize('failure', ['counts', 'duplicate', 'queue', 'cleanup', 'missing_financial', 'raw_identity', 'pending_callback', 'relationship'])
def test_bad_or_missing_evidence_fails_closed(failure):
    report, raw = records()
    if failure == 'counts': report['financial']['counts']['tickets'] = 3
    if failure == 'duplicate': report['financial']['counts']['duplicate_booked_seats'] = 1
    if failure == 'queue': report['queues_after_restoration']['kafka_total_lag'] = 1
    if failure == 'cleanup': report['restoration']['cleanup_complete'] = False
    if failure == 'missing_financial': report.pop('financial')
    if failure == 'raw_identity': raw['run'] = 'other'
    if failure == 'pending_callback': report['financial']['counts']['incomplete_callback_deliveries'] = 1
    if failure == 'relationship': report['financial']['relationships']['booking_relationship_errors'] = 1
    assert not brief.audit(report, raw)['pass']


def test_operational_recovery_never_promotes_failed_quality():
    report, raw = records()
    raw['pass'] = raw['integrity_verified'] = False
    report.update(status='FAILED_RESTORED', short_capacity_qualified=False)
    report['raw_result_sha256'] = hashlib.sha256(json.dumps(raw, sort_keys=True, separators=(',', ':')).encode()).hexdigest()
    recovery = {'run': raw['run'], 'original_result_sha256': report['raw_result_sha256'], 'pass': True,
                'capacity_qualified': False, 'remote_data_mutations': 0, 'gates': {'financial': True},
                'financial': copy.deepcopy(report['financial'])}
    assert brief.audit(report, raw, recovery)['pass']
    assert brief.compact(report)['quality'] == 'FAIL'
    assert not brief.audit(report, raw)['pass']


def test_watch_suppresses_unchanged_counts_and_stops_on_terminal():
    report, _ = records()
    now, emitted = [0], []
    def snapshot():
        value = copy.deepcopy(report)
        if now[0] < 3:
            value['status'] = 'RUNNING'
            value['customer']['fulfilled'] = now[0]
        return brief.compact(value)
    def pause(seconds): now[0] += seconds
    assert brief.watch(snapshot, emitted.append, seconds=10, interval=1, pause=pause, clock=lambda: now[0]) == 0
    assert len(emitted) == 2 and emitted[-1]['status'] == 'PASSED_RESTORED'


def test_watch_timeout_is_not_a_load_failure_or_cleanup_success():
    now, emitted = [0], []
    def pause(seconds): now[0] += seconds
    assert brief.watch(lambda: brief.compact({}), emitted.append, seconds=2, interval=1,
                       pause=pause, clock=lambda: now[0]) == 2
    assert emitted[-1]['monitor'] == 'TIMEOUT' and emitted[-1]['quality'] == 'UNKNOWN'


def test_malformed_file_is_unknown(tmp_path):
    path = tmp_path / 'malformed.json'
    path.write_text('{not-json')
    assert brief.read(path) == {} and brief.read(tmp_path / 'missing') == {}


def test_raw_window_rate_is_derived_without_a_hardcoded_duration():
    result = brief.compact({'issuance': {'window_start_epoch': 100, 'window_end_epoch': 400,
                                        'conservative_inner_window_tickets': 47419}})
    assert result['conservative_tickets_per_second'] == 158.063

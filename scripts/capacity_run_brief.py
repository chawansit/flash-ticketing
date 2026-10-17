"""ADR0340 local summaries/watch/evidence checks; no cloud calls or ledger writes."""
import argparse
import hashlib
import json
import math
import re
import time
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
STATUSES = {'PASSED_RESTORED', 'FAILED_RESTORED', 'RECOVERY_REQUIRED'}


def read(path):
    if path is None or not path.exists():
        return {}
    try:
        if path.stat().st_size > 64 * 1024 * 1024:
            return {}
        value = json.loads(path.read_text(encoding='utf-8'))
        return value if isinstance(value, dict) else {}
    except (OSError, ValueError):
        return {}


def number(value):
    return value if type(value) in (int, float) and math.isfinite(value) else None


def compact(report, stage=None, native=None, phases=None):
    stage = report.get('paid_stage', {}) or stage or report
    native = report.get('native', {}) or native or {}
    customer = stage.get('customer', {})
    issuance = stage.get('ticket_target_issuance', {}) or report.get('issuance', {})
    run = report.get('run')
    run = run if isinstance(run, str) and re.fullmatch(r'adr0151-[0-9a-f]{12}', run) else None
    status = report.get('status')
    if status not in STATUSES:
        status = ('PASSED_RESTORED' if report.get('pass') is True else
                  'FAILED_RESTORED' if report.get('integrity_verified') is True else 'RECOVERY_REQUIRED') if report.get('restoration_complete') is True else 'RUNNING'
    phase = (phases or {}).get('phases', [])
    phase = phase[-1] if isinstance(phase, list) and phase else None
    phase = phase if isinstance(phase, str) and re.fullmatch(r'[a-z0-9_-]{1,80}', phase) else None
    fields = ('scheduled', 'dispatched', 'fulfilled', 'generator_drops', 'first_attempt_error_journeys',
              'recovered_journeys', 'retry_attempts', 'final_customer_failures')
    latency = report.get('latencies_ms', customer)
    rate = number(issuance.get('conservative_rate_per_second'))
    start, end = number(issuance.get('window_start_epoch')), number(issuance.get('window_end_epoch'))
    count = number(issuance.get('conservative_inner_window_tickets'))
    if rate is None and count is not None and start is not None and end is not None and end > start:
        rate = count / (end - start)
    p95 = number(latency.get('total_journey_p95_ms'))
    return {'run': run, 'status': status, 'phase': phase,
            'customer': {k: number(customer.get(k)) for k in fields},
            'conservative_tickets': number(issuance.get('conservative_inner_window_tickets')),
            'conservative_tickets_per_second': round(rate, 3) if rate is not None else None,
            'journey_p95_ms': round(p95, 2) if p95 is not None else None,
            'cleanup_complete': native.get('cleanup_complete') is True or report.get('restoration', {}).get('cleanup_complete') is True,
            'quality': 'PASS' if status == 'PASSED_RESTORED' and report.get('short_capacity_qualified', True) is True else 'FAIL' if status in STATUSES else 'PENDING'}


def audit(report, raw, recovery=None):
    """Verify retained evidence only; never infer financial health from HTTP success."""
    financial = report.get('financial', {})
    counts = financial.get('counts', {})
    relationships = financial.get('relationships', {})
    restore = report.get('restoration', {})
    queues = report.get('queues_after_restoration', {})
    canonical = hashlib.sha256(json.dumps(raw, sort_keys=True, separators=(',', ':'), allow_nan=False).encode()).hexdigest() if raw else None
    tickets = number(counts.get('tickets'))
    recovery = recovery or {}
    independent = (recovery.get('original_result_sha256') == canonical and recovery.get('run') == raw.get('run')
                   and recovery.get('pass') is True and recovery.get('capacity_qualified') is False
                   and recovery.get('remote_data_mutations') == 0 and bool(recovery.get('gates'))
                   and all(v is True for v in recovery['gates'].values())
                   and recovery.get('financial', {}).get('counts') == counts)
    original_financial = raw.get('paid_stage', {}).get('financial', {})
    checks = {
        'raw_hash': bool(raw) and canonical == report.get('raw_result_sha256'),
        'run_identity': bool(raw) and raw.get('run') == report.get('run'),
        'raw_restoration': raw.get('restoration_complete') is True and raw.get('native', {}).get('cleanup_complete') is True and raw.get('transport_credentials_cleared') is True,
        'independent_recovery_if_needed': raw.get('integrity_verified') is True or independent,
        'financial_source': (bool(counts) and original_financial.get('counts') == counts) or independent,
        'financial_audit': financial.get('pass') is True or financial.get('strict_audit_pass') is True,
        'post_ttl': financial.get('hold_deadlines_elapsed') is True,
        'paid_ticket_counts': tickets is not None and all(number(counts.get(k)) == tickets for k in ('succeeded_payments', 'bookings', 'fulfilled_orders')) and number(relationships.get('unique_issued_tickets')) == tickets,
        'no_duplicates': counts.get('duplicate_booked_seats') == 0 and counts.get('multi_booking_orders') == 0,
        'no_pending_financial_work': all(counts.get(k) == 0 for k in ('pending_orders', 'pending_payment_attempts', 'incomplete_callback_deliveries', 'unpublished_outbox', 'dead_letters')),
        'relationships': all(relationships.get(k) == 0 for k in ('hold_relationship_errors', 'booking_relationship_errors', 'order_item_relationship_errors', 'inventory_relationship_errors', 'fulfilled_ticket_relationship_errors', 'holds_not_past_ttl')),
        'queues': queues.get('pass') is True and all(v == 0 for k, v in queues.items() if type(v) is int and k not in {'kafka_members', 'projection_kafka_members'}),
        'restoration': restore.get('original_runtime_restored') is True and (restore.get('integrity_verified') is True or restore.get('fresh_independent_integrity_verified') is True) and all(restore.get(k) is True for k in ('generator_idle', 'namespace_removed', 'cleanup_complete', 'transport_credentials_cleared')),
        'quality_classification': (report.get('status') == 'PASSED_RESTORED') == (report.get('short_capacity_qualified') is True) and bool(raw) and (raw.get('pass') is True) == (report.get('short_capacity_qualified') is True),
    }
    return {'pass': all(checks.values()), 'checks': checks,
            'scope': 'Retained evidence only; no database audit executed; failed load quality is unchanged.'}


def watch(snapshot, emit, *, seconds, interval, pause=time.sleep, clock=time.monotonic):
    deadline, previous = clock() + seconds, None
    while True:
        brief = snapshot()
        c = brief['customer']
        signature = (brief['status'], brief['phase'], bool(c['generator_drops']), bool(c['first_attempt_error_journeys']), bool(c['final_customer_failures']))
        if signature != previous:
            emit(brief)
            previous = signature
        if brief['status'] in STATUSES:
            return 0
        if clock() >= deadline:
            emit({'monitor': 'TIMEOUT', 'quality': 'UNKNOWN', 'action': 'Inspect the existing run; do not launch another stage.'})
            return 2
        pause(min(interval, max(0, deadline - clock())))


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    source = parser.add_mutually_exclusive_group(required=True)
    source.add_argument('--report', type=Path)
    source.add_argument('--run-dir', type=Path)
    parser.add_argument('--raw-report', type=Path)
    parser.add_argument('--audit', action='store_true')
    parser.add_argument('--watch', action='store_true')
    parser.add_argument('--seconds', type=int, default=3600)
    parser.add_argument('--interval', type=int, default=10)
    args = parser.parse_args()
    if not 1 <= args.seconds <= 3600 or not 1 <= args.interval <= 60:
        parser.error('Use1..3600seconds and1..60second polling interval')
    if args.audit and (not args.report or not args.raw_report or args.watch):
        parser.error('--audit requires --report and --raw-report, without --watch')
    paths = [p for p in (args.report, args.run_dir, args.raw_report) if p]
    if any(not p.resolve().is_relative_to(ROOT.resolve()) for p in paths):
        parser.error('Use explicit paths inside this repository')

    def snapshot():
        if args.report:
            return compact(read(args.report))
        directory = args.run_dir
        report = read(directory / 'cce-comparison.private.json')
        report.setdefault('run', directory.name)
        native = read(directory / 'native-lifecycle.private.json')
        return compact(report, read(directory / 'candidate/stage.private.json'), native.get('lifecycle', native), read(directory / 'state.json'))

    def emit(value):
        print(json.dumps(value, separators=(',', ':'), allow_nan=False), flush=True)

    if args.watch:
        return watch(snapshot, emit, seconds=args.seconds, interval=args.interval)
    brief = snapshot()
    if args.report and not read(args.report):
        emit({'input': 'MISSING_OR_INVALID', 'quality': 'UNKNOWN'})
        return 1
    if args.audit:
        report = read(args.report)
        name = report.get('operational_recovery_evidence')
        recovery_path = ROOT / name if isinstance(name, str) else None
        recovery = read(recovery_path) if recovery_path and recovery_path.resolve().is_relative_to(ROOT.resolve()) else {}
        brief['evidence_audit'] = audit(report, read(args.raw_report), recovery)
    emit(brief)
    return 1 if args.audit and not brief['evidence_audit']['pass'] else 0


if __name__ == '__main__':
    raise SystemExit(main())

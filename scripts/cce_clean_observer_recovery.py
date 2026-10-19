"""ADR0369 exact dispatched-cohort recovery; original quality remains failed."""
import copy
import math
from datetime import UTC, datetime
from pathlib import Path

import work_envelope as policy
from cce_replacement_rds_binding import NEW_SHA

RUN = 'adr0151-4163debac412'
LEDGER = 'bounded_cce_ticket_target__6956d6defb6a'
RESULT = 'd5f7ecb029af526d74a279e3031e2d29ea2f919f0adc3e6110b5a608959e346a'
DISPATCHED = 54000
PAID = 54000
GATES = ('runtime_restored', 'runtime_stable', 'generator_idle', 'queues_zero',
         'namespace_removed', 'owned_resources_removed', 'transport_credentials_cleared',
         'private_cleanup', 'dispatched_financial', 'original_failure_preserved')
PAID_FIELDS = ('fulfilled_orders', 'payment_attempts', 'succeeded_payments',
               'bookings', 'tickets', 'payment_callbacks', 'callback_delivery_attempts', 'callback_delivery_target')
ZERO_FIELDS = ('pending_orders', 'pending_payment_attempts',
               'incomplete_callback_deliveries', 'duplicate_booked_seats', 'multi_booking_orders',
               'unpublished_outbox', 'dead_letters')


def validate(report, entry, scope, evidence):
    stage = report.get('paid_stage', {})
    customer = stage.get('customer', {})
    financial = evidence.get('fresh_financial', {})
    counts = financial.get('counts', {})
    relations = financial.get('relationships', {})
    at = datetime.fromisoformat(evidence['verified_at_utc'])
    elapsed = evidence.get('actual_elapsed_seconds')
    native = report.get('native', {})
    restoration = report.get('restoration', {})
    valid = (
        report.get('run') == RUN and policy.digest(report) == RESULT and report.get('pass') is False,
        entry.get('ledger') == LEDGER and entry.get('status') == 'RECOVERY_REQUIRED',
        entry.get('result_sha256') == scope.get('cce_paid_result_sha256') == RESULT,
        report.get('binding_sha256') == entry.get('binding_sha256') == policy.digest(scope.get('binding', {})),
        scope.get('paid_runs_started') == 1,
        customer.get('scheduled') == 54000 and customer.get('dispatched') == DISPATCHED and customer.get('fulfilled') == PAID,
        customer.get('generator_drops') == 0 and customer.get('first_attempt_error_journeys') == 0,
        customer.get('recovered_journeys') == 0 and customer.get('final_customer_failures') == 0,
        stage.get('financial', {}).get('pass') is True,
        stage.get('financial', {}).get('expected') == 54000,
        stage.get('financial', {}).get('hold_deadlines_elapsed') is True,
        stage.get('ticket_target_issuance', {}).get('pass') is True,
        native.get('cleanup_complete') is True and native.get('cleanup_failures') == [],
        native.get('namespace_cleanup', {}).get('namespace_removed') is True,
        native.get('resource_cleanup', {}).get('owned_resources_removed') is True,
        report.get('transport_credentials_cleared') is True and stage.get('private_cleanup_pass') is True,
        restoration.get('primary_runtime_semantics_restored') is True and restoration.get('generator_idle_after') is True,
        evidence.get('decision') == 'ADR0369' and evidence.get('run') == RUN,
        evidence.get('ledger') == LEDGER and evidence.get('original_result_sha256') == RESULT,
        evidence.get('binding_sha256') == entry.get('binding_sha256'),
        evidence.get('replacement_snapshot_sha256') == NEW_SHA,
        evidence.get('gates') == dict.fromkeys(GATES, True),
        evidence.get('pass') is True and evidence.get('capacity_qualified') is False,
        at.tzinfo is not None and 0 <= (datetime.now(UTC)-at).total_seconds() <= 300,
        type(elapsed) in (int, float) and math.isfinite(elapsed) and 0 < elapsed <= 900,
        financial.get('pass') is True and financial.get('hold_deadlines_elapsed') is True,
        counts.get('expected') == counts.get('orders') == DISPATCHED and counts.get('expected_paid') == PAID and counts.get('expired_orders') == 0,
        all(counts.get(k) == PAID for k in PAID_FIELDS),
        all(counts.get(k) == 0 for k in ZERO_FIELDS),
        relations.get('unique_issued_tickets') == PAID and relations.get('holds_not_past_ttl') == 0,
        all(relations.get(k) == 0 for k in ('hold_relationship_errors','booking_relationship_errors',
             'order_item_relationship_errors','inventory_relationship_errors','fulfilled_ticket_relationship_errors')),
        all(financial.get('checks', {}).get(k) is True for k in
            ('post_ttl_complete','payments_durable','ticket_relationships_valid')),
        evidence.get('fixture_sha256') == stage.get('fixture_identity', {}).get('fixture_identity_sha256'),
    )
    if not all(valid):
        raise ValueError('Exact original failed qualification and independent dispatched-cohort recovery required')
    return True


def register(path):
    path = Path(path)
    if path.is_symlink() or not path.resolve().is_relative_to((policy.ROOT/'docs/capacity/cce').resolve()):
        raise ValueError('Owned public recovery receipt required')
    report = policy.read(policy.ROOT/'tmp'/RUN/'cce-comparison.private.json')
    records, state = policy.journal(policy.envelope()), policy.read(policy.STATE)
    entries = [e for e in records['experiments'] if e.get('ledger') == LEDGER]
    if len(entries) != 1 or policy.LOCK.exists():
        raise ValueError('One stopped exact recovery scope required')
    entry, scope = entries[0], state[LEDGER]
    if state.get('current_run') != RUN or scope.get('active_run') != RUN:
        raise ValueError('Only this consumed scope can be cleared')
    evidence = policy.read(path)
    validate(report, entry, scope, evidence)
    recovery = {'decision':'ADR0369','evidence':path.relative_to(policy.ROOT).as_posix(),
                'sha256':policy.digest(evidence),'actual_elapsed_seconds':evidence['actual_elapsed_seconds'],
                'capacity_qualified':False}
    entry.update(initial_status=entry['status'],initial_actual_elapsed_seconds=entry['actual_elapsed_seconds'],
                 status='FAILED_RESTORED',recovery=copy.deepcopy(recovery))
    entry['actual_elapsed_seconds'] += evidence['actual_elapsed_seconds']
    scope['qualification_terminal_recovery'] = copy.deepcopy(recovery)
    scope['active_run'] = None
    if 'recovery_required' in scope: scope['recovery_required'] = False
    state['current_run'] = None
    for target,value in ((policy.JOURNAL,records),(policy.STATE,state)):
        target.write_bytes((__import__('json').dumps(value,indent=2)+'\n').encode())
    return {'status':'FAILED_RESTORED','original_result_preserved':True,'capacity_qualified':False}

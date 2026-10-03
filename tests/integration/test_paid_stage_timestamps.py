from datetime import UTC, datetime, timedelta
from uuid import uuid4

import pytest

from scripts.analyze_paid_stage_timestamps import analyze
from ticketing.workers import consume_event

pytestmark = pytest.mark.integration


def test_stage_timestamps_isolate_delays_and_do_not_change_payment_state(system):
    svc, db, show = system
    hold = svc.reserve('buyer', show, ['A'], 'timed')
    attempt = svc.initiate_payment('buyer', hold['order_id'], 'pay', 'SUCCEEDED', 0, 1)
    callback_id = uuid4()
    svc.callback({'callback_id': str(callback_id), 'payment_id': attempt['payment_id'],
                  'order_id': hold['order_id'], 'amount': 100, 'currency': 'THB', 'outcome': 'SUCCEEDED'})
    with db.transaction() as conn:
        paid = conn.execute("SELECT * FROM outbox_events WHERE event_type='OrderPaid'").fetchone()
    consume_event(db, None, dict(paid, event_id=str(paid['id'])))
    start = datetime(2026, 10, 3, tzinfo=UTC)
    with db.transaction() as conn:
        conn.execute('UPDATE orders SET created_at=%s', (start,))
        conn.execute('UPDATE payment_attempts SET due_at=%s', (start+timedelta(seconds=1),))
        conn.execute('UPDATE payment_callbacks SET created_at=%s', (start+timedelta(seconds=4),))
        conn.execute("UPDATE outbox_events SET occurred_at=%s,published_at=%s WHERE event_type='OrderPaid'",
                     (start+timedelta(seconds=5), start+timedelta(seconds=7)))
        conn.execute('UPDATE tickets SET issued_at=%s', (start+timedelta(seconds=12),))
    with db.pool.connection() as conn:
        result = analyze(conn, [show], 1, 1)
    assert result['cohort_pass']
    assert result['hold_deadlines_elapsed'] is False
    assert result['latest_hold_deadline'] is not None
    assert result['observed_at_utc'].endswith('+00:00')
    assert result['timings']['payment_due_to_callback']['p95_ms'] == 3000
    assert result['timings']['paid_event_to_publication_record']['p95_ms'] == 2000
    assert result['timings']['publication_record_to_ticket']['p95_ms'] == 5000
    assert result['timings']['payment_due_to_ticket']['p95_ms'] == 11000
    assert svc.get_order('buyer', hold['order_id'])['status'] == 'FULFILLED'
    with db.pool.connection() as conn:
        wrong_expected = analyze(conn, [show], 2, 2)
    assert not wrong_expected['cohort_pass']
    with db.transaction() as conn:
        conn.execute("UPDATE outbox_events SET published_at=%s WHERE event_type='OrderPaid'",
                     (start+timedelta(seconds=13),))
    with db.pool.connection() as conn:
        early_ticket = analyze(conn, [show], 1, 1)
    assert early_ticket['cohort_pass']
    assert early_ticket['timings']['publication_record_to_ticket']['negative_samples'] == 1
    assert early_ticket['timings']['publication_record_to_ticket']['mean_ms'] == -1000


def test_stage_timestamps_missing_payment_is_visible_and_fixture_scoped(system):
    svc, db, show = system
    svc.reserve('buyer', show, ['A'], 'unpaid')
    with db.pool.connection() as conn:
        result = analyze(conn, [show], 1, 0)
    assert result['cohort_pass']
    assert result['timings']['payment_due_to_ticket']['samples'] == 0
    assert result['timings']['payment_due_to_ticket']['p95_ms'] is None
    with db.pool.connection() as conn:
        empty = analyze(conn, [uuid4()], 1, 0)
    assert not empty['cohort_pass']
    assert empty['orders'] == 0


@pytest.mark.parametrize('shows,total,paid', [([], 1, 0), ([uuid4()], 0, 0), ([uuid4()], 1, 2)])
def test_stage_timestamps_rejects_unbounded_or_invalid_inputs(shows, total, paid):
    with pytest.raises(ValueError):
        analyze(None, shows, total, paid)

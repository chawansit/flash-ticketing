"""Actual financial callbacks off/on; separate candidate, no cloud qualification."""
import hashlib
import json
import os
import time
from dataclasses import replace
from pathlib import Path
from statistics import median
from uuid import uuid4

import pytest
from test_payment_commit_recovery import process  # noqa: F401 - pytest fixture
from test_payment_commit_recovery import (
    test_callback_acknowledgement_failure_retains_lease_then_replays_exactly as acknowledgement_scenario,
)
from test_payment_commit_recovery import (
    test_lost_callback_response_commits_once_then_recovers_through_existing_lease as lost_response_scenario,
)
from test_reservations import (
    payment,
)
from test_reservations import (
    test_database_unique_booking_backstop as unique_booking_scenario,
)
from test_reservations import (
    test_duplicate_payment_and_duplicate_fulfillment as duplicate_scenario,
)
from test_reservations import (
    test_failure_then_success_refunds_and_mismatch_rolls_back as failure_scenario,
)
from test_reservations import (
    test_late_payment_cannot_take_new_hold as late_scenario,
)
from test_reservations import (
    test_payment_races_expired_seat_reclamation as reclamation_scenario,
)
from test_reservations import (
    test_simultaneous_duplicate_callbacks as concurrent_scenario,
)

from ticketing import api
from ticketing.config import Settings
from ticketing.infrastructure.postgres import MeasuredCursor

pytestmark = pytest.mark.integration


@pytest.mark.parametrize('enabled', [False, True])
@pytest.mark.parametrize('scenario', [duplicate_scenario, late_scenario, failure_scenario,
                                     unique_booking_scenario, reclamation_scenario, concurrent_scenario])
def test_existing_financial_scenarios(system, enabled, scenario):
    _, db, _ = system
    db.payment_callback_write_pipeline = enabled
    scenario(system)


@pytest.mark.parametrize('enabled', [False, True])
def test_queued_outbox_failure_rolls_back_every_financial_write(system, enabled):
    svc, db, show = system
    db.payment_callback_write_pipeline = enabled
    order = svc.reserve('owner', show, ['A'], 'hold')
    payload = payment(svc, order, 'owner')
    with db.transaction() as conn:
        conn.execute("""CREATE FUNCTION reject_paid_event() RETURNS trigger AS $$
            BEGIN IF NEW.event_type='OrderPaid' THEN RAISE EXCEPTION 'intentional outbox failure';
            END IF; RETURN NEW; END $$ LANGUAGE plpgsql""")
        conn.execute('CREATE TRIGGER reject_paid_event BEFORE INSERT ON outbox_events '
                     'FOR EACH ROW EXECUTE FUNCTION reject_paid_event()')
    with pytest.raises(Exception, match='intentional outbox failure'):
        svc.callback(payload)
    with db.transaction() as conn:
        assert conn.execute('SELECT count(*) AS n FROM bookings').fetchone()['n'] == 0
        assert conn.execute('SELECT count(*) AS n FROM payment_callbacks').fetchone()['n'] == 0
        assert conn.execute("SELECT status FROM orders WHERE id=%s", (order['order_id'],)).fetchone()['status'] == 'PENDING'
        assert conn.execute("SELECT status FROM payment_attempts WHERE id=%s", (payload['payment_id'],)).fetchone()['status'] == 'PENDING'
        conn.execute('DROP TRIGGER reject_paid_event ON outbox_events')
    assert svc.callback(payload)['status'] == 'book'
    assert svc.callback(payload)['status'] == 'duplicate'


@pytest.mark.parametrize('scenario', [acknowledgement_scenario, lost_response_scenario])
def test_opt_in_signed_http_recovery(system, process, monkeypatch, scenario):  # noqa: F811 - injected pytest fixture
    monkeypatch.setattr(api, 'settings', replace(api.settings, payment_callback_write_pipeline=True))
    process.restart()
    assert api.app.state.payment_db.payment_callback_write_pipeline is True
    scenario(system, process)


def test_default_off_and_unsupported_async_combination():
    assert Settings.__dataclass_fields__['payment_callback_write_pipeline'].default is False
    with pytest.raises(RuntimeError, match='synchronous confirmation'):
        replace(Settings(), payment_callback_write_pipeline=True, payment_confirmation_async=True).validate()


def test_bounded_actual_callback_comparison(system, monkeypatch):
    receipt = os.getenv('LOCAL_CALLBACK_BENCHMARK_RECEIPT')
    if not receipt:
        pytest.skip('optional bounded actual callback comparison')
    svc, db, show = system
    root = Path(__file__).resolve().parents[2]
    path = Path(receipt).resolve()
    assert path.is_relative_to((root/'tmp').resolve()) and not path.exists()
    samples = {'off': [], 'on': []}
    counts = {'off': [], 'on': []}
    for trial in range(6):
        arms = [False, True] if trial % 2 == 0 else [True, False]
        for enabled in arms:
            name = 'on' if enabled else 'off'
            prefix = f'bench-{trial}-{name}-'
            with db.transaction() as conn:
                conn.execute("""INSERT INTO event_seats(event_id,seat_id,price)
                    SELECT %s,%s||i::text,100 FROM generate_series(1,64)i""", (show, prefix))
            cohort = []
            for i in range(1,65):
                actor = prefix+str(i)
                order = svc.reserve(actor, show, [actor], str(uuid4()))
                cohort.append(payment(svc, order, actor))
            db.payment_callback_write_pipeline = enabled
            statements = {}
            original = MeasuredCursor.execute
            def measured(cursor, query, params=None, *, _counts=statements, _execute=original, **kwargs):
                command = query.lstrip().split()[0].upper() if isinstance(query,str) else 'OTHER'
                _counts[command] = _counts.get(command,0)+1
                return _execute(cursor,query,params,**kwargs)
            with monkeypatch.context() as context:
                context.setattr(MeasuredCursor,'execute',measured)
                started = time.perf_counter()
                for payload in cohort:
                    assert svc.callback(payload)['status'] == 'book'
                elapsed = time.perf_counter()-started
            samples[name].append(elapsed*1000/64)
            counts[name].append(statements)
            with db.transaction() as conn:
                identifiers = [payload['payment_id'] for payload in cohort]
                assert conn.execute("SELECT count(*) AS n FROM payment_attempts WHERE id=ANY(%s::uuid[]) AND status='SUCCEEDED'", (identifiers,)).fetchone()['n'] == 64
                assert conn.execute('SELECT count(*) AS n FROM payment_callbacks WHERE payment_id=ANY(%s::uuid[])', (identifiers,)).fetchone()['n'] == 64
    assert all(value == counts['off'][0] for values in counts.values() for value in values)
    medians = {name: median(values) for name,values in samples.items()}
    result = {'decision':'ADR0346','scope':'Six alternating64-callback trials per arm; actual PostgreSQL transaction; no HTTP delay or cloud contention.',
              'callback_transaction_ms':samples,'median_ms':medians,
              'local_reduction_percent':(1-medians['on']/medians['off'])*100,
              'statements_per64_callbacks':counts['off'][0],
              'financial_statements_unchanged':True,'successful_callbacks_each_arm':384,
              'local_improvement_demonstrated':medians['on']<medians['off'],
              'source_sha256':{name:hashlib.sha256((root/name).read_bytes().replace(b'\r\n',b'\n')).hexdigest()
                               for name in ('src/ticketing/api.py','src/ticketing/config.py','src/ticketing/infrastructure/reservations.py')},
              'cloud_calls':0,'cloud_capacity_improvement_measured':False}
    path.write_bytes((json.dumps(result,indent=2)+'\n').encode())

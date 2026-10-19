"""Exercise ADR0371 against real PostgreSQL using the unchanged application image."""
import os
import time
from concurrent.futures import ThreadPoolExecutor
from threading import Event

import pytest
from psycopg_pool import TooManyRequests

from ticketing.infrastructure.postgres import api_pool_budgets, create_api_databases


@pytest.mark.parametrize(('payment_maximum', 'waiting', 'successes'), [(3, 20, 20), (4, 19, 21)])
def test_payment_burst_preserves_general_reads(payment_maximum, waiting, successes):
    general, payment = create_api_databases(
        os.environ['TEST_DATABASE_URL'], 6, 500, waiting, payment_maximum,
        shared_waiting=True, reclaim_partial_timeouts=True,
        failure_diagnostics=True, explicit_start=True,
    )
    release = Event()
    entered = [Event() for _ in range(payment_maximum)]

    def transaction(held=None):
        try:
            with payment.transaction() as connection:
                assert connection.execute('SELECT 1 AS value').fetchone()['value'] == 1
                if held is not None:
                    held.set()
                    assert release.wait(2)
            return True
        except TooManyRequests:
            return False

    try:
        # Grow and warm both actual native pools before the bounded burst.
        for database, maximum in ((general, 6-payment_maximum), (payment, payment_maximum)):
            connections = [database.pool.getconn() for _ in range(maximum)]
            for connection in connections:
                database.pool.putconn(connection)
        with ThreadPoolExecutor(max_workers=22) as executor:
            held = [executor.submit(transaction, event) for event in entered]
            assert all(event.wait(1) for event in entered)
            queued = [executor.submit(transaction) for _ in range(21-payment_maximum)]
            try:
                deadline = time.monotonic()+.3
                while payment.pool.get_stats()['requests_waiting'] != 17:
                    assert time.monotonic() < deadline
                    time.sleep(.001)
                if payment_maximum == 3:
                    while not any(future.done() for future in queued):
                        assert time.monotonic() < deadline
                        time.sleep(.001)
                    assert sum(future.done() and future.result() is False for future in queued) == 1
                # Payment pressure must leave a real general SQL transaction available.
                with general.transaction() as connection:
                    assert connection.execute('SELECT 1 AS value').fetchone()['value'] == 1
                assert payment._shared_acquisition_budget.snapshot()['used'] == 17
                assert payment.pool.get_stats()['pool_max'] == payment_maximum
                assert general.pool.get_stats()['pool_max'] == 6-payment_maximum
            finally:
                release.set()
            assert sum(future.result() for future in held+queued) == successes
        assert payment._shared_acquisition_budget.snapshot()['used'] == 0
        budgets = api_pool_budgets(6, waiting, payment_maximum, True)
        assert budgets['payment']['maximum_waiting'] == 17
        assert max(item['maximum_waiting'] for item in budgets.values()) == 17
        assert sum(item['maximum'] for item in budgets.values()) == 6
    finally:
        release.set()
        payment.close()
        general.close()

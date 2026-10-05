"""Real FIFO pruning: expired prefix, live tail and fixed-budget admission."""
import threading
import time
from concurrent.futures import ThreadPoolExecutor

import pytest
from psycopg_pool import PoolTimeout, TooManyRequests

from ticketing.infrastructure.postgres import create_api_databases

pytestmark = pytest.mark.integration


def wait_until(predicate, seconds=2):
    deadline = time.monotonic() + seconds
    while not predicate():
        assert time.monotonic() < deadline, "Controlled queue boundary not reached"
        time.sleep(.001)


def exercise_partial_recovery(system, candidate=False):
    _, db, _ = system
    extra = {"reclaim_partial_timeouts": True} if candidate else {}
    general, payment = create_api_databases(db.pool.conninfo, 4, 1000, 8, 2, True, **extra)
    pool = payment.pool
    pool.resize(2, 2)
    pool.wait(timeout=10)
    budget = payment._shared_acquisition_budget
    held = []
    release = threading.Event()
    acquired = threading.Event()
    long = []
    results = {}
    def live_checkout():
        conn = pool.getconn(timeout=5)
        try:
            acquired.set()
            assert release.wait(timeout=5), "Test failed to release live checkout"
            assert conn.execute("SELECT 1 AS n").fetchone()["n"] == 1
        finally:
            pool.putconn(conn)
    try:
        with ThreadPoolExecutor(max_workers=8) as executor:
            try:
                held = [pool.getconn(), pool.getconn()]
                expired = [executor.submit(pool.getconn, timeout=.5) for _ in range(4)]
                wait_until(lambda: pool.get_stats()["requests_waiting"] == 4)
                long = [executor.submit(live_checkout) for _ in range(2)]
                wait_until(lambda: pool.get_stats()["requests_waiting"] == 6)
                for f in expired:
                    with pytest.raises(PoolTimeout):
                        f.result(timeout=2)
                results["before"] = budget.snapshot()
                assert results["before"]["retained"] == 4
                assert pool.get_stats()["requests_waiting"] == 6
                pool.putconn(held.pop())
                assert acquired.wait(timeout=2)
                wait_until(lambda: pool.get_stats()["requests_waiting"] == 1)
                results["after"] = budget.snapshot()
                results["native_waiting_after_pruning"] = 1
                # The remaining physical FIFO has one live client. No private
                # queue inspection/mutation or extra connection is used.
                long.append(executor.submit(live_checkout))
                wait_until(lambda: pool.get_stats()["requests_waiting"] == 2)
                probe = executor.submit(live_checkout)
                if candidate:
                    long.append(probe)
                    wait_until(lambda: pool.get_stats()["requests_waiting"] == 3)
                    results["additional_checkout"] = "admitted"
                    assert budget.snapshot()["used"] <= 8
                    assert budget.snapshot()["counts"]["payment"] <= 6
                else:
                    with pytest.raises(TooManyRequests):
                        probe.result(timeout=2)
                    results["additional_checkout"] = "rejected"
                with general.connection() as conn:
                    assert conn.execute("SELECT 1 AS n").fetchone()["n"] == 1
            finally:
                release.set()
                for conn in held:
                    pool.putconn(conn)
                held.clear()
                for f in long:
                    f.result(timeout=5)
        wait_until(lambda: pool.get_stats()["requests_waiting"] == 0)
        assert budget.snapshot()["used"] == 0
        assert pool.get_stats()["pool_available"] == 2
        assert general.pool.max_size + payment.pool.max_size == 4
        return results
    finally:
        payment.close()
        general.close()


def test_baseline_retains_pruned_timeout_slots_and_rejects_with_live_tail(system):
    result = exercise_partial_recovery(system)
    assert result["after"]["retained"] == 4
    assert result["after"]["acquiring"] == 1
    assert result["native_waiting_after_pruning"] == 1
    assert result["additional_checkout"] == "rejected"


def test_candidate_reclaims_excess_after_real_fifo_pruning_and_preserves_live_tail(system):
    result = exercise_partial_recovery(system, candidate=True)
    assert result["after"]["retained"] == 1
    assert result["after"]["acquiring"] == 1
    assert result["additional_checkout"] == "admitted"

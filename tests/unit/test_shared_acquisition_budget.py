import threading
from concurrent.futures import ThreadPoolExecutor
from dataclasses import replace
from unittest.mock import Mock

import pytest
from psycopg_pool import PoolTimeout, TooManyRequests

from ticketing.config import Settings
from ticketing.infrastructure.postgres import (
    AcquisitionLimitedPool,
    SharedAcquisitionBudget,
    api_pool_budgets,
    create_api_databases,
)


class Native:
    timeout = .15
    def __init__(self):
        self.waiting = 0
        self.error = None
        self.timeout_received = None
        self.closed = False
    def get_stats(self):
        return {"requests_waiting":self.waiting,"requests_num":1,"requests_errors":0}
    def getconn(self,timeout=None):
        self.timeout_received = timeout
        if self.error:
            raise self.error
        return object()
    def putconn(self,conn):
        self.waiting = 0
    def close(self):
        self.waiting = 0
        self.closed = True

def setup():
    natives = {"general":Native(),"payment":Native()}
    budget = SharedAcquisitionBudget(12,{"general":10,"payment":10},natives)
    return budget,natives,{role:AcquisitionLimitedPool(pool,budget,role) for role,pool in natives.items()}

@pytest.mark.parametrize("payment,waiting",[(0,12),(2,3)])
def test_invalid_shared_guard_settings_and_factory_before_resources(monkeypatch,payment,waiting):
    import ticketing.infrastructure.postgres as pg
    constructor = Mock()
    monkeypatch.setattr(pg,"Postgres",constructor)
    with pytest.raises(ValueError,match="Shared"):
        create_api_databases("unused",4,150,waiting,payment,True)
    constructor.assert_not_called()
    with pytest.raises(RuntimeError,match="API_POOL_SHARED_WAITING"):
        replace(Settings(),pool_max=4,pool_max_waiting=waiting,simulator_concurrency=1,
                api_payment_pool_max=payment,api_pool_shared_waiting=True).validate()

def test_overlapping_native_limits_and_shared_default():
    assert not Settings().api_pool_shared_waiting
    assert api_pool_budgets(4,12,2,True)=={
        "general":{"maximum":2,"maximum_waiting":10},
        "payment":{"maximum":2,"maximum_waiting":10},
    }
    assert api_pool_budgets(4,12,2)["payment"]["maximum_waiting"]==6

@pytest.mark.parametrize("role",["general","payment"])
def test_single_purpose_cannot_consume_other_purpose_headroom(role):
    budget,_,_=setup()
    other = "payment" if role=="general" else "general"
    for _ in range(10):
        budget.acquire(role)
    with pytest.raises(TooManyRequests):
        budget.acquire(role)
    for _ in range(2):
        budget.acquire(other)
    with pytest.raises(TooManyRequests):
        budget.acquire(other)
    assert budget.snapshot()["used"]==12
    for _ in range(10):
        budget.release(role)
    for _ in range(2):
        budget.release(other)
    assert budget.snapshot()["used"]==0

@pytest.mark.parametrize("error",[RuntimeError("native failure"),KeyboardInterrupt(),PoolTimeout()])
def test_slot_released_on_nonretaining_native_error(error):
    budget,natives,pools=setup()
    natives["payment"].error=error
    with pytest.raises(type(error)):
        pools["payment"].getconn()
    assert budget.snapshot()["used"]==0

def test_retained_timeouts_cannot_expand_native_backlog_and_clear_on_return():
    budget,natives,pools=setup()
    natives["general"].waiting=2
    natives["general"].error=PoolTimeout()
    for _ in range(2):
        with pytest.raises(PoolTimeout):
            pools["general"].getconn()
    assert budget.snapshot()["retained"]==2
    assert budget.snapshot()["acquiring"]==0
    pools["general"].putconn(object())
    assert budget.snapshot()["used"]==0

def test_closure_clears_retained_slots():
    budget,natives,pools=setup()
    natives["payment"].waiting=1
    natives["payment"].error=PoolTimeout()
    with pytest.raises(PoolTimeout):
        pools["payment"].getconn()
    assert budget.snapshot()["used"]==1
    pools["payment"].close()
    assert natives["payment"].closed
    assert budget.snapshot()["used"]==0

def test_rejection_visible_in_stats_and_deadline_does_not_restart():
    budget,natives,pools=setup()
    for _ in range(10):
        budget.acquire("general")
    with pytest.raises(TooManyRequests):
        pools["general"].getconn()
    assert pools["general"].get_stats()["requests_errors"]==1
    assert pools["general"].get_stats()["requests_num"]==2
    for _ in range(10):
        budget.release("general")
    pools["general"].getconn(timeout=.15)
    assert 0 < natives["general"].timeout_received <= .15
    assert budget.snapshot()["used"]==0

def test_guard_atomic_under_concurrent_mixed_attempts():
    budget,_,_=setup()
    barrier=threading.Barrier(41)
    release=threading.Event()
    attempted=threading.Barrier(41)
    admitted=[]
    lock=threading.Lock()
    def attempt(i):
        role="general" if i%2 else "payment"
        barrier.wait(timeout=3)
        try:
            budget.acquire(role)
        except TooManyRequests:
            attempted.wait(timeout=3)
            return
        try:
            with lock:
                admitted.append(role)
            attempted.wait(timeout=3)
            assert release.wait(timeout=3)
        finally:
            budget.release(role)
    with ThreadPoolExecutor(max_workers=40) as executor:
        futures=[executor.submit(attempt,i) for i in range(40)]
        barrier.wait(timeout=3)
        try:
            attempted.wait(timeout=3)
            assert budget.snapshot()["used"]==12
            assert len(admitted)==12
            assert all(admitted.count(role)<=10 for role in ("general","payment"))
        finally:
            release.set()
        for f in futures:
            f.result(timeout=3)
    assert budget.snapshot()["used"]==0

def test_public_connection_context_does_not_bypass_guard():
    budget,natives,pools=setup()
    class Conn:
        def __enter__(self):return self
        def __exit__(self,*args):pass
    conn=Conn()
    natives["general"].getconn=lambda timeout=None:conn
    natives["general"].putconn=Mock()
    with pools["general"].connection() as yielded:
        assert yielded is conn
        assert budget.snapshot()["used"]==0
    natives["general"].putconn.assert_called_once_with(conn)

from concurrent.futures import ThreadPoolExecutor
from contextlib import ExitStack
from threading import Event, Lock
from time import monotonic, sleep
from unittest.mock import Mock
from uuid import uuid4

import pytest

from ticketing.application.reservations import Reservations
from ticketing.domain import Failure
from ticketing.infrastructure.order_status_cache import RedisOrderStatusCache
from ticketing.infrastructure.read_coalescer import ReadCoalescer


def wait_for(predicate):
    end = monotonic() + 1
    while not predicate():
        assert monotonic() < end, "Concurrent reader did not reach the expected boundary"
        sleep(0.001)


class MemoryCache:
    def __init__(self):
        self.reads, self.rows, self.lock = ReadCoalescer(), {}, Lock()
        self.fills = []

    def lookup(self, actor, order_id):
        with self.lock:
            return self.rows.get((actor, str(order_id))), 1000

    def coalesce(self, actor, order_id):
        return self.reads.scope((actor, str(order_id)))

    def put(self, actor, order_id, row, stamp):
        with self.lock:
            self.rows[(actor, str(order_id))] = row
            self.fills.append(stamp)


def test_eight_simultaneous_cold_readers_share_one_authorized_read_and_fill():
    entered, release, oid = Event(), Event(), uuid4()
    row = {"id": str(oid), "actor": "owner", "status": "PENDING"}
    cache, store = MemoryCache(), Mock()

    def read(actor, identifier):
        assert actor == "owner" and identifier == oid
        entered.set()
        assert release.wait(1)
        return row

    store.get_order.side_effect = read
    reader = Reservations(store, cache)
    with ThreadPoolExecutor(max_workers=8) as executor:
        first = executor.submit(reader.get_order, "owner", oid)
        assert entered.wait(1)
        rest = [executor.submit(reader.get_order, "owner", oid) for _ in range(7)]
        try:
            wait_for(lambda: cache.reads._waiters == 7)
        finally:
            release.set()
        assert [f.result(1) for f in [first, *rest]] == [row]*8
    store.get_order.assert_called_once_with("owner", oid)
    assert cache.fills == [1000]
    assert cache.reads._flights == {} and cache.reads._waiters == 0


def test_other_actor_and_other_order_do_not_join_or_receive_owner_snapshot():
    entered, release, oid, other_oid = Event(), Event(), uuid4(), uuid4()
    cache, store = MemoryCache(), Mock()

    def read(actor, identifier):
        if actor != "owner":
            raise Failure("ORDER_NOT_FOUND", 404)
        if identifier == oid:
            entered.set()
            assert release.wait(1)
        return {"id": str(identifier), "actor": actor}

    store.get_order.side_effect = read
    reader = Reservations(store, cache)
    with ThreadPoolExecutor(max_workers=2) as executor:
        first = executor.submit(reader.get_order, "owner", oid)
        assert entered.wait(1)
        try:
            with pytest.raises(Failure, match="ORDER_NOT_FOUND"):
                reader.get_order("other", oid)
            assert reader.get_order("owner", other_oid)["id"] == str(other_oid)
            assert cache.reads._waiters == 0
            assert ("other", str(oid)) not in cache.rows
        finally:
            release.set()
        first.result(1)
    assert cache.reads._flights == {}


@pytest.mark.parametrize("exception", [RuntimeError("leader failure"), KeyboardInterrupt()])
def test_leader_exception_or_cancellation_wakes_followers_and_removes_entry(exception):
    reads = ReadCoalescer()
    def follow():
        with reads.scope("key"):
            return "awake"

    with ThreadPoolExecutor(max_workers=1) as executor:
        with pytest.raises(type(exception)), reads.scope("key"):
            future = executor.submit(follow)
            wait_for(lambda: reads._waiters == 1)
            raise exception
        assert future.result(1) == "awake"
    assert reads._flights == {} and reads._waiters == 0
    with reads.scope("key"):
        assert len(reads._flights) == 1
    assert reads._flights == {}


def test_timeout_allows_bounded_fallback_without_removing_active_leader():
    reads = ReadCoalescer(wait_seconds=0.01)
    with reads.scope("key"):
        flight = reads._flights["key"]
        start = monotonic()
        with reads.scope("key"):
            assert not flight.done.is_set()
            assert reads._waiters == 0
            assert reads._flights["key"] is flight
        assert monotonic()-start >= 0.009
    assert flight.done.is_set() and reads._flights == {}


def test_entry_per_key_and_global_waiter_caps_bypass_without_retaining_new_metadata():
    reads = ReadCoalescer(max_entries=2, per_key_waiters=1, max_waiters=1)
    with ExitStack() as stack:
        stack.enter_context(reads.scope("a"))
        stack.enter_context(reads.scope("b"))
        with reads.scope("c"):
            assert set(reads._flights) == {"a", "b"}

        def follower():
            with reads.scope("a"):
                return "done"

        with ThreadPoolExecutor(max_workers=1) as executor:
            future = executor.submit(follower)
            try:
                wait_for(lambda: reads._waiters == 1)
                # Both must bypass, even though each can reach a different leader.
                with reads.scope("a"), reads.scope("b"):
                    assert reads._waiters == 1
                    assert reads._flights["a"].waiters == 1
                    assert reads._flights["b"].waiters == 0
            finally:
                stack.close()
            assert future.result(1) == "done"
    assert reads._flights == {} and reads._waiters == 0


@pytest.mark.parametrize("kwargs", [{"max_entries": 0}, {"max_waiters": 129},
                                    {"per_key_waiters": True}, {"wait_seconds": 0},
                                    {"wait_seconds": 0.101}, {"wait_seconds": float("nan")}])
def test_invalid_unbounded_configuration_rejected(kwargs):
    with pytest.raises(ValueError):
        ReadCoalescer(**kwargs)


def test_recheck_hit_avoids_database_when_fill_completed_before_registration():
    cache, store = Mock(), Mock()
    cache.coalesce.return_value = ReadCoalescer().scope("key")
    row = {"actor": "owner"}
    cache.lookup.side_effect = [(None, 1000), (row, None)]
    assert Reservations(store, cache).get_order("owner", uuid4()) is row
    store.get_order.assert_not_called()
    cache.put.assert_not_called()


def test_recheck_miss_never_restarts_original_snapshot_deadline():
    cache, store = Mock(), Mock()
    cache.coalesce.return_value = ReadCoalescer().scope("key")
    cache.lookup.side_effect = [(None, 1000), (None, 1100)]
    row, oid = {"actor": "owner"}, uuid4()
    store.get_order.return_value = row
    assert Reservations(store, cache).get_order("owner", oid) is row
    cache.put.assert_called_once_with("owner", oid, row, 1000)


def test_recheck_redis_failure_returns_authorized_row_without_fill():
    cache, store = Mock(), Mock()
    cache.coalesce.return_value = ReadCoalescer().scope("key")
    cache.lookup.side_effect = [(None, 1000), (None, None)]
    row, oid = {"actor": "owner"}, uuid4()
    store.get_order.return_value = row
    assert Reservations(store, cache).get_order("owner", oid) is row
    cache.put.assert_not_called()


def test_failed_leader_does_not_cache_or_share_authorization_error():
    cache, store = MemoryCache(), Mock()
    store.get_order.side_effect = Failure("ORDER_NOT_FOUND", 404)
    reader, oid = Reservations(store, cache), uuid4()
    for _ in range(2):
        with pytest.raises(Failure, match="ORDER_NOT_FOUND"):
            reader.get_order("other", oid)
    assert store.get_order.call_count == 2
    assert cache.fills == [] and cache.reads._flights == {}


def test_redis_adapter_scopes_by_hashed_actor_and_order_without_new_redis_keys():
    cache = RedisOrderStatusCache(Mock(), 1000)
    oid = uuid4()
    with cache.coalesce("owner", oid):
        assert set(cache._reads._flights) == {cache.key("owner", oid)}
        assert "owner" not in next(iter(cache._reads._flights))
        with cache.coalesce("other", oid):
            assert len(cache._reads._flights) == 2
    cache.redis.eval.assert_not_called()
    assert cache._reads._flights == {}

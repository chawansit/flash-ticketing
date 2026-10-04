"""Bounded process-local coordination of advisory reads; no persistence authority."""

from contextlib import contextmanager
from dataclasses import dataclass, field
from threading import Event, Lock

from ticketing.observability import ORDER_STATUS_CACHE


@dataclass
class _Flight:
    done: Event = field(default_factory=Event)
    waiters: int = 0


class ReadCoalescer:
    def __init__(self, max_entries=128, per_key_waiters=8, max_waiters=128, wait_seconds=0.1):
        for value in (max_entries, per_key_waiters, max_waiters):
            if type(value) is not int or not 1 <= value <= 128:
                raise ValueError("Read coalescing limits must be integers in 1..128")
        if isinstance(wait_seconds, bool) or not 0 < wait_seconds <= 0.1:
            raise ValueError("Read coalescing wait must be in (0, 100 ms]")
        self.max_entries, self.per_key_waiters = max_entries, per_key_waiters
        self.max_waiters, self.wait_seconds = max_waiters, wait_seconds
        self._lock, self._flights, self._waiters = Lock(), {}, 0

    @contextmanager
    def scope(self, key):
        leader = False
        joined = False
        with self._lock:
            flight = self._flights.get(key)
            if flight is None and len(self._flights) < self.max_entries:
                flight = _Flight()
                self._flights[key] = flight
                leader = True
            elif (flight is not None and flight.waiters < self.per_key_waiters
                  and self._waiters < self.max_waiters):
                flight.waiters += 1
                self._waiters += 1
                joined = True
        if leader:
            try:
                ORDER_STATUS_CACHE.labels("coalesce_leader").inc()
                yield
            finally:
                with self._lock:
                    if self._flights.get(key) is flight:
                        del self._flights[key]
                    flight.done.set()
        elif joined:
            try:
                ORDER_STATUS_CACHE.labels("coalesce_joined").inc()
                completed = flight.done.wait(self.wait_seconds)
            finally:
                with self._lock:
                    flight.waiters -= 1
                    self._waiters -= 1
            ORDER_STATUS_CACHE.labels("coalesce_completed" if completed else "coalesce_timeout").inc()
            yield
        else:
            ORDER_STATUS_CACHE.labels("coalesce_bypass").inc()
            yield

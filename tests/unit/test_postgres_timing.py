import asyncio
import time
from contextlib import suppress

import pytest
from psycopg_pool import PoolTimeout

from ticketing.api import observe_event_loop_lag
from ticketing.infrastructure import postgres as postgres_module
from ticketing.infrastructure.postgres import Postgres
from ticketing.observability import (
    DB_CONNECTION_HOLD_SECONDS,
    DB_POOL_ACQUIRING,
    DB_POOL_IN_USE,
    DB_POOL_RETURN_SECONDS,
    DB_POOL_SECONDS,
    EVENT_LOOP_LAG_SECONDS,
)


class FakeConnection:
    def __enter__(self):
        return self

    def __exit__(self, *exc):
        return False

    def execute(self, query):
        return None

    def commit(self):
        time.sleep(0.005)

    def rollback(self):
        return None


class FakePool:
    def __init__(self, fail=False):
        self.connection = FakeConnection()
        self.fail = fail

    def getconn(self):
        if self.fail:
            time.sleep(0.005)
            raise PoolTimeout("test timeout")
        return self.connection

    def putconn(self, connection):
        assert connection is self.connection
        time.sleep(0.002)

    def get_stats(self):
        return {"pool_size": 1, "pool_available": 1, "requests_waiting": 0, "pool_max": 1}


def metric_sum(metric, *labels):
    child = metric.labels(*labels) if labels else metric
    return child._sum.get()


def test_pool_acquire_excludes_checked_out_lifecycle():
    db = Postgres.__new__(Postgres)
    db.pool = FakePool()
    acquire_before = metric_sum(DB_POOL_SECONDS, "ok")
    hold_before = metric_sum(DB_CONNECTION_HOLD_SECONDS)
    return_before = metric_sum(DB_POOL_RETURN_SECONDS)
    in_use_before = DB_POOL_IN_USE._value.get()

    with db.connection():
        time.sleep(0.02)

    acquire_delta = metric_sum(DB_POOL_SECONDS, "ok") - acquire_before
    hold_delta = metric_sum(DB_CONNECTION_HOLD_SECONDS) - hold_before
    return_delta = metric_sum(DB_POOL_RETURN_SECONDS) - return_before
    assert acquire_delta < 0.01
    assert hold_delta >= 0.02
    assert return_delta >= 0.002
    assert hold_delta > acquire_delta
    assert DB_POOL_IN_USE._value.get() == in_use_before


def test_failed_pool_acquire_is_timed_and_restores_gauge():
    db = Postgres.__new__(Postgres)
    db.pool = FakePool(fail=True)
    before = metric_sum(DB_POOL_SECONDS, "error")
    acquiring_before = DB_POOL_ACQUIRING._value.get()

    with pytest.raises(PoolTimeout), db.connection():
        pass

    assert metric_sum(DB_POOL_SECONDS, "error") - before >= 0.005
    assert DB_POOL_ACQUIRING._value.get() == acquiring_before


def test_event_loop_probe_records_samples():
    def sample_count():
        return next(
            sample.value
            for sample in EVENT_LOOP_LAG_SECONDS.collect()[0].samples
            if sample.name == "ticketing_event_loop_lag_seconds_count"
        )

    async def run_probe():
        before = sample_count()
        task = asyncio.create_task(observe_event_loop_lag(0.001))
        await asyncio.sleep(0.02)
        task.cancel()
        with suppress(asyncio.CancelledError):
            await task
        return sample_count() - before

    assert asyncio.run(run_probe()) >= 1


def test_slow_commit_and_pool_return_log_bounded_phase_fields(monkeypatch, caplog):
    db = Postgres.__new__(Postgres)
    db.pool = FakePool()
    monkeypatch.setattr(postgres_module, "SLOW_DB_PHASE_SECONDS", 0.001)

    with db.transaction():
        pass

    phase_rows = [
        record.fields
        for record in caplog.records
        if getattr(record, "fields", {}).get("event") == "slow_db_phase"
    ]
    assert {row["phase"] for row in phase_rows} == {"commit", "pool_return"}
    assert all(row["outcome"] == "ok" and row["duration_ms"] >= 1 for row in phase_rows)
    assert all(set(row) == {"event", "phase", "duration_ms", "outcome"} for row in phase_rows)
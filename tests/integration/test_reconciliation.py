import os
from concurrent.futures import ThreadPoolExecutor
from dataclasses import replace
from threading import Barrier, Lock
from uuid import uuid4

import pytest
from psycopg.errors import LockNotAvailable

from ticketing import workers
from ticketing.config import Settings
from ticketing.infrastructure.cache import RedisSeats
from ticketing.observability import RECONCILE_BACKLOG, RECONCILE_OVERDUE, RECONCILE_RECOVERED

pytestmark = pytest.mark.integration


@pytest.fixture
def cache():
    url = os.getenv("TEST_REDIS_URL")
    if not url:
        pytest.skip("TEST_REDIS_URL not configured")
    target = RedisSeats(url)
    yield target
    target.redis.close()


def settings(**overrides):
    return replace(Settings(), **overrides)


def make_event(db, starts="-1 day", ends="+1 day", seats=("A",)):
    """Create an event whose sale window is expressed only with existing schema fields."""
    event_id = uuid4()
    with db.transaction() as conn:
        conn.execute(
            "INSERT INTO events VALUES (%s,'Show','THB',clock_timestamp()+%s::interval,"
            "clock_timestamp()+%s::interval)",
            (event_id, starts, ends),
        )
        for seat in seats:
            conn.execute(
                "INSERT INTO event_seats(event_id,seat_id,price) VALUES (%s,%s,100)", (event_id, seat)
            )
    return event_id


def rows(db):
    with db.transaction() as conn:
        return {r["event_id"]: r for r in conn.execute("SELECT * FROM event_reconciliation").fetchall()}


def make_due(db, event_id=None):
    with db.transaction() as conn:
        if event_id:
            conn.execute(
                "UPDATE event_reconciliation SET next_due_at=clock_timestamp() WHERE event_id=%s",
                (event_id,),
            )
        else:
            conn.execute("UPDATE event_reconciliation SET next_due_at=clock_timestamp()")


class Recorder:
    """Records which events a worker actually projected, without touching Redis."""

    def __init__(self, fail=(), barrier=None):
        self.seen, self.lock, self.fail, self.barrier = [], Lock(), set(fail), barrier

    def __call__(self, db, target, event_id):
        if self.barrier:
            self.barrier.wait(timeout=10)
        with self.lock:
            self.seen.append(event_id)
        if event_id in self.fail:
            raise RuntimeError("projection failed")


def test_active_selection_uses_the_sale_window_and_prunes_closed_events(system):
    _, db, seeded_event = system
    open_now = make_event(db)
    starting_soon = make_event(db, starts="+60 seconds", ends="+2 days")
    not_yet = make_event(db, starts="+2 days", ends="+3 days")
    closed = make_event(db, starts="-3 days", ends="-2 days")
    config = settings(reconcile_window_seconds=300)

    workers.maintain_schedule(db, config)
    tracked = set(rows(db))
    assert {seeded_event, open_now, starting_soon} <= tracked
    assert not_yet not in tracked and closed not in tracked

    # A pending row whose event has left the window is claimed by nobody and pruned.
    with db.transaction() as conn:
        conn.execute("INSERT INTO event_reconciliation(event_id) VALUES (%s)", (closed,))
    workers.maintain_schedule(db, config)
    assert closed not in rows(db)

    _, claimed = workers.claim_due_events(db, config)
    assert not_yet not in claimed and closed not in claimed


def test_backlog_and_overdue_metrics_reflect_due_work(system):
    _, db, _ = system
    config = settings(reconcile_window_seconds=300)
    workers.maintain_schedule(db, config)
    with db.transaction() as conn:
        conn.execute("UPDATE event_reconciliation SET next_due_at=clock_timestamp()-interval '7 seconds'")
    workers.maintain_schedule(db, config)
    assert RECONCILE_BACKLOG._value.get() >= 1
    assert RECONCILE_OVERDUE._value.get() >= 7


def test_competing_workers_claim_disjoint_events(system, monkeypatch):
    _, db, _ = system
    for _ in range(11):
        make_event(db)
    config = settings(reconcile_batch_size=3)
    workers.maintain_schedule(db, config)
    make_due(db)
    recorder = Recorder(barrier=Barrier(2, timeout=10))
    monkeypatch.setattr(workers, "snapshot", recorder)

    def worker():
        total = 0
        for _ in range(6):
            total += workers.reconcile_batch(db, None, config)
        return total

    with ThreadPoolExecutor(max_workers=2) as pool:
        results = [future.result(timeout=30) for future in [pool.submit(worker), pool.submit(worker)]]

    assert sum(results) == 12
    assert len(recorder.seen) == len(set(recorder.seen)) == 12
    assert all(row["last_reconciled_at"] is not None for row in rows(db).values())
    assert all(row["lease_token"] is None for row in rows(db).values())


def test_one_failing_event_does_not_block_the_rest_of_the_batch(system, monkeypatch):
    _, db, _ = system
    poisoned = make_event(db)
    for _ in range(3):
        make_event(db)
    config = settings(reconcile_batch_size=8, reconcile_backoff_ms=1000)
    workers.maintain_schedule(db, config)
    make_due(db)
    recorder = Recorder(fail={poisoned})
    monkeypatch.setattr(workers, "snapshot", recorder)

    assert workers.reconcile_batch(db, None, config) == 5
    state = rows(db)
    assert poisoned in recorder.seen and len(recorder.seen) == 5
    assert state[poisoned]["consecutive_failures"] == 1
    assert state[poisoned]["last_reconciled_at"] is None
    assert state[poisoned]["lease_token"] is None  # deferred, not stuck under a live lease
    assert all(state[event]["last_reconciled_at"] is not None for event in state if event != poisoned)


def test_failure_backs_off_then_recovers_and_is_bounded(system, monkeypatch):
    _, db, event_id = system
    config = settings(reconcile_backoff_ms=1000)
    workers.maintain_schedule(db, config)
    make_due(db)
    monkeypatch.setattr(workers, "snapshot", Recorder(fail={event_id}))
    assert workers.reconcile_batch(db, None, config) == 1

    with db.transaction() as conn:
        delay = conn.execute(
            "SELECT EXTRACT(EPOCH FROM next_due_at-clock_timestamp()) AS d FROM event_reconciliation"
        ).fetchone()["d"]
    assert 0 < delay <= 1.0  # first retry uses the base delay, not the 20 s interval
    assert not workers.claim_due_events(db, config)[1]  # not due yet

    # Bounded: an event that has failed many times never exceeds base * 2**5.
    with db.transaction() as conn:
        conn.execute("UPDATE event_reconciliation SET consecutive_failures=40")
    make_due(db)
    workers.reconcile_batch(db, None, config)
    with db.transaction() as conn:
        capped = conn.execute(
            "SELECT EXTRACT(EPOCH FROM next_due_at-clock_timestamp()) AS d FROM event_reconciliation"
        ).fetchone()["d"]
    assert 30 <= capped <= 32

    make_due(db)
    monkeypatch.setattr(workers, "snapshot", Recorder())
    assert workers.reconcile_batch(db, None, config) == 1
    row = rows(db)[event_id]
    assert row["consecutive_failures"] == 0 and row["last_reconciled_at"] is not None


def test_stale_worker_cannot_acknowledge_a_reclaimed_event(system, monkeypatch):
    _, db, event_id = system
    config = settings()
    workers.maintain_schedule(db, config)
    make_due(db)

    def steal(*_):
        with db.transaction() as conn:
            conn.execute("UPDATE event_reconciliation SET lease_token=%s", (uuid4(),))

    monkeypatch.setattr(workers, "snapshot", steal)
    assert workers.reconcile_batch(db, None, config) == 1
    assert rows(db)[event_id]["last_reconciled_at"] is None

    with db.transaction() as conn:
        conn.execute("UPDATE event_reconciliation SET lease_until=clock_timestamp()-interval '1 second'")
    monkeypatch.setattr(workers, "snapshot", Recorder())
    assert workers.reconcile_batch(db, None, config) == 1
    assert rows(db)[event_id]["last_reconciled_at"] is not None


def test_abandoned_lease_is_reclaimed_after_expiry(system):
    _, db, event_id = system
    config = settings()
    workers.maintain_schedule(db, config)
    with db.transaction() as conn:
        conn.execute(
            """UPDATE event_reconciliation SET next_due_at=clock_timestamp(),
            lease_token=%s, lease_until=clock_timestamp()+interval '30 seconds'""",
            (uuid4(),),
        )
    assert not workers.claim_due_events(db, config)[1]  # live lease is respected

    before = RECONCILE_RECOVERED._value.get()
    with db.transaction() as conn:
        conn.execute("UPDATE event_reconciliation SET lease_until=clock_timestamp()-interval '1 second'")
    assert workers.claim_due_events(db, config)[1] == [event_id]
    assert RECONCILE_RECOVERED._value.get() == before + 1


def test_no_sql_lock_is_held_while_redis_is_written(system, cache, monkeypatch):
    _, db, event_id = system
    config = settings()
    workers.maintain_schedule(db, config)
    make_due(db)
    original, observed = cache.put, {}

    def put(event, version, data):
        with db.transaction() as conn:
            conn.execute(
                "SELECT event_id FROM event_reconciliation WHERE event_id=%s FOR UPDATE NOWAIT",
                (event_id,),
            )
            observed["row_lockable"] = True
        return original(event, version, data)

    try:
        monkeypatch.setattr(cache, "put", put)
        assert workers.reconcile_batch(db, cache, config) == 1
        assert observed["row_lockable"]
    finally:
        cache.redis.delete(cache.key(event_id))


def test_a_held_schedule_row_would_otherwise_be_detectable(system):
    """Guards the previous test: FOR UPDATE NOWAIT really does fail on a locked row."""
    _, db, _ = system
    workers.maintain_schedule(db, settings())
    with db.transaction() as owner:
        owner.execute("SELECT event_id FROM event_reconciliation FOR UPDATE")
        with pytest.raises(LockNotAvailable), db.transaction() as contender:
            contender.execute("SELECT event_id FROM event_reconciliation FOR UPDATE NOWAIT")


def test_cache_loss_is_repaired_by_scheduled_reconciliation(system, cache):
    svc, db, event_id = system
    config = settings()
    try:
        workers.snapshot(db, cache, event_id)
        svc.reserve("a", event_id, ["A"], "a")
        # The SeatsChanged notification is deliberately never delivered.
        cache.redis.delete(cache.key(event_id))
        workers.maintain_schedule(db, config)
        make_due(db)
        assert workers.reconcile_batch(db, cache, config) == 1
        result = cache.read(str(event_id))
        assert len(result["seats"]) == 3
        assert [s["status"] for s in result["seats"]] == ["HELD", "AVAILABLE", "AVAILABLE"]
    finally:
        cache.redis.delete(cache.key(event_id))


def test_pass_is_bounded_by_its_wall_clock_budget(system, monkeypatch):
    import time

    _, db, _ = system
    for _ in range(19):
        make_event(db)
    config = settings(reconcile_batch_size=2)
    workers.maintain_schedule(db, config)
    make_due(db)
    monkeypatch.setattr(workers, "snapshot", Recorder())

    assert workers.reconcile_pass(db, None, config, time.monotonic() - 1) == 0
    partial = workers.reconcile_pass(db, None, config, time.monotonic() + 0.001)
    assert 0 <= partial <= config.reconcile_batch_size

    total = partial
    while total < 20:
        done = workers.reconcile_pass(db, None, config, time.monotonic() + 5)
        assert done > 0
        total += done
    assert total == 20
    assert all(row["last_reconciled_at"] is not None for row in rows(db).values())


def test_oldest_deadline_is_served_first(system, monkeypatch):
    _, db, _ = system
    events = [make_event(db) for _ in range(6)]
    config = settings(reconcile_batch_size=2)
    workers.maintain_schedule(db, config)
    with db.transaction() as conn:
        # Deliberately inverse to creation order: oldest deadline wins, not insertion order.
        for offset, event_id in enumerate(events):
            conn.execute(
                "UPDATE event_reconciliation SET next_due_at=clock_timestamp()"
                "-(%s * interval '1 second') WHERE event_id=%s",
                (offset + 1, event_id),
            )
        conn.execute(
            "UPDATE event_reconciliation SET next_due_at=clock_timestamp()+interval '1 hour'"
            " WHERE event_id NOT IN (SELECT unnest(%s::uuid[]))",
            (events,),
        )
    recorder = Recorder()
    monkeypatch.setattr(workers, "snapshot", recorder)
    workers.reconcile_batch(db, None, config)
    workers.reconcile_batch(db, None, config)
    workers.reconcile_batch(db, None, config)
    assert recorder.seen == list(reversed(events))


def test_pruning_skips_row_being_leased(system):
    _, db, event = system
    config = settings()
    workers.maintain_schedule(db, config)
    with db.transaction() as conn:
        conn.execute(
            "UPDATE events SET sale_starts=clock_timestamp()-interval '2 days', sale_ends=clock_timestamp()-interval '1 day'"
        )
    with db.transaction() as owner:
        owner.execute(
            "UPDATE event_reconciliation SET lease_token=%s, lease_until=clock_timestamp()+interval '30 seconds' WHERE event_id=%s",
            (uuid4(), event),
        )
        _, pruned = workers.maintain_schedule(db, config)
        assert pruned == 0
    assert event in rows(db)


def test_deadline_releases_unstarted_batch_members(system, monkeypatch):
    from types import SimpleNamespace

    _, db, _ = system
    for _ in range(3):
        make_event(db)
    config = settings(reconcile_batch_size=8)
    workers.maintain_schedule(db, config)
    clock, seen = [0], []
    monkeypatch.setattr(workers, "time", SimpleNamespace(monotonic=lambda: clock[0]))

    def snapshot(*args):
        seen.append(args[-1])
        clock[0] = 2

    monkeypatch.setattr(workers, "snapshot", snapshot)
    workers.reconcile_pass(db, None, config, deadline=1)
    state = rows(db)
    assert len(seen) == 1
    assert sum(row["last_reconciled_at"] is not None for row in state.values()) == 1
    assert all(row["lease_token"] is None for row in state.values())


def test_stale_ack_is_not_counted_as_success(system, monkeypatch):
    from ticketing.observability import RECONCILE_EVENTS

    _, db, _ = system
    config = settings()
    workers.maintain_schedule(db, config)
    before = RECONCILE_EVENTS.labels("ok")._value.get()

    def steal(*_):
        with db.transaction() as conn:
            conn.execute("UPDATE event_reconciliation SET lease_token=%s", (uuid4(),))

    monkeypatch.setattr(workers, "snapshot", steal)
    workers.reconcile_batch(db, None, config)
    assert RECONCILE_EVENTS.labels("ok")._value.get() == before

def test_reconciler_process_recovers_expired_map_without_maintenance(system, cache, tmp_path):
    """Real worker must refresh Redis independently of the hold-expiry loop."""
    import subprocess
    import sys
    import time

    from psycopg.conninfo import make_conninfo

    svc, db, event_id = system
    held = svc.reserve('isolated-reconciler', event_id, ['A'], str(uuid4()))
    with db.transaction() as conn:
        schema = conn.execute('SELECT current_schema() AS name').fetchone()['name']
        conn.execute("UPDATE holds SET expires_at=clock_timestamp()-interval '1 second' WHERE id=%s", (held['hold_id'],))
        conn.execute("UPDATE event_seats SET reserved_until=clock_timestamp()-interval '1 second' WHERE hold_id=%s", (held['hold_id'],))
    env = {
        **os.environ,
        'DATABASE_URL': make_conninfo(os.environ['TEST_DATABASE_URL'], options=f'-c search_path={schema}'),
        'REDIS_URL': os.environ['TEST_REDIS_URL'],
        'RECONCILE_INTERVAL_SECONDS': '1',
        'WORKER_METRICS_PORT': '0',
    }
    key = cache.key(str(event_id))
    with (tmp_path/'reconciler.log').open('w') as output:
        proc = subprocess.Popen([sys.executable, '-m', 'ticketing.workers', 'reconciler'],
                                env=env, stdout=output, stderr=subprocess.STDOUT)
        try:
            deadline = time.monotonic()+10
            while not cache.redis.exists(key) and time.monotonic() < deadline:
                assert proc.poll() is None, 'Reconciler exited during startup'
                time.sleep(.05)
            assert cache.redis.exists(key), 'Dedicated reconciler did not warm inventory'
            old_incarnation = cache.redis.hget(key, 'incarnation')
            # Expire immediately, simulating the observed lost-map failure without
            # extending TTL or asking the read path to rebuild from PostgreSQL.
            cache.redis.pexpire(key, 1)
            time.sleep(.02)
            deadline = time.monotonic()+10
            while cache.redis.hget(key, 'incarnation') in (None, old_incarnation) and time.monotonic() < deadline:
                assert proc.poll() is None, 'Reconciler exited before recovery'
                time.sleep(.05)
            assert cache.redis.hget(key, 'incarnation') not in (None, old_incarnation)
            result = cache.read(str(event_id))
            assert next(s for s in result['seats'] if s['seat_id']=='A')['status'] == 'HELD'
            with db.transaction() as conn:
                assert conn.execute('SELECT status FROM holds WHERE id=%s', (held['hold_id'],)).fetchone()['status'] == 'ACTIVE'
            assert 0 < cache.redis.ttl(key) <= 30
        finally:
            proc.terminate()
            try:
                proc.wait(timeout=5)
            except subprocess.TimeoutExpired:
                proc.kill()
                proc.wait(timeout=5)
            cache.redis.delete(key)

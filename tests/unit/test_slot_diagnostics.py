import json
from concurrent.futures import ThreadPoolExecutor
from queue import Empty, Queue
from threading import Event
from types import SimpleNamespace

import pytest
from prometheus_client.parser import text_string_to_metric_families
from psycopg_pool import PoolTimeout

from ticketing import api
from ticketing.infrastructure import slot_diagnostics as sd
from ticketing.infrastructure.postgres import Postgres


@pytest.mark.parametrize("method,path,category", [
    ("POST", "/v1/orders/customer-secret/payments", "payment_request"),
    ("POST", "/v1/webhooks/payments", "payment_callback"),
    ("GET", "/v1/orders/customer-secret", "order_status"),
    ("POST", "/v1/orders", "order_create"),
    ("POST", "/v1/holds", "hold"),
    ("GET", "/v1/events/customer-secret/seats", "availability"),
    ("GET", "/unknown/customer-secret", "other"),
])
def test_operation_is_finite_and_retains_no_path(method, path, category):
    assert sd.operation_category(method, path) == category


def read_comment(tracker):
    return json.loads(tracker.comment()[len(sd.PREFIX):])


class Connection:
    autocommit = False

    def __init__(self, blocked, entered, resume):
        self.blocked, self.entered, self.resume = blocked, entered, resume

    def __enter__(self):
        return self

    def __exit__(self, *args):
        return False

    def execute(self, *args):
        return None

    def commit(self):
        if self.blocked == "commit":
            self.entered.set()
            assert self.resume.wait(3)

    def rollback(self):
        pass


class Pool:
    def __init__(self, conn):
        self.queue = Queue()
        self.queue.put(conn)

    def getconn(self):
        try:
            return self.queue.get(timeout=0.01)
        except Empty:
            raise PoolTimeout("synthetic slot timeout") from None

    def putconn(self, conn):
        if conn.blocked == "pool_return":
            conn.entered.set()
            assert conn.resume.wait(3)
        self.queue.put(conn)

    def get_stats(self):
        return {"pool_size": 1, "pool_available": self.queue.qsize(), "requests_waiting": 0, "pool_max": 1}


@pytest.mark.parametrize("blocked", ["body", "commit", "pool_return"])
def test_snapshot_captures_holder_during_checkout_failure_then_releases(blocked):
    entered, resume = Event(), Event()
    db = Postgres.__new__(Postgres)
    db.pool = Pool(Connection(blocked, entered, resume))
    db._slot_diagnostics = sd.SlotDiagnostics(1)
    db._diagnostic_role = "payment"

    def holder():
        token = sd.OPERATION.set("payment_callback")
        try:
            with db.transaction():
                if blocked == "body":
                    entered.set()
                    assert resume.wait(3)
            assert sd.ACTIVE_LEASE.get() is None
        finally:
            sd.OPERATION.reset(token)

    with ThreadPoolExecutor(max_workers=1) as threads:
        future = threads.submit(holder)
        try:
            assert entered.wait(3)
            with pytest.raises(PoolTimeout) as error, db.transaction():
                pytest.fail("failed checkout must not yield")
            ownership = error.value.acquisition_failure["slot_ownership"]
            assert len(ownership["holders"]) == 1
            assert ownership["holders"][0]["phase"] == blocked
            assert ownership["holders"][0]["operation"] == "payment_callback"
            assert ownership["holders"][0]["role"] == "payment"
            assert ownership["holders"][0]["age_ms"] > 0
            assert read_comment(db._slot_diagnostics)["records"][0] == ownership
        finally:
            resume.set()
            future.result(timeout=3)
    assert db._slot_diagnostics.active == {}
    assert sd.ACTIVE_LEASE.get() is None
    assert read_comment(db._slot_diagnostics)["complete"]


def test_nested_query_phase_restores_enclosing_phase_and_drops_sql_text():
    tracker = sd.SlotDiagnostics(1)
    lease = tracker.checkout("general")
    token = sd.ACTIVE_LEASE.set((tracker, lease))
    try:
        sd.phase("body")
        with pytest.raises(ValueError), sd.query_phase("SELECT"):
            assert tracker.failure({})["holders"][0]["phase"] == "query_SELECT"
            raise ValueError("synthetic")
        assert tracker.failure({})["holders"][0]["phase"] == "body"
    finally:
        tracker.returned(lease, True)
        sd.ACTIVE_LEASE.reset(token)
    assert "synthetic" not in tracker.comment().decode()


def test_diagnostic_failure_does_not_replace_original_driver_failure(monkeypatch):
    tracker = sd.SlotDiagnostics(1)
    monkeypatch.setattr(tracker, "failure", lambda *args: (_ for _ in ()).throw(RuntimeError("diagnostics")))
    db = Postgres.__new__(Postgres)
    db.pool = SimpleNamespace(getconn=lambda: (_ for _ in ()).throw(PoolTimeout("original")),
                              get_stats=dict)
    db._slot_diagnostics, db._diagnostic_role = tracker, "payment"
    with pytest.raises(PoolTimeout, match="original"), db.connection():
        pass
    assert read_comment(tracker)["diagnostic_errors"] == 1
    assert not read_comment(tracker)["complete"]


def test_ring_overflow_and_unreturned_connection_fail_completeness():
    tracker = sd.SlotDiagnostics(1)
    lease = tracker.checkout("general")
    assert tracker.checkout("payment") is None
    tracker.returned(lease, False)
    for _ in range(17):
        tracker.failure({"role": "payment", "reason": "native_timeout", "secret": "must-not-escape"})
    snapshot = read_comment(tracker)
    assert snapshot["failure_total"] == 17 and snapshot["overwritten_total"] == 1
    assert len(snapshot["records"]) == 16
    assert not snapshot["complete"]
    assert "secret" not in tracker.comment().decode()
    assert len(tracker.active) == 1


def test_metrics_adds_compatible_comment_only_when_enabled():
    tracker = sd.SlotDiagnostics(1)
    request = SimpleNamespace(app=SimpleNamespace(state=SimpleNamespace(db=SimpleNamespace())))
    baseline = api.metrics(request).body
    assert sd.PREFIX not in baseline
    request.app.state.db._slot_diagnostics = tracker
    enabled = api.metrics(request).body
    assert sd.PREFIX in enabled and len(tracker.comment()) <= sd.MAX_PAYLOAD
    assert [f.name for f in text_string_to_metric_families(enabled.decode())] == [
        f.name for f in text_string_to_metric_families(baseline.decode())]


@pytest.mark.parametrize("maximum", [0, 129, True])
def test_invalid_diagnostic_budget_rejected(maximum):
    with pytest.raises(ValueError):
        sd.SlotDiagnostics(maximum)


def test_native_pool_handoff_does_not_create_false_overflow_or_erase_new_owner():
    tracker = sd.SlotDiagnostics(1)
    connection = object()
    old = tracker.checkout("payment", connection)
    tracker.phase(old, "pool_return")
    new = tracker.checkout("payment", connection)
    assert new != old and new is not None
    tracker.returned(old, True)
    assert set(tracker.active) == {new}
    assert read_comment(tracker)["complete"]
    tracker.returned(new, True)
    assert not tracker.active


def test_duplicate_checkout_without_return_marks_unknown_occupancy():
    tracker = sd.SlotDiagnostics(1)
    connection = object()
    lease = tracker.checkout("payment", connection)
    assert tracker.checkout("general", connection) is None
    assert set(tracker.active) == {lease}
    assert not read_comment(tracker)["complete"]

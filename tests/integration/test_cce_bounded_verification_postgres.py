"""Execute audit and pinned observer against PostgreSQL; never starts cloud traffic."""
import json
import sys
import time
from contextlib import nullcontext
from types import SimpleNamespace
from uuid import uuid4

import cce_bounded_financial_audit as audit
import database_wait_evidence as waits
import independent_database_sampling as sampling
import psycopg
import pytest
from observe_cce_paid_pipeline import install_indexed_cohort_query
from observe_two_host_pipeline import load_frozen
from psycopg.conninfo import conninfo_to_dict

from ticketing import workers

pytestmark = pytest.mark.integration


def fulfilled(system):
    service, db, show = system
    order = service.reserve("buyer", show, ["A"], "hold")
    payment = service.initiate_payment("buyer", order["order_id"], "pay", "SUCCEEDED", 0, 1)
    service.callback({"callback_id": str(uuid4()), "payment_id": payment["payment_id"],
                      "order_id": order["order_id"], "amount": order["total"],
                      "currency": "THB", "outcome": "SUCCEEDED"})
    workers.consume_event(db, None, {"event_id": str(uuid4()), "schema_version": 1,
                          "event_type": "OrderPaid", "payload": {"order_id": order["order_id"]}})
    with db.transaction() as conn:
        conn.execute("UPDATE payment_attempts SET deliveries=1")
        conn.execute("UPDATE outbox_events SET published_at=clock_timestamp()")
        conn.execute("UPDATE holds SET expires_at=clock_timestamp()-interval '1 second'")
    return db, show


@pytest.mark.parametrize("corruption,field", [
    (None, None),
    ("UPDATE orders SET total=total+1", "order_item_relationship_errors"),
    ("UPDATE holds SET status='EXPIRED'", "hold_relationship_errors"),
    ("UPDATE event_seats SET booked_order_id=NULL", "booking_relationship_errors"),
    ("DELETE FROM tickets", "fulfilled_ticket_relationship_errors"),
])
def test_real_single_snapshot_audit_preserves_corruption_detection(system, corruption, field):
    db, show = fulfilled(system)
    if corruption:
        with db.transaction() as conn:
            conn.execute(corruption)
    scope = {}
    source = audit.program([str(show)], 1)
    exec(source.split("\nshows=")[0], scope)  # noqa: S102 - Repository-owned audit source.
    with psycopg.connect(db.pool.conninfo, autocommit=True) as conn:
        result = scope["financial_snapshot"](conn, [str(show)], 1, 1, 1,
                       audit.relationship_queries(), scope["audit"])
        assert conn.execute("SHOW transaction_read_only").fetchone() == ("off",)
    assert result["pass"] is (corruption is None)
    assert result["hold_deadlines_elapsed"]
    if field:
        assert result["relationships"][field] > 0
    else:
        assert result["relationships"]["unique_issued_tickets"] == 1
        assert result["tickets"] == result["bookings"] == result["succeeded_payments"] == 1


def test_pinned_observer_main_keeps_statistics_sampling_during_slow_metrics(system, tmp_path, monkeypatch):
    _, db, show = system
    from pathlib import Path
    frozen = load_frozen(Path(__file__).resolve().parents[2] / "artifacts/cce-frozen-harness/scripts/observe_paid_pipeline.py")
    install_indexed_cohort_query(frozen)
    waits.install(frozen)
    counts = {"data": 0, "admin": 0}
    def connect(dsn, **kwargs):
        if conninfo_to_dict(dsn).get("dbname") == "pgbouncer":
            counts["admin"] += 1
            return nullcontext(SimpleNamespace())
        counts["data"] += 1
        conn = psycopg.connect(dsn, **kwargs)
        conn.execute("SET default_transaction_read_only=on")
        return conn
    frozen.psycopg = SimpleNamespace(connect=connect, Error=psycopg.Error)
    frozen.host_cpu_ticks = lambda: [0] * 8
    frozen.pgbouncer_view = lambda *args: {}
    frozen.api_replicas = list
    def metrics(*args):
        time.sleep(.2)
        return {}
    frozen.worker_counters = metrics
    path = tmp_path / "pipeline.jsonl"
    manifest = tmp_path / "manifest.json"
    manifest.write_text(json.dumps({"environment": "development", "show_ids": [str(show)]}))
    monkeypatch.setenv("TEST_DATABASE_URL", db.pool.conninfo)
    monkeypatch.setenv("DATABASE_URL", db.pool.conninfo)
    monkeypatch.setattr(sys, "argv", ["observer", "--manifest", str(manifest), "--output", str(path), "--seconds", "4"])
    connections = sampling.install(frozen, db.pool.conninfo, tmp_path / "database-waits.jsonl", 480)
    frozen.main()
    assert counts == {"data": 1, "admin": 1}
    assert len(connections) == 1 and not connections[0].sampler.thread.is_alive()
    assert connections[0]._connection.closed
    result = waits.summarize(tmp_path / "database-waits.jsonl")
    assert result["complete"] and result["counter_continuity"]
    assert result["rows"] >= 4
    rows = [json.loads(line) for line in path.read_text().splitlines()]
    assert rows and all("database_sample_age_seconds" in row and "database_sample_utc" in row for row in rows)
    assert all("database_error" not in row for row in rows)

"""Opt-in isolated native checks; never connects to an existing database."""

import ast
import json
import os
import subprocess
import time
from contextlib import contextmanager
from pathlib import Path
from uuid import uuid4

import psycopg
import pytest
from psycopg.rows import dict_row

from ticketing import workers
from ticketing.config import Settings

pytestmark = [pytest.mark.integration, pytest.mark.skipif(
    os.getenv("LOCAL_CLAIM_TESTS") != "1", reason="requires opt-in isolated Docker PostgreSQL")]


@pytest.fixture(scope="module")
def database():
    name = "adr0341-test-" + uuid4().hex[:12]
    subprocess.run(["docker", "run", "--detach", "--pull=never", "--name", name,
                    "--tmpfs", "/var/lib/postgresql/data", "-e", "POSTGRES_HOST_AUTH_METHOD=trust",
                    "-p", "127.0.0.1::5432",
                    "postgres:15-alpine"], check=True, capture_output=True)
    try:
        port = subprocess.check_output(["docker", "port", name, "5432/tcp"], text=True).strip().split(":")[-1]
        url = f"postgresql://postgres@127.0.0.1:{port}/postgres"
        for _ in range(120):
            try:
                with psycopg.connect(url, connect_timeout=1):
                    break
            except psycopg.OperationalError:
                time.sleep(.5)
        else:
            pytest.fail("LOCAL_DATABASE_NOT_READY")

        class Database:
            @contextmanager
            def transaction(self):
                with psycopg.connect(url, row_factory=dict_row) as conn:
                    yield conn

        yield Database()
    finally:
        subprocess.run(["docker", "rm", "--force", "--volumes", name], check=True, capture_output=True)


@pytest.fixture
def db(database):
    with database.transaction() as conn:
        conn.execute("DROP TABLE IF EXISTS payment_attempts, orders CASCADE")
        conn.execute("CREATE TABLE orders (id uuid PRIMARY KEY, total int, currency text)")
        conn.execute("""CREATE TABLE payment_attempts (
            id uuid PRIMARY KEY, order_id uuid REFERENCES orders(id), due_at timestamptz,
            deliveries int DEFAULT 0, target_deliveries int DEFAULT 1,
            lease_until timestamptz, lease_token uuid, outcome text DEFAULT 'SUCCEEDED')""")
        conn.execute((Path(__file__).resolve().parents[2] / "migrations/007_payment_dispatch_index.sql").read_text())
    return database


def attempt(db, *, due="-1 second", lease=None, deliveries=0, target=1):
    identifier, order = uuid4(), uuid4()
    with db.transaction() as conn:
        conn.execute("INSERT INTO orders VALUES (%s,100,'THB')", (order,))
        conn.execute("""INSERT INTO payment_attempts
            (id,order_id,due_at,lease_until,deliveries,target_deliveries)
            VALUES (%s,%s,clock_timestamp()+%s::interval,
                    clock_timestamp()+%s::interval,%s,%s)""",
                     (identifier, order, due, lease, deliveries, target))
    return identifier


class Transport:
    def __init__(self, db, *, fail=False, replace_token=False):
        self.db, self.fail, self.replace_token = db, fail, replace_token
        self.delivered = []

    def post(self, raw, headers):
        payload = json.loads(raw)
        self.delivered.append(payload)
        # Callback sees a committed lease, and can acquire its row lock:
        # claim transaction/connection ownership has ended before HTTP.
        with self.db.transaction() as conn:
            conn.execute("SET LOCAL lock_timeout='100ms'")
            row = conn.execute("SELECT * FROM payment_attempts WHERE id=%s FOR UPDATE NOWAIT",
                               (payload["payment_id"],)).fetchone()
            assert row["lease_token"] and row["lease_until"]
            if self.replace_token:
                conn.execute("UPDATE payment_attempts SET lease_token=%s WHERE id=%s", (uuid4(), row["id"]))
        if self.fail:
            raise TimeoutError("test delivery failure")


def test_actual_claim_eligibility_duplicate_delivery_and_ack(db):
    future = attempt(db, due="1 hour")
    leased = attempt(db, lease="1 hour")
    attempt(db, deliveries=1)
    expired = attempt(db, lease="-1 second", target=2)
    transport = Transport(db)
    assert workers.simulate_one(db, Settings(), transport)
    assert workers.simulate_one(db, Settings(), transport)
    assert not workers.simulate_one(db, Settings(), transport)
    assert [p["payment_id"] for p in transport.delivered] == [str(expired)] * 2
    with db.transaction() as conn:
        rows = {r["id"]: r for r in conn.execute("SELECT * FROM payment_attempts").fetchall()}
    assert rows[expired]["deliveries"] == 2 and rows[expired]["lease_until"] is None
    assert rows[future]["deliveries"] == rows[leased]["deliveries"] == 0


def test_locked_due_row_is_skipped(db):
    locked, eligible = attempt(db), attempt(db)
    with db.transaction() as conn:
        conn.execute("SELECT * FROM payment_attempts WHERE id=%s FOR UPDATE", (locked,))
        transport = Transport(db)
        assert workers.simulate_one(db, Settings(), transport)
        assert transport.delivered[0]["payment_id"] == str(eligible)


def test_failed_delivery_recovers_after_lease_with_same_callback_id(db):
    identifier = attempt(db)
    failed = Transport(db, fail=True)
    with pytest.raises(TimeoutError, match="test delivery"):
        workers.simulate_one(db, Settings(), failed)
    assert not workers.simulate_one(db, Settings(), Transport(db))
    with db.transaction() as conn:
        row = conn.execute("SELECT * FROM payment_attempts WHERE id=%s", (identifier,)).fetchone()
        assert row["deliveries"] == 0
        conn.execute("UPDATE payment_attempts SET lease_until=clock_timestamp()-interval '1 second'")
    recovered = Transport(db)
    assert workers.simulate_one(db, Settings(), recovered)
    assert recovered.delivered[0]["callback_id"] == failed.delivered[0]["callback_id"]


def test_stale_ack_cannot_increment_replaced_lease(db):
    attempt(db)
    assert workers.simulate_one(db, Settings(), Transport(db, replace_token=True))
    with db.transaction() as conn:
        row = conn.execute("SELECT * FROM payment_attempts").fetchone()
    assert row["deliveries"] == 0 and row["lease_until"] is not None


def test_native_plan_bounds_future_pending_scan(db):
    # Extract the actual query executed by simulate_one, rather than a mirrored SQL fixture.
    tree = ast.parse((Path(__file__).resolve().parents[2] / "src/ticketing/workers.py").read_text())
    function = next(n for n in tree.body if isinstance(n, ast.FunctionDef) and n.name == "simulate_one")
    query = next(n.value for n in ast.walk(function) if isinstance(n, ast.Constant)
                 and isinstance(n.value, str) and "SELECT p.*" in n.value)
    original = query.replace("p.due_at<=statement_timestamp()", "p.due_at<=clock_timestamp()")
    with db.transaction() as conn:
        order = uuid4()
        conn.execute("INSERT INTO orders VALUES (%s,100,'THB')", (order,))
        conn.execute("""INSERT INTO payment_attempts (id,order_id,due_at)
            SELECT md5(i::text)::uuid,%s,clock_timestamp()+interval '1 hour'
            FROM generate_series(1,20000) i""", (order,))
        conn.execute("ANALYZE payment_attempts")
        conn.execute("ANALYZE orders")
        plans = [conn.execute("EXPLAIN (ANALYZE, BUFFERS, FORMAT JSON) " + sql).fetchone()["QUERY PLAN"][0]
                 for sql in (original, query)]

    def nodes(plan):
        yield plan
        for child in plan.get("Plans", []):
            yield from nodes(child)

    before, after = [list(nodes(p["Plan"])) for p in plans]
    assert sum(p.get("Rows Removed by Filter", 0) for p in before) >= 20000
    assert any("due_at <= statement_timestamp()" in p.get("Index Cond", "") for p in after)
    assert sum(p.get("Rows Removed by Filter", 0) for p in after) == 0
    assert all(p["Actual Rows"] == 0 for p in [plans[0]["Plan"], plans[1]["Plan"]])

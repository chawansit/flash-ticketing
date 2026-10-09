"""Real server cancellation, rollback and connection-reuse contracts."""

import os
from contextlib import contextmanager, nullcontext
from time import monotonic, sleep
from uuid import uuid4

import psycopg
import pytest
from psycopg import sql
from psycopg.errors import DivisionByZero, LockNotAvailable, QueryCanceled
from psycopg.pq import TransactionStatus

from ticketing.infrastructure.postgres import Postgres

pytestmark = pytest.mark.integration


@pytest.fixture
def single_db():
    url = os.getenv("TEST_DATABASE_URL")
    if not url:
        pytest.skip("TEST_DATABASE_URL not configured")
    db = Postgres(url, maximum=1)
    db.pool.wait(timeout=10)
    try:
        yield db
    finally:
        db.close()


def settings(conn):
    return conn.execute(
        "SELECT current_setting('lock_timeout') AS lock, "
        "current_setting('statement_timeout') AS statement, "
        "current_setting('idle_in_transaction_session_timeout') AS idle"
    ).fetchone()


def baseline(db):
    with db.connection() as conn:
        return conn.info.backend_pid, settings(conn)


def assert_reusable(db, pid, original):
    with db.connection() as conn:
        assert conn.info.transaction_status == TransactionStatus.IDLE
        assert conn.info.backend_pid == pid
        assert settings(conn) == original
        assert conn.execute("SELECT 42 AS answer").fetchone()["answer"] == 42


@pytest.mark.parametrize("fail", [False, True])
def test_local_timeouts_reset_on_commit_or_body_rollback(single_db, fail):
    db = single_db
    pid, original = baseline(db)
    with pytest.raises(DivisionByZero) if fail else nullcontext(), db.transaction() as conn:
        assert settings(conn) == {"lock": "75ms", "statement": "1500ms", "idle": "3s"}
        conn.execute("CREATE TEMP TABLE setup_rollback_probe(n int) ON COMMIT DROP")
        conn.execute("INSERT INTO setup_rollback_probe VALUES (1)")
        if fail:
            conn.execute("SELECT 1/0")
    assert_reusable(db, pid, original)
    with db.transaction() as conn:
        assert conn.execute("SELECT to_regclass('pg_temp.setup_rollback_probe') AS t").fetchone()["t"] is None


def test_statement_timeout_rolls_back_and_reuses_connection(single_db):
    db = single_db
    pid, original = baseline(db)
    started = monotonic()
    with pytest.raises(QueryCanceled) as error, db.transaction() as conn:
        conn.execute("CREATE TEMP TABLE canceled_probe(n int)")
        conn.execute("SELECT pg_sleep(2)")
    assert error.value.sqlstate == "57014"
    assert 1.2 <= monotonic() - started < 4
    assert_reusable(db, pid, original)
    with db.transaction() as conn:
        assert conn.execute("SELECT to_regclass('pg_temp.canceled_probe') AS t").fetchone()["t"] is None


def test_lock_timeout_rolls_back_and_reuses_connection(single_db):
    db = single_db
    pid, original = baseline(db)
    table = sql.Identifier("lock_probe_" + uuid4().hex)
    with psycopg.connect(os.environ["TEST_DATABASE_URL"], autocommit=True) as admin:
        admin.execute(sql.SQL("CREATE TABLE {}(id int PRIMARY KEY, n int)").format(table))
        admin.execute(sql.SQL("INSERT INTO {} VALUES(1, 0)").format(table))
        try:
            with psycopg.connect(os.environ["TEST_DATABASE_URL"]) as locker:
                locker.execute(sql.SQL("SELECT * FROM {} WHERE id=1 FOR UPDATE").format(table))
                started = monotonic()
                with pytest.raises(LockNotAvailable) as error, db.transaction() as conn:
                    conn.execute(sql.SQL("UPDATE {} SET n=1 WHERE id=1").format(table))
                assert error.value.sqlstate == "55P03"
                assert 0.04 <= monotonic() - started < 2
                assert_reusable(db, pid, original)
            with db.transaction() as conn:
                assert (
                    conn.execute(
                        sql.SQL("UPDATE {} SET n=2 WHERE id=1 RETURNING n").format(table)
                    ).fetchone()["n"]
                    == 2
                )
        finally:
            admin.execute(sql.SQL("DROP TABLE {}").format(table))


def test_setup_failure_never_yields_and_rolls_back(single_db, monkeypatch):
    db = single_db
    pid, original = baseline(db)
    real_connection = db.connection
    rollbacks = []
    yielded = False

    class SetupFailure:
        def __init__(self, conn):
            self.conn = conn

        def execute(self, query):
            if query.startswith("SELECT set_config"):
                self.conn.execute("CREATE TEMP TABLE failed_setup_probe(n int)")
                return self.conn.execute("SELECT 1/0")
            return self.conn.execute(query)

        def rollback(self):
            rollbacks.append(True)
            self.conn.rollback()

    @contextmanager
    def injected_connection():
        with real_connection() as conn:
            yield SetupFailure(conn)

    with monkeypatch.context() as patch:
        patch.setattr(db, "connection", injected_connection)
        with pytest.raises(DivisionByZero), db.transaction():
            yielded = True
    assert not yielded
    assert rollbacks == [True]
    assert_reusable(db, pid, original)
    with db.transaction() as conn:
        assert conn.execute("SELECT to_regclass('pg_temp.failed_setup_probe') AS t").fetchone()["t"] is None


def test_idle_timeout_discards_connection_then_allows_new_transaction(single_db):
    db = single_db
    pid, original = baseline(db)
    with pytest.raises(psycopg.OperationalError), db.transaction() as conn:
        conn.execute("CREATE TEMP TABLE idle_timeout_probe(n int)")
        sleep(3.3)
        conn.execute("SELECT 1")
    db.pool.wait(timeout=10)
    with db.transaction() as conn:
        assert conn.info.backend_pid != pid
        assert settings(conn) == {"lock": "75ms", "statement": "1500ms", "idle": "3s"}
        assert conn.execute("SELECT to_regclass('pg_temp.idle_timeout_probe') AS t").fetchone()["t"] is None
    with db.connection() as conn:
        assert settings(conn) == original

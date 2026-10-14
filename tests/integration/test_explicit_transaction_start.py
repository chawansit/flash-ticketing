"""ADR0323 real driver protocol and transaction atomicity."""
import psycopg
import pytest
from psycopg.pq import TransactionStatus

from ticketing.infrastructure.postgres import Postgres

pytestmark = pytest.mark.integration


@pytest.fixture(params=[False, True], ids=["reference", "explicit"])
def adapter(system, request):
    _, existing, _ = system
    db = Postgres(existing.pool.conninfo, maximum=2, explicit_start=request.param)
    db.pool.wait(timeout=10)
    try:
        yield db, request.param
    finally:
        db.close()


def test_protocol_has_one_explicit_begin_and_one_commit(adapter):
    db, explicit = adapter
    def counts(conn):
        return dict(conn.execute("SELECT query,sum(calls) FROM public.pg_stat_statements WHERE query IN ('BEGIN','COMMIT') GROUP BY query").fetchall())
    with psycopg.connect(db.pool.conninfo, autocommit=True) as observer:
        before = counts(observer)
        with db.transaction() as active:
            assert active.execute("SELECT 1 AS value").fetchone()["value"] == 1
        after = counts(observer)
    assert after.get('BEGIN', 0)-before.get('BEGIN', 0) == (1 if explicit else 2)
    assert after.get('COMMIT', 0)-before.get('COMMIT', 0) == 1


def test_explicit_boundary_hides_writes_until_commit(adapter):
    db, _ = adapter
    with db.transaction() as conn:
        conn.execute("CREATE TABLE explicit_atomic(id integer PRIMARY KEY)")
    with psycopg.connect(db.pool.conninfo, autocommit=True) as other:
        with db.transaction() as conn:
            conn.execute("INSERT INTO explicit_atomic VALUES (1)")
            assert other.execute("SELECT count(*) FROM explicit_atomic").fetchone()[0] == 0
        assert other.execute("SELECT count(*) FROM explicit_atomic").fetchone()[0] == 1


def test_body_and_deferred_commit_failure_rollback_and_reuse(adapter):
    db, _ = adapter
    with db.transaction() as conn:
        conn.execute("CREATE TABLE explicit_atomic(id integer UNIQUE DEFERRABLE INITIALLY DEFERRED)")
    with pytest.raises(RuntimeError, match="abort body"), db.transaction() as conn:
        conn.execute("INSERT INTO explicit_atomic VALUES (1)")
        raise RuntimeError("abort body")
    with pytest.raises(psycopg.errors.UniqueViolation), db.transaction() as conn:
        conn.execute("INSERT INTO explicit_atomic VALUES (2),(2)")
    with db.transaction() as conn:
        assert conn.execute("SELECT count(*) AS n FROM explicit_atomic").fetchone()["n"] == 0
        assert conn.execute("SHOW lock_timeout").fetchone()["lock_timeout"] == "75ms"
        assert conn.execute("SHOW statement_timeout").fetchone()["statement_timeout"] == "1500ms"
    conn = db.pool.getconn()
    try:
        assert conn.info.transaction_status == TransactionStatus.IDLE
    finally:
        db.pool.putconn(conn)


def test_pipeline_and_savepoint_preserve_atomicity(adapter):
    db, _ = adapter
    with db.transaction() as conn:
        conn.execute("CREATE TABLE explicit_atomic(id integer PRIMARY KEY)")
    with db.transaction(pipeline=True) as conn:
        conn.execute("INSERT INTO explicit_atomic VALUES (1)")
        assert conn.execute("SELECT count(*) AS n FROM explicit_atomic").fetchone()["n"] == 1
    with db.transaction() as conn:
        conn.execute("SAVEPOINT item")
        with pytest.raises(psycopg.errors.UniqueViolation):
            conn.execute("INSERT INTO explicit_atomic VALUES (1)")
        conn.execute("ROLLBACK TO SAVEPOINT item")
        conn.execute("INSERT INTO explicit_atomic VALUES (2)")
        conn.execute("RELEASE SAVEPOINT item")
    with db.transaction() as conn:
        assert conn.execute("SELECT count(*) AS n FROM explicit_atomic").fetchone()["n"] == 2

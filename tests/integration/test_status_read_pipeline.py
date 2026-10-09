import pytest
from psycopg.errors import UndefinedTable

from ticketing.domain import Failure

pytestmark = pytest.mark.integration


@pytest.mark.parametrize("pipeline", [False, True])
def test_status_read_preserves_authorization_and_connection_reuse(system, pipeline):
    svc, db, show = system
    db.order_status_read_pipeline = pipeline
    order = svc.reserve("owner", show, ["A"], "hold")
    result = svc.get_order("owner", order["order_id"])
    assert result["status"] == "PENDING" and result["tickets"] == []
    with pytest.raises(Failure) as error:
        svc.get_order("another", order["order_id"])
    assert error.value.code == "ORDER_NOT_FOUND"
    assert svc.get_order("owner", order["order_id"])["id"] == result["id"]
    assert db.pool.get_stats()["pool_available"] == db.pool.get_stats()["pool_size"]


@pytest.mark.parametrize("pipeline", [False, True])
def test_transaction_timeout_settings_and_rollback_are_unchanged(system, pipeline):
    _, db, _ = system
    with db.transaction(pipeline=pipeline) as conn:
        settings = conn.execute("SELECT current_setting('lock_timeout') AS lock, "
            "current_setting('statement_timeout') AS statement, "
            "current_setting('idle_in_transaction_session_timeout') AS idle").fetchone()
        assert settings == {"lock": "75ms", "statement": "1500ms", "idle": "3s"}
    with pytest.raises(UndefinedTable), db.transaction(pipeline=pipeline) as conn:
        conn.execute("SELECT * FROM deliberately_absent_adr0255_table").fetchall()
    with db.transaction(pipeline=pipeline) as conn:
        assert conn.execute("SELECT 1 AS n").fetchone()["n"] == 1
    assert db.pool.get_stats()["pool_available"] == db.pool.get_stats()["pool_size"]

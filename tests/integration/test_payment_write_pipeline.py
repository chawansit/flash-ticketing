"""Late SQL failure must roll back both queued financial writes."""
import pytest
from psycopg.errors import CheckViolation

pytestmark = pytest.mark.integration


@pytest.mark.parametrize("pipeline", [False, True])
def test_payment_response_write_failure_rolls_back_attempt_and_reuses_connection(system, pipeline):
    svc, db, show = system
    db.payment_write_pipeline = pipeline
    order = svc.reserve("buyer", show, ["A"], "hold")
    with db.transaction() as conn:
        conn.execute("""CREATE FUNCTION reject_payment_response() RETURNS trigger LANGUAGE plpgsql AS $$
            BEGIN
                IF NEW.operation='payment' AND NEW.response IS NOT NULL THEN
                    RAISE EXCEPTION 'injected response write failure' USING ERRCODE='23514';
                END IF;
                RETURN NEW;
            END $$""")
        conn.execute("""CREATE TRIGGER reject_payment_response BEFORE UPDATE ON idempotency_records
            FOR EACH ROW EXECUTE FUNCTION reject_payment_response()""")
    with pytest.raises(CheckViolation, match="injected response write failure"):
        svc.initiate_payment("buyer", order["order_id"], "pay", "SUCCEEDED", 0, 1)
    with db.transaction() as conn:
        assert conn.execute("SELECT count(*) AS n FROM payment_attempts").fetchone()["n"] == 0
        assert conn.execute(
            "SELECT count(*) AS n FROM idempotency_records WHERE operation='payment'"
        ).fetchone()["n"] == 0
        assert conn.execute("SELECT status FROM orders").fetchone()["status"] == "PENDING"
        assert conn.execute("SELECT status FROM holds").fetchone()["status"] == "ACTIVE"
        conn.execute("DROP TRIGGER reject_payment_response ON idempotency_records")
    result = svc.initiate_payment("buyer", order["order_id"], "pay", "SUCCEEDED", 0, 1)
    assert svc.initiate_payment("buyer", order["order_id"], "pay", "SUCCEEDED", 0, 1) == result
    with db.transaction() as conn:
        assert conn.execute("SELECT count(*) AS n FROM payment_attempts").fetchone()["n"] == 1
    assert db.pool.get_stats()["pool_available"] == db.pool.get_stats()["pool_size"]

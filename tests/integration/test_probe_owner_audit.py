"""Native PostgreSQL proof for the isolated100-actor safety audit."""
import sys
from pathlib import Path
from uuid import uuid4

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[2] / "scripts"))
import probe_two_host_safety as probe

pytestmark = pytest.mark.integration


@pytest.fixture
def owner(system, monkeypatch):
    svc, db, event = system
    run = "adr0147-" + uuid4().hex
    actor = probe.probe_actors(run)[4]
    with db.transaction() as conn:
        conn.execute("INSERT INTO event_seats(event_id,seat_id,price) VALUES (%s,'S0',100)", (event,))
    body = svc.reserve(actor, str(event), ["S0"], run + "-4")
    monkeypatch.setenv("DATABASE_URL", db.pool.conninfo)
    return db, str(event), body, run, actor


def test_actor_scoped_owner_audit_succeeds_with_unrelated_history(owner):
    db, event, body, run, _actor = owner
    with db.transaction() as conn:
        conn.execute("""INSERT INTO idempotency_records(actor,operation,key,request_hash,response)
        SELECT 'unrelated-'||n::text,'hold','unrelated-key-'||n::text,'hash',NULL
        FROM generate_series(1,4000) n""")
    assert probe.audit_one_owner(event, body, run)


@pytest.mark.parametrize("defect", ["missing", "extra_key", "actor_mismatch", "seat_pointer", "expired", "other_actor_active_owner"])
def test_actor_scoped_audit_still_rejects_bad_or_duplicate_ownership(owner, defect):
    db, event, body, run, actor = owner
    with db.transaction() as conn:
        if defect == "missing":
            conn.execute("DELETE FROM idempotency_records WHERE actor=%s AND operation='hold'", (actor,))
        elif defect == "extra_key":
            conn.execute("""INSERT INTO idempotency_records SELECT actor,operation,key||'-extra',request_hash,response
            FROM idempotency_records WHERE actor=%s AND operation='hold'""", (actor,))
        elif defect == "actor_mismatch":
            conn.execute("UPDATE orders SET actor='wrong' WHERE id=%s", (body['order_id'],))
        elif defect == "seat_pointer":
            conn.execute("UPDATE event_seats SET hold_id=NULL,reserved_until=NULL WHERE event_id=%s AND seat_id='S0'", (event,))
        elif defect == "expired":
            conn.execute("UPDATE holds SET expires_at=clock_timestamp()-interval '1 second' WHERE id=%s", (body['hold_id'],))
        else:
            hold, order = uuid4(), uuid4()
            conn.execute("INSERT INTO holds VALUES (%s,'nonparticipant',%s,clock_timestamp()+interval '5 minutes','ACTIVE')", (hold,event))
            conn.execute("""INSERT INTO orders(id,actor,hold_id,event_id,total,currency,status)
            VALUES (%s,'nonparticipant',%s,%s,100,'THB','PENDING')""", (order,hold,event))
            conn.execute("INSERT INTO order_items VALUES (%s,%s,'S0',100)", (order,event))
    with pytest.raises(ValueError, match="Exactly one durable seat owner"):
        probe.audit_one_owner(event, body, run)

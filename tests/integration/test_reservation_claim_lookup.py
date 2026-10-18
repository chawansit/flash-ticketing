"""ADR0362 real database replay, uniqueness and post-conflict snapshot behavior."""
import copy
import time
from concurrent.futures import ThreadPoolExecutor
from datetime import UTC, datetime, timedelta
from uuid import uuid4

import psycopg
import pytest
from psycopg.conninfo import make_conninfo

from ticketing.domain import Failure
from ticketing.infrastructure.postgres import Postgres
from ticketing.infrastructure.reservations import PostgresReservations, digest

pytestmark = pytest.mark.integration


def command(event, seat="A", *, actor="claim-owner", key="claim-key"):
    expiry = (datetime.now(UTC) + timedelta(seconds=120)).isoformat()
    payload = {"event_id":str(event), "seat_ids":[seat], "actor":actor,
               "idempotency_key":key, "command_id":str(uuid4()),
               "hold_id":str(uuid4()), "order_id":str(uuid4()), "expires_at":expiry,
               "request_hash":digest({"event_id":str(event),"seats":[seat]})}
    response = {"hold_id":payload["hold_id"], "order_id":payload["order_id"],
                "expires_at":expiry, "seats":[seat]}
    return payload,response


@pytest.mark.parametrize("enabled", [False, True])
def test_existing_command_replays_without_new_claim_or_sale_read(system, enabled):
    _,db,event = system
    store = PostgresReservations(db,None,persist_write_pipeline=True,persist_lookup_claim=enabled)
    payload,response = command(event)
    durable = store.persist_reservation_command(payload,response)
    with db.transaction() as conn:
        conn.execute("DELETE FROM idempotency_records")
        conn.execute("UPDATE events SET sale_ends=clock_timestamp()-interval '1 second' WHERE id=%s",(event,))
    assert store.persist_reservation_command(payload,response) == durable
    with db.transaction() as conn:
        assert conn.execute("SELECT count(*) AS n FROM idempotency_records").fetchone()["n"] == 0
        assert conn.execute("SELECT count(*) AS n FROM holds").fetchone()["n"] == 1
        assert conn.execute("SELECT count(*) AS n FROM reservation_commands").fetchone()["n"] == 1


@pytest.mark.parametrize("enabled", [False, True])
def test_changed_key_payload_is_rejected_before_seat_mutation(system, enabled):
    _,db,event = system
    store = PostgresReservations(db,None,persist_write_pipeline=True,persist_lookup_claim=enabled)
    payload,response = command(event)
    store.persist_reservation_command(payload,response)
    changed,answer = command(event,"B")
    with pytest.raises(Failure,match="IDEMPOTENCY_MISMATCH"):
        store.persist_reservation_command(changed,answer)
    with db.transaction() as conn:
        assert conn.execute("SELECT hold_id FROM event_seats WHERE event_id=%s AND seat_id='B'",(event,)).fetchone()["hold_id"] is None
        assert conn.execute("SELECT count(*) AS n FROM orders").fetchone()["n"] == 1


@pytest.mark.parametrize("enabled", [False, True])
@pytest.mark.parametrize("same_command", [False, True])
def test_conflicting_claim_reads_response_after_concurrent_commit(system, enabled, same_command):
    _,db,event = system
    payload,response = command(event)
    competing = copy.deepcopy(payload)
    if not same_command:
        competing["command_id"] = str(uuid4())
    application = "adr0362-waiter-" + uuid4().hex[:12]
    waiter_db = Postgres(make_conninfo(db.pool.conninfo,application_name=application),maximum=2,explicit_start=True)
    waiter_db.pool.wait(timeout=10)
    first = PostgresReservations(db,None,persist_write_pipeline=True,persist_lookup_claim=enabled)
    second = PostgresReservations(waiter_db,None,persist_write_pipeline=True,persist_lookup_claim=enabled)
    try:
        with ThreadPoolExecutor(max_workers=1) as executor:
            with db.transaction() as conn:
                durable = first._persist_reservation_command(conn,payload,response)
                pending = executor.submit(second.persist_reservation_command,competing,response)
                with psycopg.connect(db.pool.conninfo,autocommit=True) as observer:
                    deadline = time.monotonic()+5
                    blocked = False
                    while time.monotonic() < deadline:
                        blocked = bool(observer.execute(
                            "SELECT count(*) FROM pg_stat_activity WHERE application_name=%s AND wait_event_type='Lock'",
                            (application,)).fetchone()[0])
                        if blocked:
                            break
                        time.sleep(.01)
                assert blocked and not pending.done()
            # The second statement must see the row committed after its first snapshot.
            assert pending.result(timeout=5) == durable
        with db.transaction() as conn:
            for table in ("holds","orders","reservation_commands","idempotency_records","outbox_events"):
                assert conn.execute(f"SELECT count(*) AS n FROM {table}").fetchone()["n"] == 1
    finally:
        waiter_db.close()

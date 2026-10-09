"""Real PostgreSQL durability, replay, fencing and seat expiry for queued confirmation."""
import hashlib
import hmac
import json
import time
from concurrent.futures import ThreadPoolExecutor
from contextlib import contextmanager
from dataclasses import replace
from unittest.mock import Mock
from uuid import uuid4

import jwt
import pytest
from fastapi.testclient import TestClient
from psycopg.errors import LockNotAvailable

from ticketing import api
from ticketing.config import Settings
from ticketing.domain import Failure
from ticketing.infrastructure.payment_confirmation import LeaseLost, PostgresPaymentConfirmation
from ticketing.workers import consume_event, publish_batch

pytestmark = pytest.mark.integration


@pytest.fixture
def queued(system):
    svc, db, show = system
    settings = replace(Settings(), payment_confirmation_async=True, confirmation_max_pending=10)
    q = PostgresPaymentConfirmation(db, settings)
    hold = svc.reserve("one", show, ["A"], "hold")
    p = svc.initiate_payment("one", hold["order_id"], "payment", "SUCCEEDED", 0, 1)
    body = {"callback_id": str(uuid4()), "payment_id": p["payment_id"], "order_id": hold["order_id"],
            "amount": hold["total"], "currency": "THB", "outcome": "SUCCEEDED"}
    return svc, db, show, q, hold, body


def count(db, table):
    # Names are test constants, never external values.
    with db.transaction() as conn:
        return conn.execute("SELECT count(*) AS n FROM " + table).fetchone()["n"]


def fulfill(db, hold):
    consume_event(db, None, {"event_id": str(uuid4()), "schema_version": 1,
        "event_type": "OrderPaid", "payload": {"order_id": hold["order_id"]}})


def test_receipt_is_durable_without_financial_effect_then_commits_once(queued):
    svc, db, _, q, hold, body = queued
    assert q.receive(body) == {"status": "received", "receipt_id": body["callback_id"]}
    assert svc.get_order("one", hold["order_id"])["status"] == "PENDING"
    assert count(db, "bookings") == count(db, "payment_callbacks") == 0
    # Fresh queue instance models restart of in-memory ownership.
    restarted = PostgresPaymentConfirmation(db, q.settings)
    assert restarted.process_one(svc.store)
    assert not restarted.process_one(svc.store)
    fulfill(db, hold)
    assert count(db, "bookings") == count(db, "tickets") == 1
    assert q.sample()["pending"] == 0
    with db.transaction() as conn:
        assert conn.execute("SELECT outstanding FROM payment_receipt_capacity").fetchone()["outstanding"] == 0


def test_duplicate_receipts_same_key_and_semantic_duplicates(queued):
    svc, db, _, q, hold, body = queued
    q.receive(body)
    assert q.receive(body)["receipt_id"] == body["callback_id"]
    q.receive({**body, "callback_id": str(uuid4())})
    assert q.process_one(svc.store) and q.process_one(svc.store)
    fulfill(db, hold)
    assert count(db, "bookings") == count(db, "tickets") == 1
    assert count(db, "payment_callbacks") == 2
    assert q.sample()["pending"] == 0


def test_concurrent_admission_is_bounded_and_replays_do_not_consume_capacity(queued):
    _, db, _, q, _, body = queued
    q = PostgresPaymentConfirmation(db, replace(q.settings, confirmation_max_pending=2))
    def receive(index):
        try:
            q.receive({**body, "callback_id": str(uuid4())})
            return True
        except Failure as exc:
            assert exc.code == "CONFIRMATION_BACKLOG_FULL"
            return False
    with ThreadPoolExecutor(max_workers=8) as pool:
        result = list(pool.map(receive, range(8)))
    assert sum(result) == 2
    assert count(db, "payment_webhook_receipts") == 2
    with db.transaction() as conn:
        assert conn.execute("SELECT outstanding FROM payment_receipt_capacity").fetchone()["outstanding"] == 2
        stored = conn.execute("SELECT payload FROM payment_webhook_receipts LIMIT 1").fetchone()["payload"]
    assert q.receive(stored)["status"] == "received"
    with pytest.raises(Failure, match="CALLBACK_MISMATCH"):
        q.receive({**stored, "amount": stored["amount"]+1})


def test_parallel_distinct_receipts_apply_payment_once_and_retry_locks(queued):
    svc, db, _, q, hold, body = queued
    for _ in range(8):
        q.receive({**body, "callback_id": str(uuid4())})
    with ThreadPoolExecutor(max_workers=8) as pool:
        assert all(pool.map(lambda _: q.process_one(svc.store), range(8)))
    # Explicit deterministic recovery; retain original transient outcomes in receipt attempts.
    with db.transaction() as conn:
        conn.execute("UPDATE payment_webhook_receipts SET next_attempt_at=clock_timestamp() WHERE status='RETRY'")
    while q.process_one(svc.store):
        pass
    fulfill(db, hold)
    assert count(db, "bookings") == count(db, "tickets") == 1
    assert q.sample()["pending"] == 0


def test_claim_crash_recovers_with_stale_token_fencing(queued):
    svc, db, _, q, _, body = queued
    q.receive(body)
    old = q.claim()
    assert q.claim() is None
    with db.transaction() as conn:
        conn.execute("UPDATE payment_webhook_receipts SET lease_until=clock_timestamp()-interval '1 second'")
    new = q.claim()
    assert new["lease_token"] != old["lease_token"]
    with pytest.raises(LeaseLost):
        q.apply(old, svc.store)
    assert q.apply(new, svc.store)["status"] == "book"
    assert not q.defer(old, LockNotAvailable())
    assert count(db, "bookings") == 1


def test_expiry_during_financial_transaction_rolls_back_all_effects(queued):
    svc, db, _, q, _, body = queued
    q.receive(body)
    row = q.claim()
    class ExpireDuringApply:
        def apply_callback(self, conn, payload):
            result = svc.store.apply_callback(conn, payload)
            conn.execute("UPDATE payment_webhook_receipts SET lease_until=clock_timestamp()-interval '1 second'")
            return result
    with pytest.raises(LeaseLost):
        q.apply(row, ExpireDuringApply())
    assert count(db, "bookings") == count(db, "payment_callbacks") == 0
    assert q.apply(row, svc.store)["status"] == "book"


def test_transient_failure_has_bounded_retry_and_then_progress(queued):
    svc, db, _, q, _, body = queued
    q.receive(body)
    class Busy:
        def apply_callback(self, conn, payload):
            raise LockNotAvailable()
    assert q.process_one(Busy())
    with db.transaction() as conn:
        row = conn.execute("SELECT * FROM payment_webhook_receipts").fetchone()
        assert row["status"] == "RETRY" and row["attempts"] == 1
        conn.execute("UPDATE payment_webhook_receipts SET next_attempt_at=clock_timestamp()")
    assert q.process_one(svc.store)
    assert q.sample()["pending"] == 0


@pytest.mark.parametrize("change", [{"amount": 999}, {"currency": "USD"}, {"order_id": str(uuid4())}])
def test_invalid_financial_identity_is_visible_review_without_booking(queued, change):
    svc, db, _, q, _, body = queued
    q.receive({**body, **change})
    assert q.process_one(svc.store)
    assert q.sample()["review"] == q.sample()["pending"] == 1
    assert count(db, "bookings") == count(db, "payment_callbacks") == 0
    with db.transaction() as conn:
        assert conn.execute("SELECT error_code FROM payment_webhook_receipts").fetchone()["error_code"] == "PAYMENT_MISMATCH"
        assert conn.execute("SELECT outstanding FROM payment_receipt_capacity").fetchone()["outstanding"] == 1


def test_expired_reassigned_seat_refunds_without_reclaiming_inventory(queued):
    svc, db, show, q, hold, body = queued
    q.receive(body)
    with db.transaction() as conn:
        conn.execute("UPDATE holds SET expires_at=clock_timestamp()-interval '1 second'")
        conn.execute("UPDATE event_seats SET reserved_until=clock_timestamp()-interval '1 second' WHERE hold_id=%s", (hold["hold_id"],))
    new = svc.reserve("two", show, ["A"], "new")
    assert q.process_one(svc.store)
    assert svc.get_order("one", hold["order_id"])["status"] == "REFUND_PENDING"
    with db.transaction() as conn:
        assert str(conn.execute("SELECT hold_id FROM event_seats WHERE seat_id='A'").fetchone()["hold_id"]) == new["hold_id"]
    assert count(db, "refund_requests") == 1 and count(db, "bookings") == 0
    assert q.sample()["pending"] == 0


def test_mixed_direct_and_queued_replay_never_regresses_success(queued):
    svc, db, _, q, hold, body = queued
    q.receive(body)
    assert svc.callback(body)["status"] == "book"
    assert q.process_one(svc.store)
    q.receive({**body, "callback_id": str(uuid4()), "outcome": "FAILED"})
    assert q.process_one(svc.store)
    fulfill(db, hold)
    assert svc.get_order("one", hold["order_id"])["status"] == "FULFILLED"
    assert count(db, "bookings") == count(db, "tickets") == 1


def test_receipt_commit_response_loss_replay_is_durable(queued):
    svc, db, _, q, _, body = queued
    class LoseResponse:
        @contextmanager
        def transaction(self):
            with db.transaction() as conn:
                yield conn
            raise ConnectionError("lost response after receipt commit")
    broken = PostgresPaymentConfirmation(LoseResponse(), q.settings)
    with pytest.raises(ConnectionError):
        broken.receive(body)
    assert q.receive(body)["status"] == "received"
    assert count(db, "payment_webhook_receipts") == 1
    assert q.process_one(svc.store)
    assert count(db, "bookings") == 1


def test_financial_commit_ack_loss_cannot_reapply_receipt(queued):
    svc, db, _, q, _, body = queued
    q.receive(body)
    row = q.claim()
    class LoseResponse:
        @contextmanager
        def transaction(self):
            with db.transaction() as conn:
                yield conn
            raise ConnectionError("lost after financial commit")
    with pytest.raises(ConnectionError):
        PostgresPaymentConfirmation(LoseResponse(),q.settings).apply(row,svc.store)
    with pytest.raises(LeaseLost):
        q.apply(row,svc.store)
    assert q.receive(body)["status"] == "received" and not q.process_one(svc.store)
    assert count(db,"bookings") == 1 and q.sample()["pending"] == 0


def test_kafka_outage_retains_committed_financial_outbox(queued):
    svc, db, _, q, hold, body = queued
    q.receive(body)
    q.process_one(svc.store)
    producer=Mock();producer.send.return_value.get.side_effect=ConnectionError("broker unavailable")
    with pytest.raises(ConnectionError):
        publish_batch(db,producer,32)
    assert svc.get_order("one",hold["order_id"])["status"] == "PAID"
    assert count(db,"bookings") == 1 and count(db,"tickets") == 0
    with db.transaction() as conn:
        assert conn.execute("SELECT count(*) AS n FROM outbox_events WHERE published_at IS NULL").fetchone()["n"] > 0
        conn.execute("UPDATE outbox_events SET lease_until=clock_timestamp()-interval '1 second'")
    producer.send.return_value.get.side_effect=None
    assert publish_batch(db,producer,32)
    fulfill(db,hold)
    assert count(db,"tickets") == 1 and q.sample()["pending"] == 0


def test_exhausted_crash_claims_and_corrupt_receipt_enter_review(queued):
    svc, db, _, q, _, body = queued
    q.receive(body)
    with db.transaction() as conn:
        conn.execute("UPDATE payment_webhook_receipts SET attempts=%s",(q.settings.confirmation_max_attempts,))
    q.process_one(svc.store)
    assert q.sample()["review"] == 1 and count(db,"bookings") == 0


@pytest.mark.parametrize("signature_valid",[True,False])
def test_signed_webhook_acknowledges_receipt_before_financial_work(queued,monkeypatch,signature_valid):
    svc,db,_,q,hold,body=queued
    monkeypatch.setattr(api,"settings",replace(api.settings,payment_confirmation_async=True,order_status_poll_ms=500))
    monkeypatch.setattr(api.app.state,"payment_confirmation",q,raising=False)
    api.app.dependency_overrides[api.service]=lambda:svc
    try:
        client=TestClient(api.app)
        raw=json.dumps(body).encode();timestamp=str(int(time.time()))
        signature=hmac.new(api.settings.webhook_secret.encode(),timestamp.encode()+b"."+raw,hashlib.sha256).hexdigest()
        response=client.post('/v1/webhooks/payments',content=raw,headers={'Content-Type':'application/json','X-Payment-Timestamp':timestamp,'X-Payment-Signature':signature if signature_valid else 'bad'})
        assert response.status_code == (200 if signature_valid else 401)
        assert count(db,'bookings') == 0
        assert count(db,'payment_webhook_receipts') == int(signature_valid)
        if signature_valid:
            assert response.json()['status']=='received'
            q.process_one(svc.store)
            token=jwt.encode({'sub':'other','exp':int(time.time())+60,'aud':'ticketing','iss':'ticketing'},api.settings.jwt_secret,algorithm='HS256')
            assert client.get('/v1/orders/'+hold['order_id'],headers={'Authorization':'Bearer '+token}).status_code==404
    finally:
        api.app.dependency_overrides.pop(api.service,None)


def test_receipt_precommit_failure_never_acknowledges_or_reserves_capacity(queued):
    _,db,_,q,_,body=queued
    class FailBeforeCommit:
        @contextmanager
        def transaction(self):
            with db.transaction() as conn:
                yield conn
                raise ConnectionError("before receipt commit")
    with pytest.raises(ConnectionError):
        PostgresPaymentConfirmation(FailBeforeCommit(),q.settings).receive(body)
    assert count(db,"payment_webhook_receipts")==0
    assert count(db,"payment_receipt_capacity")==0


def test_corrupt_stored_payload_enters_review_without_financial_effects(queued):
    svc,db,_,q,_,body=queued
    q.receive(body)
    with db.transaction() as conn:
        conn.execute("UPDATE payment_webhook_receipts SET payload=payload || '{\"amount\":999}'::jsonb")
    assert q.process_one(svc.store)
    assert q.sample()["review"]==1 and count(db,"bookings")==0
    with db.transaction() as conn:
        assert conn.execute("SELECT error_code FROM payment_webhook_receipts").fetchone()["error_code"]=="RECEIPT_CORRUPT"


def test_receipt_completion_and_counter_rollback_together_on_invariant_failure(queued):
    svc,db,_,q,_,body=queued
    q.receive(body)
    row=q.claim()
    with db.transaction() as conn:
        conn.execute("UPDATE payment_receipt_capacity SET outstanding=0")
    with pytest.raises(RuntimeError,match="capacity invariant"):
        q.apply(row,svc.store)
    assert count(db,"bookings")==count(db,"payment_callbacks")==0
    with db.transaction() as conn:
        assert conn.execute("SELECT status FROM payment_webhook_receipts").fetchone()["status"]=="PROCESSING"

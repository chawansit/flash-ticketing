"""Execute ADR0161's audit SQL against native PostgreSQL, not mocked counts."""
import io
import json
import os
import sys
from contextlib import redirect_stdout
from dataclasses import replace
from pathlib import Path
from uuid import uuid4

import psycopg
import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[2] / "scripts"))
import async_confirmation_contract as policy

from ticketing.config import Settings
from ticketing.infrastructure.payment_confirmation import PostgresPaymentConfirmation

pytestmark = pytest.mark.integration


def setup(system, monkeypatch):
    svc, db, show = system
    monkeypatch.setenv("DATABASE_URL", db.pool.conninfo)
    q = PostgresPaymentConfirmation(db, replace(Settings(), payment_confirmation_async=True))
    hold = svc.reserve("owner", show, ["A"], "hold-" + str(uuid4()))
    payment = svc.initiate_payment("owner", hold["order_id"], "pay", "SUCCEEDED", 0, 1)
    payload = {"callback_id": payment["payment_id"], "payment_id": payment["payment_id"],
               "order_id": hold["order_id"], "amount": hold["total"], "currency": "THB", "outcome": "SUCCEEDED"}
    return svc, db, show, q, payload


def receipt_state():
    namespace = {"result": {"pass": True}, "psycopg": psycopg, "os": os}
    exec(policy.RECEIPT_AUDIT, namespace)  # noqa: S102 - execute the exact repository-owned audit SQL
    return namespace["result"]


class Session:
    def api(self, cid, program, timeout):
        output = io.StringIO()
        with redirect_stdout(output):
            exec(program, {})  # noqa: S102 - execute the exact trusted generated audit program
        return json.loads(output.getvalue())


def test_scoped_receipt_audit_requires_completed_payment_and_exact_count(system, monkeypatch):
    svc, _db, show, q, payload = setup(system, monkeypatch)
    plan = json.loads(policy.PLAN.read_text())
    contract = policy.AsyncConfirmationContract(plan["artifact_receipt"], "candidate", plan["expected_runtime_source_sha256"])
    q.receive(payload)
    pending = receipt_state()
    assert not pending["pass"] and pending["pending_confirmation_receipts"] == 1
    with pytest.raises(ValueError, match="Scoped durable"):
        contract.stage_receipt_audit(Session(), "unused", [str(show)], 1)
    assert q.process_one(svc.store)
    assert receipt_state()["pass"]
    assert contract.stage_receipt_audit(Session(), "unused", [str(show)], 1) == {
        "receipts": 1, "completed": 1, "expected": 1, "pass": True}
    with pytest.raises(ValueError):
        contract.stage_receipt_audit(Session(), "unused", [str(uuid4())], 1)


def test_review_and_counter_corruption_fail_global_receipt_drain(system, monkeypatch):
    svc, db, _show, q, payload = setup(system, monkeypatch)
    q.receive({**payload, "amount": 1})
    q.process_one(svc.store)
    state = receipt_state()
    assert state["review_confirmation_receipts"] == 1
    assert state["confirmation_capacity_outstanding"] == 1
    assert state["confirmation_capacity_mismatches"] == 0
    assert not policy.receipt_drained(state)
    with db.transaction() as conn:
        conn.execute("UPDATE payment_receipt_capacity SET outstanding=0")
    assert receipt_state()["confirmation_capacity_mismatches"] == 1

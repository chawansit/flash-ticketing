"""Fresh18000-seat fixture export, disjoint allocation and real financial replay."""
import hashlib
import hmac
import json
import sys
import tempfile
import time
from contextlib import ExitStack
from pathlib import Path
from uuid import UUID, uuid4

import jwt
import pytest
from fastapi.testclient import TestClient

sys.path.insert(0, str(Path(__file__).resolve().parents[2] / "scripts"))
from conftest import NoShield

from scripts import export_load_manifest, prepare_capacity_fixture
from scripts.paid_ticket_sharded_generator import split_manifest
from ticketing import api
from ticketing.application.reservations import Reservations
from ticketing.infrastructure.postgres import create_api_databases
from ticketing.infrastructure.reservations import PostgresReservations
from ticketing.workers import consume_event

pytestmark = pytest.mark.integration


def test_large_single_concert_fixture_export_and_disjoint_paid_replay(system, monkeypatch):
    svc, db, _ = system
    monkeypatch.setenv("TEST_DATABASE_URL", db.pool.conninfo)
    from ticketing.config import Settings
    monkeypatch.setattr(prepare_capacity_fixture, "Settings", lambda: Settings(environment="development"))
    monkeypatch.setattr(export_load_manifest, "Settings", lambda: api.settings)
    # Credential-bearing export stays beneath the repository's ignored tmp.
    with tempfile.TemporaryDirectory(prefix="single-concert-local-", dir=Path.cwd()/"tmp") as private:
        fixture_path = Path(private)/"fixture.json"
        manifest_path = Path(private)/"manifest.json"
        monkeypatch.setattr(sys, "argv", ["prepare", "--output", str(fixture_path), "--shows", "1",
                            "--seats", "18000", "--sale-hours", "1", "--fixture-layout", "single-concert"])
        prepare_capacity_fixture.main()
        monkeypatch.setattr(sys, "argv", ["export", "--results", str(fixture_path), "--origin",
                            "http://127.0.0.1:8000", "--output", str(manifest_path),
                            "--viewers", "18000", "--seat-offset", "0"])
        export_load_manifest.main()
        fixture = json.loads(fixture_path.read_text())
        manifest = json.loads(manifest_path.read_text())
        assert fixture["shows"] == len(manifest["show_ids"]) == 1
        assert fixture["seats_per_show"] == manifest["seats_per_show"] == 18000
        parts = split_manifest(manifest, 2, 9000)
        assert [part["seat_offset"] for part in parts] == [0, 9000]
        assert len(set(parts[0]["viewer_tokens"]) & set(parts[1]["viewer_tokens"])) == 0
        show = UUID(manifest["show_ids"][0])
        with db.transaction() as conn:
            assert conn.execute("SELECT count(*) AS n FROM event_seats WHERE event_id=%s",
                                (show,)).fetchone()["n"] == 18000
        general, financial = create_api_databases(db.pool.conninfo, 4, 1000, 12, 2, True)
        previous = dict(api.app.dependency_overrides)
        api.app.dependency_overrides[api.service] = lambda: Reservations(PostgresReservations(financial, NoShield(), 120))
        try:
            with ExitStack() as resources:
                resources.callback(general.close)
                resources.callback(financial.close)
                client = resources.enter_context(TestClient(api.app))
                for shard, local_indices in [(0, [0, 499, 8999]), (1, [0, 1500, 8999])]:
                    part = parts[shard]
                    token = part["viewer_tokens"][0]
                    owner = jwt.decode(token, api.settings.jwt_secret, algorithms=["HS256"],
                                       audience="ticketing", issuer="ticketing")["sub"]
                    for index in local_indices:
                        seat = "S"+str(part["seat_offset"]+index)
                        order = svc.reserve(owner, show, [seat], str(uuid4()))
                        url = "/v1/orders/"+order["order_id"]+"/payments"
                        headers = {"Authorization": "Bearer "+token, "Idempotency-Key": str(uuid4())}
                        response = client.post(url, json={"outcome": "SUCCEEDED"}, headers=headers)
                        assert response.status_code == 202
                        assert client.post(url, json={"outcome": "SUCCEEDED"}, headers=headers).json() == response.json()
                        body = {"callback_id": str(uuid4()), "payment_id": response.json()["payment_id"],
                                "order_id": order["order_id"], "amount": 100, "currency": "THB", "outcome": "SUCCEEDED"}
                        raw = json.dumps(body).encode()
                        stamp = str(int(time.time()))
                        signature = hmac.new(api.settings.webhook_secret.encode(), stamp.encode()+b"."+raw,
                                             hashlib.sha256).hexdigest()
                        signed = {"X-Payment-Timestamp": stamp, "X-Payment-Signature": signature,
                                  "Content-Type": "application/json"}
                        assert client.post("/v1/webhooks/payments", content=raw, headers=signed).json()["status"] == "book"
                        assert client.post("/v1/webhooks/payments", content=raw, headers=signed).json()["status"] == "duplicate"
                        with db.transaction() as conn:
                            paid = conn.execute("SELECT id,payload FROM outbox_events WHERE event_type='OrderPaid' "
                                                "AND aggregate_id=%s", (order["order_id"],)).fetchone()
                        envelope = {"event_id": str(paid["id"]), "schema_version": 1,
                                    "event_type": "OrderPaid", "payload": paid["payload"]}
                        consume_event(db, None, envelope)
                        consume_event(db, None, envelope)
                        assert svc.get_order(owner, order["order_id"])["status"] == "FULFILLED"
                assert general._shared_acquisition_budget.snapshot()["used"] == 0
            with db.transaction() as conn:
                for table in ("orders", "payment_attempts", "payment_callbacks", "bookings", "tickets"):
                    assert conn.execute(f"SELECT count(*) AS n FROM {table}").fetchone()["n"] == 6
                for kind in ("OrderPaid", "TicketsIssued"):
                    assert conn.execute("SELECT count(*) AS n FROM outbox_events WHERE event_type=%s",
                                        (kind,)).fetchone()["n"] == 6
                assert conn.execute("SELECT count(*) AS n FROM refund_requests").fetchone()["n"] == 0
                assert conn.execute("SELECT count(*) AS n FROM (SELECT event_id,seat_id FROM bookings "
                                    "GROUP BY event_id,seat_id HAVING count(*)>1) d").fetchone()["n"] == 0
                assigned = conn.execute("SELECT seat_id FROM bookings").fetchall()
                assert {row["seat_id"] for row in assigned} == {"S0","S499","S8999","S9000","S10500","S17999"}
        finally:
            api.app.dependency_overrides.clear()
            api.app.dependency_overrides.update(previous)
            financial.close()
            general.close()
    assert not Path(private).exists()

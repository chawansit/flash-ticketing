"""Execute the real pinned producer against owned local PostgreSQL and Redis data."""
import hashlib
import json
import sys
from pathlib import Path
from types import ModuleType

import pytest
from redis import Redis

sys.path.insert(0, str(Path(__file__).resolve().parents[2] / "scripts"))
import fixture_identity_evidence as identity
from run_two_host_paid_comparison import frozen_bundle

from ticketing.config import Settings
from ticketing.infrastructure.cache import RedisSeats

pytestmark = pytest.mark.integration


def test_pinned_producer_output_is_retained_before_dispatch(system, tmp_path, monkeypatch):
    _svc, db, _event = system
    source = frozen_bundle()["scripts/prepare_capacity_fixture.py"]
    fingerprint = hashlib.sha256(source.replace(b"\r\n", b"\n")).hexdigest()
    assert fingerprint == identity.LEGACY_DISTRIBUTED_PRODUCER
    producer = ModuleType("pinned_capacity_fixture")
    exec(compile(source, "pinned_capacity_fixture", "exec"), producer.__dict__)  # noqa: S102 - exact Git producer hash verified above
    monkeypatch.setattr(producer, "Settings", lambda: Settings(environment="development"))
    monkeypatch.setenv("TEST_DATABASE_URL", db.pool.conninfo)
    monkeypatch.setenv("TEST_REDIS_URL", "redis://127.0.0.1:6379/15")
    output = tmp_path / "fixture.json"
    monkeypatch.setattr(sys, "argv", ["prepare", "--output", str(output), "--shows", "1", "--seats", "300", "--sale-hours", "1"])
    fixture = None
    try:
        producer.main()
        fixture = json.loads(output.read_text())
        assert "fixture_layout" not in fixture
        local = tmp_path / "control"; local.mkdir()
        record = {"arm": "control", "customers_dispatched": False}
        receipt = identity.retain_fixture_identity(local, record, fixture, 1, producer_sha256=fingerprint)
        assert receipt["fixture_identity"]["fixture_layout"] == "distributed"
        assert receipt["producer_sha256"] == fingerprint
        assert json.loads((local / "stage.private.json").read_text())["customers_dispatched"] is False
        with db.transaction() as conn:
            assert conn.execute("SELECT count(*) AS n FROM event_seats WHERE event_id=%s", (fixture["show_ids"][0],)).fetchone()["n"] == 300
    finally:
        if fixture:
            client = Redis.from_url("redis://127.0.0.1:6379/15")
            try:
                for show in fixture["show_ids"]:
                    client.delete(RedisSeats.key(show), RedisSeats.delta_key(show))
            finally:
                client.close()

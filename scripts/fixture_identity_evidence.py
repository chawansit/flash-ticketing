"""ADR0185 minimal owned fixture identity, persisted before buyer dispatch."""
import hashlib
import json
import os
from datetime import UTC, datetime
from pathlib import Path
from uuid import UUID

LEGACY_DISTRIBUTED_PRODUCER = "99d09157747843478768a5d8be7d3cb326cbd2d4d4387e8873c501fca4fcc13c"


def fixture_identity(fixture, expected_shows, *, now=None, producer_sha256=None):
    if "fixture_layout" not in fixture and producer_sha256 == LEGACY_DISTRIBUTED_PRODUCER:
        fixture = {**fixture, "fixture_layout": "distributed"}
    now = now or datetime.now(UTC)
    if type(expected_shows) is not int or expected_shows not in {1, 60, 84}:
        raise ValueError("Exact bounded stage show count required")
    if (fixture.get("schema_version") != 1 or type(fixture.get("schema_version")) is not int
            or fixture.get("environment") != "development" or fixture.get("fixture_layout") != "distributed"
            or type(fixture.get("shows")) is not int or fixture["shows"] != expected_shows
            or type(fixture.get("seats_per_show")) is not int or fixture["seats_per_show"] != 300):
        raise ValueError("Exact distributed development fixture required")
    shows = fixture.get("show_ids")
    if not isinstance(shows, list) or len(shows) != expected_shows:
        raise ValueError("Exact show identity count required")
    for value in [fixture.get("fixture_id"), *shows]:
        if not isinstance(value, str) or str(UUID(value)) != value:
            raise ValueError("Canonical UUID ownership required")
    if len(set(shows)) != len(shows):
        raise ValueError("Distinct show identities required")
    created, end = (datetime.fromisoformat(fixture[k]) for k in ("created_at", "sale_ends"))
    if (created.tzinfo is None or end.tzinfo is None or now.tzinfo is None
            or not 0 <= (now - created).total_seconds() <= 120
            or not 0 < (end - created).total_seconds() <= 3600 or end <= now):
        raise ValueError("Fresh open fixture window required")
    keys = ("schema_version", "environment", "fixture_id", "created_at", "sale_ends", "shows",
            "seats_per_show", "fixture_layout", "show_ids")
    return {k: list(fixture[k]) if k == "show_ids" else fixture[k] for k in keys}


def retain_fixture_identity(local, record, fixture, expected_shows, *, now=None, producer_sha256=None):
    local = Path(local)
    if (record.get("arm") not in {"control", "candidate"} or local.name != record["arm"]
            or local.is_symlink() or not local.is_dir() or local.absolute() != local.resolve()
            or record.get("customers_dispatched") is not False):
        raise ValueError("Owned undispatched stage directory required")
    identity = fixture_identity(fixture, expected_shows, now=now, producer_sha256=producer_sha256)
    canonical = json.dumps(identity, sort_keys=True, separators=(",", ":")).encode()
    receipt = {"decision": "ADR0185", "arm": record["arm"], "fixture_identity": identity,
               "fixture_identity_sha256": hashlib.sha256(canonical).hexdigest()}
    if producer_sha256 is not None:
        if not isinstance(producer_sha256, str) or len(producer_sha256) != 64 or any(c not in "0123456789abcdef" for c in producer_sha256):
            raise ValueError("Canonical producer fingerprint required")
        receipt["producer_sha256"] = producer_sha256
    with (local / "fixture-identity.json").open("x", encoding="utf-8") as target:
        target.write(json.dumps(receipt, indent=2) + "\n")
        target.flush()
        os.fsync(target.fileno())
    record["fixture_identity"] = receipt
    with (local / "stage.private.json").open("x", encoding="utf-8") as target:
        target.write(json.dumps(record, indent=2) + "\n")
        target.flush()
        os.fsync(target.fileno())
    return receipt

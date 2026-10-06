"""ADR0181 transfer, interruption and exact profile contracts; no cloud calls."""
import copy
import hashlib
import io
import sys
from pathlib import Path
from types import SimpleNamespace

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[2] / "scripts"))
import diagnostic_placement_contract as policy
import diagnostic_runner_connection as staging
import run_diagnostic_placement_comparison as runner
import run_two_host_paid_comparison as paid
import work_envelope as envelope
from test_api_placement_comparison import observed
from test_work_envelope import area  # noqa: F401 - isolated state fixture

CERT = b"-----BEGIN CERTIFICATE-----\nfixture\n-----END CERTIFICATE-----\n"
SECRET = "synthetic-credential-never-published"
TARGET = {"host": "10.0.0.1", "port": 5432, "dbname": "ticketing", "user": "root",
          "ca_source_path": "/root/existing/secrets/ca.pem", "ca_sha256": hashlib.sha256(CERT).hexdigest()}


@pytest.mark.parametrize("key,value", [("host", "8.8.8.8"), ("port", True), ("port", 6432),
    ("user", "application"), ("dbname", "other database"), ("ca_source_path", "/root/x/../ca.pem"),
    ("ca_source_path", "/tmp/ca.pem"), ("ca_sha256", "0"), ("password", SECRET)])
def test_target_drift_rejected_before_cloud(key, value):
    bad = dict(TARGET, **{key: value})
    with pytest.raises(ValueError):
        staging.ProtectedContext(bad, SECRET)


def test_context_is_immutable_secret_repr_and_clearable():
    context = staging.ProtectedContext(TARGET, SECRET)
    assert SECRET not in repr(context)
    with pytest.raises(TypeError):
        context.target["host"] = "10.0.0.2"
    context.clear()
    assert context.password is None


class Transport:
    def __enter__(self): return self
    def __exit__(self, *args): return None
    def open(self, *args): return io.BytesIO(CERT)
    def close(self): return None


class Session:
    def __init__(self, failure=None):
        self.calls, self.failure = [], failure
        self.clients = {"primary": SimpleNamespace(open_sftp=Transport)}

    def api(self, cid, program, timeout):
        compile(program, "remote-fixture", "exec")
        self.calls.append(("api", program))
        assert SECRET not in program
        if "diagnostic_helpers_verified" in program and "SimpleNamespace" not in program:
            return {"diagnostic_helpers_verified": True}
        if "SimpleNamespace" in program:
            if self.failure == "preflight": raise ValueError("preflight unavailable")
            return {"pass": True, "diagnostic_helpers_verified": True, "read_only_connection_verified": True,
                    "verified_tls": True, "one_data_connection": True,
                    "identity_sha256": staging.identity("root", "ticketing"), "max_collection_ms": 2}
        if self.failure == "container_cleanup": raise KeyboardInterrupt()
        return {"diagnostic_credentials_removed": True}

    def call(self, role, program, timeout):
        compile(program, "remote-fixture", "exec")
        self.calls.append(("call", program))
        assert SECRET not in program
        if "docker" in program:
            return [{"Mounts": [{"Destination": "/etc/pgbouncer/rds-ca.pem", "Source": TARGET["ca_source_path"]}]}]
        if self.failure == "host_cleanup": raise OSError("unavailable")
        return {"diagnostic_credentials_removed": True}


@pytest.mark.parametrize("failure", [None, "preflight", "partial_ca", "partial_bundle"])
def test_transfer_secret_only_in_protected_content_cleanup_after_partial_failure(failure):
    session = Session(failure)
    uploads = []
    context = staging.ProtectedContext(TARGET, SECRET)
    _, inventory = observed("control")

    def upload(s, cid, owner, directory, name, content):
        uploads.append((name, content))
        if name == ("diagnostic-ca.pem" if failure == "partial_ca" else "diagnostic.private.json" if failure == "partial_bundle" else ""):
            raise KeyboardInterrupt()
        return directory + "/" + name

    try:
        if failure:
            with pytest.raises((ValueError, KeyboardInterrupt)):
                staging.prepare(session, "container", "/owned/arm", "/tmp/owned-arm", inventory, upload, context)
        else:
            result = staging.prepare(session, "container", "/owned/arm", "/tmp/owned-arm", inventory, upload, context)
            assert result["diagnostic_connection_preflight"]["pass"] is True
            assert SECRET not in repr(result) and SECRET not in repr(inventory)
        assert all(SECRET not in content for name, content in uploads if name != "diagnostic.private.json")
    finally:
        assert staging.cleanup(session, "container", "/owned/arm", "/tmp/owned-arm") == {"diagnostic_credentials_removed": True}
        context.clear()
    assert len([row for row in session.calls if "diagnostic_credentials_removed" in row[1]]) == 2


@pytest.mark.parametrize("failure", ["container_cleanup", "host_cleanup"])
def test_cleanup_attempts_both_sites_and_reports_uncertainty(failure):
    session = Session(failure)
    with pytest.raises(RuntimeError, match="recovery"):
        staging.cleanup(session, "container", "/owned/arm", "/tmp/owned-arm")
    assert len(session.calls) == 2


def test_missing_context_never_transfers_or_connects():
    session = Session()
    with pytest.raises(ValueError):
        staging.prepare(session, "container", "/owned/arm", "/tmp/owned-arm", {}, None, None)
    assert not session.calls


def test_profile_pins_all_new_sources_and_preserves_placement_budget():
    engine = runner.create_runner()
    identifiers = engine.identity()
    for name in ("diagnostic_connection.py", "diagnostic_runner_connection.py", "run_diagnostic_placement_comparison.py", "diagnostic_placement_contract.py"):
        assert "scripts/" + name in identifiers
    plan = policy.plan()
    off = engine.StatusRefreshContract(plan["artifact_receipt"], "control", plan["expected_runtime_source_sha256"])
    on = engine.StatusRefreshContract(plan["artifact_receipt"], "candidate", plan["expected_runtime_source_sha256"])
    assert off.api_settings == on.api_settings and off.images == on.images
    assert off.measured_api_counts == {"primary": 2, "secondary": 2}
    assert on.measured_api_counts == {"primary": 1, "secondary": 3}
    assert off.inventory_marker()["decision"] == "ADR0174"
    paid.Stages(False, {}, ledger_key=runner.LEDGER, stage_limit=1, contract=off)
    with pytest.raises(ValueError, match="context"):
        engine.binding_for({}, {}, {})
    engine.configure_diagnostic(TARGET, SECRET)
    assert engine.StatusRefreshContract(plan["artifact_receipt"], "control", plan["expected_runtime_source_sha256"]).diagnostic_context is engine.diagnostic_context
    engine.clear_diagnostic()
    assert engine.diagnostic_context.password is None


def test_registration_requires_exact_target_and_preserves_failed_scope_history(area):  # noqa: F811 - shared pytest fixture
    binding, _, _ = area
    plan = policy.plan()
    configured = envelope.read(envelope.ENVELOPE)
    configured["qualified_profiles"] = list(envelope.PROFILES)[:-1]
    envelope.write(envelope.ENVELOPE, configured)
    assert "diagnostic_placement" not in configured["qualified_profiles"]
    with pytest.raises(ValueError, match="Unknown diagnostic profile"):
        envelope.reserve(binding, plan, profile="diagnostic_placement")
    configured["qualified_profiles"] = list(envelope.PROFILES)
    envelope.write(envelope.ENVELOPE, configured)
    with pytest.raises(ValueError, match="target binding"):
        envelope.reserve(binding, plan, profile="diagnostic_placement")
    binding = {**binding, "diagnostic_target_sha256": envelope.digest(TARGET)}
    entry = envelope.reserve(binding, plan, profile="diagnostic_placement")
    assert entry["profile"] == "diagnostic_placement"
    assert envelope.scope_authorized(envelope.read(envelope.STATE), entry["ledger"], binding) == entry
    drift = copy.deepcopy(binding); drift["diagnostic_target_sha256"] = "0" * 64
    with pytest.raises(ValueError):
        envelope.scope_authorized(envelope.read(envelope.STATE), entry["ledger"], drift)


def test_target_cannot_change_after_configuration():
    engine = runner.create_runner()
    engine.configure_diagnostic_target(TARGET)
    with pytest.raises(ValueError, match="cannot change"):
        engine.configure_diagnostic(dict(TARGET, host="10.0.0.2"), SECRET)
    assert engine.diagnostic_context is None
    with pytest.raises(TypeError):
        engine.diagnostic_target["host"] = "10.0.0.2"


@pytest.mark.parametrize("failure", ["cleanup", "preflight", "visibility", "payment", "drain"])
def test_new_profile_never_weakens_customer_financial_or_diagnostic_gates(monkeypatch, failure):
    engine = runner.create_runner()
    monkeypatch.setattr(engine.comparison, "gates_for_stage", lambda *a: {"paid": {"cache_disabled": True}, "additional": {}})
    monkeypatch.setattr(engine, "evaluate_gates", lambda *a: {"all_required_gates_pass": True, "failed_gates": []})
    plan = policy.plan()
    c = engine.StatusRefreshContract(plan["artifact_receipt"], "control", plan["expected_runtime_source_sha256"])
    monkeypatch.setattr(c, "validate_inventory_receipt", lambda *a, **k: True)
    record = {"confirmation_receipts": {"pass": True}, "global_queues": {"pass": True,
        "pending_confirmation_receipts": 0, "review_confirmation_receipts": 0,
        "confirmation_capacity_outstanding": 0, "confirmation_capacity_mismatches": 0},
        "admission_failure_capture": {"complete": True}, "slow_database_capture": {"complete": True},
        "database_wait_capture": {"complete": True}, "diagnostic_credentials_removed": True,
        "diagnostic_connection_preflight": {k: True for k in ("pass", "diagnostic_helpers_verified", "read_only_connection_verified", "verified_tls", "one_data_connection")}}
    key = {"cleanup": "diagnostic_credentials_removed", "preflight": "diagnostic_connection_verified",
           "visibility": "database_wait_evidence_complete", "payment": "durable_confirmation_receipts_complete",
           "drain": "durable_confirmation_global_drain"}[failure]
    if failure == "cleanup": record["diagnostic_credentials_removed"] = False
    elif failure == "preflight": record["diagnostic_connection_preflight"]["verified_tls"] = False
    elif failure == "visibility": record["database_wait_capture"]["complete"] = False
    elif failure == "payment": record["confirmation_receipts"]["pass"] = False
    else: record["global_queues"]["pending_confirmation_receipts"] = 1
    result = engine.stage_gates(record, {}, {}, c)
    assert not result["all_required_gates_pass"] and key in result["failed_gates"]


def test_final_diagnostic_inventory_receipt_rejects_old_digest_and_preserves_chronology(tmp_path):
    from datetime import UTC, datetime, timedelta

    c, data = observed("control")
    record = {"arm": "control", "inventory_qualification": c.qualify_inventory(data),
              "inventory_contract": {"inventory_contract_pass": True}, "pre_dispatch_qualified": True}
    data["diagnostic_connection_binding"] = {"decision": "ADR0180", "bundle_sha256": "a" * 64}
    now = datetime.now(UTC)
    record.update(pre_dispatch_qualified_at_utc=now.isoformat(), dispatch_requested_at_utc=now.isoformat(),
                  scheduled_offered_start_utc=(now + timedelta(seconds=5)).isoformat())
    with pytest.raises(ValueError, match="inventory receipt"):
        c.validate_inventory_receipt(record, data, final=False, now=now)
    staging.qualify_bound_inventory(c, data, record, tmp_path)
    now = datetime.now(UTC)
    record.update(pre_dispatch_qualified_at_utc=now.isoformat(), dispatch_requested_at_utc=now.isoformat())
    c.validate_inventory_receipt(record, data, final=False, now=now)
    data["diagnostic_connection_binding"]["bundle_sha256"] = "b" * 64
    with pytest.raises(ValueError, match="inventory receipt"):
        c.validate_inventory_receipt(record, data, final=False, now=now)

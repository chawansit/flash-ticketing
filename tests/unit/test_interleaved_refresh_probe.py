"""ADR0222 consumer-only 84/s runner seams and correctness obligations; no cloud calls."""
import copy
import json
import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[2] / "scripts"))
import interleaved_refresh_probe_contract as policy
import observe_two_host_pipeline as pipeline
import run_async_confirmation_comparison as factory
import run_interleaved_refresh_probe as runner
import run_two_host_paid_comparison as paid
import work_envelope as envelope
from prepare_two_host_scaling import validate_inventory
from status_refresh_contract import digest
from test_diagnostic_runner_connection import TARGET
from test_shared_callback_placement_comparison import observed, samples
from test_work_envelope import area  # noqa: F401


def contract():
    data = policy.plan()
    return policy.InterleavedRefreshProbeContract(data["artifact_receipt"], "candidate", data["expected_runtime_source_sha256"])


def probe_inventory():
    _old, data = observed("candidate")
    c = contract()
    data["status_refresh_contract"] = c.inventory_marker()
    for row in data["worker_sources"]:
        row["image_id"] = c.images[row["role"]]
        row["source_identity"]["resolved_imports"] = {p: {"sha256": h} for p,h in c.source_map(row["role"]).items()}
    return data


def test_only_consumer_changes_from_identified_failed_baseline():
    old, _ = observed("candidate")
    new = contract()
    assert {r for r in new.roles if new.images[r] != old.images[r]} == {"consumer"}
    assert new.sources == old.sources and new.parents == old.parents
    assert {p for p in new.source_map("consumer") if new.source_map("consumer")[p] != old.sources[p]} == {"src/ticketing/workers.py"}
    assert all(new.source_map(r) == old.source_map(r) for r in new.roles if r != "consumer")
    assert all(new.settings(r) == old.settings(r) for r in new.roles)
    assert new.measured_api_counts == {"primary": 1, "secondary": 3}
    assert new.cpu_placement == "one-plus-three"
    assert policy.plan()["common"] == policy.parent.plan()["common"]
    assert policy.plan()["arms"] == ["candidate"]


def test_real_constructor_expected_fixture_and_financial_counts():
    engine = runner.create_runner()
    engine.configure_diagnostic_target(TARGET)
    assert engine.ARMS == ("candidate",)
    stage = engine.RefreshStages(False, {}, contract(), "adr0151-" + "a" * 12)
    assert stage.rate == 84 and stage.expected_tickets == 25200 and stage.stage_limit == 1
    audit = paid.financial_audit_program(["00000000-0000-0000-0000-000000000001"], stage.expected_tickets)
    assert "audit(conn,events,25200,25200,1)" in audit
    compile(audit, "financial-audit", "exec")
    assert {"scripts/interleaved_refresh_probe_contract.py", "scripts/run_interleaved_refresh_probe.py",
            "scripts/shared_callback_placement_contract.py", "scripts/database_wait_evidence.py",
            "scripts/admission_failure_evidence.py"} <= set(engine.identity())
    for rate in (60, 85, True):
        with pytest.raises(ValueError):
            paid.Stages(False, {}, rate=rate, ledger_key=runner.LEDGER, stage_limit=1, contract=contract())
    with pytest.raises(ValueError):
        factory.create_runner(policy_module=policy, decision="ADR0222")
    with pytest.raises(ValueError):
        policy.InterleavedRefreshProbeContract(policy.plan()["artifact_receipt"], "control", policy.plan()["expected_runtime_source_sha256"])


def test_real_inspected_observer_and_shared_callback_gate(tmp_path):
    data = probe_inventory()
    validate_inventory(data, image_id=contract().images["api"], contract=contract())
    pipeline.verify_admission_factor_evidence(data)
    frozen = pipeline.load_frozen(Path("scripts/observe_paid_pipeline.py"))
    endpoints = pipeline.install_adapter(frozen, data, image_id=contract().images["api"], approved_inventory_sha256=digest(data))
    assert sum(e["host_role"] == "secondary" for e in endpoints.values()) == 3
    rows, _old, window = samples("candidate")
    path = tmp_path / "pipeline.jsonl"
    path.write_text("\n".join(json.dumps(r) for r in rows))
    assert runner.create_runner().measurements(window, path, data)["callback_routing"]["callback_routing_verified"]
    data["status_refresh_contract"]["arm"] = "control"
    with pytest.raises(ValueError): pipeline.verify_admission_factor_evidence(data)


def test_exact_fresh_single_candidate_reservation(area):  # noqa: F811
    binding, _, old = area
    data = envelope.read(envelope.ENVELOPE)
    data["qualified_profiles"] = list(envelope.PROFILES)
    envelope.write(envelope.ENVELOPE, data)
    binding = {**binding, "diagnostic_target_sha256": digest(TARGET)}
    entry = envelope.reserve(binding, policy.plan(), profile="interleaved_refresh_probe")
    state = envelope.read(envelope.STATE)
    assert state[envelope.BASE_LEDGER] == old[envelope.BASE_LEDGER]
    assert state[entry["ledger"]]["paid_runs_authorized"] == 1
    assert state[entry["ledger"]]["safety_tickets_authorized"] == 2
    assert envelope.scope_authorized(state, entry["ledger"], binding) == entry


@pytest.mark.parametrize("drift", ["rate", "arms", "duration", "allowance", "target"])
def test_scope_drift_never_consumes_reservation(area, drift):  # noqa: F811
    binding, _, _ = area
    data = envelope.read(envelope.ENVELOPE); data["qualified_profiles"] = list(envelope.PROFILES)
    envelope.write(envelope.ENVELOPE, data)
    binding = {**binding, "diagnostic_target_sha256": digest(TARGET)}
    plan = copy.deepcopy(policy.plan())
    if drift == "rate": plan["common"]["buyer_journeys_per_second"] = 85
    elif drift == "arms": plan["arms"] = ["control", "candidate"]
    elif drift == "duration": plan["common"]["duration_seconds"] = 30
    elif drift == "allowance": plan["allowance"]["paid_runs_authorized"] = 2
    else: binding.pop("diagnostic_target_sha256")
    with pytest.raises(ValueError): envelope.reserve(binding, plan, profile="interleaved_refresh_probe")
    assert envelope.read(envelope.JOURNAL)["experiments"] == []


def test_real_cli_constructor_reaches_reservation_without_cloud(monkeypatch, tmp_path):
    import run_status_refresh_comparison as original
    import run_work_envelope as cli

    cfg = {"primary": {"repo": "/root/test-primary", "private_ipv4": "10.0.0.1"},
           "secondary": {"prepared_directory": "/root/test-secondary", "private_ipv4": "10.0.0.2"},
           "generator": {"repo": "/root/test-generator", "private_ipv4": "10.0.0.3"}}
    config_path = tmp_path / "config.json"; config_path.write_text(json.dumps(cfg))
    target_path = tmp_path / "target.json"; target_path.write_text(json.dumps(TARGET))
    monkeypatch.setattr(sys.stdin, "isatty", lambda: True)
    class Lock:
        def __init__(self, identity): pass
        def release(self): pass
    monkeypatch.setattr(original, "RunLock", Lock)
    monkeypatch.setattr(envelope, "LOCK", tmp_path / "lock")
    def reserve(binding, plan, **kwargs):
        assert plan == policy.plan() and kwargs["profile"] == "interleaved_refresh_probe"
        assert binding["diagnostic_target_sha256"] == digest(TARGET)
        raise RuntimeError("local-reservation-boundary-reached")
    monkeypatch.setattr(envelope, "reserve", reserve)
    with pytest.raises(RuntimeError, match="local-reservation-boundary-reached"):
        cli.execute(config_path, None, tmp_path, profile_name="interleaved_refresh_probe", diagnostic_target=target_path)

@pytest.mark.parametrize("role", ["api", "simulator", "reservation-writer", "consumer"])
def test_inspected_role_map_drift_rejected(role):
    data = probe_inventory()
    row = next(w for w in data["worker_sources"] if w["role"] == ("publisher" if role == "api" else role))
    row["source_identity"]["resolved_imports"]["src/ticketing/workers.py"]["sha256"] = "0" * 64
    with pytest.raises(ValueError, match="per-role"):
        contract().verify_inventory(data)


def test_per_role_image_labels_and_staging_candidate_arm():
    c = contract()
    assert c.image_manifest("consumer") != c.image_manifest("api")
    assert all(c.image_manifest(r) == c.source_manifest for r in c.roles if r != "consumer")
    program = c.image_program(c.roles)
    compile(program, "per-role-images", "exec")
    assert c.image_manifest("consumer") in program and c.source_manifest in program
    engine = runner.create_runner()
    assert hasattr(engine, "stage_images")
    assert {"artifacts/interleaved-seat-refresh/manifest.json", "artifacts/interleaved-seat-refresh/adr0222.patch",
            "scripts/prepare_interleaved_refresh.py"} <= set(engine.identity())


@pytest.mark.parametrize("role", ["api", "simulator", "reservation-writer"])
def test_other_role_image_changes_rejected(role):
    data = copy.deepcopy(policy.plan())
    data["artifact_receipt"]["images"][role] = "sha256:" + "0" * 64
    with pytest.raises(ValueError):
        policy.InterleavedRefreshProbeContract(data["artifact_receipt"], "candidate", data["expected_runtime_source_sha256"])

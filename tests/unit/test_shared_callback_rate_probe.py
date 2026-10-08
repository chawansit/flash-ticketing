"""ADR0219 exact 84/s runner seams and correctness obligations; no cloud calls."""
import copy
import json
import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[2] / "scripts"))
import observe_two_host_pipeline as pipeline
import run_async_confirmation_comparison as factory
import run_shared_callback_rate_probe as runner
import run_two_host_paid_comparison as paid
import shared_callback_rate_probe_contract as policy
import work_envelope as envelope
from prepare_two_host_scaling import validate_inventory
from status_refresh_contract import digest
from test_diagnostic_runner_connection import TARGET
from test_shared_callback_placement_comparison import observed, samples
from test_work_envelope import area  # noqa: F401


def contract():
    data = policy.plan()
    return policy.SharedCallbackRateProbeContract(data["artifact_receipt"], "candidate", data["expected_runtime_source_sha256"])


def probe_inventory():
    _old, data = observed("candidate")
    data["status_refresh_contract"] = contract().inventory_marker()
    return data


def test_rate_only_changes_from_identified_measured_baseline():
    old, _ = observed("candidate")
    new = contract()
    assert new.images == old.images and new.sources == old.sources
    assert all(new.settings(role) == old.settings(role) for role in new.roles)
    assert new.measured_api_counts == {"primary": 1, "secondary": 3}
    assert new.cpu_placement == "one-plus-three"
    common = copy.deepcopy(policy.plan()["common"])
    common.update(primary_apis=2, secondary_apis=2, buyer_journeys_per_second=60)
    assert common == policy.parent.plan()["common"]
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
    assert {"scripts/shared_callback_rate_probe_contract.py", "scripts/run_shared_callback_rate_probe.py",
            "scripts/shared_callback_placement_contract.py", "scripts/database_wait_evidence.py",
            "scripts/admission_failure_evidence.py"} <= set(engine.identity())
    for rate in (60, 85, True):
        with pytest.raises(ValueError):
            paid.Stages(False, {}, rate=rate, ledger_key=runner.LEDGER, stage_limit=1, contract=contract())
    with pytest.raises(ValueError):
        factory.create_runner(policy_module=policy, decision="ADR0219")
    with pytest.raises(ValueError):
        policy.SharedCallbackRateProbeContract(policy.plan()["artifact_receipt"], "control", policy.plan()["expected_runtime_source_sha256"])


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
    entry = envelope.reserve(binding, policy.plan(), profile="shared_callback_rate_probe")
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
    with pytest.raises(ValueError): envelope.reserve(binding, plan, profile="shared_callback_rate_probe")
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
        assert plan == policy.plan() and kwargs["profile"] == "shared_callback_rate_probe"
        assert binding["diagnostic_target_sha256"] == digest(TARGET)
        raise RuntimeError("local-reservation-boundary-reached")
    monkeypatch.setattr(envelope, "reserve", reserve)
    with pytest.raises(RuntimeError, match="local-reservation-boundary-reached"):
        cli.execute(config_path, None, tmp_path, profile_name="shared_callback_rate_probe", diagnostic_target=target_path)

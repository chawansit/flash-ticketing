"""ADR0174 placement, financial gate and scope regressions; no cloud calls."""
import copy
import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[2] / "scripts"))
import api_placement_contract as policy
import database_wait_contract as baseline
import observe_two_host_pipeline as pipeline
import run_api_placement_comparison as runner
import run_two_host_paid_comparison as paid
import work_envelope as envelope
from prepare_two_host_scaling import validate_inventory
from test_async_confirmation_comparison import inventory
from test_two_host_deployment import fixture
from test_work_envelope import area  # noqa: F401 - shared isolated state fixture
from two_host_topology import deployment_model, snapshot


def contract(arm="control"):
    data = policy.plan()
    return policy.ApiPlacementContract(data["artifact_receipt"], arm, data["expected_runtime_source_sha256"])


def observed(arm):
    c = contract(arm)
    data = inventory(c)
    if arm == "candidate":
        a = data["apis"][1]
        a.update(host_role="secondary", private_ipv4=data["hosts"]["secondary"]["private_ipv4"],
                 database_url_host=data["hosts"]["primary"]["private_ipv4"])
    return c, data


def test_one_factor_only_and_no_background_or_budget_changes():
    off, on = contract(), contract("candidate")
    original = baseline.plan()
    assert policy.plan()["common"] == original["common"]
    assert off.images == on.images and off.sources == on.sources
    assert off.api_settings == on.api_settings
    assert all(off.settings(r) == on.settings(r) for r in off.roles)
    assert on.api_settings["API_PARTIAL_TIMEOUT_RECLAIM"] == "0"
    cfg, rows = fixture()
    saved = snapshot(cfg, rows, image_id=policy.frozen.export.base.IMAGE)
    model = deployment_model(saved, primary_ip="10.0.0.1", nginx_path="/owned/nginx.conf")
    assert off.primary_model(model) == on.primary_model(model)
    assert off.secondary_model({"services": {"api": {"environment": {}}}}) == on.secondary_model({"services": {"api": {"environment": {}}}})


@pytest.mark.parametrize("arm", ["control", "candidate"])
def test_complete_inventory_and_observer_require_exact_placement(arm):
    c, data = observed(arm)
    view = validate_inventory(data, image_id=c.images["api"], contract=c)
    assert view["api_counts"] == policy.PLACEMENTS[arm]
    assert view["api_connections_total"] == 16
    assert view["payment_connections_included"] == 8
    assert view["pgbouncer_server_connections"] == 24
    pipeline.verify_admission_factor_evidence(data)
    rows = [{"Id": a["container_id"], "Config": {"Labels": {"com.docker.compose.service": "api"}}} for a in data["apis"]]
    for role in ("primary", "secondary"):
        selected = [r for r, a in zip(rows, data["apis"], strict=True) if a["host_role"] == role]
        emitted = paid.cpu_spec(arm, role, selected, data, fixed_two_host=True, placement=c.cpu_placement)
        assert emitted["placement"] == c.cpu_placement
    bad = copy.deepcopy(data)
    bad["apis"][1]["host_role"] = "primary" if arm == "candidate" else "secondary"
    with pytest.raises(ValueError):
        pipeline.verify_admission_factor_evidence(bad)
    with pytest.raises(ValueError):
        validate_inventory(bad, image_id=c.images["api"], contract=c)


@pytest.mark.parametrize("drift", ["pool", "callback", "background", "flag", "image", "worker", "marker", "pooler"])
def test_candidate_rejects_nonplacement_drift(drift):
    c, data = observed("candidate")
    if drift == "pool": data["apis"][0]["settings"]["DB_POOL_MAX"] = "8"
    elif drift == "callback": data["apis"][0]["settings"]["API_CALLBACK_ACQUISITION_RESERVE"] = "1"
    elif drift == "background": data["background"]["consumer"]["replicas"] = 7
    elif drift == "flag": data["apis"][0]["settings"]["API_PARTIAL_TIMEOUT_RECLAIM"] = "1"
    elif drift == "image": data["apis"][0]["image_id"] = "sha256:" + "f" * 64
    elif drift == "worker": data["worker_sources"].pop()
    elif drift == "marker": data["status_refresh_contract"]["api_counts"] = {"primary": 2, "secondary": 2}
    else: data["pgbouncer"]["server_pool"] = 48
    with pytest.raises(ValueError):
        validate_inventory(data, image_id=c.images["api"], contract=c)


def test_real_constructors_and_identity_preserve_diagnostics():
    engine = runner.create_runner()
    assert engine.ARMS == ("control", "candidate")
    assert "scripts/database_wait_evidence.py" in engine.identity()
    for arm in engine.ARMS:
        stage = engine.RefreshStages(False, {}, contract(arm), "adr0151-" + "a" * 12)
        assert stage.rate == 60 and stage.expected_tickets == 18000
    with pytest.raises(ValueError):
        paid.Stages(False, {}, ledger_key=runner.LEDGER, stage_limit=1, contract=None)
    with pytest.raises(ValueError):
        paid.Stages(False, {}, ledger_key=runner.LEDGER, stage_limit=1, contract=baseline.DatabaseWaitContract(
            baseline.plan()["artifact_receipt"], "control", baseline.plan()["expected_runtime_source_sha256"]))


def test_fresh_pair_scope_has_exact_budget_and_cannot_be_single_control(area):  # noqa: F811 - pytest injects imported fixture
    binding, _, _ = area
    initial = envelope.read(envelope.ENVELOPE)
    initial["qualified_profiles"] = [envelope.PROFILE, "database_wait_control"]
    envelope.write(envelope.ENVELOPE, initial)
    with pytest.raises(ValueError, match="Unknown diagnostic profile"):
        envelope.reserve(binding, policy.plan(), profile="api_placement_rebalance")
    data = envelope.read(envelope.ENVELOPE)
    data["qualified_profiles"] = list(envelope.PROFILES)
    envelope.write(envelope.ENVELOPE, data)
    wrong = policy.plan(); wrong["arms"] = ["control"]
    with pytest.raises(ValueError):
        envelope.reserve(binding, wrong, profile="api_placement_rebalance")
    entry = envelope.reserve(binding, policy.plan(), profile="api_placement_rebalance")
    state = envelope.read(envelope.STATE)[entry["ledger"]]
    assert state["paid_runs_authorized"] == 2 and state["safety_tickets_authorized"] == 4
    assert envelope.base_ledger(entry["ledger"]) == runner.LEDGER
    assert envelope.scope_authorized(envelope.read(envelope.STATE), entry["ledger"], binding) == entry


def test_missing_database_diagnostics_blocks_adoption(monkeypatch):
    engine = runner.create_runner()
    monkeypatch.setattr(engine.comparison, "gates_for_stage", lambda *a: {"paid": {"cache_disabled": True}, "additional": {}})
    monkeypatch.setattr(engine, "evaluate_gates", lambda *a: {"all_required_gates_pass": True, "failed_gates": []})
    c = contract("candidate")
    monkeypatch.setattr(c, "validate_inventory_receipt", lambda *a, **k: True)
    record = {"confirmation_receipts": {"pass": True}, "global_queues": {"pass": True,
        "pending_confirmation_receipts": 0, "review_confirmation_receipts": 0,
        "confirmation_capacity_outstanding": 0, "confirmation_capacity_mismatches": 0},
        "admission_failure_capture": {"complete": True}, "slow_database_capture": {"complete": True}}
    result = engine.stage_gates(record, {}, {}, c)
    assert result["all_required_gates_pass"] is False
    assert "database_wait_evidence_complete" in result["failed_gates"]


@pytest.mark.parametrize("arm", ["control", "candidate"])
def test_rebalance_rejects_wrong_host_cpu_count(arm):
    c, data = observed(arm)
    wrong_rows = [{"Id": str(i + 1).zfill(64), "Config": {"Labels": {"com.docker.compose.service": "api"}}} for i in range(4)]
    with pytest.raises(ValueError):
        paid.cpu_spec(arm, "secondary", wrong_rows, data, fixed_two_host=True, placement=c.cpu_placement)


def test_single_arm_cannot_use_pair_decision():
    from run_async_confirmation_comparison import create_runner
    with pytest.raises(ValueError):
        create_runner(policy_module=policy, decision="ADR0174", arms=("control",))


@pytest.mark.parametrize("arm", ["control", "candidate"])
def test_full_installed_observer_keeps_real_routes_and_confirmation_metrics(arm):
    from status_refresh_contract import digest
    c, data = observed(arm)
    frozen = pipeline.load_frozen(Path("scripts/observe_paid_pipeline.py"))
    endpoints = pipeline.install_adapter(frozen, data, image_id=c.images["api"], approved_inventory_sha256=digest(data))
    assert len(endpoints) == 4
    assert {r: sum(a["host_role"] == r for a in endpoints.values()) for r in ("primary", "secondary")} == c.measured_api_counts
    assert frozen.METRICS["confirmation"] == ("confirm_one", "http://confirmation:9101/metrics")
    bad = copy.deepcopy(data); bad["apis"][0]["settings"]["DB_POOL_MAX"] = "8"
    with pytest.raises(ValueError):
        pipeline.install_adapter(frozen, bad, image_id=c.images["api"], approved_inventory_sha256=digest(bad))


@pytest.mark.parametrize("arm", ["control", "candidate"])
def test_actual_scrape_includes_all_admission_counters_and_rejects_missing_family(arm):
    import io

    from status_refresh_contract import digest
    from test_two_host_observers import payload
    c, data = observed(arm)
    frozen = pipeline.load_frozen(Path("scripts/observe_paid_pipeline.py"))
    body = payload() + "# TYPE ticketing_db_acquisition_failures_total counter\n"
    pipeline.install_adapter(frozen, data, image_id=c.images["api"], approved_inventory_sha256=digest(data),
                             fetch=lambda *a, **k: io.BytesIO(body.encode()))
    rows = {label: frozen.api_metrics(label) for label in frozen.api_replicas()}
    pipeline.admission_startup({"api_replicas": rows})
    assert all(len([k for k in v if k.startswith("acquisition_failure:")]) == 8 for v in rows.values())
    rows[next(iter(rows))].pop("acquisition_failure:payment:native_timeout")
    with pytest.raises(ValueError):
        pipeline.admission_startup({"api_replicas": rows})
    bad = pipeline.load_frozen(Path("scripts/observe_paid_pipeline.py"))
    pipeline.install_adapter(bad, data, image_id=c.images["api"], approved_inventory_sha256=digest(data),
                             fetch=lambda *a, **k: io.BytesIO(payload().encode()))
    with pytest.raises(ValueError):
        bad.api_metrics(bad.api_replicas()[0])


def test_placement_collects_slow_phase_and_failure_evidence(monkeypatch, tmp_path):
    import slow_database_evidence
    seen = []
    def capture(session, data, output):
        seen.append((session, data, output))
        return {"complete": True, "failure_count": 2, "counter_coverage": True}
    monkeypatch.setattr(slow_database_evidence, "collect", capture)
    value = paid.collect_profile_failure_evidence("session", {"apis": []}, tmp_path, runner.LEDGER)
    assert seen == [("session", {"apis": []}, tmp_path)]
    assert value["slow_database_capture"]["complete"]
    assert value["admission_failure_capture"]["record_count"] == 2

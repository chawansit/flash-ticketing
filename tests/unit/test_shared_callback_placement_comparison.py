"""ADR0217 exact placement and callback coverage regressions; no cloud calls."""
import copy
import json
import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[2] / "scripts"))
import callback_routing_contract as baseline
import observe_two_host_pipeline as pipeline
import run_callback_routing_comparison as callbacks
import run_shared_callback_placement_comparison as runner
import run_two_host_paid_comparison as paid
import shared_callback_placement_contract as policy
import work_envelope as envelope
from prepare_two_host_scaling import validate_inventory
from status_refresh_contract import digest
from test_async_confirmation_comparison import inventory
from test_callback_routing_comparison import samples as routing_samples
from test_diagnostic_runner_connection import TARGET
from test_two_host_deployment import fixture
from test_work_envelope import area  # noqa: F401 - isolated accounting fixture
from two_host_topology import deployment_model, snapshot


def contract(arm="control"):
    data = policy.plan()
    return policy.SharedCallbackPlacementContract(data["artifact_receipt"], arm, data["expected_runtime_source_sha256"])


def observed(arm):
    c = contract(arm)
    data = inventory(c)
    if arm == "candidate":
        data["apis"][1].update(host_role="secondary", private_ipv4=data["hosts"]["secondary"]["private_ipv4"],
                               database_url_host=data["hosts"]["primary"]["private_ipv4"])
    return c, data


def samples(arm):
    rows, _, window = routing_samples("candidate")
    _c, data = observed(arm)
    for row in rows:
        metrics = list(row["api_replicas"].values())
        row["api_replicas"] = {a["host_role"] + ":" + a["container_id"]: metric
                               for a, metric in zip(data["apis"], metrics, strict=True)}
    return rows, data, window


def test_only_placement_changes_after_routing_fix():
    off, on = contract(), contract("candidate")
    assert policy.plan()["common"] == baseline.plan()["common"]
    assert off.images == on.images and off.sources == on.sources
    assert off.api_settings == on.api_settings
    assert all(off.settings(role) == on.settings(role) for role in off.roles)
    assert off.settings("simulator")["API_URL"] == "http://load-balancer:8000"
    assert off.measured_api_counts == {"primary": 2, "secondary": 2}
    assert on.measured_api_counts == {"primary": 1, "secondary": 3}
    assert off.cpu_placement == "two-plus-two" and on.cpu_placement == "one-plus-three"
    cfg, rows = fixture()
    saved = snapshot(cfg, rows, image_id=policy.parent.frozen.export.base.IMAGE)
    model = deployment_model(saved, primary_ip="10.0.0.1", nginx_path="/owned/nginx.conf")
    assert off.primary_model(model) == on.primary_model(model)
    assert off.secondary_model({"services": {"api": {"environment": {}}}}) == on.secondary_model({"services": {"api": {"environment": {}}}})


@pytest.mark.parametrize("arm", ["control", "candidate"])
def test_real_inventory_observer_and_cpu_keep_exact_placement(arm):
    c, data = observed(arm)
    proof = validate_inventory(data, image_id=c.images["api"], contract=c)
    assert proof["api_connections_total"] == 16 and proof["payment_connections_included"] == 8
    assert proof["pgbouncer_server_connections"] == 24
    pipeline.verify_admission_factor_evidence(data)
    frozen = pipeline.load_frozen(Path("scripts/observe_paid_pipeline.py"))
    endpoints = pipeline.install_adapter(frozen, data, image_id=c.images["api"], approved_inventory_sha256=digest(data))
    assert {role: sum(a["host_role"] == role for a in endpoints.values())
            for role in ("primary", "secondary")} == c.measured_api_counts
    for role in ("primary", "secondary"):
        rows = [{"Id": a["container_id"], "Config": {"Labels": {"com.docker.compose.service": "api"}}}
                for a in data["apis"] if a["host_role"] == role]
        assert paid.cpu_spec(arm, role, rows, data, fixed_two_host=True, placement=c.cpu_placement)["placement"] == c.cpu_placement


@pytest.mark.parametrize("arm", ["control", "candidate"])
@pytest.mark.parametrize("drift", ["route", "placement", "pool", "worker", "marker"])
def test_inspected_drift_rejected_before_dispatch(arm, drift):
    c, data = observed(arm)
    if drift == "route": next(w for w in data["worker_sources"] if w["role"] == "simulator")["settings"]["API_URL"] = "http://api:8000"
    elif drift == "placement": data["apis"][0]["host_role"] = "secondary"
    elif drift == "pool": data["apis"][0]["settings"]["API_PAYMENT_POOL_MAX"] = "4"
    elif drift == "worker": data["worker_sources"].pop()
    else: data["status_refresh_contract"]["callback_url"] = "http://api:8000"
    with pytest.raises(ValueError): pipeline.verify_admission_factor_evidence(data)
    with pytest.raises(ValueError): validate_inventory(data, image_id=c.images["api"], contract=c)


@pytest.mark.parametrize("arm", ["control", "candidate"])
def test_actual_callbacks_required_on_every_replica_in_both_arms(arm):
    rows, data, window = samples(arm)
    kwargs = {**window, "expected_decision": "ADR0217", "routes": policy.ROUTES}
    assert callbacks.summarize_callbacks(rows, data, **kwargs)["callback_routing_verified"]
    label = next(k for k in rows[0]["api_replicas"] if k.startswith("secondary:"))
    for row in rows: row["api_replicas"][label][callbacks.PREFIX + "200"] = 0
    assert callbacks.summarize_callbacks(rows, data, **kwargs)["callback_routing_verified"] is False


def test_profile_uses_protected_runner_and_exact_constructor_identity():
    engine = runner.create_runner()
    with pytest.raises(ValueError, match="context"): engine.binding_for({}, {}, {})
    engine.configure_diagnostic_target(TARGET)
    assert {"scripts/shared_callback_placement_contract.py", "scripts/run_shared_callback_placement_comparison.py",
            "scripts/run_callback_routing_comparison.py", "scripts/callback_routing_contract.py",
            "scripts/database_wait_evidence.py", "scripts/diagnostic_connection.py"} <= set(engine.identity())
    for arm in engine.ARMS:
        stage = engine.RefreshStages(False, {}, contract(arm), "adr0151-" + "a" * 12)
        assert stage.expected_tickets == 18000 and stage.stage_limit == 1
    with pytest.raises(ValueError):
        paid.Stages(False, {}, ledger_key=runner.LEDGER, stage_limit=1,
                    contract=baseline.CallbackRoutingContract(baseline.plan()["artifact_receipt"], "candidate", baseline.plan()["expected_runtime_source_sha256"]))


def test_fresh_scope_requires_exact_plan_and_target(area):  # noqa: F811 - imported fixture
    binding, _, old = area
    data = envelope.read(envelope.ENVELOPE); data["qualified_profiles"] = list(envelope.PROFILES)
    envelope.write(envelope.ENVELOPE, data)
    with pytest.raises(ValueError, match="target"): envelope.reserve(binding, policy.plan(), profile="shared_callback_placement")
    binding = {**binding, "diagnostic_target_sha256": digest(TARGET)}
    wrong = copy.deepcopy(policy.plan()); wrong["callback_routes"]["control"] = "http://api:8000"
    with pytest.raises(ValueError): envelope.reserve(binding, wrong, profile="shared_callback_placement")
    entry = envelope.reserve(binding, policy.plan(), profile="shared_callback_placement")
    state = envelope.read(envelope.STATE)
    assert state[envelope.BASE_LEDGER] == old[envelope.BASE_LEDGER]
    assert state[entry["ledger"]]["paid_runs_authorized"] == 2
    assert state[entry["ledger"]]["safety_tickets_authorized"] == 4
    assert envelope.base_ledger(entry["ledger"]) == runner.LEDGER
    assert envelope.scope_authorized(state, entry["ledger"], binding) == entry


@pytest.mark.parametrize("drift", ["route", "placement", "duration"])
def test_plan_drift_rejected(monkeypatch, tmp_path, drift):
    data = copy.deepcopy(policy.plan())
    if drift == "route": data["callback_routes"]["control"] = "http://api:8000"
    elif drift == "placement": data["placements"]["candidate"] = {"primary": 2, "secondary": 2}
    else: data["common"]["duration_seconds"] = 30
    path = tmp_path / "wrong.json"; path.write_text(json.dumps(data))
    monkeypatch.setattr(policy, "PLAN", path)
    with pytest.raises(ValueError, match="Exact"): policy.plan()


@pytest.mark.parametrize("arm", ["control", "candidate"])
def test_runner_adds_measured_callback_gate_for_actual_window(arm, tmp_path):
    rows, data, window = samples(arm)
    path = tmp_path / "pipeline.jsonl"; path.write_text("\n".join(json.dumps(row) for row in rows))
    proof = runner.create_runner().measurements(window, path, data)["callback_routing"]
    assert proof["callback_routing_verified"] and proof["callback_url"] == "http://load-balancer:8000"

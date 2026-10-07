"""ADR0216 single-factor routing and complete coverage regressions; no cloud calls."""
import copy
import json
import sys
from datetime import UTC, datetime, timedelta
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[2] / "scripts"))
import callback_routing_contract as policy
import observe_two_host_pipeline as pipeline
import run_callback_routing_comparison as runner
import run_two_host_paid_comparison as paid
import work_envelope as envelope
from prepare_two_host_scaling import validate_inventory
from status_refresh_contract import digest
from test_async_confirmation_comparison import inventory
from test_diagnostic_runner_connection import TARGET
from test_two_host_deployment import fixture
from test_work_envelope import area  # noqa: F401 - isolated ledger fixture
from two_host_topology import deployment_model, snapshot


def contract(arm="control"):
    data = policy.plan()
    return policy.CallbackRoutingContract(data["artifact_receipt"], arm, data["expected_runtime_source_sha256"])


def test_model_changes_only_simulator_destination():
    off, on = contract(), contract("candidate")
    cfg, rows = fixture()
    saved = snapshot(cfg, rows, image_id=policy.parent.frozen.export.base.IMAGE)
    model = deployment_model(saved, primary_ip="10.0.0.1", nginx_path="/owned/nginx.conf")
    a, b = off.primary_model(model), on.primary_model(model)
    assert a["services"]["simulator"]["environment"]["API_URL"] == policy.ROUTES["control"]
    assert b["services"]["simulator"]["environment"]["API_URL"] == policy.ROUTES["candidate"]
    b["services"]["simulator"]["environment"]["API_URL"] = policy.ROUTES["control"]
    assert a == b
    assert off.images == on.images and off.sources == on.sources and off.api_settings == on.api_settings
    assert off.measured_api_counts == on.measured_api_counts == {"primary": 2, "secondary": 2}
    assert off.cpu_placement == on.cpu_placement == "two-plus-two"
    assert off.secondary_model({"services": {"api": {"environment": {}}}}) == on.secondary_model({"services": {"api": {"environment": {}}}})


@pytest.mark.parametrize("arm", ["control", "candidate"])
def test_real_inventory_and_remote_observer_keep_budgets(arm):
    c = contract(arm)
    data = inventory(c)
    proof = validate_inventory(data, image_id=c.images["api"], contract=c)
    assert proof["api_connections_total"] == 16 and proof["payment_connections_included"] == 8
    assert proof["pgbouncer_server_connections"] == 24
    pipeline.verify_admission_factor_evidence(data)
    frozen = pipeline.load_frozen(Path("scripts/observe_paid_pipeline.py"))
    endpoints = pipeline.install_adapter(frozen, data, image_id=c.images["api"], approved_inventory_sha256=digest(data))
    assert len(endpoints) == 4 and sum(a["host_role"] == "secondary" for a in endpoints.values()) == 2
    assert "confirmation" in frozen.METRICS


@pytest.mark.parametrize("drift", ["route", "missing_route", "placement", "worker", "pool", "source", "image", "marker"])
def test_route_and_identity_drift_fail_before_dispatch(drift):
    c = contract("candidate")
    data = inventory(c)
    simulator = next(w for w in data["worker_sources"] if w["role"] == "simulator")
    if drift == "route": simulator["settings"]["API_URL"] = policy.ROUTES["control"]
    elif drift == "missing_route": simulator["settings"].pop("API_URL")
    elif drift == "placement": data["apis"][1]["host_role"] = "secondary"
    elif drift == "worker": data["worker_sources"].pop()
    elif drift == "pool": data["apis"][0]["settings"]["API_PAYMENT_POOL_MAX"] = "4"
    elif drift == "source": simulator["source_identity"]["source_hashes_match"] = False
    elif drift == "image": simulator["image_id"] = "sha256:" + "0" * 64
    else: data["status_refresh_contract"]["callback_url"] = policy.ROUTES["control"]
    with pytest.raises(ValueError):
        validate_inventory(data, image_id=c.images["api"], contract=c)
    if drift in {"route", "missing_route", "placement", "worker", "pool", "marker"}:
        with pytest.raises(ValueError):
            pipeline.verify_admission_factor_evidence(data)


def samples(arm="candidate"):
    data = inventory(contract(arm))
    start = datetime(2026, 10, 8, tzinfo=UTC)
    rows = []
    for i in range(5):
        metrics = {}
        for a in data["apis"]:
            label = a["host_role"] + ":" + a["container_id"]
            count = i if arm == "candidate" or a["host_role"] == "primary" else 0
            metrics[label] = {"process_start_time_seconds": 100, "business_http_requests_total": i * 10,
                              runner.PREFIX + "200": count}
        rows.append({"utc": (start + timedelta(seconds=i)).isoformat(), "api_replicas": metrics})
    window = {"offered_start_utc": start.isoformat(), "offered_end_utc": (start + timedelta(seconds=4)).isoformat()}
    return rows, data, window


@pytest.mark.parametrize("arm", ["control", "candidate"])
def test_measured_callback_distribution_matches_the_inspected_route(arm):
    rows, data, window = samples(arm)
    result = runner.summarize_callbacks(rows, data, **window)
    assert result["callback_routing_verified"]
    assert result["callback_url"] == policy.ROUTES[arm]
    assert all(v == 4 for k, v in result["successful_callback_request_deltas"].items() if arm == "candidate" or k.startswith("primary:"))


@pytest.mark.parametrize("drift", ["restart", "counter_reset", "series_missing", "missing_replica", "gap", "negative", "nan", "unbracketed"])
def test_incomplete_or_reset_callback_evidence_never_qualifies(drift):
    rows, data, window = samples()
    label = next(iter(rows[2]["api_replicas"]))
    metric = rows[2]["api_replicas"][label]
    if drift == "restart": metric["process_start_time_seconds"] += 1
    elif drift == "counter_reset": metric[runner.PREFIX + "200"] = 0
    elif drift == "series_missing": metric.pop(runner.PREFIX + "200")
    elif drift == "missing_replica": rows[2]["api_replicas"].pop(label)
    elif drift == "gap": rows.pop(1); rows.pop(1)
    elif drift == "negative": metric[runner.PREFIX + "200"] = -1
    elif drift == "nan": metric[runner.PREFIX + "200"] = float("nan")
    else: rows.pop(0)
    with pytest.raises(ValueError):
        runner.summarize_callbacks(rows, data, **window)


def test_secondary_receives_other_business_but_no_callbacks_is_not_success():
    rows, data, window = samples()
    for row in rows:
        for label, metric in row["api_replicas"].items():
            if label.startswith("secondary:"): metric[runner.PREFIX + "200"] = 0
    assert pipeline.summarize_distribution(rows, data, **window)["all_four_replicas_observed"]
    assert runner.summarize_callbacks(rows, data, **window)["callback_routing_verified"] is False


def test_profile_preflight_is_exact_and_secret_free():
    engine = runner.create_runner()
    with pytest.raises(ValueError, match="context"):
        engine.binding_for({}, {}, {})
    engine.configure_diagnostic_target(TARGET)
    keys = engine.identity()
    assert {"scripts/callback_routing_contract.py", "scripts/run_callback_routing_comparison.py",
            "scripts/run_diagnostic_placement_comparison.py", "scripts/diagnostic_connection.py",
            "scripts/database_wait_evidence.py"} <= set(keys)
    for arm in engine.ARMS:
        stage = engine.RefreshStages(False, {}, contract(arm), "adr0151-" + "a" * 12)
        assert stage.expected_tickets == 18000 and stage.stage_limit == 1
    with pytest.raises(ValueError):
        paid.Stages(False, {}, ledger_key=runner.LEDGER, stage_limit=1, contract=policy.parent.ApiPlacementContract(
            policy.plan()["artifact_receipt"], "control", policy.plan()["expected_runtime_source_sha256"]))


def test_fresh_registration_exact_plan_and_protected_binding(area):  # noqa: F811 - imported pytest fixture
    binding, _, old = area
    data = envelope.read(envelope.ENVELOPE)
    data["qualified_profiles"] = list(envelope.PROFILES)
    envelope.write(envelope.ENVELOPE, data)
    with pytest.raises(ValueError, match="target"):
        envelope.reserve(binding, policy.plan(), profile="callback_routing")
    binding = {**binding, "diagnostic_target_sha256": digest(TARGET)}
    bad = copy.deepcopy(policy.plan()); bad["placements"]["candidate"]["primary"] = 1
    with pytest.raises(ValueError):
        envelope.reserve(binding, bad, profile="callback_routing")
    entry = envelope.reserve(binding, policy.plan(), profile="callback_routing")
    state = envelope.read(envelope.STATE)
    assert state[envelope.BASE_LEDGER] == old[envelope.BASE_LEDGER]
    assert state[entry["ledger"]]["paid_runs_authorized"] == 2
    assert state[entry["ledger"]]["safety_tickets_authorized"] == 4
    assert envelope.base_ledger(entry["ledger"]) == runner.LEDGER
    assert envelope.scope_authorized(state, entry["ledger"], binding) == entry


def test_missing_trace_fails_route_measurement(tmp_path):
    engine = runner.create_runner()
    result = engine.measurements({}, tmp_path / "missing.jsonl", inventory(contract()))
    assert result["callback_routing"]["callback_routing_verified"] is False


def test_route_proof_uses_actual_measured_window(tmp_path):
    engine = runner.create_runner()
    rows, data, window = samples()
    path = tmp_path / "pipeline.jsonl"
    path.write_text("\n".join(json.dumps(r) for r in rows))
    result = engine.measurements(window, path, data)
    assert result["callback_routing"]["callback_routing_verified"]


def test_naming_and_exact_plan_drift(monkeypatch, tmp_path):
    bad = policy.plan(); bad["callback_routes"]["candidate"] = "http://load-balancer:80"
    path = tmp_path / "wrong.json"; path.write_text(json.dumps(bad))
    monkeypatch.setattr(policy, "PLAN", path)
    with pytest.raises(ValueError, match="Exact"):
        policy.plan()


@pytest.mark.parametrize("verified", [True, False])
def test_routing_gate_fails_closed_after_existing_financial_gates(monkeypatch, verified):
    from types import SimpleNamespace
    fake = SimpleNamespace()
    fake.measurements = lambda *a: {}
    fake.run_arm = lambda *a, **k: {"pass": True, "measurements": {"callback_routing": {"callback_routing_verified": verified}},
        "gates": {"all_required_gates_pass": True, "failed_gates": []}}
    monkeypatch.setattr(runner, "protected_runner", lambda **k: fake)
    result = runner.create_runner().run_arm(execute=True)
    assert result["pass"] is verified
    assert result["gates"]["all_required_gates_pass"] is verified
    assert ("callback_routing_verified" in result["gates"]["failed_gates"]) is not verified

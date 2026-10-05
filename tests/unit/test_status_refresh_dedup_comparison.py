import copy
import json
import subprocess
import sys
from datetime import UTC, datetime, timedelta
from pathlib import Path
from uuid import uuid4

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[2] / "scripts"))
import observe_two_host_pipeline as observer
import run_status_refresh_comparison as original
import run_status_refresh_dedup_comparison as profile
import status_refresh_dedup_contract as policy
from prepare_two_host_scaling import EXTRA_GATES, validate_inventory
from status_refresh_contract import IMAGE
from test_status_refresh_comparison import observed, qualification, state
from test_two_host_paid_runner import gate_evidence


def contract(arm="control"):
    data = policy.plan()
    return policy.StatusRefreshDedupContract(data["artifact_receipt"], arm, data["expected_runtime_source_sha256"])


def inventory(c):
    data = observed(c)
    for api in data["apis"]:
        api["ORDER_STATUS_EVENT_REFRESH_DEDUP"] = "0"
    data["status_refresh_contract"] = c.inventory_marker()
    return data


def new_state(binding):
    data = state(binding)
    ledger = data.pop(original.LEDGER)
    ledger["authorization_id"] = profile.AUTHORIZATION
    data[profile.LEDGER] = ledger
    return data


def test_private_profile_does_not_mutate_old_engine_and_shares_exclusive_lock():
    before = (original.PLAN, original.LEDGER, original.AUTHORIZATION, original.StatusRefreshContract)
    engine = profile.create_runner()
    assert engine.LOCK == original.LOCK
    assert engine.LEDGER != original.LEDGER and engine.AUTHORIZATION != original.AUTHORIZATION
    assert engine.StatusRefreshContract is policy.StatusRefreshDedupContract
    assert before == (original.PLAN, original.LEDGER, original.AUTHORIZATION, original.StatusRefreshContract)
    assert engine is not profile.create_runner()
    identity = engine.identity()
    assert "scripts/run_status_refresh_dedup_comparison.py" in identity
    assert "scripts/collect_two_host_inventory.py" in identity
    assert "artifacts/status-refresh-dedup/manifest.json" in identity


def test_same_models_images_budgets_and_refresh_change_only_dedup():
    from test_two_host_deployment import fixture
    from two_host_topology import deployment_model, snapshot

    cfg, rows = fixture()
    saved = snapshot(cfg, rows, image_id=IMAGE)
    model = deployment_model(saved, primary_ip="10.0.0.1", nginx_path="/owned/nginx.conf")
    off, on = contract().primary_model(model), contract("candidate").primary_model(model)
    assert off != on
    assert off["services"]["consumer"]["environment"]["ORDER_STATUS_EVENT_REFRESH"] == "1"
    assert on["services"]["consumer"]["environment"]["ORDER_STATUS_EVENT_REFRESH"] == "1"
    on["services"]["consumer"]["environment"]["ORDER_STATUS_EVENT_REFRESH_DEDUP"] = "0"
    assert off == on


@pytest.mark.parametrize("arm", ["control", "candidate"])
def test_fixed_inventory_and_observer_qualify_explicit_factor(arm):
    c = contract(arm)
    data = inventory(c)
    view = validate_inventory(data, image_id=c.images["api"], contract=c)
    assert view["api_counts"] == {"primary": 2, "secondary": 2}
    assert view["api_connections_total"] == 16 and view["payment_connections_included"] == 8
    frozen = observer.load_frozen(policy.ROOT / "scripts/observe_paid_pipeline.py")
    before = copy.deepcopy(data)
    assert len(observer.install_adapter(frozen, data, image_id=c.images["api"],
                                       approved_inventory_sha256=original.digest(data))) == 4
    assert data == before


@pytest.mark.parametrize("drift", ["marker", "marker_extra", "consumer_off", "dedup", "missing_worker_flag", "api_on", "api_missing", "cache", "worker", "missing_worker", "digest"])
def test_factor_drift_rejects_inventory_and_observer(drift):
    c = contract("candidate")
    data = inventory(c)
    approved = original.digest(data)
    consumer = next(w for w in data["worker_sources"] if w["role"] == "consumer")
    if drift == "marker":
        data["status_refresh_contract"]["decision"] = "ADR0151"
    elif drift == "marker_extra":
        data["status_refresh_contract"]["extra"] = True
    elif drift == "consumer_off":
        consumer["settings"]["ORDER_STATUS_EVENT_REFRESH"] = "0"
    elif drift == "dedup":
        consumer["settings"]["ORDER_STATUS_EVENT_REFRESH_DEDUP"] = "0"
    elif drift == "missing_worker_flag":
        consumer["settings"].pop("ORDER_STATUS_EVENT_REFRESH_DEDUP")
    elif drift == "api_on":
        data["apis"][0]["ORDER_STATUS_EVENT_REFRESH_DEDUP"] = "1"
    elif drift == "api_missing":
        data["apis"][0].pop("ORDER_STATUS_EVENT_REFRESH_DEDUP")
    elif drift == "cache":
        data["apis"][0]["settings"]["ORDER_STATUS_CACHE_MS"] = "3000"
    elif drift == "worker":
        next(w for w in data["worker_sources"] if w["role"] == "simulator")["settings"]["ORDER_STATUS_EVENT_REFRESH_DEDUP"] = "1"
    elif drift == "missing_worker":
        data["worker_sources"].pop()
    else:
        data["apis"][0]["image_id"] = IMAGE
    with pytest.raises(ValueError):
        validate_inventory(data, image_id=c.images["api"], contract=c)
    frozen = observer.load_frozen(policy.ROOT / "scripts/observe_paid_pipeline.py")
    with pytest.raises(ValueError):
        observer.install_adapter(frozen, data, image_id=c.images["api"], approved_inventory_sha256=approved)
    if drift not in {"marker", "digest"}:
        with pytest.raises(ValueError):
            observer.install_adapter(frozen, data, image_id=c.images["api"], approved_inventory_sha256=original.digest(data))


def test_old_ledger_and_old_qualification_cannot_release_new_profile():
    engine, binding = profile.create_runner(), {"test": "binding"}
    with pytest.raises(ValueError):
        engine.validate_release(state(binding), binding, execute=False)
    current = new_state(binding)
    engine.validate_release(current, binding, execute=False)
    current[profile.LEDGER].update(qualification_protocols_started=1, safety_protocols_started=2)
    old = qualification(binding)
    with pytest.raises(ValueError):
        engine.validate_release(current, binding, execute=True, qualification=old)
    new = {**old, "experiment_decision": "ADR0157", "factor": policy.FACTOR}
    engine.validate_release(current, binding, execute=True, qualification=new)
    new["factor"] = "different"
    with pytest.raises(ValueError):
        engine.validate_release(current, binding, execute=True, qualification=new)


def test_all32_gates_preserved_including_financial_and_full_queues():
    engine, c = profile.create_runner(), contract()
    record, _, restore = gate_evidence()
    data = inventory(c)
    captured = datetime.now(UTC) - timedelta(seconds=600)
    data["captured_at"] = captured.isoformat()
    record.update(arm="control", pre_dispatch_qualified=True, customers_dispatched=True,
                  inventory_qualification=c.qualify_inventory(data, now=captured+timedelta(seconds=1)),
                  pre_dispatch_qualified_at_utc=(captured+timedelta(seconds=5)).isoformat(),
                  dispatch_requested_at_utc=(captured+timedelta(seconds=10)).isoformat(),
                  scheduled_offered_start_utc=(captured+timedelta(seconds=40)).isoformat(),
                  offered_start_utc=(captured+timedelta(seconds=40)).isoformat(),
                  offered_end_utc=(captured+timedelta(seconds=340)).isoformat())
    result = engine.stage_gates(record, data, restore, c)
    assert len(result["paid"]) == 22 and set(result["additional"]) == set(EXTRA_GATES)
    assert result["all_required_gates_pass"]
    for section, key, value in [("financial", "pass", False), ("financial", "duplicate_booked_seats", 1),
                                ("global_queues", "pass", False)]:
        bad = copy.deepcopy(record)
        bad[section][key] = value
        assert not engine.stage_gates(bad, data, restore, c)["all_required_gates_pass"]


@pytest.mark.parametrize("failed,ambiguous", [(None, False), ("control", False), ("candidate", False), ("control", True)])
def test_scoped_protocol_orders_stops_and_preserves_ambiguous_consumption(tmp_path, monkeypatch, failed, ambiguous):
    engine, binding = profile.create_runner(), {"test": "binding"}
    engine.ROOT, engine.STATE = tmp_path, tmp_path / "state.json"
    (tmp_path / "tmp").mkdir()
    engine.LOCK = tmp_path / "tmp/run.lock"
    engine.STATE.write_text(json.dumps(new_state(binding)))
    monkeypatch.setattr(engine, "binding_for", lambda *_args: binding)
    calls = []

    def arm(_config, _artifact, _sources, _bundle, name, run_id, _output, *, execute):
        engine.reserve_arm(run_id, name, execute=execute)
        calls.append(name)
        if ambiguous:
            raise RuntimeError("ambiguous owned synthetic dispatch")
        return {"pass": name != failed, "restoration_complete": True,
                "pre_safety_source_pass": True, "capacity_stages_started": 0}

    monkeypatch.setattr(engine, "run_arm", arm)
    report = engine.protocol({}, {}, {}, {}, binding, execute=False)
    assert report["experiment_decision"] == "ADR0157" and report["factor"] == policy.FACTOR
    assert calls == (["control"] if failed == "control" else ["control", "candidate"])
    persisted = json.loads(engine.STATE.read_text())
    assert persisted[profile.LEDGER]["safety_protocols_started"] == len(calls)
    assert engine.LOCK.exists() == ambiguous
    assert bool(persisted["current_run"]) == ambiguous
    assert report["pass"] == (failed is None)


def test_default_preparation_verifies_source_without_state_or_cloud_changes(tmp_path, monkeypatch):
    engine = profile.create_runner()
    before = engine.STATE.read_bytes()
    monkeypatch.setattr(engine, "run", lambda *_a, **_k: pytest.fail("No cloud calls allowed"))
    output = policy.ROOT / "tmp" / ("adr0157-prep-"+uuid4().hex+".json")
    assert not output.exists()
    result = engine.prepare(output)
    assert result["cloud_calls"] == result["paid_launches"] == 0
    assert result["ledger"] == profile.LEDGER and result["experiment_decision"] == "ADR0157"
    assert len(result["runtime_source_sha256"]) == 19
    assert engine.STATE.read_bytes() == before


def test_remote_observer_needs_only_the_existing_two_uploaded_scripts(tmp_path):
    for name in ("observe_two_host_pipeline.py", "prepare_two_host_scaling.py"):
        (tmp_path / name).write_bytes((policy.ROOT / "scripts" / name).read_bytes())
    data = inventory(contract("candidate"))
    program = "import json,sys;sys.path.insert(0,sys.argv[1]);import observe_two_host_pipeline as o;o.verify_dedup_factor_evidence(json.loads(sys.argv[2]));print('factor_verified')"
    result = subprocess.run([sys.executable, "-I", "-c", program, str(tmp_path), json.dumps(data)],
                            capture_output=True, text=True, timeout=15, check=False)
    assert result.returncode == 0, result.stderr
    assert result.stdout.strip() == "factor_verified"


def test_image_staging_uses_new_contract_and_fails_before_cloud_without_allowance(tmp_path, monkeypatch):
    import stage_status_refresh_images as old_stager

    before = (old_stager.source_contract, old_stager.StatusRefreshContract, old_stager.validate_release)
    stage = profile.create_stager()
    assert stage.StatusRefreshContract is policy.StatusRefreshDedupContract
    assert before == (old_stager.source_contract, old_stager.StatusRefreshContract, old_stager.validate_release)
    monkeypatch.setattr(stage, "validate_config", lambda _c: None)
    monkeypatch.setattr(stage, "command", lambda *_a, **_k: pytest.fail("No image work or cloud allowed"))
    with pytest.raises(ValueError):
        stage.stage({}, policy.plan()["artifact_receipt"], tmp_path / "unused", None)
    assert not (tmp_path / "unused").exists()


@pytest.mark.parametrize("drift", ["artifact", "sources", "plan"])
def test_prepared_identity_or_factor_drift_is_rejected(tmp_path, monkeypatch, drift):
    data = policy.plan()
    if drift == "artifact":
        data["artifact_receipt"]["images"]["api"] = "sha256:"+"0"*64
    elif drift == "sources":
        data["expected_runtime_source_sha256"]["src/ticketing/api.py"] = "0"*64
    else:
        data["common"]["consumer_event_refresh"] = "0"
        path = tmp_path / "plan.json"
        path.write_text(json.dumps(data))
        monkeypatch.setattr(policy, "PLAN", path)
    with pytest.raises(ValueError):
        policy.StatusRefreshDedupContract(data["artifact_receipt"], "control", data["expected_runtime_source_sha256"])


@pytest.mark.parametrize("role", ["consumer", "simulator"])
def test_live_worker_configuration_cannot_infer_missing_disabled_flag(role):
    c = contract("control")
    settings = c.settings(role)
    c.verify_worker_settings(role, settings)
    settings.pop("ORDER_STATUS_EVENT_REFRESH_DEDUP")
    with pytest.raises(ValueError):
        c.verify_worker_settings(role, settings)


@pytest.mark.parametrize("outcome", ["success", "ambiguous", "unproven"])
def test_staging_lock_releases_only_after_runtime_preserving_success(tmp_path, monkeypatch, outcome):
    stager = profile.create_stager()
    locks = []

    class Lock:
        def __init__(self, identifier):
            assert identifier.startswith("adr0151-")
            self.released = False
            locks.append(self)

        def release(self):
            self.released = True

    monkeypatch.setattr(stager, "run_lock", Lock)
    monkeypatch.setattr(stager, "validate_config", lambda _c: None)
    monkeypatch.setattr(stager, "validate_release", lambda *_a, **_k: None)
    monkeypatch.setattr(stager, "binding_for", lambda *_a: {})
    monkeypatch.setattr(stager, "STATE", tmp_path / "state.json")
    stager.STATE.write_text("{}")

    def owned_stage(*_args):
        if outcome == "ambiguous":
            raise RuntimeError("Synthetic uncertain image staging")
        return {"pass": True, "runtime_identities_unchanged": outcome != "unproven"}

    monkeypatch.setattr(stager, "owned_stage", owned_stage)
    if outcome == "success":
        result = stager.stage({}, policy.plan()["artifact_receipt"], tmp_path, None)
        assert result["experiment_decision"] == "ADR0157" and locks[0].released
    else:
        with pytest.raises((ValueError, RuntimeError)):
            stager.stage({}, policy.plan()["artifact_receipt"], tmp_path, None)
        assert len(locks) == 1 and not locks[0].released

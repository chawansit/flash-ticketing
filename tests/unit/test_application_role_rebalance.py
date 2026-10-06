"""ADR0177 complete local contract, observer and failure-boundary regressions."""
import json
import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[2] / "scripts"))
import application_database_wait_evidence as scoped
import application_role_rebalance_contract as policy
import application_role_runner_diagnostics as diagnostics
import observe_two_host_pipeline as pipeline
import run_application_role_rebalance_comparison as runner
import run_two_host_paid_comparison as paid
import work_envelope as envelope
from prepare_two_host_scaling import validate_inventory
from status_refresh_contract import digest
from test_application_database_wait_evidence import binding, rows, sample, trace
from test_async_confirmation_comparison import inventory, queues
from test_work_envelope import area  # noqa: F401 - isolated reservation state


def observed(arm="control"):
    p = policy.plan()
    c = policy.ApplicationRoleRebalanceContract(p["artifact_receipt"], arm, p["expected_runtime_source_sha256"])
    data = inventory(c)
    if arm == "candidate":
        a = data["apis"][1]
        a.update(host_role="secondary", private_ipv4=data["hosts"]["secondary"]["private_ipv4"],
                 database_url_host=data["hosts"]["primary"]["private_ipv4"])
    data["application_database_role_binding"] = c.bind_database_roles(rows(), "postgresql://application@pool/ticketing")
    return c, data


@pytest.mark.parametrize("arm", ["control", "candidate"])
def test_actual_contract_and_observer_preserve_budgets_and_scope(arm):
    c, data = observed(arm)
    view = validate_inventory(data, image_id=c.images["api"], contract=c)
    assert view["api_connections_total"] == 16 and view["payment_connections_included"] == 8
    assert view["pgbouncer_server_connections"] == 24 and view["api_counts"] == c.measured_api_counts
    frozen = pipeline.load_frozen(Path("scripts/observe_paid_pipeline.py"))
    endpoints = pipeline.install_adapter(frozen, data, image_id=c.images["api"], approved_inventory_sha256=digest(data))
    assert len(endpoints) == 4 and "confirmation" in frozen.METRICS
    stage = runner.create_runner().RefreshStages(False, {}, c, "adr0151-" + "a" * 12)
    assert stage.expected_tickets == 18000


@pytest.mark.parametrize("field", ["diagnostic_scope", "diagnostic_decision", "placements", "common", "allowance"])
def test_plan_drift_is_rejected_before_reservation(monkeypatch, tmp_path, field):
    value = policy.plan()
    value[field] = "drift"
    path = tmp_path / "plan.json"; path.write_text(json.dumps(value))
    monkeypatch.setattr(policy, "PLAN", path)
    with pytest.raises(ValueError): policy.plan()


@pytest.mark.parametrize("drift", ["binding", "marker", "source", "worker", "pool", "placement"])
def test_inventory_cannot_hide_role_source_or_budget_drift(drift):
    c, data = observed("candidate")
    if drift == "binding": data["application_database_role_binding"]["bound_replicas"].pop("confirmation")
    elif drift == "marker": data["status_refresh_contract"]["diagnostic_scope"] = "full_database"
    elif drift == "source": data["worker_sources"][-1]["source_identity"]["source_hashes_match"] = False
    elif drift == "worker": data["worker_sources"].pop()
    elif drift == "pool": data["apis"][0]["settings"]["DB_POOL_MAX"] = "8"
    else: data["apis"][1]["host_role"] = "primary"
    with pytest.raises(ValueError): validate_inventory(data, image_id=c.images["api"], contract=c)
    if drift in {"binding", "marker", "pool", "placement"}:
        with pytest.raises(ValueError): pipeline.verify_admission_factor_evidence(data)


def test_exact_fresh_profile_pair_and_scope(area):  # noqa: F811
    value, _, old = area
    wrong = policy.plan(); wrong["diagnostic_scope"] = "full_database"
    with pytest.raises(ValueError): envelope.reserve(value, wrong, profile="application_role_rebalance")
    entry = envelope.reserve(value, policy.plan(), profile="application_role_rebalance")
    state = envelope.read(envelope.STATE)
    assert state[envelope.BASE_LEDGER] == old[envelope.BASE_LEDGER]
    assert state[entry["ledger"]]["paid_runs_authorized"] == 2
    assert state[entry["ledger"]]["safety_tickets_authorized"] == 4
    assert envelope.base_ledger(entry["ledger"]) == runner.LEDGER


def test_identity_pins_new_collector_parent_and_transfer(monkeypatch):
    engine = runner.create_runner()
    original = engine.identity()
    for name in ("application_database_wait_evidence.py", "application_role_runner_diagnostics.py",
                 "application_role_rebalance_contract.py", "api_placement_contract.py"):
        assert "scripts/" + name in original
    original_hash = engine.comparison.source_sha256
    monkeypatch.setattr(engine.comparison, "source_sha256", lambda data: "changed" if b"ADR0177 explicit scoped" in data else original_hash(data))
    assert original != engine.identity()


@pytest.mark.parametrize("mode", ["missing", "full", "both", "wrong_inventory"])
def test_cli_rejects_missing_or_downgraded_scope_before_loading_observer(tmp_path, monkeypatch, mode):
    _, data = observed()
    if mode == "wrong_inventory": data["status_refresh_contract"]["decision"] = "ADR0174"
    path = tmp_path / "inventory.json"; path.write_text(json.dumps(data))
    args = ["--inventory", str(path), "--frozen-observer", "unused", "--approved-inventory-sha256", digest(data)]
    if mode in {"full", "both"}: args += ["--database-wait-diagnostics"]
    if mode in {"both", "wrong_inventory"}: args += ["--application-database-wait-diagnostics"]
    monkeypatch.setattr(pipeline, "load_frozen", lambda *a: pytest.fail("Observer loaded before mode rejection"))
    with pytest.raises((ValueError, SystemExit)): pipeline.main(args)


def test_cli_installs_only_scoped_sampler_with_existing_connection(tmp_path, monkeypatch):
    _, data = observed()
    path = tmp_path / "inventory.json"; path.write_text(json.dumps(data))
    frozen = pipeline.load_frozen(Path("scripts/observe_paid_pipeline.py"))
    used, seen = [], []
    connection = object()
    frozen.sample = lambda conn, ids: {"original": conn is connection}
    monkeypatch.setattr(scoped.Collector, "collect", lambda self, conn: used.append(conn) or sample())
    monkeypatch.setattr(pipeline, "load_frozen", lambda *a: frozen)
    monkeypatch.setattr(frozen, "main", lambda: seen.append(frozen.sample(connection, ["fixture"])))
    monkeypatch.setattr(sys, "argv", ["local-observer-test"])
    pipeline.main(["--inventory", str(path), "--frozen-observer", "unused",
                   "--approved-inventory-sha256", digest(data), "--application-database-wait-diagnostics"])
    assert used == [connection] and seen[0]["original"]
    assert "database_wait_diagnostics" not in seen[0]
    assert seen[0]["application_database_wait_diagnostics"]["full_database_visibility_complete"] is False


@pytest.mark.parametrize("drift", [None, "transferred_bytes", "preflight_role"])
def test_generated_transfer_readback_and_effective_preflight_are_ordered(tmp_path, drift):
    _, data = observed()
    seen = []
    def upload(session, cid, owner, directory, name, content):
        (Path(directory) / name).write_text(content)
        seen.append("upload:" + name)
    class Session:
        def api(self, cid, program, timeout):
            compile(program, "generated", "exec")
            if "expected=" in program:
                seen.append("readback")
                if drift == "transferred_bytes": (tmp_path / "application_database_wait_evidence.py").write_text("drift")
                exec(program, {})  # noqa: S102 - locally generated bounded hash check, no network.
                return {"transferred_scoped_observer_identity": True}
            seen.append("preflight")
            return {"pass": True, "diagnostic_scope": scoped.SCOPE, "role_identity_sha256":
                    "f" * 64 if drift == "preflight_role" else binding().identity_sha256,
                    "full_database_visibility_complete": False}
    if drift:
        with pytest.raises((AssertionError, ValueError)):
            diagnostics.prepare(Session(), "cid", "owner", str(tmp_path), data, upload)
        if drift == "transferred_bytes": assert "preflight" not in seen
    else:
        result = diagnostics.prepare(Session(), "cid", "owner", str(tmp_path), data, upload)
        assert result["application_database_wait_preflight"]["pass"]
        assert seen[-2:] == ["readback", "preflight"]


def valid_record(tmp_path, data):
    return {"confirmation_receipts": {"pass": True}, "global_queues": queues(),
            "admission_failure_capture": {"complete": True}, "slow_database_capture": {"complete": True},
            "application_database_wait_startup": diagnostics.startup({"application_database_wait_diagnostics": sample()}, data),
            "application_database_wait_capture": diagnostics.summarize(trace(tmp_path, [sample(0), sample(1)]), data),
            "application_database_wait_preflight": {"pass": True, "diagnostic_scope": scoped.SCOPE,
                "role_identity_sha256": binding().identity_sha256, "full_database_visibility_complete": False},
            "transferred_scoped_observer_identity": True}


@pytest.mark.parametrize("failure", [None, "identity", "scope", "preflight", "transfer", "visibility", "startup"])
def test_scoped_gate_requires_full_bound_chain_and_has_no_global_alias(monkeypatch, tmp_path, failure):
    c, data = observed()
    engine = runner.create_runner()
    monkeypatch.setattr(engine.comparison, "gates_for_stage", lambda *a: {"paid": {"cache_disabled": True}, "additional": {}})
    monkeypatch.setattr(engine, "evaluate_gates", lambda *a: {"all_required_gates_pass": True, "failed_gates": []})
    monkeypatch.setattr(c, "validate_inventory_receipt", lambda *a, **k: True)
    record = valid_record(tmp_path, data)
    if failure == "identity": record["application_database_wait_capture"]["role_identity_sha256"] = "e" * 64
    elif failure == "scope": record["application_database_wait_capture"]["diagnostic_scope"] = "full_database"
    elif failure == "preflight": record["application_database_wait_preflight"]["pass"] = False
    elif failure == "transfer": record["transferred_scoped_observer_identity"] = False
    elif failure == "visibility": record["application_database_wait_capture"]["application_database_wait_evidence_complete"] = False
    elif failure == "startup": record["application_database_wait_startup"] = {}
    result = engine.stage_gates(record, data, {}, c)
    assert result["all_required_gates_pass"] is (failure is None)
    assert "database_wait_evidence_complete" not in result["confirmation"]
    assert record["application_database_wait_capture"]["full_database_visibility_complete"] is False


def test_scoped_preflight_failure_does_not_create_fixture_and_cleanup_runs(monkeypatch, tmp_path):
    c, data = observed()
    stage = paid.Stages(False, {}, ledger_key=runner.LEDGER, stage_limit=1, contract=c)
    stage.bundle = {"scripts/" + name: b"test" for name in ("observe_paid_pipeline.py", "kafka_lag_observe.py",
                   "prepare_capacity_fixture.py", "audit_checkout_smoke.py", "capacity_queue_state.py")}
    monkeypatch.setattr(paid, "observe", lambda *a, **k: (data, {"inventory_contract_pass": True}, [], []))
    monkeypatch.setattr(diagnostics, "prepare", lambda *a, **k: (_ for _ in ()).throw(ValueError("Synthetic preflight failure")))
    monkeypatch.setattr(paid, "collect_profile_failure_evidence", lambda *a: {"admission_failure_capture": {"complete": True}})
    programs = []
    class Session:
        def __init__(self):
            self.state = {}
            self.config = {"generator": {"repo": "/synthetic"}}
        def call(self, role, program, *a): programs.append(program); return {}
        def api(self, cid, program, *a):
            assert "prepare_capacity_fixture.py','--output'" not in program
            programs.append(program)
            return {"frozen_helpers_verified": True}
        def checkpoint(self): pass
    with pytest.raises(ValueError, match="Synthetic preflight failure"):
        stage(Session(), "control", [{"host_role": "primary", "container_id": "cid"}],
              {"model": {"services": {"api": {"environment": {}}, **{r: {"image": c.images[r]} for r in c.background}}}},
              "/synthetic-owner", tmp_path)
    result = stage.results["control"]
    assert result["failure_type"] == "ValueError"
    assert "Synthetic preflight failure" in (tmp_path / "control/failure.private.log").read_text()
    assert not result["customers_dispatched"] and result["private_cleanup_pass"]
    assert not result["application_database_wait_capture"]["application_database_wait_evidence_complete"]
    assert sum("private_manifests_removed" in x for x in programs) == 2


def test_registry_dispatch_qualifies_real_profile_before_reservation(area, monkeypatch, tmp_path):  # noqa: F811
    import run_status_refresh_comparison as shared
    import run_work_envelope as entrypoint

    value, _, _ = area
    engine = runner.create_runner()
    monkeypatch.setattr(runner, "create_runner", lambda: engine)
    monkeypatch.setattr(engine.comparison, "validate_config", lambda *a: None)
    monkeypatch.setattr(engine, "binding_for", lambda *a: value)
    monkeypatch.setattr(shared, "LOCK", envelope.LOCK)
    envelope.LOCK.unlink()
    config = tmp_path / "config.json"; config.write_text("{}")
    calls = []
    def reject(binding_value, plan, *, profile):
        assert profile == "application_role_rebalance" and binding_value == value
        assert plan == policy.plan()
        calls.append(profile)
        raise ValueError("Synthetic reservation stop before cloud")
    monkeypatch.setattr(envelope, "reserve", reject)
    with pytest.raises(ValueError, match="Synthetic reservation stop"):
        entrypoint.execute(config, None, tmp_path, profile_name="application_role_rebalance")
    assert calls == ["application_role_rebalance"] and not envelope.LOCK.exists()
    assert envelope.read(envelope.JOURNAL)["experiments"] == []

import copy
import json
import subprocess
import sys
from datetime import UTC, datetime, timedelta
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[2] / "scripts"))
import run_status_refresh_comparison as runner
import status_refresh_contract as policy
from prepare_two_host_scaling import API_SETTINGS, BACKGROUND, EXTRA_GATES, validate_inventory
from test_two_host_paid_runner import gate_evidence
from test_two_host_scaling_preparation import inventory

SOURCES = {"src/ticketing/api.py": "a" * 64}


def artifact():
    return {
        "images": {r: "sha256:" + f"{i + 1:064x}" for i, r in enumerate(policy.ROLES)},
        "parent_images": {r: policy.IMAGE for r in policy.ROLES},
        "source_manifest_sha256": policy.digest(
            {"base_revision": policy.REVISION, "runtime_source_sha256": SOURCES}
        ),
    }


def contract(arm="control"):
    return policy.StatusRefreshContract(artifact(), arm, SOURCES)


def observed(c):
    data = inventory("candidate")
    data["arm"] = c.arm
    data["captured_at"] = datetime.now(UTC).isoformat()
    for api in data["apis"]:
        api["image_id"] = c.images["api"]
        api["settings"] = {**API_SETTINGS, "ORDER_STATUS_CACHE_MS": "1000"}
        api["ORDER_STATUS_EVENT_REFRESH"] = "0"
    data["worker_sources"] = []
    for role, config in BACKGROUND.items():
        for _i in range(config["replicas"]):
            data["worker_sources"].append(
                {
                    "container_id": f"{len(data['worker_sources']) + 100:064x}",
                    "role": role,
                    "image_id": c.images[role],
                    "source_identity": {"source_hashes_match": True},
                    "settings": c.settings(role),
                }
            )
    return data


def test_same_models_images_and_budgets_change_only_one_consumer_flag():
    from test_two_host_deployment import fixture
    from two_host_topology import deployment_model, snapshot

    cfg, rows = fixture()
    saved = snapshot(cfg, rows, image_id=policy.IMAGE)
    model = deployment_model(saved, primary_ip="10.0.0.1", nginx_path="/owned/nginx.conf")
    off = contract("control").primary_model(model)
    on = contract("candidate").primary_model(model)
    assert off != on
    assert off["services"]["api"]["environment"]["ORDER_STATUS_CACHE_MS"] == "1000"
    assert on["services"]["consumer"]["environment"]["ORDER_STATUS_EVENT_REFRESH"] == "1"
    on["services"]["consumer"]["environment"]["ORDER_STATUS_EVENT_REFRESH"] = "0"
    assert off == on and model["services"]["api"]["environment"]["ORDER_STATUS_CACHE_MS"] == "0"


@pytest.mark.parametrize("arm", ["control", "candidate"])
def test_fixed_two_plus_two_inventory_and_cache_contract(arm):
    c = contract(arm)
    data = observed(c)
    view = validate_inventory(data, image_id=c.images["api"], contract=c)
    assert view["api_counts"] == {"primary": 2, "secondary": 2}
    assert view["api_connections_total"] == 16 and view["payment_connections_included"] == 8
    with pytest.raises(ValueError):
        validate_inventory(data, image_id=c.images["api"])


@pytest.mark.parametrize(
    "drift",
    [
        "cache0",
        "cache3000",
        "pool",
        "callback",
        "workerflag",
        "consumerflag",
        "workercache",
        "backgroundpool",
        "source",
        "placement",
        "worker_missing",
        "worker_duplicate",
        "workerimage",
        "apiimage",
        "api_refresh",
        "authorities",
    ],
)
def test_candidate_drift_prevents_qualification(drift):
    c = contract("candidate")
    data = observed(c)
    if drift.startswith("cache"):
        data["apis"][0]["settings"]["ORDER_STATUS_CACHE_MS"] = drift.removeprefix("cache")
    elif drift == "pool":
        data["apis"][0]["settings"]["DB_POOL_MAX"] = "5"
    elif drift == "callback":
        data["apis"][0]["settings"]["API_CALLBACK_ACQUISITION_RESERVE"] = "1"
    elif drift == "workerflag":
        next(w for w in data["worker_sources"] if w["role"] == "simulator")["settings"][
            "ORDER_STATUS_EVENT_REFRESH"
        ] = "1"
    elif drift == "consumerflag":
        data["worker_sources"][0]["settings"]["ORDER_STATUS_EVENT_REFRESH"] = "0"
    elif drift == "workercache":
        data["worker_sources"][0]["settings"]["ORDER_STATUS_CACHE_MS"] = "3000"
    elif drift == "backgroundpool":
        data["background"]["consumer"]["pool_per_replica"] = 9
    elif drift == "source":
        data["worker_sources"][0]["source_identity"]["source_hashes_match"] = False
    elif drift == "placement":
        data["apis"][2]["host_role"] = "primary"
    elif drift == "worker_missing":
        data["worker_sources"].pop()
    elif drift == "worker_duplicate":
        data["worker_sources"][1]["container_id"] = data["worker_sources"][0]["container_id"]
    elif drift == "workerimage":
        data["worker_sources"][0]["image_id"] = policy.IMAGE
    elif drift == "apiimage":
        data["apis"][0]["image_id"] = policy.IMAGE
    elif drift == "api_refresh":
        data["apis"][0]["ORDER_STATUS_EVENT_REFRESH"] = "1"
    elif drift == "authorities":
        data["apis"][0]["redis_authority_sha256"] = "b" * 64
    with pytest.raises(ValueError):
        validate_inventory(data, image_id=c.images["api"], contract=c)


@pytest.mark.parametrize("bad", ["tag", "missingrole", "manifest", "parent"])
def test_artifact_receipt_rejects_unpinned_images_or_wrong_source(bad):
    item = artifact()
    if bad == "tag":
        item["images"]["api"] = "candidate:latest"
    if bad == "missingrole":
        item["images"].pop("consumer")
    if bad == "manifest":
        item["source_manifest_sha256"] = "b" * 64
    if bad == "parent":
        item["parent_images"]["api"] = "sha256:" + "b" * 64
    with pytest.raises(ValueError):
        policy.StatusRefreshContract(item, "control", SOURCES)


@pytest.mark.parametrize("bad", [None, "layers", "label", "config", "id"])
def test_embedded_artifact_parent_and_config_verification(monkeypatch, capsys, bad):
    c = contract()
    parent = {
        "Id": c.parents["api"],
        "RootFS": {"Layers": ["base"]},
        "Config": {"Env": ["same=1"], "User": "10001"},
    }
    candidate = copy.deepcopy(parent)
    candidate["Id"] = c.images["api"]
    candidate["RootFS"]["Layers"].append("candidate")
    candidate["Config"]["Labels"] = {policy.LABEL: c.source_manifest}
    if bad == "layers":
        candidate["RootFS"]["Layers"] = ["different"]
    if bad == "label":
        candidate["Config"]["Labels"][policy.LABEL] = "other"
    if bad == "config":
        candidate["Config"]["User"] = "root"
    if bad == "id":
        candidate["Id"] = policy.IMAGE
    monkeypatch.setattr(
        subprocess,
        "check_output",
        lambda args, **_kwargs: json.dumps([candidate if args[-1] == c.images["api"] else parent]),
    )
    code = c.image_program(("api",))
    if bad is None:
        exec(compile(code, "artifact-proof", "exec"), {})  # noqa: S102
        assert json.loads(capsys.readouterr().out) == {"artifact_roles_verified": ["api"]}
    else:
        with pytest.raises(ValueError):
            exec(compile(code, "artifact-proof", "exec"), {})  # noqa: S102


def qualified_gate_evidence():
    record, _old, restore = gate_evidence()
    c = contract()
    data = observed(c)
    captured = datetime.now(UTC) - timedelta(seconds=600)
    data["captured_at"] = captured.isoformat()
    record.update(
        arm="control",
        pre_dispatch_qualified=True,
        customers_dispatched=True,
        inventory_qualification=c.qualify_inventory(data, now=captured + timedelta(seconds=1)),
        pre_dispatch_qualified_at_utc=(captured + timedelta(seconds=5)).isoformat(),
        dispatch_requested_at_utc=(captured + timedelta(seconds=10)).isoformat(),
        scheduled_offered_start_utc=(captured + timedelta(seconds=40)).isoformat(),
        offered_start_utc=(captured + timedelta(seconds=40)).isoformat(),
        offered_end_utc=(captured + timedelta(seconds=340)).isoformat(),
    )
    return record, data, restore, c


def test_all32_gates_retained_with_only_cache_policy_renamed():
    record, data, restore, c = qualified_gate_evidence()
    result = runner.stage_gates(record, data, restore, c)
    original = runner.comparison.gates_for_stage(record, inventory(), restore)
    assert len(result["paid"]) == 22 and set(result["additional"]) == set(EXTRA_GATES)
    assert set(result["paid"]) == (set(original["paid"]) - {"cache_disabled"}) | {"bounded_equal_cache_age"}
    assert result["all_required_gates_pass"]
    for section, key in [
        ("financial", "pass"),
        ("financial", "duplicate_booked_seats"),
        ("global_queues", "pass"),
    ]:
        broken = copy.deepcopy(record)
        broken[section][key] = 1 if key == "duplicate_booked_seats" else False
        assert not runner.stage_gates(broken, data, restore, c)["all_required_gates_pass"]
    bad = copy.deepcopy(data)
    bad["apis"][0]["settings"]["ORDER_STATUS_CACHE_MS"] = "0"
    assert "bounded_equal_cache_age" in runner.stage_gates(record, bad, restore, c)["failed_gates"]


def state(binding, *, dry_only=False):
    return {
        "current_run": None,
        "cloud_load_requires_resume": False,
        runner.LEDGER: {
            "authorization_id": runner.AUTHORIZATION,
            "binding": binding,
            "qualification_runs_authorized": 1,
            "paid_runs_authorized": 0 if dry_only else 2,
            "safety_tickets_authorized": 2 if dry_only else 4,
            "qualification_protocols_started": 0,
            "safety_protocols_started": 0,
            "paid_protocols_started": 0,
            "paid_runs_started": 0,
        },
    }


def qualification(binding):
    return {
        "kind": "status_refresh_dry_pair",
        "pass": True,
        "binding": binding,
        "finished_at_utc": datetime.now(UTC).isoformat(),
        "capacity_stages_started": 0,
        "safety_protocols_started": 2,
        "arms": {
            arm: {"pass": True, "restoration_complete": True, "pre_safety_source_pass": True}
            for arm in ("control", "candidate")
        },
    }


@pytest.mark.parametrize(
    "drift", ["pause", "active", "oldledger", "binding", "paid", "safety", "bool", "missing"]
)
def test_release_rejects_old_or_consumed_or_changed_approval(drift):
    binding = {"exact": "binding"}
    item = state(binding)
    if drift == "pause":
        item["cloud_load_requires_resume"] = True
    if drift == "active":
        item["current_run"] = "other"
    if drift == "oldledger":
        item[runner.LEDGER]["authorization_id"] = "adr0148"
    if drift == "binding":
        item[runner.LEDGER]["binding"] = {}
    if drift == "paid":
        item[runner.LEDGER]["paid_protocols_started"] = 1
    if drift == "safety":
        item[runner.LEDGER]["safety_protocols_started"] = 1
    if drift == "bool":
        item[runner.LEDGER]["paid_runs_started"] = False
    if drift == "missing":
        item[runner.LEDGER].pop("qualification_protocols_started")
    with pytest.raises(ValueError):
        runner.validate_release(item, binding, execute=False)


def test_dry_only_scope_cannot_start_paid_or_autoqualify():
    binding = {"exact": "binding"}
    item = state(binding, dry_only=True)
    runner.validate_release(item, binding, execute=False)
    with pytest.raises(ValueError):
        runner.validate_release(item, binding, execute=True, qualification=qualification(binding))
    item = state(binding)
    with pytest.raises(ValueError):
        runner.validate_release(item, binding, execute=True, qualification=qualification(binding))
    item[runner.LEDGER].update(qualification_protocols_started=1, safety_protocols_started=2)
    runner.validate_release(item, binding, execute=True, qualification=qualification(binding))


@pytest.mark.parametrize(
    "drift", ["age", "future", "binding", "restore", "source", "bool", "missingarm", "paid", "failed"]
)
def test_dry_qualification_must_match_fresh_complete_restored_evidence(drift):
    binding = {"exact": "binding"}
    report = qualification(binding)
    if drift == "age":
        report["finished_at_utc"] = (datetime.now(UTC) - timedelta(hours=2)).isoformat()
    if drift == "future":
        report["finished_at_utc"] = (datetime.now(UTC) + timedelta(minutes=1)).isoformat()
    if drift == "binding":
        report["binding"] = {}
    if drift == "restore":
        report["arms"]["control"]["restoration_complete"] = False
    if drift == "source":
        report["arms"]["candidate"]["pre_safety_source_pass"] = False
    if drift == "bool":
        report["capacity_stages_started"] = False
    if drift == "missingarm":
        report["arms"].pop("control")
    if drift == "paid":
        report["capacity_stages_started"] = 1
    if drift == "failed":
        report["pass"] = False
    assert not runner.qualification_matches(report, binding)


@pytest.fixture
def journal(monkeypatch, tmp_path):
    binding = {"exact": "binding"}
    monkeypatch.setattr(runner, "binding_for", lambda *_args: binding)
    monkeypatch.setattr(runner, "ROOT", tmp_path)
    monkeypatch.setattr(runner, "STATE", tmp_path / "state.json")
    (tmp_path / "tmp").mkdir()
    monkeypatch.setattr(runner, "LOCK", tmp_path / "tmp/run.lock")
    runner.STATE.write_text(json.dumps(state(binding)))
    return binding


@pytest.mark.parametrize(
    "failed,restore", [(None, True), ("control", True), ("candidate", True), ("control", False)]
)
def test_dry_pair_order_stop_and_restore_before_candidate(monkeypatch, journal, failed, restore):
    calls = []

    def arm(_config, _artifact, _sources, _bundle, name, run_id, output, *, execute):
        runner.reserve_arm(run_id, name, execute=execute)
        calls.append(name)
        return {
            "pass": name != failed,
            "restoration_complete": restore,
            "pre_safety_source_pass": True,
            "capacity_stages_started": 0,
        }

    monkeypatch.setattr(runner, "run_arm", arm)
    result = runner.protocol({}, {}, {}, {}, journal, execute=False)
    assert calls == (["control"] if failed == "control" else ["control", "candidate"])
    assert result["pass"] is (failed is None and restore)
    saved = json.loads(runner.STATE.read_text())
    assert saved[runner.LEDGER]["safety_protocols_started"] == len(calls)
    assert saved[runner.LEDGER]["qualification_protocols_started"] == 1
    assert runner.LOCK.exists() is (not restore)
    assert bool(saved["current_run"]) is (not restore)
    with pytest.raises(ValueError):
        runner.validate_release(saved, journal, execute=False)


def test_ambiguous_protocol_retains_lock_consumption_and_active_marker(monkeypatch, journal):
    def arm(_c, _a, _s, _b, name, run_id, _o, *, execute):
        runner.reserve_arm(run_id, name, execute=execute)
        raise ConnectionResetError("ambiguous")

    monkeypatch.setattr(runner, "run_arm", arm)
    result = runner.protocol({}, {}, {}, {}, journal, execute=False)
    assert result["pass"] is False and result["failure_type"] == "ConnectionResetError"
    assert result["safety_protocols_started"] == 1
    assert runner.LOCK.exists() and json.loads(runner.STATE.read_text())["current_run"]


def test_exclusive_lock_blocks_second_protocol_and_wrong_receipt(journal):
    lock = runner.RunLock("adr0151-" + "a" * 12)
    with pytest.raises(FileExistsError):
        runner.RunLock("adr0151-" + "b" * 12)
    runner.LOCK.write_text(json.dumps({"run": "other", "pid": 0}))
    with pytest.raises(ValueError):
        lock.release()
    assert runner.LOCK.exists()


def test_paid_launch_claim_is_durable_once_and_keeps_global_count_across_arms(journal):
    from types import SimpleNamespace

    run_id = "adr0151-" + "a" * 12
    item = json.loads(runner.STATE.read_text())
    item["current_run"] = run_id
    item[runner.LEDGER].update(
        active_run=run_id, qualification_protocols_started=1, safety_protocols_started=2
    )
    runner.STATE.write_text(json.dumps(item))
    for arm in ("control", "candidate"):
        runner.reserve_arm(run_id, arm, execute=True)
        stage = runner.RefreshStages(True, {}, contract(arm), run_id)
        session = SimpleNamespace(state={"capacity_stages_started": 0}, checkpoint=lambda: None)
        stage.claim_paid_stage(session, arm)
        with pytest.raises(ValueError):
            stage.claim_paid_stage(session, arm)
    saved = json.loads(runner.STATE.read_text())[runner.LEDGER]
    assert saved["paid_runs_started"] == 2 and saved["paid_protocols_started"] == 2
    assert saved["attempted_paid_arms"] == ["control", "candidate"] and saved["safety_protocols_started"] == 4
    with pytest.raises(ValueError):
        runner.reserve_arm(run_id, "candidate", execute=True)


def test_physical_four_primary_stage_never_dispatches(monkeypatch):
    stage = runner.RefreshStages(False, {}, contract(), "adr0151-" + "a" * 12)
    called = []
    monkeypatch.setattr(
        runner.comparison.Stages, "__call__", lambda self, session, arm, *args: called.append(arm)
    )
    stage(None, "control", None, None, None, None)
    assert called == []
    monkeypatch.setattr(stage.contract, "stage_snapshot", lambda saved: saved)
    stage(None, "candidate", None, {}, None, None)
    assert called == ["control"]


def test_default_preparation_verifies_full_isolated_tree_without_cloud_or_state_change(tmp_path, monkeypatch):
    before = runner.STATE.read_bytes()
    monkeypatch.setattr(runner, "run", lambda *_a, **_k: pytest.fail("local preparation cannot connect"))
    # Preparation itself requires a fresh file inside owned workspace tmp.
    target = policy.ROOT / "tmp" / ("adr0151-test-" + __import__("uuid").uuid4().hex + ".json")
    try:
        result = runner.prepare(target)
        assert len(result["runtime_source_sha256"]) == 19
        assert result["cloud_calls"] == 0 and result["paid_launches"] == 0
        assert runner.STATE.read_bytes() == before
        with pytest.raises(ValueError):
            runner.prepare(target)
    finally:
        target.unlink(missing_ok=True)


@pytest.mark.parametrize("failure_phase", ["pre-mutation", "pre-safety", "paid-hook"])
def test_driver_policy_failure_restores_and_never_reports_false_success(monkeypatch, tmp_path, failure_phase):
    import qualify_two_host_deployment as driver
    from test_two_host_deployment import fixture

    cfg, rows = fixture()
    actions = []

    class Session:
        def __init__(self, _config, _output, _password):
            self.state = {
                "phases": [],
                "restore_pass": False,
                "capacity_stages_started": 0,
                "safety_probe_attempted": False,
            }
            self.restoring = False

        def phase(self, name):
            self.state["phases"].append(name)

        def checkpoint(self):
            pass

        def close(self):
            actions.append("close")

        def call(self, role, code, *_args):
            if code == driver.INSPECT:
                return copy.deepcopy(rows)
            if "generator_idle" in code:
                return {"generator_idle": True}
            if ".env.rds" in code:
                return cfg
            if "docker','ps','-aq'" in code:
                return []
            if "Path(" in code and ").read_text()" in code:
                return "events {}"
            if "returncode" in code and "subprocess" in code:
                return {"returncode": 0, "safety": {"pass": True}}
            if "removed_files" in code:
                actions.append("credentials-cleaned")
            return {"ok": True}

        def api(self, _cid, code, *_args):
            if code == driver.GLOBAL_AUDIT:
                return {
                    "pass": True,
                    "kafka_members": 1
                    if self.restoring or not self.state.get("four_primary_control_ready")
                    else 6,
                }
            if "'--shows','1'" in code:
                return {"show_ids": ["00000000-0000-0000-0000-000000000001"]}
            if "result=audit" in code:
                return {"pass": True, "hold_deadlines_elapsed": True}
            return {"uid": 10001, "mode": 384, "readable_by_api_user": True, "api_uid": 10001}

        def put(self, *_args):
            pass

        def up(self, role, path, _counts, _services):
            actions.append("restore" if path.endswith("restore.compose.json") else "apply")
            if path.endswith("restore.compose.json"):
                self.restoring = True

        def wait(self, role, count):
            apis = [
                copy.deepcopy(r) for r in rows if r["Config"]["Labels"]["com.docker.compose.service"] == "api"
            ][:count]
            for i, row in enumerate(apis):
                row["Id"] = f"{i + 100 if role == 'secondary' else i + 1:064x}"
                row["Config"]["Labels"]["com.docker.compose.project"] = (
                    "flash-ticketing-api-secondary" if role == "secondary" else "flash-ticketing"
                )
                row["Config"]["Env"] = [k + "=" + v for k, v in API_SETTINGS.items()]
                row["State"]["Health"] = {"Status": "healthy"}
                row["HostConfig"]["PortBindings"] = {
                    "8000/tcp": [
                        {
                            "HostIp": "10.0.0.2" if role == "secondary" else "10.0.0.1",
                            "HostPort": str(8101 + i),
                        }
                    ]
                }
                row["NetworkSettings"] = {"Ports": row["HostConfig"]["PortBindings"]}
            return apis

    class Policy:
        def __init__(self):
            self.images = {"api": driver.IMAGE}
            self.api_settings = API_SETTINGS

        def pre_mutation(self, *_args):
            if failure_phase == "pre-mutation":
                raise ValueError("image receipt failed")

        def primary_model(self, model):
            return model

        def secondary_model(self, model):
            return model

        def pre_safety(self, *_args):
            if failure_phase == "pre-safety":
                raise ValueError("source proof failed")
            return {"verified": True}

    def hook(_session, arm, *_args):
        if arm == "candidate":
            raise RuntimeError("paid control failed after safety")

    monkeypatch.setattr(driver, "Session", Session)
    monkeypatch.setattr(driver.sys.stdin, "isatty", lambda: True)
    monkeypatch.setattr(driver.getpass, "getpass", lambda _p: "synthetic")
    config = {
        "primary": {"repo": "/owned", "private_ipv4": "10.0.0.1"},
        "secondary": {"prepared_directory": "/owned-secondary", "private_ipv4": "10.0.0.2"},
    }
    result = driver.run(config, tmp_path, stage_hook=hook, runtime_policy=Policy())
    assert result["pass"] is False
    assert result.get("failure_type") in {"RuntimeError", "ValueError"}
    assert actions[-1] == "close" and "credentials-cleaned" in actions
    if failure_phase == "pre-mutation":
        assert "apply" not in actions and not result["safety_probe_attempted"]
    elif failure_phase == "pre-safety":
        assert "restore" in actions and not result["safety_probe_attempted"]
    else:
        assert result.get("qualification_checks_pass") is True
        assert result.get("restore_pass") is True and "restore" in actions


@pytest.mark.parametrize("bad", [None, "digest", "cache", "placement", "budget", "marker"])
def test_observer_installs_only_exact_host_qualified_inventory(monkeypatch, bad):
    import observe_two_host_pipeline as observer

    c = contract()
    data = observed(c)
    data["status_refresh_contract"] = {
        "decision": "ADR0151",
        "cache_age_ms": 1000,
        "api_image_id": c.images["api"],
        "arm": "control",
    }
    if bad == "cache":
        data["apis"][0]["settings"]["ORDER_STATUS_CACHE_MS"] = "3000"
    if bad == "placement":
        data["apis"][0]["host_role"] = "secondary"
    if bad == "budget":
        data["apis"][0]["settings"]["DB_POOL_MAX"] = "8"
    if bad == "marker":
        data["status_refresh_contract"].pop("decision")
    approved = policy.digest(data)
    if bad == "digest":
        data["apis"][0]["image_id"] = policy.IMAGE
    frozen = observer.load_frozen(policy.ROOT / "scripts/observe_paid_pipeline.py")
    if bad is None:
        assert (
            len(
                observer.install_adapter(
                    frozen, data, image_id=policy.IMAGE, approved_inventory_sha256=approved
                )
            )
            == 4
        )
        assert data["apis"][0]["settings"]["ORDER_STATUS_CACHE_MS"] == "1000"
        assert data["arm"] == "control"
    else:
        with pytest.raises(ValueError):
            observer.install_adapter(frozen, data, image_id=policy.IMAGE, approved_inventory_sha256=approved)


def test_bounded_cache_metrics_are_parsed_and_duplicate_labels_rejected():
    import observe_two_host_pipeline as observer

    raw = 'process_start_time_seconds 1000\nticketing_order_status_cache_total{outcome="hit"} 12\n'
    assert observer.extra_api_metrics(raw)["status_cache:hit"] == 12
    with pytest.raises(ValueError):
        observer.extra_api_metrics(raw + raw.splitlines()[1] + "\n")
    with pytest.raises(ValueError):
        observer.extra_api_metrics(raw.replace('"hit"', '"user-id-123"'))


@pytest.mark.parametrize("bad", [None, "reset", "gap", "missing"])
def test_cache_measurements_never_infer_hit_share_from_partial_or_reset_data(tmp_path, bad):
    c = contract()
    data = observed(c)
    labels = [f"{a['host_role']}:{a['container_id']}" for a in data["apis"]]
    rows = []
    for i in range(2):
        rows.append(
            {
                "utc": f"2026-10-05T00:00:0{i}+00:00",
                "api_replicas": {
                    label: {
                        "process_start_time_seconds": 1000,
                        "status_cache:hit": i * 3,
                        "status_cache:miss": i,
                    }
                    for label in labels
                },
            }
        )
    if bad == "reset":
        rows[0]["api_replicas"][labels[0]]["status_cache:hit"] = 20
    if bad == "gap":
        rows[1]["utc"] = "2026-10-05T00:00:05+00:00"
    if bad == "missing":
        rows[1]["api_replicas"].pop(labels[0])
    path = tmp_path / "pipeline.jsonl"
    path.write_text("".join(json.dumps(row) + "\n" for row in rows))
    result = runner.measurements(
        {"customer": {"dispatched": 10, "physical_http_attempts": {"orders": 25}}}, path, data
    )
    assert result["status_checks_per_dispatched_journey"] == 2.5
    if bad is None:
        assert result["cache_observed"] and result["cache_hit_share"] == 0.75
        assert result["cache_outcome_deltas"] == {"hit": 12, "miss": 4}
    else:
        assert result["cache_observed"] is False and result["cache_hit_share"] is None


def test_protocol_rechecks_actual_binding_before_lock_or_cloud(monkeypatch, journal):
    monkeypatch.setattr(runner, "binding_for", lambda *_a: {"actual": "changed"})
    monkeypatch.setattr(runner, "run_arm", lambda *_a, **_k: pytest.fail("must reject before cloud access"))
    before = runner.STATE.read_bytes()
    with pytest.raises(ValueError, match="binding differs"):
        runner.protocol({}, {}, {}, {}, journal, execute=False)
    assert not runner.LOCK.exists() and runner.STATE.read_bytes() == before


@pytest.mark.parametrize("cache_measurement", [{"cache_observed": True, "cache_series_present": True}, None])
def test_paid_pair_uses_separate_restored_protocols_and_claims_exactly_two_launches(
    monkeypatch, journal, cache_measurement
):
    saved = json.loads(runner.STATE.read_text())
    saved[runner.LEDGER].update(qualification_protocols_started=1, safety_protocols_started=2)
    runner.STATE.write_text(json.dumps(saved))
    order = []

    def arm(_c, _a, _s, _b, name, run_id, _o, *, execute):
        runner.reserve_arm(run_id, name, execute=execute)
        stage = runner.RefreshStages(True, {}, contract(name), run_id)
        from types import SimpleNamespace

        stage.claim_paid_stage(
            SimpleNamespace(state={"capacity_stages_started": 0}, checkpoint=lambda: None), name
        )
        order.extend([name, "restored"])
        return {
            "pass": True,
            "restoration_complete": True,
            "pre_safety_source_pass": True,
            "capacity_stages_started": 1,
            "measurements": cache_measurement,
        }

    monkeypatch.setattr(runner, "run_arm", arm)
    result = runner.protocol({}, {}, {}, {}, journal, execute=True, qualification=qualification(journal))
    assert (
        result["pass"] and result["capacity_stages_started"] == 2 and result["safety_protocols_started"] == 2
    )
    assert result["performance_measurement_complete"] is (cache_measurement is not None)
    assert order == ["control", "restored", "candidate", "restored"] and not runner.LOCK.exists()


@pytest.mark.parametrize("report", [None, [], {"arms": None}, {"arms": {"control": None}}])
def test_malformed_qualification_never_releases_paid_load(report):
    assert runner.qualification_matches(report, {}) is False


@pytest.mark.parametrize(
    "receipt", [None, [], {"images": None, "parent_images": {}, "source_manifest_sha256": "wrong"}]
)
def test_malformed_artifact_receipt_fails_before_cloud(receipt):
    with pytest.raises(ValueError):
        policy.StatusRefreshContract(receipt, "control", SOURCES)


@pytest.mark.parametrize("context", ["admission", "late-analysis"])
def test_fresh_dispatch_receipt_survives_long_test_and_final_audit(context):
    record, data, restore, c = qualified_gate_evidence()
    captured = datetime.fromisoformat(data["captured_at"])
    if context == "admission":
        assert c.validate_inventory_receipt(record, data, final=False, now=captured + timedelta(seconds=10))
    else:
        # Old wall-clock final revalidation reproduces the exact live failure.
        with pytest.raises(ValueError, match="stale"):
            validate_inventory(data, image_id=c.images["api"], contract=c)
        assert runner.stage_gates(record, data, restore, c)["all_required_gates_pass"]


@pytest.mark.parametrize(
    "defect",
    [
        "missing",
        "inventory-digest",
        "source-digest",
        "image",
        "arm",
        "ready-missing",
        "ready-naive",
        "future-validation",
        "validation-before-capture",
        "launch-before-ready",
        "delayed-launch",
        "offered-before-launch",
        "offered-start-gap",
        "offered-end-future",
        "preflight-failed",
        "not-dispatched",
        "schema-bool",
        "unexpected-field",
        "tampered-settings",
    ],
)
def test_missing_mutated_or_stale_dispatch_evidence_fails_closed(defect):
    record, data, restore, c = qualified_gate_evidence()
    receipt = record["inventory_qualification"]
    captured = datetime.fromisoformat(data["captured_at"])
    if defect == "missing":
        record.pop("inventory_qualification")
    elif defect == "inventory-digest":
        receipt["inventory_sha256"] = "f" * 64
    elif defect == "source-digest":
        receipt["source_manifest_sha256"] = "f" * 64
    elif defect == "image":
        receipt["api_image_id"] = policy.IMAGE
    elif defect == "arm":
        receipt["arm"] = "candidate"
    elif defect == "ready-missing":
        record.pop("pre_dispatch_qualified_at_utc")
    elif defect == "ready-naive":
        record["pre_dispatch_qualified_at_utc"] = captured.replace(tzinfo=None).isoformat()
    elif defect == "future-validation":
        receipt["validated_at_utc"] = (datetime.now(UTC) + timedelta(seconds=1)).isoformat()
    elif defect == "validation-before-capture":
        receipt["validated_at_utc"] = (captured - timedelta(seconds=1)).isoformat()
    elif defect == "launch-before-ready":
        record["dispatch_requested_at_utc"] = captured.isoformat()
    elif defect == "delayed-launch":
        record["dispatch_requested_at_utc"] = (captured + timedelta(seconds=301)).isoformat()
        record["scheduled_offered_start_utc"] = (captured + timedelta(seconds=302)).isoformat()
    elif defect == "offered-before-launch":
        record["offered_start_utc"] = captured.isoformat()
    elif defect == "offered-start-gap":
        record["offered_start_utc"] = (captured + timedelta(seconds=42)).isoformat()
    elif defect == "offered-end-future":
        record["offered_end_utc"] = (datetime.now(UTC) + timedelta(seconds=1)).isoformat()
    elif defect == "preflight-failed":
        record["pre_dispatch_qualified"] = False
    elif defect == "not-dispatched":
        record["customers_dispatched"] = False
    elif defect == "schema-bool":
        receipt["schema"] = True
    elif defect == "unexpected-field":
        receipt["renewed"] = True
    elif defect == "tampered-settings":
        data["apis"][0]["settings"]["ORDER_STATUS_CACHE_MS"] = "3000"
    result = runner.stage_gates(record, data, restore, c)
    assert not result["all_required_gates_pass"] and "bounded_equal_cache_age" in result["failed_gates"]


def test_old_inventory_cannot_mint_a_fresh_receipt_or_launch_late():
    record, data, _restore, c = qualified_gate_evidence()
    with pytest.raises(ValueError, match="stale"):
        c.qualify_inventory(data)
    with pytest.raises(ValueError, match="stale at launch"):
        c.validate_inventory_receipt(record, data, final=False)


def test_stale_scheduled_start_is_rejected_before_launch_even_when_request_is_fresh():
    record, data, _restore, c = qualified_gate_evidence()
    captured = datetime.fromisoformat(data["captured_at"])
    record["scheduled_offered_start_utc"] = (captured + timedelta(seconds=301)).isoformat()
    with pytest.raises(ValueError, match="expired before scheduled dispatch"):
        c.validate_inventory_receipt(record, data, final=False, now=captured + timedelta(seconds=10))

import copy
import json
import sys
from pathlib import Path
from typing import ClassVar

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[2] / "scripts"))
import async_confirmation_contract as policy
import observe_two_host_pipeline as observer
import run_async_confirmation_comparison as profile
import run_status_refresh_comparison as original
from prepare_two_host_scaling import validate_inventory
from test_status_refresh_comparison import observed, state
from test_two_host_deployment import fixture
from two_host_topology import deployment_model, snapshot


def contract(arm="control"):
    plan = json.loads(policy.PLAN.read_text())
    return policy.AsyncConfirmationContract(plan["artifact_receipt"], arm, plan["expected_runtime_source_sha256"])


def inventory(c):
    data = observed(c)
    data["background"] = copy.deepcopy(c.background)
    for api in data["apis"]:
        api["settings"] = c.api_settings.copy()
    data["worker_sources"].append({"container_id": "f" * 64, "role": "confirmation",
                                  "image_id": c.images["confirmation"],
                                  "source_identity": {"source_hashes_match": True},
                                  "settings": c.settings("confirmation")})
    data["status_refresh_contract"] = c.inventory_marker()
    return data


def queues():
    return {"pass": True, "pending_confirmation_receipts": 0, "review_confirmation_receipts": 0,
            "confirmation_capacity_outstanding": 0, "confirmation_capacity_mismatches": 0}


def test_private_profile_preserves_old_engine_and_shared_lock():
    before = (original.PLAN, original.LEDGER, original.AUTHORIZATION, original.StatusRefreshContract)
    engine = profile.create_runner()
    assert engine.LOCK == original.LOCK
    assert engine.LEDGER != original.LEDGER
    assert before == (original.PLAN, original.LEDGER, original.AUTHORIZATION, original.StatusRefreshContract)
    assert engine is not profile.create_runner()


def test_only_intake_factor_changes_with_identical_images_total_budget_and_polling():
    config, rows = fixture()
    saved = snapshot(config, rows, image_id=policy.export.base.IMAGE)
    model = deployment_model(saved, primary_ip="10.0.0.1", nginx_path="/owned/nginx.conf")
    off, on = [contract(arm).primary_model(model) for arm in ("control", "candidate")]
    assert off["services"]["simulator"]["environment"]["DB_POOL_MAX"] == "10"
    conf = off["services"]["confirmation"]
    assert conf["environment"]["DB_POOL_MAX"] == "2"
    assert conf["command"] == ["python", "-m", "ticketing.workers", "confirmation"]
    assert "ports" not in conf
    for result in (off, on):
        assert result["services"]["api"]["environment"]["ORDER_STATUS_POLL_MS"] == "500"
        assert result["services"]["consumer"]["environment"]["ORDER_STATUS_EVENT_REFRESH"] == "1"
        assert result["services"]["consumer"]["environment"]["ORDER_STATUS_EVENT_REFRESH_DEDUP"] == "0"
    off["services"]["api"]["environment"]["PAYMENT_CONFIRMATION_ASYNC"] = "1"
    assert off == on


@pytest.mark.parametrize("arm", ["control", "candidate"])
def test_complete_inventory_includes_confirmation_role_and_exact_common_budget(arm):
    c = contract(arm)
    assert validate_inventory(inventory(c), image_id=c.images["api"], contract=c)["inventory_contract_pass"]


@pytest.mark.parametrize("drift", ["missing_worker", "wrong_pool", "missing_flag", "wrong_poll", "image", "source", "simulator_budget", "dedup", "duplicate_worker"])
def test_inventory_blocks_drift_before_dispatch(drift):
    c = contract()
    data = inventory(c)
    if drift == "missing_worker":
        data["worker_sources"].pop()
    if drift == "wrong_pool":
        data["worker_sources"][-1]["settings"]["DB_POOL_MAX"] = "3"
    if drift == "missing_flag":
        data["worker_sources"][0]["settings"].pop("PAYMENT_CONFIRMATION_ASYNC")
    if drift == "wrong_poll":
        data["apis"][0]["settings"]["ORDER_STATUS_POLL_MS"] = "100"
    if drift == "image":
        data["worker_sources"][-1]["image_id"] = "sha256:" + "a" * 64
    if drift == "source":
        data["worker_sources"][-1]["source_identity"]["source_hashes_match"] = False
    if drift == "simulator_budget":
        data["background"]["simulator"]["pool_per_replica"] = 12
    if drift == "dedup":
        data["worker_sources"][0]["settings"]["ORDER_STATUS_EVENT_REFRESH_DEDUP"] = "1"
    if drift == "duplicate_worker":
        data["worker_sources"][-1]["container_id"] = data["worker_sources"][0]["container_id"]
    with pytest.raises(ValueError):
        validate_inventory(data, image_id=c.images["api"], contract=c)


@pytest.mark.parametrize("field", ["pending_confirmation_receipts", "review_confirmation_receipts", "confirmation_capacity_outstanding", "confirmation_capacity_mismatches"])
def test_no_missing_or_pending_receipts_can_claim_global_drain(field):
    data = queues()
    assert policy.receipt_drained(data)
    data[field] = 1
    assert not policy.receipt_drained(data)
    data.pop(field)
    assert not policy.receipt_drained(data)
    data[field] = False
    assert not policy.receipt_drained(data)


def test_real_constructor_checks_run_before_allowance_reservation(monkeypatch):
    engine = profile.create_runner()
    monkeypatch.setattr(engine, "validate_release", lambda *a, **k: pytest.fail("Must fail before release or SSH"))
    artifact = json.loads(policy.PLAN.read_text())["artifact_receipt"]
    artifact["images"].pop("confirmation")
    with pytest.raises(ValueError):
        engine.protocol({}, artifact, policy.source_contract(), {}, {}, execute=False)


def test_old_consumed_ledger_cannot_authorize_new_factor():
    engine = profile.create_runner()
    with pytest.raises(ValueError):
        engine.validate_release(state({}), {}, execute=False)


def test_blocked_receipt_drain_preserves_worker_and_ownership(monkeypatch):
    c = contract()
    class Session:
        state: ClassVar[dict] = {}
        def api(self, *args):
            data = queues()
            data["review_confirmation_receipts"] = 1
            return data
        def call(self, *args):
            pytest.fail("Must retain worker")
        def checkpoint(self):
            pass
    clock = iter([0, 61])
    monkeypatch.setattr(policy.time, "monotonic", lambda: next(clock))
    with pytest.raises(ValueError, match="Keep confirmation worker"):
        c.before_restore(Session(), "a" * 64, "/owned/live.json")


def test_drain_before_restore_never_removes_live_intake_worker():
    c = contract()
    class Session:
        state: ClassVar[dict] = {}
        def api(self, *args):
            return queues()
        def call(self, *args):
            pytest.fail("Must restore intake before removing worker")
        def checkpoint(self):
            pass
    c.before_restore(Session(), "a" * 64, "/owned/live.json")


def test_startup_requires_queue_and_worker_cpu_metrics():
    metric = {"process_cpu_seconds_total": .2,
              "confirmation_metric:ticketing_payment_confirmation_pending": 0,
              "confirmation_metric:ticketing_payment_confirmation_review": 0,
              "confirmation_metric:ticketing_payment_confirmation_oldest_seconds": 0}
    row = {"confirmation_replicas": 1, "confirmation_db_replicas": {"worker": metric}}
    observer.confirmation_startup(row)
    metric.pop("confirmation_metric:ticketing_payment_confirmation_review")
    with pytest.raises(ValueError):
        observer.confirmation_startup(row)


def test_offline_source_matches_native_tested_complete_file_set():
    data = json.loads(policy.PLAN.read_text())
    fresh = policy.ROOT / data["isolated_source_directory"]
    prior = policy.ROOT / "tmp/adr0160-checks/source"
    maps = [{p.relative_to(path).as_posix(): p.read_bytes().replace(b"\r\n", b"\n")
             for p in path.rglob("*") if p.is_file()} for path in (prior, fresh)]
    assert maps[0] == maps[1]
    assert len(policy.source_contract()) == 21


def test_global_receipt_audit_and_remote_migration_program_are_valid_python():
    c = contract()
    compile(c.global_audit, "receipt-audit", "exec")
    _config, rows = fixture()
    for row in rows:
        role = row["Config"]["Labels"]["com.docker.compose.service"]
        if role in c.original_parents:
            row["Image"] = c.original_parents[role]
    saved = {"model": {"services": {r: {"image": v} for r, v in c.original_parents.items()}}}
    class Session:
        state: ClassVar[dict] = {}
        programs: ClassVar[list] = []
        def call(self, role, program, timeout=60):
            compile(program, "remote", "exec")
            return rows if program == policy.INSPECT else {}
        def api(self, cid, program, timeout):
            compile(program, "remote-api", "exec")
            self.programs.append(program)
            return {**queues(), "kafka_members": 1} if program == c.global_audit else {"receipt_schema_installed": True}
        def checkpoint(self):
            pass
    session = Session()
    c.pre_mutation(session, saved)
    assert session.state["receipt_schema_installed"] is True
    assert any("schema_migrations" in p and "009_payment_confirmation_receipts.sql" in p for p in session.programs)


def test_new_profile_cannot_use_old_contract_at_real_stage_boundary():
    old = original.StatusRefreshContract
    from test_status_refresh_comparison import SOURCES, artifact
    c = old(artifact(), "control", SOURCES)
    with pytest.raises(ValueError, match="Exact durable confirmation"):
        original.comparison.Stages(False, {}, ledger_key=profile.LEDGER, stage_limit=1, contract=c)


def test_remote_receipt_audit_expects_one_stable_simulator_receipt_per_payment():
    c = contract("candidate")
    class Session:
        def api(self, cid, program, timeout):
            compile(program, "scoped-receipt-audit", "exec")
            assert "p.id=r.callback_id" in program
            assert "o.event_id=ANY" in program
            assert "EXPECTED" not in program
            return {"pass": False}
    with pytest.raises(ValueError, match="Scoped durable"):
        c.stage_receipt_audit(Session(), "a" * 64, [], 18000)


def test_confirmation_observation_rejects_missing_series_and_restart(tmp_path):
    engine = profile.create_runner()
    c = contract()
    data = inventory(c)
    trace = tmp_path / "pipeline.jsonl"
    rows = []
    for i in range(3):
        metric = {"process_start_time_seconds": 100,
                  "confirmation_metric:ticketing_payment_confirmation_pending": i,
                  "confirmation_metric:ticketing_payment_confirmation_review": 0,
                  "confirmation_metric:ticketing_payment_confirmation_oldest_seconds": i}
        rows.append({"utc": "2026-10-05T12:00:0" + str(i) + "+00:00",
                     "confirmation_replicas": 1, "confirmation_db_replicas": {"worker": metric}})
    trace.write_text("\n".join(json.dumps(row) for row in rows))
    result = engine.measurements({}, trace, data)
    assert result["confirmation_observed"] and result["confirmation_backlog_peak"] == 2
    rows[1]["confirmation_db_replicas"]["worker"]["process_start_time_seconds"] = 101
    trace.write_text("\n".join(json.dumps(row) for row in rows))
    assert engine.measurements({}, trace, data)["confirmation_observed"] is False
    rows[1].pop("confirmation_db_replicas")
    trace.write_text("\n".join(json.dumps(row) for row in rows))
    assert engine.measurements({}, trace, data)["confirmation_observed"] is False

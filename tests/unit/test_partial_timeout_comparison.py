"""Exact bounded runner and failure diagnostic integration; no cloud calls."""
import copy
import json
import sys
from pathlib import Path
from typing import ClassVar

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[2] / "scripts"))
import async_confirmation_contract as receipt_policy
import observe_two_host_pipeline as observer
import partial_timeout_contract as policy
import run_async_confirmation_comparison as historical
import run_partial_timeout_comparison as runner
import run_status_refresh_comparison as original
from test_async_confirmation_comparison import inventory, queues
from test_status_refresh_comparison import state
from test_two_host_deployment import fixture


def contract(arm="control"):
    plan = json.loads(policy.PLAN.read_text())
    return policy.AdmissionReclaimContract(plan["artifact_receipt"], arm, plan["expected_runtime_source_sha256"])


def test_private_engine_and_lock_preserve_historical_defaults_and_real_stage_constructor():
    before = (original.PLAN, original.LEDGER, original.AUTHORIZATION, original.StatusRefreshContract)
    engine = runner.create_runner()
    assert engine.LOCK == original.LOCK
    assert before == (original.PLAN, original.LEDGER, original.AUTHORIZATION, original.StatusRefreshContract)
    assert historical.create_runner().LEDGER == historical.LEDGER
    assert engine.LEDGER == runner.LEDGER
    for arm in ("control", "candidate"):
        c = contract(arm)
        stage = engine.RefreshStages(False, {}, c, "adr0151-" + "a" * 12)
        assert stage.rate == 60 and stage.expected_tickets == 18000
    with pytest.raises(ValueError):
        original.comparison.Stages(False, {}, ledger_key=runner.LEDGER, stage_limit=1, contract=historical.policy if hasattr(historical, "policy") else None)


def test_old_consumed_scope_cannot_authorize_new_profile():
    with pytest.raises(ValueError):
        runner.create_runner().validate_release(state({}), {}, execute=False)


def test_constructor_validation_precedes_state_reservation_and_cloud(monkeypatch):
    engine = runner.create_runner()
    monkeypatch.setattr(engine, "validate_release", lambda *a, **k: pytest.fail("must not reach release"))
    artifact = json.loads(policy.PLAN.read_text())["artifact_receipt"]
    artifact["images"].pop("confirmation")
    with pytest.raises(ValueError):
        engine.protocol({}, artifact, policy.source_contract(), {}, {}, execute=False)


@pytest.mark.parametrize("arm", ["control", "candidate"])
def test_observer_accepts_exact_admission_profile_and_retains_evidence(arm):
    data = inventory(contract(arm)); before = copy.deepcopy(data)
    observer.verify_admission_factor_evidence(data)
    assert data == before


@pytest.mark.parametrize("drift", ["flag", "async", "reserve", "pool", "waiters", "worker", "marker"])
def test_observer_rejects_factor_or_budget_drift(drift):
    data = inventory(contract("candidate"))
    changes = {"flag": ("API_PARTIAL_TIMEOUT_RECLAIM", "0"), "async": ("PAYMENT_CONFIRMATION_ASYNC", "1"),
               "reserve": ("API_CALLBACK_ACQUISITION_RESERVE", "1"), "pool": ("DB_POOL_MAX", "5"),
               "waiters": ("DB_POOL_MAX_WAITING", "13")}
    if drift in changes:
        key, value = changes[drift];data["apis"][0]["settings"][key] = value
    elif drift == "worker":
        data["worker_sources"][0]["settings"]["API_PARTIAL_TIMEOUT_RECLAIM"] = "1"
    else:
        data["status_refresh_contract"]["async_intake"] = "1"
    with pytest.raises(ValueError):
        observer.verify_admission_factor_evidence(data)


def test_admission_counter_zero_family_and_bounded_reason_series():
    prefix = "# TYPE ticketing_db_acquisition_failures_total counter"
    zero = observer.admission_failure_metrics(prefix)
    assert len(zero) == 8 and sum(zero.values()) == 0
    data = observer.admission_failure_metrics(prefix + '\nticketing_db_acquisition_failures_total{reason="native_timeout",role="payment"} 3')
    assert data["acquisition_failure:payment:native_timeout"] == 3


@pytest.mark.parametrize("line", ["", 'ticketing_db_acquisition_failures_total{role="payment",reason="secret"} 1',
                                  'ticketing_db_acquisition_failures_total{role="payment",reason="native_timeout"} NaN',
                                  'ticketing_db_acquisition_failures_total{role="payment",reason="native_timeout"} -1'])
def test_missing_or_invalid_admission_metric_rejected(line):
    payload = line if not line else "# TYPE ticketing_db_acquisition_failures_total counter\n" + line
    with pytest.raises(ValueError):
        observer.admission_failure_metrics(payload)


@pytest.mark.parametrize("arm", ["control", "candidate"])
def test_synchronous_scoped_receipt_audit_requires_zero_for_both_arms(arm):
    c = contract(arm)
    class Session:
        def api(self, cid, program, timeout):
            compile(program, "receipt", "exec")
            assert "'expected':0" in program
            return {"pass": True}
    assert c.stage_receipt_audit(Session(), "a" * 64, [], 18000)["pass"]


def test_preflight_receipt_schema_is_read_only_and_global_drain_is_mandatory():
    c = contract();_config, rows = fixture()
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
            compile(program, "remote-api", "exec");self.programs.append(program)
            return {**queues(), "kafka_members": 1} if program == c.global_audit else {"receipt_schema_unchanged": True}
        def checkpoint(self):
            pass
    session = Session();c.pre_mutation(session, saved)
    schema = next(p for p in session.programs if "schema_migrations" in p)
    assert "SET TRANSACTION READ ONLY" in schema and "INSERT" not in schema and "ALTER" not in schema
    assert session.state["receipt_schema_unchanged"] is True


def test_pending_review_cannot_remove_confirmation_worker(monkeypatch):
    c = contract()
    class Session:
        state: ClassVar[dict] = {}
        def api(self, *args):
            q = queues();q["review_confirmation_receipts"] = 1;return q
        def call(self, *args):
            pytest.fail("must preserve worker")
        def checkpoint(self):
            pass
    stamps = iter([0, 61]);monkeypatch.setattr(receipt_policy.time, "monotonic", lambda: next(stamps))
    with pytest.raises(ValueError):
        c.before_restore(Session(), "a" * 64, "/owned/live")


@pytest.mark.parametrize("drift", [None, "missing", "reset", "wrong_replica"])
def test_admission_measurements_require_complete_nonreset_api_counter_coverage(tmp_path, drift):
    engine = runner.create_runner();keys = observer.admission_failure_metrics("# TYPE ticketing_db_acquisition_failures_total counter")
    metric = {"process_cpu_seconds_total": 1, "process_start_time_seconds": 1,
              "confirmation_metric:ticketing_payment_confirmation_pending": 0,
              "confirmation_metric:ticketing_payment_confirmation_review": 0,
              "confirmation_metric:ticketing_payment_confirmation_oldest_seconds": 0}
    rows = [{"utc": "2026-10-06T00:00:0" + str(i) + "+00:00", "confirmation_replicas": 1,
             "confirmation_db_replicas": {"worker": metric},
             "api_replicas": {str(j): {**keys, "acquisition_failure:payment:native_timeout": i + 1} for j in range(4)}} for i in range(2)]
    if drift == "missing":
        rows[-1]["api_replicas"]["0"].pop("acquisition_failure:payment:native_timeout")
    elif drift == "reset":
        rows[-1]["api_replicas"]["0"]["acquisition_failure:payment:native_timeout"] = 0
    elif drift == "wrong_replica":
        rows[-1]["api_replicas"]["changed"] = rows[-1]["api_replicas"].pop("0")
    path = tmp_path / "trace.jsonl";path.write_text("\n".join(json.dumps(r) for r in rows))
    result = engine.measurements({}, path, {"apis": []})
    assert result["admission_diagnostics_observed"] is (drift is None)
    if drift is None:
        assert result["admission_failure_deltas"]["acquisition_failure:payment:native_timeout"] == 4

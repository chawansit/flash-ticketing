"""Observer identity is independent of application activation; drift fails closed."""
import sys
from pathlib import Path
from types import SimpleNamespace

sys.path.insert(0, str(Path(__file__).resolve().parents[2] / "scripts"))

import cce_committed_status_profile as committed
import cce_runtime_cohort_profile as profile
import paid_cohort_query as query
import pytest
import work_envelope as policy
from observe_cce_paid_pipeline import install_indexed_cohort_query
from observe_paid_pipeline import PAID_COHORT_QUERY
from test_cce_committed_status_comparison import goal as application_goal


def goal(arm="control"):
    value = application_goal("candidate")
    value.update(observer_cost_decision="ADR0283",
                 runtime_cohort_decision="ADR0304", runtime_cohort_arm=arm,
                 runtime_cohort_contract_sha256=policy.digest(query.runtime_contract(arm)))
    if arm == "candidate":
        value.update(runtime_cohort_control_ledger="bounded_cce_paid_comparison__123456abcdef",
                     runtime_cohort_control_report_path="tmp/adr0151-123456abcdef/cce-comparison.private.json")
    return value


@pytest.mark.parametrize("arm", ["control", "candidate"])
def test_source_bound_receipt_and_unchanged_counts(arm):
    counts = {k: i for i, k in enumerate(query.SAMPLE_COLUMNS)}
    module = SimpleNamespace(PAID_COHORT_QUERY=PAID_COHORT_QUERY, sample=lambda *_: dict(counts))
    install_indexed_cohort_query(module)
    contract = query.runtime_contract(arm)
    query.install_runtime_comparison(module, contract)
    value = module.sample(None, ["show"])
    assert value.pop("runtime_cohort_contract") == contract
    assert value == counts
    assert (module.PAID_COHORT_QUERY == query.PAID_COHORT_QUERY) == (arm == "candidate")
    with pytest.raises(ValueError):
        # Candidate SQL or an already wrapped baseline cannot be silently activated twice.
        query.install_runtime_comparison(module, {**contract, "helper_sha256": "0" * 64})


@pytest.mark.parametrize("key,value", [
    ("helper_sha256", "0" * 64), ("active_query_sha256", "0" * 64),
    ("interval_seconds", 2), ("additional_database_connections", 1),
    ("sample_columns", []), ("hourly", True)])
def test_transfer_drift_rejected_before_mutation(key, value):
    sample = lambda *_: {}
    module = SimpleNamespace(PAID_COHORT_QUERY=PAID_COHORT_QUERY, sample=sample)
    install_indexed_cohort_query(module)
    before = module.PAID_COHORT_QUERY
    with pytest.raises(ValueError):
        query.install_runtime_comparison(module, {**query.runtime_contract("candidate"), key: value})
    assert module.PAID_COHORT_QUERY == before and module.sample is sample


@pytest.mark.parametrize("arm", ["control", "candidate"])
def test_exact_short_scope(arm):
    value = goal(arm)
    assert profile.selected(value)
    assert committed.selected(value)


@pytest.mark.parametrize("key,value", [
    ("runtime_cohort_decision", "ADR0303"), ("runtime_cohort_arm", "typo"),
    ("runtime_cohort_contract_sha256", "0" * 64), ("committed_status_arm", "control"),
    ("offered_seconds", 3600), ("acquisition_budget", 21), ("payment_pipeline_arm", "candidate")])
def test_scope_drift_rejected(key, value):
    original = goal()
    with pytest.raises(ValueError):
        profile.selected({**original, key: value})


def test_default_unchanged_and_orphan_rejected():
    assert not profile.selected(None)
    assert not profile.selected({})
    with pytest.raises(ValueError):
        profile.selected({"runtime_cohort_control_ledger": "old"})
    with pytest.raises(ValueError):
        profile.selected({**goal(), "runtime_cohort_control_ledger": "old"})


def test_control_plan_keeps_application_flag_enabled():
    value = goal()
    actual = committed.extend_plan({}, value)
    assert actual["committed_status_snapshot"] == "1"
    plan = profile.extend_plan(actual, value)
    assert plan["single_changed_factor"] == "observer_runtime_cohort_query"
    assert plan["runtime_cohort_contract"] == query.runtime_contract("control")


def test_failed_control_blocks_candidate(monkeypatch):
    value = goal("candidate")
    monkeypatch.setattr(policy, "envelope", dict)
    monkeypatch.setattr(policy, "read", lambda _: {})
    monkeypatch.setattr(policy, "journal", lambda _: {"experiments": []})
    with pytest.raises(ValueError, match="Passing restored"):
        profile.require_control(value)


def test_candidate_rejects_changed_configuration(monkeypatch):
    value = goal("candidate")
    old = {"configuration_sha256": "same", "cce_acquisition_budget": 20,
           "cce_runtime_cohort_contract": query.runtime_contract("control"),
           "cce_manifest_sha256": "old", "baseline_sha256": "old"}
    monkeypatch.setattr(profile, "require_control", lambda _: ("retained", {}))
    monkeypatch.setattr(policy, "read", lambda _: {value["runtime_cohort_control_ledger"]: {"binding": old}})
    candidate = {**old, "cce_manifest_sha256": "new", "baseline_sha256": "new",
                 "cce_runtime_cohort_contract": query.runtime_contract("candidate")}
    assert profile.validate_candidate_binding(value, candidate)
    with pytest.raises(ValueError):
        profile.validate_candidate_binding(value, {**candidate, "cce_acquisition_budget": 21})
    with pytest.raises(ValueError):
        profile.validate_candidate_binding(value, {**candidate, "configuration_sha256": "changed"})

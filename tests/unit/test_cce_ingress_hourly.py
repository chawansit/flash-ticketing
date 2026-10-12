"""ADR0293 duration binding must not loosen routing, accounting or quality."""
import copy

import cce_api_adapter as api
import cce_dependency_probe as dependency
import cce_nginx_ingress as ingress
import cce_status_reuse_hourly as hourly
import pytest
import work_envelope as policy
from cce_paid_profiles import HOURLY
from test_cce_status_reuse_hourly import goal as parent_goal


def goal():
    return {**parent_goal(), "ingress_route_decision": ingress.DECISION,
            "ingress_route_contract_sha256": policy.digest(ingress.contract()),
            "ingress_route_arm": "candidate", "ingress_hourly_decision": "ADR0293",
            "ingress_hourly_prerequisite_sha256": ingress.HOURLY_BASELINE_SHA256,
            "baseline_sha256": ingress.HOURLY_BASELINE_SHA256}


def test_hourly_route_keeps_same_runtime_and_distinct_hour_target(monkeypatch):
    value = copy.deepcopy(policy.envelope())
    value["spending"]["temporary_cce_pilot_exception"]["goal_bounded_authorization"] = goal()
    monkeypatch.setattr(policy, "envelope", lambda: value)
    dependency.authorized_today(value)
    assert hourly.active(goal())
    plan = hourly.plan(goal())
    assert plan["common"] == {"buyer_journeys_per_second": 84, "duration_seconds": 3600}
    assert plan["kind"] == "existing_nginx_ingress_hourly_qualification"
    assert plan["baseline_sha256"] == ingress.HOURLY_BASELINE_SHA256
    assert plan["baseline_is_passing_control"] is True
    assert plan["matched_performance_comparison"] is False
    assert (plan["expected_terminal_tickets"], plan["minimum_issued_inside_hour"]) == (302400, 300000)
    assert (plan["maximum_pods"], plan["pooler_server_connections"], plan["acquisition_budget"]) == (18, 24, 20)
    assert ingress.origin(goal()) == ingress.CANDIDATE_ORIGIN
    import run_cce_paid_comparison as runner
    assert runner.plan(profile=HOURLY) == plan


@pytest.mark.parametrize("key,value", [
    ("ingress_hourly_decision", "unknown"),
    ("ingress_hourly_prerequisite_sha256", "0"*64),
    ("ingress_route_arm", "control"),
    ("ingress_route_contract_sha256", "0"*64),
    ("maximum_paid_stages", 2),
    ("baseline_sha256", hourly.BASELINE_SHA256),
    ("offered_seconds", 300),
    ("maximum_pods", 19),
    ("maximum_experiment_seconds", 5401),
])
def test_wrong_hourly_route_or_budget_rejected(key, value):
    with pytest.raises(ValueError):
        hourly.active({**goal(), key: value})


def test_hour_without_explicit_extension_rejected():
    value = goal()
    value.pop("ingress_hourly_decision")
    value.pop("ingress_hourly_prerequisite_sha256")
    with pytest.raises(ValueError):
        ingress.selected(value)


def test_hourly_binding_cannot_silently_fall_back_to_ecs():
    value = goal()
    bound = {"cce_ingress_route_decision": ingress.DECISION,
             "cce_ingress_route_contract_sha256": policy.digest(ingress.contract()),
             "cce_ingress_route_arm": "candidate"}
    with pytest.raises(ValueError, match="hourly ingress"):
        ingress.validate_binding(value, bound)
    bound.update(cce_ingress_hourly_decision="ADR0293",
                 cce_ingress_hourly_prerequisite_sha256=ingress.HOURLY_BASELINE_SHA256)
    ingress.validate_binding(value, bound)


def test_altered_or_failed_short_result_cannot_authorize_hour(monkeypatch):
    original = policy.read
    def read(path):
        value = original(path)
        if path == policy.ROOT / ingress.HOURLY_BASELINE:
            value["financial"]["pass"] = False
        return value
    monkeypatch.setattr(policy, "read", read)
    with pytest.raises(ValueError, match="prerequisite"):
        ingress.hourly_baseline(goal())


def test_unrestored_prerequisite_ledger_cannot_authorize_hour(monkeypatch):
    journal = copy.deepcopy(policy.journal(policy.envelope()))
    baseline = policy.read(policy.ROOT / ingress.HOURLY_BASELINE)
    entry = next(e for e in journal["experiments"] if e["ledger"] == baseline["ledger"])
    entry["status"] = "FAILED_RESTORED"
    monkeypatch.setattr(policy, "journal", lambda value: journal)
    with pytest.raises(ValueError, match="ledger"):
        ingress.hourly_baseline(goal())


def test_spent_hourly_allowance_cannot_create_another_stage(monkeypatch):
    envelope = copy.deepcopy(policy.envelope())
    journal = copy.deepcopy(policy.journal(envelope))
    value = {**goal(), "ledger_start_index": len(journal["experiments"])}
    envelope["spending"]["temporary_cce_pilot_exception"]["goal_bounded_authorization"] = value
    journal["experiments"].append({"ledger": "spent-hour", "profile": HOURLY.name,
                                  "paid_runs_started": 1, "status": "PASSED_RESTORED"})
    monkeypatch.setattr(policy, "envelope", lambda: envelope)
    monkeypatch.setattr(policy, "journal", lambda value: journal)
    with pytest.raises(ValueError, match="already consumed"):
        api.authorized_creation(envelope)

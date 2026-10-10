"""Execute the registered workload contract and reject drift before dispatch."""
import copy
import hashlib
from types import SimpleNamespace
from uuid import uuid4

import cce_api_adapter as api
import cce_ticket_target_profile as target
import cce_transaction_profile as transaction
import cce_worker_rebalance_profile as placement
import customer_recovery_bundle as recovery
import pytest
import run_cce_paid_comparison as runner
import work_envelope as policy
from cce_paid_profiles import SHORT, TICKET_TARGET, for_guard
from cce_paid_stage import PaidStage, generator_arguments, generator_bundle
from fixture_identity_evidence import fixture_identity


def goal():
    return {"decision": "ADR0277", "extension_decision": "ADR0277", "profile": "cce_ticket_target",
            "comparison_arm": "correction", "offered_journeys_per_second": 168,
            "offered_seconds": 300, "generator_concurrency": 1000, "target_unique_tickets": 50000,
            "acquisition_budget": 20, "candidate_factor": placement.factor()}


def select(monkeypatch):
    envelope = copy.deepcopy(policy.envelope())
    envelope["spending"]["temporary_cce_pilot_exception"]["goal_bounded_authorization"] = goal()
    monkeypatch.setattr(policy, "envelope", lambda: envelope)
    return envelope


def test_registered_plan_preserves_application_and_database_budgets(monkeypatch):
    select(monkeypatch)
    plan = runner.plan(profile=TICKET_TARGET)
    assert plan == target.plan(goal()) == transaction.plan()
    assert plan["decision"] == "ADR0277"
    assert plan["common"] == {"buyer_journeys_per_second": 168, "duration_seconds": 300}
    assert plan["pooler_server_connections"] == 24
    assert plan["consumer_local_pool_budget"] == 48
    assert plan["customer_recovery_max_attempts"] == 3
    assert plan["maximum_pods"] == 18
    assert api.contract()["workload"]["concurrency"] == 1000
    assert api.contract()["workload"]["offered_journeys_per_second"] == 168
    assert api.api_image() == placement.parent.image()["registry_image"]
    assert runner.entry_for(TICKET_TARGET) is runner


@pytest.mark.parametrize("field,value", [("offered_journeys_per_second", 84), ("generator_concurrency", 2000),
                                         ("target_unique_tickets", 49999), ("acquisition_budget", 24)])
def test_goal_rejects_drift(field, value):
    value_goal = goal(); value_goal[field] = value
    with pytest.raises(ValueError): target.active(value_goal)


def test_actual_paid_stage_selects_derived_recovery_and_bound_arguments(monkeypatch, tmp_path):
    select(monkeypatch)
    parent = generator_bundle()
    root = policy.ROOT
    for n in ("docs/capacity/flash-sale-opening/frozen-baseline-cpu-diagnostic-plan-2026-10-05.json",
              "artifacts/generator-completion/manifest.json"):
        # ARTIFACTS remains bound to the original checkout; only the parent is read under ROOT.
        source = root/n
        if source.exists():
            p=tmp_path/n;p.parent.mkdir(parents=True,exist_ok=True);p.write_bytes(source.read_bytes())
    monkeypatch.setattr(policy, "ROOT", tmp_path)
    monkeypatch.setattr(transaction, "active", lambda: goal())
    monkeypatch.setattr(recovery, "raw", lambda n: (root/n).read_bytes().replace(b"\r\n",b"\n"))
    run="adr0151-"+"a"*12;output=tmp_path/"tmp"/run/"candidate";output.mkdir(parents=True)
    guard=SimpleNamespace(key="bounded_cce_ticket_target__"+"b"*12,binding={"cce_recovery_max_attempts":3})
    session=SimpleNamespace(config={"generator":{"repo":"/root/flash-generator"}})
    # The local coordinator path is read before it is replaced by the sealed recovery adapter.
    p=tmp_path/"scripts/run_synchronized_paid_generator.py";p.parent.mkdir(exist_ok=True);p.write_bytes((root/"scripts/run_synchronized_paid_generator.py").read_bytes())
    stage=PaidStage(session,guard,run,"c"*64,output,bundle=parent)
    assert stage.profile == TICKET_TARGET == for_guard(guard)
    assert stage.record["customer_recovery_max_attempts"] == 3
    leaf=stage.bundle["scripts/paid_ticket_sharded_generator.py"]
    assert b"2 <= args.rate <= 168" in leaf
    assert hashlib.sha256(leaf).hexdigest().encode() in stage.coordinator
    args=generator_arguments(stage.gen,stage.origin,0,profile=stage.profile,recovery_max_attempts=3)
    assert args[args.index("--rate")+1] == "168"
    assert args[args.index("--concurrency")+1] == "1000"
    assert args[-2:] == ["--recovery-max-attempts","3"]


def test_historical_workload_and_window_are_not_relabelled():
    args=generator_arguments("/root/test/tmp/adr0151-"+"a"*12+"-cce-candidate","http://10.1.137.69:8000",0,profile=SHORT)
    assert args[args.index("--rate")+1] == "84"
    assert SHORT.expected == 25200 and TICKET_TARGET.expected == 50400
    ids=[str(uuid4()) for _ in range(168)]
    program=target.issuance_program(ids,1000)
    assert "start+300" in program and "50000<=row[4]" in program
    assert "t.issued_at<to_timestamp(%s+300)" in program
    with pytest.raises(ValueError): target.issuance_program(ids[:84],1000)
    with pytest.raises(ValueError): fixture_identity({},167)

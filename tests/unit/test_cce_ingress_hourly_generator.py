"""ADR0295 exercise the real coordinator checks and sealed paid-stage composition."""
import copy
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[2] / "scripts"))

import cce_hourly_paid_generator as parent
import cce_ingress_hourly_generator as adapter
import cce_paid_stage as stage
import cce_transaction_profile as transaction
import pytest
from cce_paid_profiles import HOURLY
from test_cce_hourly_generator import arguments, cohort  # noqa: F401
from test_cce_ingress_hourly import goal


def test_adapted_validation_preserves_actual_network_origin_and_frozen_checks(cohort, monkeypatch):  # noqa: F811
    monkeypatch.setattr(parent, "time", lambda: 1000)
    args = arguments()
    args.origin = adapter.INGRESS_ORIGIN
    data = {**cohort, "origin": args.origin}
    with pytest.raises(ValueError, match="coordinator"):
        parent.validate(args, data)
    adapter.validate(parent.validate, args, data)
    assert args.origin == data["origin"] == adapter.INGRESS_ORIGIN
    assert cohort["origin"] == adapter.CONTROL_ORIGIN


@pytest.mark.parametrize("origin,manifest_origin", [
    (adapter.CONTROL_ORIGIN, adapter.CONTROL_ORIGIN),
    ("http://127.0.0.1:8000", "http://127.0.0.1:8000"),
    (adapter.INGRESS_ORIGIN, adapter.CONTROL_ORIGIN),
    (adapter.INGRESS_ORIGIN+"/", adapter.INGRESS_ORIGIN+"/"),
])
def test_ingress_adapter_rejects_unknown_or_mismatched_targets(cohort, origin, manifest_origin, monkeypatch):  # noqa: F811
    monkeypatch.setattr(parent, "time", lambda: 1000)
    args = arguments()
    args.origin = origin
    with pytest.raises(ValueError, match="Exact existing CCE"):
        adapter.validate(parent.validate, args, {**cohort, "origin": manifest_origin})


@pytest.mark.parametrize("key,value", [("rate",85),("concurrency",1000),("seconds",300),
                                      ("poll_seconds",.2),("start_at_epoch",1000)])
def test_ingress_does_not_relax_coordinator_workload_or_deadlines(cohort, key, value, monkeypatch):  # noqa: F811
    monkeypatch.setattr(parent, "time", lambda: 1000)
    args = arguments()
    args.origin = adapter.INGRESS_ORIGIN
    setattr(args,key,value)
    with pytest.raises(ValueError, match="coordinator"):
        adapter.validate(parent.validate, args, {**cohort,"origin":adapter.INGRESS_ORIGIN})


def test_actual_registered_launch_arguments_pass_the_full_hourly_validator(cohort, monkeypatch):  # noqa: F811
    monkeypatch.setattr(parent, "time", lambda: 1000)
    monkeypatch.setattr(transaction, "active", goal)
    data = {**cohort, "origin":adapter.INGRESS_ORIGIN}
    argv = stage.generator_arguments("/root/project/tmp/adr0151-123456789abc-cce-candidate",
        data["origin"],1030,profile=HOURLY,recovery_max_attempts=3)
    assert argv[1].endswith("/cce_ingress_hourly_generator.py")
    assert adapter.preflight(argv, data)
    assert argv[argv.index("--origin")+1] == adapter.INGRESS_ORIGIN
    with pytest.raises(ValueError,match="duplicate"):
        adapter.preflight(argv+["--rate","84"],data)
    with pytest.raises(ValueError,match="coordinator"):
        wrong = copy.copy(argv)
        wrong[wrong.index("--concurrency")+1]="1000"
        adapter.preflight(wrong,data)


def test_legacy_hourly_path_and_sealed_adapter_bytes_are_retained(cohort, monkeypatch):  # noqa: F811
    import cce_nginx_ingress as ingress
    import cce_recovery_hourly_profile as sealed
    monkeypatch.setattr(parent,"time",lambda:1000)
    monkeypatch.setattr(transaction,"active",lambda:None)
    argv=stage.generator_arguments("/root/project/tmp/adr0151-123456789abc-cce-candidate",
        adapter.CONTROL_ORIGIN,1030,profile=HOURLY)
    assert argv[1].endswith("/cce_hourly_paid_generator.py")
    assert adapter.preflight(argv,cohort)
    assert sealed.qualify_hourly_adapters()["decision"] == "ADR0256"
    assert ingress.contract()["hourly_generator_decision"] == "ADR0295"
    assert "scripts/cce_ingress_hourly_generator.py" in stage.identity()


def test_adapter_restores_original_validator_after_execution(monkeypatch):
    original=parent.validate
    observed=[]
    monkeypatch.setattr(parent,"main",lambda: observed.append(parent.validate is not original))
    adapter.main()
    assert observed == [True] and parent.validate is original

def test_invalid_hourly_launch_is_rejected_before_allowance_consumption(cohort, monkeypatch):  # noqa: F811
    from types import SimpleNamespace

    import work_envelope as policy
    monkeypatch.setattr(parent, "time", lambda: 1000)
    monkeypatch.setattr(stage.time, "time", lambda: 1000)
    monkeypatch.setattr(transaction, "active", goal)
    monkeypatch.setattr("cce_ecs_transition.receipt_gate", lambda *args: True)
    monkeypatch.setattr(stage.cce, "contract", lambda: {"api_sources":{}, "resources":{}})
    read = policy.read
    def guarded_read(path):
        if path == policy.STATE:
            pytest.fail("Invalid launch must not consume or read paid scope")
        return read(path)
    monkeypatch.setattr(policy, "read", guarded_read)
    value=stage.PaidStage.__new__(stage.PaidStage)
    value.check=lambda *args: None
    value.checkpoint=lambda: None
    value.prepared=True
    value.record={"customers_dispatched":False}
    value.run="adr0151-123456789abc"
    value.guard=SimpleNamespace(binding={})
    value.gen="/root/project/tmp/"+value.run+"-cce-candidate"
    value.origin=adapter.INGRESS_ORIGIN
    value.profile=HOURLY
    value.recovery_enabled=True
    value.manifest=cohort  # Deliberately retains the ECS origin.
    admission={"run":value.run,"binding_sha256":policy.digest({}),
               "all_required_pre_dispatch_gates_pass":True,
               "cce_api_receipts":[{"startup_proof":{"sources":{}},"resources":{}}]}
    with pytest.raises(ValueError,match="matching manifest"):
        value.dispatch(admission,start_at_epoch=1030)
    assert value.record["customers_dispatched"] is False

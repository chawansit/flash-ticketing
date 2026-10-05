"""ADR0170 single-control limits/lifecycle and immutable runtime; no cloud calls."""
import json
import sys
from datetime import UTC, datetime, timedelta
from pathlib import Path

import pytest

sys.path.insert(0,str(Path(__file__).resolve().parents[2]/"scripts"))
import partial_timeout_contract as baseline
import payment_stall_contract as policy
import run_async_confirmation_comparison as shared
import run_partial_timeout_comparison as pair
import run_payment_stall_diagnostics as runner
import run_status_refresh_comparison as historical
from async_confirmation_contract import FACTOR as ASYNC_FACTOR
from test_two_host_deployment import fixture


def contract():
    plan=policy.plan()
    return policy.PaymentStallContract(plan["artifact_receipt"],"control",plan["expected_runtime_source_sha256"])


def state(engine,binding):
    return {"cloud_load_requires_resume":False,"current_run":None,"consumed_old_scope":{"status":"CLOSED","paid_runs_started":2},
            engine.LEDGER:{"authorization_id":engine.AUTHORIZATION,"binding":binding,"qualification_runs_authorized":1,
                           "paid_runs_authorized":1,"safety_tickets_authorized":2,"qualification_protocols_started":0,
                           "paid_runs_started":0,"paid_protocols_started":0,"safety_protocols_started":0,"active_run":None}}


def qualification(engine,binding):
    return {"experiment_decision":"ADR0170","factor":policy.FACTOR,"kind":"status_refresh_dry_control","pass":True,
            "binding":binding,"finished_at_utc":datetime.now(UTC).isoformat(),"capacity_stages_started":0,
            "safety_protocols_started":1,"arms":{"control":{"pass":True,"restoration_complete":True,"pre_safety_source_pass":True}}}


def test_constructor_only_control_and_native_limits_unchanged():
    plan=policy.plan();c=contract();off=baseline.AdmissionReclaimContract(plan["artifact_receipt"],"control",plan["expected_runtime_source_sha256"])
    for role in c.roles:assert c.settings(role)==off.settings(role)
    assert c.images==off.images and c.inventory_marker()==off.inventory_marker()
    assert c.settings("api")["API_PARTIAL_TIMEOUT_RECLAIM"]=="0"
    assert c.settings("api")[ASYNC_FACTOR.removeprefix("api_")]=="0"
    with pytest.raises(ValueError):policy.PaymentStallContract(plan["artifact_receipt"],"candidate",plan["expected_runtime_source_sha256"])
    engine=runner.create_runner();assert engine.ARMS==("control",)
    engine.RefreshStages(False,{},c,"adr0151-"+"a"*12)
    assert pair.create_runner().ARMS==historical.ARMS==("control","candidate")


@pytest.mark.parametrize("arms",[("candidate",),("control","control"),(),("control",)])
def test_unrecognized_profile_cannot_select_single_arm(arms):
    with pytest.raises(ValueError):shared.create_runner(arms=arms)


@pytest.mark.parametrize("mutation",["pause","active","wrong_binding","old_scope","pair_allowance","spent","missing_counter"])
def test_release_rejects_bad_scope_or_consumption(mutation):
    e=runner.create_runner();binding={"exact":"binding"};s=state(e,binding)
    if mutation=="pause":s["cloud_load_requires_resume"]=True
    if mutation=="active":s["current_run"]="other"
    if mutation=="wrong_binding":s[e.LEDGER]["binding"]={}
    if mutation=="old_scope":s[e.LEDGER]["authorization_id"]=pair.AUTHORIZATION
    if mutation=="pair_allowance":s[e.LEDGER].update(paid_runs_authorized=2,safety_tickets_authorized=4)
    if mutation=="spent":s[e.LEDGER]["paid_protocols_started"]=1
    if mutation=="missing_counter":del s[e.LEDGER]["paid_runs_started"]
    with pytest.raises(ValueError):e.validate_release(s,binding,execute=False)


@pytest.mark.parametrize("drift",[None,"old","candidate","pair","failed","identity"])
def test_single_control_qualification_and_paid_limits(drift):
    e=runner.create_runner();binding={"exact":"binding"};q=qualification(e,binding)
    if drift=="old":q["finished_at_utc"]=(datetime.now(UTC)-timedelta(hours=2)).isoformat()
    if drift=="candidate":q["arms"]["candidate"]=q["arms"].pop("control")
    if drift=="pair":q["kind"]="status_refresh_dry_pair";q["safety_protocols_started"]=2
    if drift=="failed":q["pass"]=False
    if drift=="identity":q["binding"]={}
    assert e.qualification_matches(q,binding) is (drift is None)
    s=state(e,binding);s[e.LEDGER].update(qualification_protocols_started=1,safety_protocols_started=1)
    if drift is None:e.validate_release(s,binding,execute=True,qualification=q)
    else:
        with pytest.raises(ValueError):e.validate_release(s,binding,execute=True,qualification=q)


@pytest.mark.parametrize("failed,restored",[(False,True),(True,True),(True,False),(False,False)])
def test_single_dry_lifecycle_never_candidate_and_preserves_old_ledger(monkeypatch,tmp_path,failed,restored):
    e=runner.create_runner();binding={"exact":"binding"};(tmp_path/"tmp").mkdir()
    monkeypatch.setattr(e,"ROOT",tmp_path);monkeypatch.setattr(e,"STATE",tmp_path/"state.json")
    monkeypatch.setattr(e,"LOCK",tmp_path/"tmp/run.lock");monkeypatch.setattr(e,"binding_for",lambda *_:binding)
    e.STATE.write_text(json.dumps(state(e,binding)));calls=[]
    def arm(_c,_a,_s,_b,name,run_id,_out,*,execute):
        e.reserve_arm(run_id,name,execute=execute);calls.append(name)
        return {"pass":not failed,"restoration_complete":restored,"pre_safety_source_pass":True}
    monkeypatch.setattr(e,"run_arm",arm)
    plan=policy.plan();cfg,*_=fixture()
    result=e.protocol(cfg,plan["artifact_receipt"],plan["expected_runtime_source_sha256"],{},binding,execute=False)
    assert calls==["control"] and result["pass"] is (not failed and restored)
    saved=json.loads(e.STATE.read_text());assert saved["consumed_old_scope"]==state(e,binding)["consumed_old_scope"]
    assert saved[e.LEDGER]["safety_protocols_started"]==1 and saved[e.LEDGER]["qualification_protocols_started"]==1
    assert e.LOCK.exists() is (not restored)
    with pytest.raises(ValueError):e.validate_release(saved,binding,execute=False)


def test_no_candidate_reservation_or_second_paid_launch(monkeypatch,tmp_path):
    e=runner.create_runner();binding={"exact":"binding"};run="adr0151-"+"a"*12;s=state(e,binding)
    s["current_run"]=run;s[e.LEDGER].update(active_run=run,qualification_protocols_started=1,safety_protocols_started=1)
    monkeypatch.setattr(e,"STATE",tmp_path/"state.json");e.STATE.write_text(json.dumps(s))
    with pytest.raises(ValueError):e.reserve_arm(run,"candidate",execute=True)
    assert json.loads(e.STATE.read_text())==s
    e.reserve_arm(run,"control",execute=True)
    with pytest.raises(ValueError):e.reserve_arm(run,"control",execute=True)
    saved=json.loads(e.STATE.read_text());assert saved[e.LEDGER]["safety_protocols_started"]==2
    assert saved[e.LEDGER]["paid_protocols_started"]==1


def test_adapter_identity_covers_capture_owner_factory_and_plan():
    e=runner.create_runner();ident=e.identity()
    for name in ("run_payment_stall_diagnostics.py","payment_stall_contract.py","admission_failure_evidence.py","fetch_status_refresh_parents.py","stage_status_refresh_images.py"):
        assert "scripts/"+name in ident
    assert "experiment_plan" in ident


def test_paid_protocol_exactly_one_stage_then_scope_consumed(monkeypatch,tmp_path):
    e=runner.create_runner();binding={"exact":"binding"};(tmp_path/"tmp").mkdir()
    monkeypatch.setattr(e,"ROOT",tmp_path);monkeypatch.setattr(e,"STATE",tmp_path/"state.json")
    monkeypatch.setattr(e,"LOCK",tmp_path/"tmp/run.lock");monkeypatch.setattr(e,"binding_for",lambda *_:binding)
    saved=state(e,binding);saved[e.LEDGER].update(qualification_protocols_started=1,safety_protocols_started=1)
    e.STATE.write_text(json.dumps(saved));calls=[]
    def arm(_c,_a,_s,_b,name,run_id,_out,*,execute):
        assert execute;e.reserve_arm(run_id,name,execute=True);calls.append(name)
        claimed=json.loads(e.STATE.read_text());claimed[e.LEDGER]["paid_runs_started"]+=1;e.write_state(claimed,run_id)
        return {"pass":True,"restoration_complete":True,"pre_safety_source_pass":True,
                "measurements":{"cache_observed":True,"cache_series_present":True}}
    monkeypatch.setattr(e,"run_arm",arm);plan=policy.plan();cfg,*_=fixture()
    result=e.protocol(cfg,plan["artifact_receipt"],plan["expected_runtime_source_sha256"],{},binding,execute=True,
                      qualification=qualification(e,binding))
    assert result["pass"] and calls==["control"] and result["capacity_stages_started"]==1
    assert result["kind"]=="payment_stall_paid_control"
    saved=json.loads(e.STATE.read_text());assert saved[e.LEDGER]["paid_protocols_started"]==1
    assert saved[e.LEDGER]["safety_protocols_started"]==2 and not saved["current_run"] and not e.LOCK.exists()
    with pytest.raises(ValueError):e.validate_release(saved,binding,execute=True,qualification=qualification(e,binding))

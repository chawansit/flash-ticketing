"""ADR0363 exact new-image authority, worker flag and portable helper qualification."""
import copy
import hashlib
import json
import os
import subprocess
import sys
from datetime import UTC, datetime
from pathlib import Path
from types import SimpleNamespace

import pytest

sys.path.insert(0,str(Path(__file__).resolve().parents[2]/"scripts"))
import cce_projection_refresh_pipeline as claim
import cce_rate_qualification as qualification
import cce_simulator_roundtrip_profile as profile
import cce_transaction_profile as transaction
import cce_worker_identity as identity
import prepare_rate_qualification as preparer
import work_envelope as policy
from test_cce_fixed_worker_allocation import allocated


def goal(rate=180):
    base=policy.read(policy.ROOT/"tmp/adr0354-control-goal.prepared.private.json")
    base.update(fixed_worker_allocation_decision="ADR0359",projection_refresh_pipeline_decision="ADR0363")
    ref=policy.read(policy.ROOT/"tmp/adr0359-200-reference.private.json")
    pair={"id":"3"*32,"started_at_utc":None,"ledger_start_index":len(policy.journal(policy.envelope())["experiments"])}
    return preparer.goal_for(base,rate,ref,pair)


def test_actual_parent180_authorizes_only_new_image180():
    value=goal()
    assert claim.selected(value) and qualification.selected(value) and profile.selected(value)
    assert qualification.contract(value)["single_changed_factor"]=="projection_refresh_pipeline"
    report,binding=qualification.require_previous(value)
    assert report["run"]=="adr0151-6668a7e46ed3"
    assert binding["cce_simulator_roundtrip_contract"]["source"]==profile.SOURCE
    assert profile.image(value)["registry_manifest_digest"]==claim.MANIFEST
    assert profile.contract(value)["source"]==claim.SOURCE
    assert profile.binding_contract(value)["worker_allocation"]["application_image_unchanged"] is False
    assert binding["cce_simulator_roundtrip_contract"]["worker_allocation"]["application_image_unchanged"] is True
    assert claim.contract(value)["transaction_pipeline"] is True
    assert claim.contract(value)["sql_unchanged"] is True
    with pytest.raises(ValueError):qualification.require_previous(goal(200))


@pytest.mark.parametrize("fault",["decision","allocation","arm","rate","old160","failed_result","hash"])
def test_no_false_prerequisite_or_unreviewed_mode(fault):
    value=goal()
    if fault=="decision":value[claim.KEY]="unknown"
    elif fault=="allocation":value["fixed_worker_allocation_decision"]="ADR0357"
    elif fault=="arm":value["simulator_roundtrip_arm"]="control"
    elif fault=="rate":value["rate_qualification_rate"]=180.0
    elif fault=="old160":
        value["rate_qualification_reference"]=policy.read(policy.ROOT/"tmp/adr0357-180b-control-goal.prepared.private.json")["rate_qualification_reference"]
    elif fault=="failed_result":
        failed=policy.read(policy.ROOT/"tmp/adr0359-200b-sequence.approved.private.json")
        value["rate_qualification_reference"].update(path=failed["candidate_report_path"],ledger=failed["candidate_ledger"],sha256=failed["candidate_result_sha256"])
    else:value["rate_qualification_reference"]["sha256"]="0"*64
    with pytest.raises(ValueError):
        claim.selected(value)
        qualification.require_previous(value)


def test_projection_image_preserves_original_role_environments():
    value=goal();settings={"unchanged":"value"}
    for role in ("reservation-writer","consumer","projection-consumer","publisher","maintenance","reconciler","simulator"):
        assert claim.environment(role,settings,value)==settings
    for selected in ("0","1"):
        with pytest.raises(ValueError):claim.environment("reservation-writer",{"RESERVATION_LOOKUP_CLAIM":selected},value)
    from cce_image_qualification import for_goal
    value["reservation_claim_lookup_decision"]="ADR0362"
    with pytest.raises(ValueError):for_goal(value)


def test_new_transferred_identity_needs_no_local_authority(monkeypatch):
    value,bundle=allocated(monkeypatch,"ADR0359",goal=goal())
    assert len(identity.validate_bundle(bundle))==15
    monkeypatch.setattr(transaction,"active",lambda:(_ for _ in ()).throw(AssertionError("local authority")))
    monkeypatch.setattr(policy,"read",lambda *args:(_ for _ in ()).throw(AssertionError("local files")))
    assert len(identity.validate_transferred_bundle(bundle))==15
    for decision in ("ADR0345","ADR0357"):
        bad=copy.deepcopy(bundle);bad["decision"]=decision
        with pytest.raises(ValueError):identity.validate_transferred_bundle(bad)
    assert value["fixed_worker_allocation_decision"]=="ADR0359"


@pytest.mark.parametrize("rate",[180,200])
def test_actual_sealed_generator_guard_for_new_image_mode(monkeypatch,rate):
    import test_cce_rate_qualification as existing
    monkeypatch.setattr(existing,"actual",lambda selected=rate:goal(selected))
    existing.test_actual_sealed_coordinator_accepts_only_exact_new_rate(rate)


def test_actual_parent_startup_and_unchanged_six_member_drain(monkeypatch):
    import cce_fixed_worker_allocation as allocation
    import cce_writer_scale_verification as verification
    import run_cce_paid_comparison as core
    from observe_paid_pipeline import pipeline_startup_view
    _,bundle=allocated(monkeypatch,"ADR0359",goal=goal())
    contract=core.worker_contract()
    assert "result['kafka_members']==6" in contract.global_audit
    assert allocation.native_drain_options(SimpleNamespace(cleanup_mode=False))=={}
    row=policy.read(policy.ROOT/"tmp/adr0151-ccb85bad6425/candidate/pipeline-startup-row.private.json")
    contract.verify_pipeline_startup(row)
    assert verification.startup(row,bundle,pipeline_startup_view)["pass"]


def test_optional_exact_new_image_transferred_helpers(monkeypatch,tmp_path):
    target=os.getenv("QUALIFY_PROJECTION_HELPERS_RECEIPT")
    if not target:pytest.skip("Explicit exact-image rehearsal not requested")
    import test_cce_transferred_combined_workers as retained
    from cce_recovery_headroom import runtime_view
    value,bundle=allocated(monkeypatch,"ADR0359",goal=goal())
    monkeypatch.setattr(transaction,"active",lambda:runtime_view(value))
    monkeypatch.setattr(retained,"arguments",lambda *args:(value,{},{"run":bundle["run"],"workers_before":bundle},[]))
    image,helpers=retained.capture_helpers(monkeypatch,tmp_path,"candidate")
    inventory=policy.read(policy.ROOT/"tmp/adr0151-ccb85bad6425/candidate/pre-transition-inventory.private.json")
    inventory["captured_at"]=datetime.now(UTC).isoformat()
    (tmp_path/"inventory.json").write_bytes(json.dumps(inventory).encode())
    program=retained.PROGRAM.replace("metadata=json.load(sys.stdin)","metadata=json.load(sys.stdin)\nfor name,want in metadata['runtime_sources'].items():assert hashlib.sha256((Path('/app')/name).read_bytes()).hexdigest()==want")
    metadata={"helpers":{name:hashlib.sha256(raw).hexdigest() for name,raw in helpers.items()},
              "runtime_sources":image["runtime_source_sha256"]}
    result=subprocess.run(["docker","run","--rm","--network=none","-i","--entrypoint","python","--mount",
                           "type=bind,source="+str(tmp_path.resolve())+",target=/qualified,readonly",
                           image["local_image_id"],"-c",program],input=json.dumps(metadata),
                          text=True,capture_output=True,timeout=90,check=False)
    (policy.ROOT/"tmp/adr0363-actual-helper.log").write_text(result.stderr,encoding="utf-8")
    assert result.returncode==0,"Inspect owned exact-image helper log"
    path=Path(target);assert not path.exists() and path.resolve().is_relative_to((policy.ROOT/"tmp").resolve())
    path.write_bytes((json.dumps({"decision":"ADR0363",**json.loads(result.stdout),
                                 "runtime_modules_verified":22,"synthetic_inventory_timestamp":True},indent=2)+"\n").encode())

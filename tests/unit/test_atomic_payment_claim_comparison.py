"""ADR0193 regressions: exact bytes, unchanged budgets, guarded fresh scope."""
import hashlib
import json
import sys
from pathlib import Path
from types import SimpleNamespace

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[2] / "scripts"))
import atomic_payment_claim_contract as policy
import observe_two_host_pipeline as observer
import run_atomic_payment_claim_comparison as profile
import run_status_refresh_comparison as original
import run_two_host_paid_comparison as paid
import run_work_envelope as runner
import stage_status_refresh_images as staging
import work_envelope as envelope
from prepare_two_host_scaling import validate_inventory
from test_async_confirmation_comparison import inventory
from test_work_envelope import area  # noqa: F401 - isolated journal fixture


def contract(arm="control"):
    p = policy.plan()
    return policy.AtomicPaymentClaimContract(p["artifact_receipt"], arm, p["expected_runtime_source_sha256"])


def observed(arm):
    c = contract(arm)
    data = inventory(c)
    for row in data["worker_sources"]:
        row["source_identity"]["resolved_imports"] = {"src/ticketing/workers.py": {
            "sha256": c.sources["src/ticketing/workers.py"]}}
    return c, data


def test_only_claim_source_changes_no_budget_or_flag_changes():
    a, b = contract(), contract("candidate")
    assert {p for p in a.sources if a.sources[p] != b.sources[p]} == {"src/ticketing/workers.py"}
    assert a.parents == b.parents and a.images != b.images
    assert a.background == b.background and a.api_settings == b.api_settings
    assert all(a.settings(r) == b.settings(r) for r in a.roles)
    assert b.api_settings["API_PARTIAL_TIMEOUT_RECLAIM"] == "0"
    assert b.api_settings["PAYMENT_CONFIRMATION_ASYNC"] == "0"
    assert tuple(c.arm for c in a.staging_contracts()) == ("control", "candidate")


@pytest.mark.parametrize("arm", ["control", "candidate"])
def test_actual_inventory_and_observer_validate_arm_bytes_budgets(arm):
    c, data = observed(arm)
    result = validate_inventory(data, image_id=c.images["api"], contract=c)
    assert result["api_counts"] == {"primary": 2, "secondary": 2}
    assert result["pgbouncer_server_connections"] == 24
    observer.verify_admission_factor_evidence(data)
    frozen = SimpleNamespace(METRICS={}, parse_api_metrics=lambda _: {}, api_replicas=lambda *_: [])
    digest = hashlib.sha256(json.dumps(data,sort_keys=True,separators=(",", ":")).encode()).hexdigest()
    observer.install_adapter(frozen,data,image_id=c.images["api"],approved_inventory_sha256=digest)
    assert "confirmation" in frozen.METRICS and len(frozen.api_replicas()) == 4


@pytest.mark.parametrize("drift", ["claim", "source", "source_missing", "flag", "pool", "worker", "image", "pooler"])
def test_wrong_runtime_identity_or_budget_blocks_candidate(drift):
    c, data = observed("candidate")
    if drift == "claim": data["status_refresh_contract"]["claim_statements"] = 2
    elif drift == "source": data["worker_sources"][0]["source_identity"]["resolved_imports"]["src/ticketing/workers.py"]["sha256"] = "f" * 64
    elif drift == "source_missing": data["worker_sources"][0]["source_identity"].pop("resolved_imports")
    elif drift == "flag": data["apis"][0]["settings"]["API_PARTIAL_TIMEOUT_RECLAIM"] = "1"
    elif drift == "pool": data["apis"][0]["settings"]["DB_POOL_MAX"] = "8"
    elif drift == "worker": data["worker_sources"].pop()
    elif drift == "image": data["apis"][0]["image_id"] = "sha256:" + "f" * 64
    else: data["pgbouncer"]["server_pool"] = 48
    with pytest.raises(ValueError):
        # Combined host qualification and source-bound observer admission are mandatory.
        validate_inventory(data,image_id=c.images["api"],contract=c)
        observer.verify_admission_factor_evidence(data)


@pytest.mark.parametrize("drift", ["swapped", "extra_source", "missing_role", "parent", "manifest"])
def test_pair_constructor_rejects_receipt_drift(drift):
    p = policy.plan(); a = p["artifact_receipt"]; s = p["expected_runtime_source_sha256"]
    if drift == "swapped": a["candidate"],a["control"] = a["control"],a["candidate"]
    elif drift == "extra_source": s["candidate"]["src/ticketing/api.py"] = "f" * 64
    elif drift == "missing_role": a["candidate"]["images"].pop("confirmation")
    elif drift == "parent": a["candidate"]["parent_images"]["consumer"] = "sha256:" + "f" * 64
    else: a["candidate"]["source_manifest_sha256"] = "f" * 64
    with pytest.raises(ValueError):
        policy.AtomicPaymentClaimContract(a,"candidate",s)


def test_real_runner_constructor_identity_and_failed_control_progression():
    e = profile.create_runner()
    assert e.ARMS == ("control", "candidate") and e.LOCK == original.LOCK
    names = e.identity()
    assert all("scripts/" + n in names for n in ("prepare_atomic_payment_claim.py", "slow_database_contract.py",
        "prepare_partial_timeout_reclamation.py", "prepare_async_confirmation.py", "stage_status_refresh_images.py"))
    for arm in e.ARMS:
        s = e.RefreshStages(False,{},contract(arm),"adr0151-" + "a" * 12)
        assert s.expected_tickets == 18000 and s.rate == 60
    with pytest.raises(ValueError):
        paid.Stages(False,{},ledger_key=profile.LEDGER,stage_limit=1,contract=None)
    # Qualification results from another decision cannot authorize this pair.
    assert not e.qualification_matches({"experiment_decision":"ADR0171"}, {})


def test_registered_pair_requires_exact_plan_and_fresh_scope(area):  # noqa: F811
    binding,_,_ = area
    data = envelope.read(envelope.ENVELOPE)
    data["qualified_profiles"] = list(envelope.PROFILES)[:-1]
    envelope.write(envelope.ENVELOPE,data)
    with pytest.raises(ValueError,match="Unknown diagnostic profile"):
        envelope.reserve(binding,policy.plan(),profile="atomic_payment_claim")
    data["qualified_profiles"] = list(envelope.PROFILES);envelope.write(envelope.ENVELOPE,data)
    bad = policy.plan();bad["arm_settings"]["candidate"]["api"]["ORDER_STATUS_POLL_MS"] = "100"
    with pytest.raises(ValueError):
        envelope.reserve(binding,bad,profile="atomic_payment_claim")
    entry = envelope.reserve(binding,policy.plan(),profile="atomic_payment_claim")
    recorded = envelope.read(envelope.STATE)[entry["ledger"]]
    assert recorded["paid_runs_authorized"] == 2 and recorded["safety_tickets_authorized"] == 4
    assert envelope.base_ledger(entry["ledger"]) == profile.LEDGER
    with pytest.raises(ValueError):
        envelope.reserve(binding,policy.plan(),profile="atomic_payment_claim")


@pytest.mark.parametrize("qualification_pass", [False, True])
def test_single_command_stages_under_reservation_then_qualifies(area,tmp_path,monkeypatch,qualification_pass):  # noqa: F811
    binding,_,_ = area
    data = envelope.read(envelope.ENVELOPE);data["qualified_profiles"] = list(envelope.PROFILES)
    envelope.write(envelope.ENVELOPE,data);envelope.LOCK.unlink()
    monkeypatch.setattr(original,"LOCK",envelope.LOCK)
    calls = []
    outcomes = []
    fake = SimpleNamespace(ARMS=("control","candidate"),source_contract=dict,
        StatusRefreshContract=lambda *_: object(),RefreshStages=lambda *_: None,binding_for=lambda *_:binding,
        comparison=SimpleNamespace(validate_config=lambda _:None,frozen_bundle=dict,
            subprocess=SimpleNamespace(run=lambda *_a,**_k:None,DEVNULL=-3)))
    def stage(_c,_a,password,guard):
        guard.check();assert password == "test-only"
        assert envelope.read(envelope.JOURNAL)["experiments"][-1]["status"] == "ACTIVE"
        calls.append("stage")
    def protocol(*_a,execute,qualification=None):
        fake.ENVELOPE_GUARD.check();calls.append(execute)
        if execute: assert qualification is outcomes[0] and qualification_pass
        result={"run":"adr0151-"+('2' if execute else '1')*12,"pass":qualification_pass,
            "capacity_stages_started":2 if execute else 0,"arms":{}}
        for a in fake.ARMS:
            result["arms"][a]={"pass":qualification_pass,"restoration_complete":True,
                "gates":{"paid":{g:True for g in ("post_ttl_financial","zero_double_booking","full_keyspace_queue_drain","kafka_drain")}}}
        outcomes.append(result);return result
    fake.stage_images,fake.protocol=stage,protocol
    monkeypatch.setattr(profile,"create_runner",lambda:fake)
    monkeypatch.setattr(runner.getpass,"getpass",lambda _:"test-only")
    envelope.write(tmp_path/'config',{})
    runner.execute(tmp_path/'config',None,tmp_path,profile_name="atomic_payment_claim")
    assert calls == (["stage",False,True] if qualification_pass else ["stage",False])
    assert not envelope.LOCK.exists()


def test_failure_capture_uses_same_slow_database_evidence(monkeypatch,tmp_path):
    import slow_database_evidence
    monkeypatch.setattr(slow_database_evidence,"collect",lambda *_:{"complete":True,"failure_count":2,"counter_coverage":True})
    r = paid.collect_profile_failure_evidence(None,{},tmp_path,profile.LEDGER)
    assert r["admission_failure_capture"]["record_count"] == 2 and r["slow_database_capture"]["complete"]


def test_pair_staging_rejects_local_image_before_ssh(monkeypatch,tmp_path):
    p=policy.plan(); c=contract(); calls=[]
    monkeypatch.setattr(staging,"source_contract",lambda:p["expected_runtime_source_sha256"])
    monkeypatch.setattr(staging,"StatusRefreshContract",lambda *_:c)
    monkeypatch.setattr(staging,"validate_config",lambda _:None)
    monkeypatch.setattr(staging,"validate_release",lambda *_a,**_k:None)
    monkeypatch.setattr(staging,"binding_for",lambda *_:{})
    monkeypatch.setattr(staging,"STATE",tmp_path/'state');(tmp_path/'state').write_text('{}')
    monkeypatch.setattr(staging,"owned_output",lambda path:path.mkdir() or path)
    def command(_cmd,**kw):
        calls.append(kw['log'].name)
        if len(calls)==2: raise ValueError('candidate image differs')
    monkeypatch.setattr(staging,"command",command)
    monkeypatch.setattr(staging,"Session",lambda *_a,**_k:pytest.fail('No SSH after failed local image proof'))
    with pytest.raises(ValueError):
        staging.stage({},p['artifact_receipt'],tmp_path/'stage','test-only')
    assert calls == ['local-images.txt','local-candidate-images.txt']


def test_staging_union_and_legacy_single_receipt_are_exact():
    a, b = contract(), contract("candidate")
    pair = staging.archive_images((a,b))
    assert set(pair["primary"]) == set(a.images.values()) | set(b.images.values())
    assert set(pair["secondary"]) == {a.images["api"],b.images["api"],a.parents["api"]}
    single = staging.archive_images((a,))
    assert single["primary"] == sorted(set(a.images.values()))
    assert single["secondary"] == sorted({a.images["api"],a.parents["api"]})
    b.parents["api"] = "sha256:" + "f" * 64
    with pytest.raises(ValueError): staging.archive_images((a,b))


@pytest.mark.parametrize("bad", [False, True])
def test_archive_bound_counts_verified_shared_parents_once(monkeypatch,bad):
    a=SimpleNamespace(images={"api":"control"},parents={"api":"parent"})
    b=SimpleNamespace(images={"api":"candidate"},parents={"api":"parent"})
    infos={"parent":{"Size":100,"RootFS":{"Layers":["base"]}},
           "control":{"Size":110,"RootFS":{"Layers":["base","old"]}},
           "candidate":{"Size":112,"RootFS":{"Layers":["wrong" if bad else "base","new"]}}}
    monkeypatch.setattr(staging,"inspect_image",lambda image:infos[image])
    if bad:
        with pytest.raises(ValueError): staging.archive_upper_bound(["control","candidate"],(a,b))
    else:
        assert staging.archive_upper_bound(["control","candidate"],(a,b)) == 122
        assert staging.archive_upper_bound(["control"],(a,)) == 110


def test_owned_sealed_cleanup_survives_pause_without_enabling_more_actions(monkeypatch):
    session=SimpleNamespace(cleanup_mode=False)
    session.begin_cleanup=lambda:setattr(session,"cleanup_mode",True)
    def call(role,program,timeout):
        assert session.cleanup_mode and role == "primary" and timeout == 30
        return {"owned_archive_removed":True}
    session.call=call
    monkeypatch.setattr(staging,"cleanup_program",lambda _:"verified exact seal")
    host={}
    staging.cleanup_verified_archive(session,"primary",{},host)
    assert host["owned_archive_removed"] and not session.cleanup_mode

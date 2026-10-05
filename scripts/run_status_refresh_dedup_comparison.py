"""ADR0157: separate exact-bound profile, default-local; cloud needs a new allowance."""

import importlib.util
import json
from uuid import uuid4

import status_refresh_dedup_contract as policy
from qualify_two_host_deployment import ROOT

LEDGER = "bounded_status_refresh_dedup"
AUTHORIZATION = "adr0157-status-refresh-dedup-pair-2026-10-05"


def create_runner():
    # Load a private namespace; never mutate the imported historical runner or copy its engine.
    spec = importlib.util.spec_from_file_location(
        "adr0157_private_engine", ROOT / "scripts/run_status_refresh_comparison.py"
    )
    engine = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(engine)
    engine.PLAN, engine.LEDGER, engine.AUTHORIZATION = policy.PLAN, LEDGER, AUTHORIZATION
    engine.StatusRefreshContract = policy.StatusRefreshDedupContract
    engine.source_contract = policy.source_contract
    original_identity = engine.identity
    original_matches = engine.qualification_matches
    original_protocol, original_prepare = engine.protocol, engine.prepare

    def identity():
        return {**original_identity(), **{
            "scripts/" + name: engine.comparison.source_sha256((ROOT / "scripts" / name).read_bytes())
            for name in ("run_status_refresh_dedup_comparison.py", "status_refresh_dedup_contract.py",
                         "prepare_status_refresh_dedup.py", "prepare_status_refresh_artifacts.py",
                         "status_refresh_image_install.py", "stage_status_refresh_images.py",
                         "stage_status_refresh_dedup_images.py", "fetch_status_refresh_parents.py")},
            **{"artifacts/status-refresh-dedup/" + name: engine.comparison.source_sha256(
                (ROOT / "artifacts/status-refresh-dedup" / name).read_bytes())
               for name in ("manifest.json", "adr0156.patch")}}

    def qualification_matches(report, binding, *, now=None):
        return (isinstance(report, dict) and report.get("experiment_decision") == "ADR0157"
                and report.get("factor") == policy.FACTOR
                and original_matches(report, binding, now=now))

    def protocol(config, artifact, sources, bundle, binding, *, execute, qualification=None):
        # Exercise both real constructor boundaries before a lock, reservation or cloud call.
        for arm in ("control", "candidate"):
            contract = engine.StatusRefreshContract(artifact, arm, sources)
            engine.RefreshStages(execute, bundle, contract, "adr0151-" + uuid4().hex[:12])
        report = original_protocol(config, artifact, sources, bundle, binding,
                                   execute=execute, qualification=qualification)
        report.update(experiment_decision="ADR0157", factor=policy.FACTOR,
                      scope="Fixed2+2API, same images/1000ms cache/refresh-on/budgets; only consumer dedup0->1.60buyers/s300s each if authorized; no hourly capacity claim.")
        path = engine.ROOT / "tmp" / report["run"] / "comparison-summary.json"
        if path.exists():
            path.write_text(json.dumps(report, indent=2) + "\n")
        return report

    def prepare(output):
        result = original_prepare(output)
        result.update(kind="status_refresh_dedup_runner_preparation", experiment_decision="ADR0157",
                      factor=policy.FACTOR, ledger=LEDGER, authorization_id=AUTHORIZATION,
                      status="LOCAL_IMAGES_VERIFIED_FRESH_APPROVAL_AND_DRY_PENDING")
        output.resolve().write_text(json.dumps(result, indent=2) + "\n")
        return result

    engine.identity, engine.qualification_matches = identity, qualification_matches
    engine.protocol, engine.prepare = protocol, prepare
    return engine


def create_stager():
    engine = create_runner()
    spec = importlib.util.spec_from_file_location(
        "adr0157_private_stager", ROOT / "scripts/stage_status_refresh_images.py"
    )
    stager = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(stager)
    stager.STATE = engine.STATE
    stager.source_contract = policy.source_contract
    stager.StatusRefreshContract = policy.StatusRefreshDedupContract
    stager.binding_for, stager.validate_release = engine.binding_for, engine.validate_release
    stager.owned_stage, stager.run_lock = stager.stage, engine.RunLock

    def stage(config, artifact, output, password):
        stager.validate_config(config)
        sources = stager.source_contract()
        stager.StatusRefreshContract(artifact, "control", sources)
        stager.validate_release(json.loads(stager.STATE.read_text()),
                                stager.binding_for(config, artifact, sources), execute=False)
        lock = stager.run_lock("adr0151-" + uuid4().hex[:12])
        # Retain the lock on any ambiguous staging failure; do not replay or guess remote cleanup.
        result = stager.owned_stage(config, artifact, output, password)
        if result.get("pass") is not True or result.get("runtime_identities_unchanged") is not True:
            raise ValueError("Staging success and unchanged runtime proof required")
        result.update(experiment_decision="ADR0157", factor=policy.FACTOR)
        (output / "staging-summary.json").write_text(json.dumps(result, indent=2) + "\n")
        lock.release()
        return result

    stager.stage = stage
    return stager


if __name__ == "__main__":
    create_runner().main()

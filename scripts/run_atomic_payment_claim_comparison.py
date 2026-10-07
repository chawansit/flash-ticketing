"""ADR0193: matched callback-claim pair through the standing work envelope."""
import importlib.util
import json
from uuid import uuid4

import atomic_payment_claim_contract as policy
from qualify_two_host_deployment import ROOT
from run_async_confirmation_comparison import create_runner as shared_runner
from stage_status_refresh_images import new_stage_output

LEDGER = "bounded_atomic_payment_claim"
AUTHORIZATION = "adr0193-atomic-payment-claim-pair-2026-10-07"


def create_runner():
    engine = shared_runner(policy_module=policy, ledger=LEDGER, authorization=AUTHORIZATION,
                           decision="ADR0193", profile_name="atomic_payment_claim",
                           artifact_directory="partial-timeout-reclamation", patch_name="adr0163.patch",
                           runner_filename="run_atomic_payment_claim_comparison.py",
                           preparer_filename="prepare_atomic_payment_claim.py",
                           extra_identity=("prepare_partial_timeout_reclamation.py", "prepare_async_confirmation.py",
                                           "prepare_status_refresh_dedup.py", "slow_database_contract.py",
                                           "partial_timeout_contract.py", "async_confirmation_contract.py"))

    def stage_images(config, artifact, password, guard):
        # The existing source-bound reservation owns staging; no deployment or customer launch.
        spec = importlib.util.spec_from_file_location("adr0193_private_stager", ROOT / "scripts/stage_status_refresh_images.py")
        stager = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(stager)
        stager.STATE, stager.ROLES = engine.STATE, engine.ROLES
        stager.source_contract, stager.StatusRefreshContract = engine.source_contract, engine.StatusRefreshContract
        stager.binding_for, stager.validate_release = engine.binding_for, engine.validate_release
        stager.ENVELOPE_GUARD = guard
        guard.check()
        output = new_stage_output()
        lock = engine.RunLock("adr0151-" + uuid4().hex[:12])
        try:
            state = json.loads(engine.STATE.read_text())
            state[engine.LEDGER]["staging_evidence_directory"] = output.relative_to(ROOT).as_posix()
            engine.STATE.write_text(json.dumps(state,indent=2)+"\n")
            result = stager.stage(config,artifact,output,password)
            if result.get("pass") is not True or result.get("runtime_identities_unchanged") is not True:
                raise ValueError("Source-pinned staging and unchanged runtime proof required")
            lock.release()
            return result
        finally:
            password = None  # Ambiguous failure deliberately preserves the owned lock/evidence.

    engine.stage_images = stage_images
    return engine


if __name__ == "__main__":
    create_runner().main()

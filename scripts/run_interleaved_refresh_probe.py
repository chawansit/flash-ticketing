"""ADR0222 fresh, source-bound consumer correction through the proven paid runner."""
import importlib.util
import json
from uuid import uuid4

import interleaved_refresh_probe_contract as policy
from qualify_two_host_deployment import ROOT
from run_callback_routing_comparison import create_runner as routing_runner
from stage_status_refresh_images import new_stage_output

LEDGER = "bounded_interleaved_refresh_probe"
AUTHORIZATION = "adr0222-interleaved-refresh-84-probe-2026-10-08"


def create_runner(*, policy_module=policy, ledger=LEDGER, authorization=AUTHORIZATION,
                  decision="ADR0222", profile_name="interleaved_refresh_probe",
                  runner_filename="run_interleaved_refresh_probe.py", extra_identity=()):
    engine = routing_runner(policy_module=policy_module, ledger=ledger, authorization=authorization,
                            decision=decision, profile_name=profile_name,
                            runner_filename=runner_filename, arms=("candidate",),
                            extra_identity=("run_callback_routing_comparison.py", "callback_routing_contract.py",
                                            "shared_callback_placement_contract.py", "shared_callback_rate_probe_contract.py",
                                            "prepare_interleaved_refresh.py", *extra_identity))
    original_identity = engine.identity

    def identity():
        return {**original_identity(), **{
            "artifacts/interleaved-seat-refresh/" + name: engine.comparison.source_sha256(
                (ROOT / "artifacts/interleaved-seat-refresh" / name).read_bytes())
            for name in ("manifest.json", "adr0222.patch")}}

    def stage_images(config, artifact, password, guard):
        spec = importlib.util.spec_from_file_location("adr0222_private_stager", ROOT / "scripts/stage_status_refresh_images.py")
        stager = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(stager)
        stager.STATE, stager.ROLES = engine.STATE, engine.ROLES
        stager.STAGING_ARM = "candidate"
        stager.source_contract, stager.StatusRefreshContract = engine.source_contract, engine.StatusRefreshContract
        stager.binding_for, stager.validate_release = engine.binding_for, engine.validate_release
        stager.ENVELOPE_GUARD = guard
        guard.check()
        output = new_stage_output()
        lock = engine.RunLock("adr0151-" + uuid4().hex[:12])
        try:
            state = json.loads(engine.STATE.read_text())
            state[engine.LEDGER]["staging_evidence_directory"] = output.relative_to(ROOT).as_posix()
            engine.STATE.write_text(json.dumps(state, indent=2) + "\n")
            result = stager.stage(config, artifact, output, password)
            if result.get("pass") is not True or result.get("runtime_identities_unchanged") is not True:
                raise ValueError("Source-pinned staging and unchanged runtime proof required")
            lock.release()
            return result
        finally:
            password = None  # Preserve ownership lock/evidence on ambiguous failure.

    engine.identity, engine.stage_images = identity, stage_images
    return engine

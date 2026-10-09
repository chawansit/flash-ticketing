"""ADR0226 one frozen-generator correction through the proven protected runner."""
import json

import generator_completion_probe_contract as policy
from prepare_generator_completion import ARTIFACTS, PATH, corrected_bundle, corrected_source
from run_writer_write_pipeline_probe import create_runner as parent_runner

LEDGER = "bounded_generator_completion_probe"
AUTHORIZATION = "adr0226-generator-completion-84-probe-2026-10-08"


def create_runner():
    engine = parent_runner(policy_module=policy, ledger=LEDGER, authorization=AUTHORIZATION,
        decision="ADR0226", profile_name="generator_completion_probe",
        runner_filename="run_generator_completion_probe.py",
        extra_identity=("run_writer_write_pipeline_probe.py", "writer_write_pipeline_probe_contract.py",
                        "prepare_generator_completion.py"))
    original_identity, original_protocol = engine.identity, engine.protocol
    def identity():
        # Qualify the real frozen patch before scope allocation and cloud access.
        corrected_source(engine.comparison.frozen_bundle()[PATH])
        return {**original_identity(), **{
            "artifacts/generator-completion/" + name: engine.comparison.source_sha256((ARTIFACTS / name).read_bytes())
            for name in ("manifest.json", "adr0226.patch")}}
    def protocol(config, artifact, sources, bundle, binding, *, execute, qualification=None):
        candidate = corrected_bundle(bundle)
        report = original_protocol(config, artifact, sources, candidate, binding,
                                   execute=execute, qualification=qualification)
        report.update(generator_correction=policy.plan()["generator_manifest"],
            scope="Only completed-task cleanup changes in the exact frozen generator. ADR0225 backend images/settings, 84/s300s workload, 1+3 APIs, budgets and gates unchanged. No backend or hourly capacity claim.")
        path = policy.ROOT / "tmp" / report["run"] / "comparison-summary.json"
        if path.exists(): path.write_text(json.dumps(report, indent=2) + "\n")
        return report
    engine.identity, engine.protocol = identity, protocol
    return engine

"""ADR0225 writer-only factor through the proven protected paid runner."""
import writer_write_pipeline_probe_contract as policy
from run_orders_event_index_probe import create_runner as parent_runner

LEDGER = "bounded_writer_write_pipeline_probe"
AUTHORIZATION = "adr0225-writer-write-pipeline-84-probe-2026-10-08"


def create_runner():
    engine = parent_runner(policy_module=policy, ledger=LEDGER, authorization=AUTHORIZATION,
        decision="ADR0225", profile_name="writer_write_pipeline_probe",
        runner_filename="run_writer_write_pipeline_probe.py",
        extra_identity=("run_orders_event_index_probe.py", "orders_event_index_probe_contract.py",
                        "prepare_writer_write_pipeline.py"))
    original_identity = engine.identity

    def identity():
        return {**original_identity(), **{
            "artifacts/writer-write-pipeline/" + name: engine.comparison.source_sha256(
                (policy.ROOT / "artifacts/writer-write-pipeline" / name).read_bytes())
            for name in ("manifest.json", "adr0225.patch")}}

    engine.identity = identity
    return engine

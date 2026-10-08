"""ADR0226 exact generator bookkeeping factor; backend configuration unchanged."""
import copy
import json

import writer_write_pipeline_probe_contract as parent
from prepare_generator_completion import ARTIFACTS
from qualify_two_host_deployment import ROOT
from status_refresh_contract import digest

FACTOR = "completed_generator_slots_at_84_writer_pipeline"
PLAN = ROOT / "docs/capacity/flash-sale-opening/generator-completion-probe-plan-2026-10-08.json"
MIGRATION = parent.MIGRATION
ROUTES, PLACEMENTS = parent.ROUTES, parent.PLACEMENTS
GLOBAL_RECEIPT_AUDIT, receipt_drained = parent.GLOBAL_RECEIPT_AUDIT, parent.receipt_drained


def plan():
    expected = copy.deepcopy(parent.plan())
    for key in ("status", "scope"):
        expected.pop(key)
    evidence = "docs/capacity/flash-sale-opening/writer-write-pipeline-probe-2026-10-08.json"
    recovery = "docs/capacity/flash-sale-opening/writer-write-pipeline-recovery-2026-10-08.json"
    failed, recovered = (json.loads((ROOT / p).read_text()) for p in (evidence, recovery))
    if (failed.get("pass") is not False or failed.get("stage", {}).get("dispatched") != 25196
            or failed["stage"].get("generator_drops") != 4
            or recovered.get("dispatched_paid_tickets") != 25196
            or recovered.get("original_experiment_pass") is not False
            or not recovered.get("gates") or not all(recovered["gates"].values())):
        raise ValueError("Exact failed writer reference and independent recovery required")
    generator = json.loads((ARTIFACTS / "manifest.json").read_text())
    expected.update(decision="ADR0226", factor=FACTOR, baseline_evidence=evidence,
        baseline_sha256=digest(failed), baseline_recovery_evidence=recovery,
        baseline_recovery_sha256=digest(recovered), generator_manifest=generator)
    data = json.loads(PLAN.read_text())
    if set(data) != {*expected, "status", "scope"} or any(digest(data.get(k)) != digest(v) for k, v in expected.items()):
        raise ValueError("Exact generator-only correction probe required")
    return data


def source_contract():
    plan()
    return parent.source_contract()


class GeneratorCompletionProbeContract(parent.WriterWritePipelineProbeContract):
    def __init__(self, artifact, arm, sources):
        data = plan()
        if artifact != data["artifact_receipt"] or sources != data["expected_runtime_source_sha256"]:
            raise ValueError("Backend images and source maps must remain identical")
        super().__init__(artifact, arm, sources)

    def inventory_marker(self):
        return {**super().inventory_marker(), "decision": "ADR0226", "factor": FACTOR}


AsyncConfirmationContract = GeneratorCompletionProbeContract

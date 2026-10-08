"""ADR0225 writer-only transport factor over the retained ADR0224 reference."""
import copy
import json
import re

import orders_event_index_probe_contract as parent
from prepare_writer_write_pipeline import manifest, verify_source
from qualify_two_host_deployment import ROOT
from status_refresh_contract import digest

FACTOR = "writer_write_pipeline_at_84_shared_1_3"
PLAN = ROOT / "docs/capacity/flash-sale-opening/writer-write-pipeline-probe-plan-2026-10-08.json"
MIGRATION = parent.MIGRATION
ROUTES, PLACEMENTS = parent.ROUTES, parent.PLACEMENTS
GLOBAL_RECEIPT_AUDIT, receipt_drained = parent.GLOBAL_RECEIPT_AUDIT, parent.receipt_drained


def expected_plan():
    baseline = parent.plan()
    expected = copy.deepcopy(baseline)
    for key in ("status", "scope"):
        expected.pop(key)
    candidate = json.loads(PLAN.read_text())
    artifact, old = candidate.get("artifact_receipt", {}), baseline["artifact_receipt"]
    images = artifact.get("images", {})
    if (set(artifact) != set(old) or set(images) != set(old["images"])
            or artifact.get("parent_images") != old["parent_images"]
            or artifact.get("source_manifest_sha256") != old["source_manifest_sha256"]
            or any(images[r] != old["images"][r] for r in images if r != "reservation-writer")
            or images.get("reservation-writer") == old["images"]["reservation-writer"]
            or not re.fullmatch(r"sha256:[0-9a-f]{64}", images.get("reservation-writer", ""))):
        raise ValueError("Only the frozen reservation-writer image may change")
    source = candidate.get("writer_isolated_source_directory", "")
    verify_source(ROOT / source)
    evidence = "docs/capacity/flash-sale-opening/orders-event-index-probe-2026-10-08.json"
    recovery = "docs/capacity/flash-sale-opening/orders-event-index-recovery-2026-10-08.json"
    failed, recovered = (json.loads((ROOT / p).read_text()) for p in (evidence, recovery))
    if (failed.get("pass") is not False or failed.get("stage", {}).get("dispatched") != 24423
            or failed["stage"].get("generator_drops") != 777
            or recovered.get("dispatched_paid_tickets") != 24423
            or recovered.get("original_experiment_pass") is not False
            or not recovered.get("gates") or not all(recovered["gates"].values())):
        raise ValueError("Exact retained failed reference and independent recovery required")
    expected.update(decision="ADR0225", factor=FACTOR, artifact_receipt=artifact,
                    writer_isolated_source_directory=source,
                    writer_runtime_source_sha256=manifest()["runtime_source_sha256"],
                    writer_parent_image=old["images"]["reservation-writer"],
                    baseline_evidence=evidence, baseline_sha256=digest(failed),
                    baseline_recovery_evidence=recovery, baseline_recovery_sha256=digest(recovered))
    return expected


def plan():
    data, expected = json.loads(PLAN.read_text()), expected_plan()
    if set(data) != {*expected, "status", "scope"} or any(digest(data.get(k)) != digest(v) for k, v in expected.items()):
        raise ValueError("Exact registered writer-only transport probe required")
    return data


def source_contract():
    plan()
    return parent.source_contract()


class WriterWritePipelineProbeContract(parent.OrdersEventIndexProbeContract):
    index_verify_only = True

    def __init__(self, artifact, arm, sources):
        data = plan()
        if arm != "candidate" or artifact != data["artifact_receipt"] or sources != data["expected_runtime_source_sha256"]:
            raise ValueError("Exact writer image and source maps required")
        super().__init__(parent.plan()["artifact_receipt"], arm, sources)
        self.images["reservation-writer"] = artifact["images"]["reservation-writer"]
        self.writer_sources = copy.deepcopy(data["writer_runtime_source_sha256"])

    def source_map(self, role):
        if role == "reservation-writer":
            return copy.deepcopy(self.writer_sources)
        return super().source_map(role)

    def settings(self, role):
        result = super().settings(role)
        if role == "reservation-writer":
            result["RESERVATION_WRITE_PIPELINE"] = "1"
        return result

    def inventory_marker(self):
        return {**super().inventory_marker(), "decision": "ADR0225", "factor": FACTOR}


AsyncConfirmationContract = WriterWritePipelineProbeContract

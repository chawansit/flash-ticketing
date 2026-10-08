"""ADR0222 exact consumer-only correction probe against the failed 84/s reference."""
import copy
import json
import re

import shared_callback_rate_probe_contract as parent
from prepare_interleaved_refresh import manifest, verify_source
from qualify_two_host_deployment import ROOT
from status_refresh_contract import REVISION, digest

FACTOR = "consumer_interleaved_refresh_at_84_shared_1_3"
PLAN = ROOT / "docs/capacity/flash-sale-opening/interleaved-refresh-probe-plan-2026-10-08.json"
ROUTES = parent.ROUTES
PLACEMENTS = parent.PLACEMENTS
GLOBAL_RECEIPT_AUDIT = parent.GLOBAL_RECEIPT_AUDIT
receipt_drained = parent.receipt_drained


def expected_plan():
    baseline = parent.plan()
    expected = copy.deepcopy(baseline)
    for key in ("status", "scope"):
        expected.pop(key)
    candidate = json.loads(PLAN.read_text())
    artifact = candidate.get("artifact_receipt", {})
    old = baseline["artifact_receipt"]
    images = artifact.get("images", {})
    if (set(artifact) != set(old) or set(images) != set(old["images"])
            or artifact.get("parent_images") != old["parent_images"]
            or artifact.get("source_manifest_sha256") != old["source_manifest_sha256"]
            or any(images[r] != old["images"][r] for r in images if r != "consumer")
            or images.get("consumer") == old["images"]["consumer"]
            or not re.fullmatch(r"sha256:[0-9a-f]{64}", images.get("consumer", ""))):
        raise ValueError("Only the frozen consumer image may change")
    source = candidate.get("isolated_source_directory", "")
    verify_source(ROOT / source)
    evidence = "docs/capacity/flash-sale-opening/shared-callback-rate-probe-2026-10-08.json"
    recovery = "docs/capacity/flash-sale-opening/shared-callback-rate-probe-recovery-2026-10-08.json"
    failed = json.loads((ROOT / evidence).read_text())
    recovered = json.loads((ROOT / recovery).read_text())
    if failed.get("pass") is not False or not all(recovered.get("gates", {}).get(k) is True for k in ("dispatched_cohort_durable", "post_ttl_relationships", "zero_double_booking", "full_queue_drain", "exact_runtime_restored", "generator_idle", "owned_cleanup", "original_failure_preserved")):
        raise ValueError("Retained failed baseline and independent recovery required")
    expected.update(decision="ADR0222", factor=FACTOR, artifact_receipt=artifact,
                    isolated_source_directory=source, consumer_runtime_source_sha256=manifest()["runtime_source_sha256"],
                    consumer_parent_image=old["images"]["consumer"], baseline_evidence=evidence,
                    baseline_sha256=digest(failed), baseline_recovery_evidence=recovery,
                    baseline_recovery_sha256=digest(recovered))
    return expected


def plan():
    expected = expected_plan()
    data = json.loads(PLAN.read_text())
    if set(data) != {*expected, "status", "scope"} or any(digest(data.get(k)) != digest(v) for k, v in expected.items()):
        raise ValueError("Exact registered consumer correction probe required")
    return data


def source_contract():
    plan()
    return parent.source_contract()


class InterleavedRefreshProbeContract(parent.SharedCallbackRateProbeContract):
    def __init__(self, artifact, arm, sources):
        data = plan()
        if arm != "candidate" or artifact != data["artifact_receipt"] or sources != data["expected_runtime_source_sha256"]:
            raise ValueError("Exact consumer correction images and source maps required")
        super().__init__(parent.plan()["artifact_receipt"], arm, sources)
        self.images["consumer"] = artifact["images"]["consumer"]
        self.consumer_sources = copy.deepcopy(data["consumer_runtime_source_sha256"])

    def source_map(self, role):
        if role == "consumer":
            return copy.deepcopy(self.consumer_sources)
        return super().source_map(role)

    def image_manifest(self, role):
        return digest({"base_revision": REVISION, "runtime_source_sha256": self.source_map(role)})

    def inventory_marker(self):
        return {**super().inventory_marker(), "decision": "ADR0222", "factor": FACTOR}

    def verify_inventory(self, data):
        super().verify_inventory(data)
        for row in data["worker_sources"]:
            expected = self.source_map(row["role"])
            actual = row.get("source_identity", {}).get("resolved_imports", {})
            if set(actual) != set(expected) or any(actual[p].get("sha256") != h for p, h in expected.items()):
                raise ValueError("Worker imports differ from the exact per-role source map")


AsyncConfirmationContract = InterleavedRefreshProbeContract

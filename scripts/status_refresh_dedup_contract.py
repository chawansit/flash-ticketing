"""ADR0157 fixed deduplication factor; source verification performs no cloud calls."""

import json

import prepare_status_refresh_dedup as export
from observe_two_host_pipeline import verify_dedup_factor_evidence
from status_refresh_contract import ROOT, StatusRefreshContract

PLAN = ROOT / "docs/capacity/flash-sale-opening/order-status-refresh-dedup-comparison-plan-2026-10-05.json"
FACTOR = "consumer_ORDER_STATUS_EVENT_REFRESH_DEDUP"


def plan():
    data = json.loads(PLAN.read_text())
    manifest = export.manifest()
    if (data["decision"] != "ADR0157" or data["base_revision"] != manifest["base_revision"]
            or data["expected_runtime_source_sha256"] != manifest["runtime_source_sha256"]
            or data["factor"] != FACTOR or data["control"] != "0" or data["candidate"] != "1"
            or data["common"] != {"consumer_event_refresh": "1", "cache_age_ms": 1000,
                                   "primary_apis": 2, "secondary_apis": 2,
                                   "buyer_journeys_per_second": 60, "duration_seconds": 300}):
        raise ValueError("Pinned deduplication-only plan required")
    return data


def source_contract():
    data = plan()
    source = (ROOT / data["isolated_source_directory"]).resolve()
    if not source.is_relative_to((ROOT / "tmp").resolve()):
        raise ValueError("Owned isolated source required")
    receipt = json.loads((source.parent / "dedup-source-receipt.json").read_text())
    export.base.verify_tree(source, receipt["source_file_sha256"])
    expected = data["expected_runtime_source_sha256"]
    actual = {p.relative_to(source).as_posix() for p in (source / "src/ticketing").rglob("*.py")}
    if (actual != set(expected)
            or any(export.base.sha((source / p).read_bytes()) != h for p, h in expected.items())
            or receipt["source_manifest_sha256"] != data["artifact_receipt"]["source_manifest_sha256"]):
        raise ValueError("Pinned isolated runtime differs")
    return expected


class StatusRefreshDedupContract(StatusRefreshContract):
    def __init__(self, artifact, arm, sources):
        data = plan()
        if artifact != data["artifact_receipt"] or sources != data["expected_runtime_source_sha256"]:
            raise ValueError("Exact prepared deduplication images and sources required")
        super().__init__(artifact, arm, sources)

    def settings(self, role):
        return {"ORDER_STATUS_CACHE_MS": "1000" if role in {"api", "consumer"} else "0",
                "ORDER_STATUS_EVENT_REFRESH": "1" if role == "consumer" else "0",
                "ORDER_STATUS_EVENT_REFRESH_DEDUP": self.flag if role == "consumer" else "0"}

    def inventory_marker(self):
        return {"decision": "ADR0157", "cache_age_ms": 1000, "arm": self.arm,
                "api_image_id": self.images["api"], "consumer_event_refresh": "1",
                "consumer_event_refresh_dedup": self.flag}

    def verify_worker_settings(self, role, environment):
        if any(environment.get(k) != v for k, v in self.settings(role).items()):
            raise ValueError("Explicit worker settings required; missing flags cannot be inferred")

    def verify_inventory(self, data):
        super().verify_inventory(data)
        if data.get("status_refresh_contract") != self.inventory_marker():
            raise ValueError("Recorded deduplication factor differs")
        verify_dedup_factor_evidence(data)

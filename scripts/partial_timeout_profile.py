"""ADR0163 offline admission profile. Cloud runner integration is not implemented."""

import copy
import json
from pathlib import Path
from typing import ClassVar

import prepare_partial_timeout_reclamation as export
from async_confirmation_contract import AsyncConfirmationContract
from status_refresh_contract import StatusRefreshContract

FACTOR = "api_API_PARTIAL_TIMEOUT_RECLAIM"
COMMON = {"primary_apis": 2, "secondary_apis": 2, "api_pool": 4, "api_waiters": 12,
          "payment_pool": 2, "pool_wait_ms": 500, "async_intake": "0", "callback_reserve": "0",
          "cache_age_ms": 1000, "consumer_event_refresh": "1", "consumer_dedup": "0",
          "poll_ms": 500, "simulator_pool": 10, "confirmation_pool": 2,
          "buyer_journeys_per_second": 60, "duration_seconds": 300}


def source_contract(source):
    source = Path(source).resolve()
    if not source.is_relative_to((export.base.ROOT / "tmp").resolve()):
        raise ValueError("Owned isolated source under tmp required")
    receipt = json.loads((source.parent / "admission-source-receipt.json").read_text())
    manifest = export.manifest()
    if (receipt.get("decision") != "ADR0163" or receipt.get("patch_sha256") != manifest["patch_sha256"]
            or any(receipt.get(k) is not True for k in (
                "financial_transaction_ast_unchanged", "qualified_method_ast_parity", "callback_reserve_wiring_excluded"))):
        raise ValueError("Exact qualified admission source receipt required")
    expected = export.expected_source_map()
    if receipt.get("source_file_sha256") != expected:
        raise ValueError("Full source/dependency receipt differs from immutable inputs")
    export.base.verify_tree(source, expected)
    # Do not trust the receipt's runtime map: bind every file to the committed overlay.
    for path, expected in manifest["runtime_source_sha256"].items():
        if export.base.sha((source / path).read_bytes()) != expected:
            raise ValueError("Pinned runtime source differs")
    expected_digest = export.base.digest({"base_revision": export.base.REVISION,
                                         "runtime_source_sha256": manifest["runtime_source_sha256"]})
    if receipt.get("source_manifest_sha256") != expected_digest:
        raise ValueError("Complete runtime digest differs")
    return manifest["runtime_source_sha256"]


class PartialTimeoutProfile(StatusRefreshContract):
    background: ClassVar[dict] = copy.deepcopy(AsyncConfirmationContract.background)
    roles = ("api", *background)

    def __init__(self, artifact, arm, sources):
        if sources != export.manifest()["runtime_source_sha256"]:
            raise ValueError("Exact admission runtime required")
        super().__init__(artifact, arm, sources)
        if self.images["confirmation"] != self.images["simulator"] or self.parents["confirmation"] != self.parents["simulator"]:
            raise ValueError("Idle confirmation worker must share simulator image")
        self.api_settings.update(self.settings("api"))

    def settings(self, role):
        if role not in self.roles:
            raise ValueError("Unknown profile role")
        return {"ORDER_STATUS_CACHE_MS": "1000" if role in {"api", "consumer"} else "0",
                "ORDER_STATUS_EVENT_REFRESH": "1" if role == "consumer" else "0",
                "ORDER_STATUS_EVENT_REFRESH_DEDUP": "0", "ORDER_STATUS_POLL_MS": "500",
                "PAYMENT_CALLBACK_PROVIDER": "simulator", "CONFIRMATION_MAX_PENDING": "10000",
                "CONFIRMATION_LEASE_SECONDS": "30", "CONFIRMATION_MAX_ATTEMPTS": "8",
                "CONFIRMATION_RETRY_MS": "100",
                "PAYMENT_CONFIRMATION_ASYNC": "1" if role == "confirmation" else "0",
                "API_PARTIAL_TIMEOUT_RECLAIM": self.flag if role == "api" else "0",
                "API_CALLBACK_ACQUISITION_RESERVE": "0",
                **({"DB_POOL_MAX": "10"} if role == "simulator" else {}),
                **({"DB_POOL_MAX": "2", "CONFIRMATION_CONCURRENCY": "2", "SIMULATOR_CONCURRENCY": "1"}
                   if role == "confirmation" else {})}

    def primary_model(self, model):
        result = copy.deepcopy(model)
        confirmation = copy.deepcopy(result["services"]["simulator"])
        confirmation["command"] = ["python", "-m", "ticketing.workers", "confirmation"]
        confirmation.pop("profiles", None)
        confirmation.pop("ports", None)
        result["services"]["confirmation"] = confirmation
        return super().primary_model(result)

    def inventory_marker(self):
        return {"decision": "ADR0163", "arm": self.arm, "factor": FACTOR,
                "partial_timeout_reclaim": self.flag, "async_intake": "0",
                "cache_age_ms": 1000, "poll_ms": 500, "api_image_id": self.images["api"]}

    def verify_worker_settings(self, role, environment):
        if any(environment.get(k) != v for k, v in self.settings(role).items()):
            raise ValueError("Explicit common background settings required")

    def verify_inventory(self, data):
        super().verify_inventory(data)
        if data.get("status_refresh_contract") != self.inventory_marker():
            raise ValueError("Admission factor marker differs")
        apis = data.get("apis", [])
        if len(apis) != 4 or any(any(a.get("settings", {}).get(k) != v for k, v in self.api_settings.items()) for a in apis):
            raise ValueError("Four exact-budget APIs and sole admission factor required")


def prepare_profile(source, artifact):
    sources = source_contract(source)
    off, on = [PartialTimeoutProfile(artifact, arm, sources) for arm in ("control", "candidate")]
    return {"decision": "ADR0163", "factor": FACTOR, "common": copy.deepcopy(COMMON),
            "source_directory": Path(source).resolve().relative_to(export.base.ROOT).as_posix(),
            "expected_runtime_source_sha256": sources, "artifact_receipt": artifact,
            "arm_settings": {c.arm: {r: c.settings(r) for r in c.roles} for c in (off, on)},
            "status": "OFFLINE_PROFILE_VERIFIED_CLOUD_RUNNER_PENDING",
            "cloud_calls": 0, "cloud_execution_implemented": False,
            "fresh_scope_required": True, "prior_scope_reopened": False,
            "mandatory_future_gates": ["100_way_atomic_hold", "authorization_and_idempotent_replay",
                                       "payment_durability", "post_TTL", "zero_double_booking",
                                       "complete_global_queues_and_Kafka_drain", "restoration"]}

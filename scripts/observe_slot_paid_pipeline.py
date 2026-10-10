"""ADR0241 observer entry: retain slot snapshots from the same admitted metrics read."""
import copy
import hashlib
import json

import observe_cce_paid_pipeline as native
import slot_failure_evidence as slots


def install(module, value, **kwargs):
    starts = native.install(module, value, **kwargs)
    slots.install(module)
    return starts


def correction_inventory(inventory, approved_digest):
    """Verify actual twelve-job evidence before projecting the legacy structural view."""
    marker = inventory.get("status_refresh_contract", {})
    if marker.get("shared_image_decision") == "ADR0259":
        from cce_worker_identity import digest, verify_ecs_shared_inventory
        verify_ecs_shared_inventory(inventory, approved_digest)
        legacy = copy.deepcopy(inventory)
        for key in ("shared_image_decision", "shared_image_source_sha256", "shared_image_configuration_digest", "shared_image_receipt_sha256",
                    "correction_decision", "correction_factor", "simulator_concurrency", "simulator_database_pool_max", "simulator_image_id", "simulator_receipt_sha256"):
            legacy["status_refresh_contract"].pop(key)
        legacy["background"]["simulator"]["concurrency"] = 8
        return legacy, digest(legacy)
    if "correction_decision" not in marker:
        return inventory, approved_digest
    def digest(value):
        return hashlib.sha256(json.dumps(value, sort_keys=True, separators=(",", ":")).encode()).hexdigest()
    expected = {"correction_decision": "ADR0251", "correction_factor": "callback_dispatch_concurrency",
                "simulator_concurrency": 12, "simulator_database_pool_max": 10,
                "simulator_image_id": "sha256:a6cdfd34312cabf61c276233bb53f741a70c5567a36c8bc3ec11844c67a06107",
                "simulator_receipt_sha256": "aede238a4a8f56a565fac1003633d6df8947f42d9a9aa8704240ac5f1857bc4b"}
    simulators = [w for w in inventory.get("worker_sources", []) if w.get("role") == "simulator"]
    if (digest(inventory) != approved_digest or marker.get("decision") != "ADR0226"
            or inventory.get("arm") != "candidate" or any(marker.get(k) != v for k, v in expected.items())
            or inventory.get("background", {}).get("simulator") != {
                "replicas": 1, "concurrency": 12, "dispatch_mode": "refill", "pool_per_replica": 10}
            or len(simulators) != 1):
        raise ValueError("Exact qualified simulator correction inventory required")
    worker = simulators[0]
    settings = worker.get("settings", {})
    source = worker.get("source_identity", {})
    if (worker.get("image_id") != expected["simulator_image_id"]
            or source.get("source_hashes_match") is not True
            or source.get("resolved_imports", {}).get("src/ticketing/config.py", {}).get("sha256")
                != "20d35f8e00aed40c60dcfa5482090e0923dd34db35b9b3e6df0b8297b1c9966c"
            or any(settings.get(k) != v for k, v in {
                "SIMULATOR_CONCURRENCY": "12", "SIMULATOR_DISPATCH_MODE": "refill", "DB_POOL_MAX": "10"}.items())):
        raise ValueError("Simulator correction runtime differs")
    legacy = copy.deepcopy(inventory)
    for k in expected:
        legacy["status_refresh_contract"].pop(k)
    legacy["background"]["simulator"]["concurrency"] = 8
    # Only historical structural assertions see this copy. Retained inventory is unchanged.
    return legacy, digest(legacy)


def main(argv=None):
    # Delegate the proven receipt, worker, diagnostic and cleanup argument handling.
    original = native.install
    original_inventory = native.observer.install_adapter

    def inventory_adapter(module, inventory, **kwargs):
        view, approved = correction_inventory(inventory, kwargs.get("approved_inventory_sha256"))
        return original_inventory(module, view, **{**kwargs, "approved_inventory_sha256": approved})

    def adapter(module, value, **kwargs):
        starts = original(module, value, **kwargs)
        slots.install(module)
        return starts

    native.install = adapter
    native.observer.install_adapter = inventory_adapter
    try:
        native.main(argv)
    finally:
        native.install = original
        native.observer.install_adapter = original_inventory


if __name__ == "__main__":
    main()

"""ADR0266 standalone immutable worker and projection evidence."""
import copy
import hashlib
import json

SOURCE = "b90880f171b957a05d1dd26649357cc33f9e5bcd9fdef15cf2258097dc1b32da"
IMAGE_ID = "sha256:d9d7cb17433782ca863d8b93ef987a670708d4f6f9719fdecc2c4c21125d23fe"
CONFIG = "sha256:1c3abed1d05dede9dbc5c44f553e4c5c71a827d41224ec773a886f6cbd6267f8"
RECEIPT_SHA256 = "86e7655ecdd8496a7f95af5b8f805bbf351779344b0103168cbd22c61225b80a"
RECEIPT = "docs/capacity/cce/event-lane-fetch-image-2026-10-10.json"
COUNTS = {"consumer": 6, "projection-consumer": 1, "reservation-writer": 3,
          "publisher": 1, "maintenance": 1, "reconciler": 1, "simulator": 1, "confirmation": 1}


def digest(value):
    return hashlib.sha256(json.dumps(value, sort_keys=True, separators=(",", ":"), allow_nan=False).encode()).hexdigest()


def projection_summary_pass(summary):
    return (summary.get("samples", 0) >= 5 and summary.get("sample_errors") == 0
            and summary.get("members_min") == summary.get("members_max") == 1
            and summary.get("ownership_observed") is True
            and summary.get("max_partitions_per_member") == 6
            and summary.get("last_member_partition_groups") == [list(range(6))]
            and summary.get("last_total_lag") == 0)


def verify_inventory(inventory, approved_digest):
    marker = inventory.get("status_refresh_contract", {})
    wanted = {"event_lane_decision": "ADR0266", "shared_image_decision": "ADR0267",
              "shared_image_source_sha256": SOURCE, "shared_image_configuration_digest": CONFIG,
              "shared_image_receipt_sha256": RECEIPT_SHA256, "simulator_concurrency": 16,
              "event_consumer_pool_budget": 48}
    if digest(inventory) != approved_digest or any(marker.get(k) != v for k, v in wanted.items()):
        raise ValueError("Exactly sealed event-lane image/budget required")
    apis = inventory.get("apis", [])
    if len(apis) != 4 or any(
            api.get("settings", {}).get(flag) != "0"
            for api in apis for flag in ("EVENT_CONSUMER_SEPARATION", "RESERVATION_WRITE_PIPELINE")):
        raise ValueError("Four APIs with explicit disabled event-lane flags required")
    counts, ids = dict.fromkeys(COUNTS, 0), set()
    for row in inventory.get("worker_sources", []):
        role, proof, settings = row.get("role"), row.get("source_identity", {}), row.get("settings", {})
        imports = proof.get("resolved_imports", {})
        sources = {k: v.get("sha256") for k, v in imports.items()}
        if (role not in counts or row.get("container_id") in ids or row.get("image_id") != IMAGE_ID
                or len(sources) != 22 or hashlib.sha256(json.dumps(sources, sort_keys=True).encode()).hexdigest() != SOURCE
                or any(proof.get(k) is not True for k in ("source_hashes_match", "app_source_hashes_match", "import_source_hashes_match", "import_code_matches_source"))
                or any(v.get("code_matches_source") is not True for v in imports.values())):
            raise ValueError("Every actual event-lane worker source/image must match")
        want = {"EVENT_CONSUMER_SEPARATION": "1" if role in {"consumer", "projection-consumer"} else "0",
                "RESERVATION_WRITE_PIPELINE": "1" if role == "reservation-writer" else "0"}
        if role in {"consumer", "projection-consumer"}:
            want["DB_POOL_MAX"] = "7" if role == "consumer" else "6"
        if role == "simulator":
            want.update(SIMULATOR_CONCURRENCY="16", SIMULATOR_DISPATCH_MODE="refill", DB_POOL_MAX="10")
        if any(settings.get(k) != v for k, v in want.items()):
            raise ValueError("Explicit event-lane settings or pool allocation differs")
        counts[role] += 1
        ids.add(row["container_id"])
    bg = inventory.get("background", {})
    if (counts != COUNTS or bg.get("consumer") != {"replicas": 6, "pool_per_replica": 7}
            or bg.get("projection-consumer") != {"replicas": 1, "pool_per_replica": 6}
            or bg.get("simulator") != {"replicas": 1, "concurrency": 16, "dispatch_mode": "refill", "pool_per_replica": 10}):
        raise ValueError("Complete event-lane role and local connection budget required")
    return True


def historical_view(inventory, approved_digest):
    verify_inventory(inventory, approved_digest)
    legacy = copy.deepcopy(inventory)
    marker = legacy["status_refresh_contract"]
    for key in tuple(marker):
        if key.startswith(("shared_image_", "event_", "simulator_")) or key in {"dispatch_correction_decision", "correction_decision", "correction_factor"}:
            marker.pop(key)
    for api in legacy["apis"]:
        # Remove only verified historical-equivalent flags from the copied view.
        api["settings"].pop("EVENT_CONSUMER_SEPARATION")
        api["settings"].pop("RESERVATION_WRITE_PIPELINE")
    legacy["background"].pop("projection-consumer")
    legacy["background"]["consumer"]["pool_per_replica"] = 8
    legacy["background"]["simulator"]["concurrency"] = 8
    legacy["worker_sources"] = [row for row in legacy["worker_sources"] if row["role"] != "projection-consumer"]
    for row in legacy["worker_sources"]:
        row["settings"].pop("EVENT_CONSUMER_SEPARATION", None)
        if row["role"] == "consumer":
            row["settings"]["DB_POOL_MAX"] = "8"
    return legacy, digest(legacy)

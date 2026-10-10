"""ADR0259 native worker identity/progress evidence; independent of Docker inventory."""
import hashlib
import ipaddress
import json
import math
import re

from cce_shared_worker_image_identity import CONFIG, ECS_IMAGE_ID, MANIFEST, SOURCE

COUNTS = {"consumer": 6, "reservation-writer": 3, "publisher": 1, "maintenance": 1, "reconciler": 1, "simulator": 1}
RESOURCES = {k: {"cpu": "250m", "memory": "512Mi"} for k in ("requests", "limits")}


def specification(decision="ADR0259"):
    if decision == "ADR0259":
        return {"decision": decision, "counts": dict(COUNTS), "manifest": MANIFEST,
                "image_id": ECS_IMAGE_ID, "source": SOURCE, "resources": RESOURCES}
    if decision != "ADR0271":
        raise ValueError("Unknown native worker placement decision")
    import cce_event_lane_identity as pins
    return {"decision": decision, "counts": {k: v for k, v in pins.COUNTS.items() if k != "confirmation"},
            "manifest": pins.IMAGE_ID, "image_id": pins.IMAGE_ID, "source": pins.SOURCE, "resources": RESOURCES}


def digest(value):
    return hashlib.sha256(json.dumps(value, sort_keys=True, separators=(",", ":"), allow_nan=False).encode()).hexdigest()


def verify_sources(sources, decision="ADR0259"):
    if (not isinstance(sources, dict) or len(sources) != 22
            or any(not re.fullmatch(r"src/ticketing/[a-z_/]+\.py", k) or not re.fullmatch(r"[0-9a-f]{64}", v) for k, v in sources.items())
            or hashlib.sha256(json.dumps(sources, sort_keys=True).encode()).hexdigest() != specification(decision)["source"]):
        raise ValueError("Exact shared 22-module source identity required")
    return True


def validate_bundle(value):
    selected = specification(value.get("decision"))
    counts, manifest = selected["counts"], selected["manifest"]
    size = sum(counts.values())
    if (set(value) != {"decision", "run", "sources", "manifest_digest", "resources", "receipts"}
            or not re.fullmatch(r"adr0151-[0-9a-f]{12}", value["run"])
            or value["manifest_digest"] != manifest or value["resources"] != RESOURCES):
        raise ValueError("Exact native worker admission bundle required")
    verify_sources(value["sources"], value["decision"])
    expected = {f"{role}-{i}" for role, count in counts.items() for i in range(count)}
    rows = value["receipts"]
    if (len(rows) != size or {r["pod_name"] for r in rows} != expected
            or any(len({r[k] for r in rows}) != size for k in ("pod_uid", "private_ipv4"))):
        raise ValueError("Exact unique worker pod identities required")
    for row in rows:
        role = row["role"]
        addr = ipaddress.IPv4Address(row["private_ipv4"])
        proof = row["startup_proof"]
        started = row["process_start_time_seconds"]
        if (role not in counts or row["pod_name"] not in {f"{role}-{i}" for i in range(counts[role])}
                or not any(addr in ipaddress.IPv4Network(n) for n in ("10.0.0.0/8", "172.16.0.0/12", "192.168.0.0/16"))
                or row["image_id"].split("@")[-1] != manifest or row["resources"] != RESOURCES
                or proof.get("run") != value["run"] or proof.get("pod_uid") != row["pod_uid"]
                or proof.get("sources") != value["sources"]
                or not re.fullmatch(r"[0-9a-f]{64}", proof.get("environment_sha256", ""))
                or proof.get("command_sha256") != digest(["python", "-m", "ticketing.workers", role])
                or type(started) not in (int, float) or not math.isfinite(started) or started <= 0):
            raise ValueError("Native worker runtime/command identity differs")
    return {r["private_ipv4"]: r["process_start_time_seconds"] for r in rows}


def verify_ecs_shared_inventory(inventory, approved_digest):
    """Validate actual sources before adapting historical structural assertions."""
    marker = inventory.get("status_refresh_contract", {})
    if digest(inventory) != approved_digest or marker.get("shared_image_decision") != "ADR0259" or marker.get("shared_image_source_sha256") != SOURCE or marker.get("shared_image_configuration_digest") != CONFIG:
        raise ValueError("Exactly sealed shared-image inventory required")
    correction = marker.get("dispatch_correction_decision")
    if correction not in (None, "ADR0263"):
        raise ValueError("Unknown shared dispatch correction")
    concurrency = 16 if correction == "ADR0263" else 12
    if marker.get("simulator_concurrency") != concurrency:
        raise ValueError("Sealed simulator concurrency marker differs")
    rows = inventory.get("worker_sources", [])
    counts = {role: 0 for role in {**COUNTS, "confirmation": 1}}
    ids = set()
    for row in rows:
        role = row.get("role")
        proof = row.get("source_identity", {})
        imports = proof.get("resolved_imports", {})
        if (role not in counts or row.get("container_id") in ids or row.get("image_id") != ECS_IMAGE_ID
                or any(proof.get(k) is not True for k in ("source_hashes_match", "app_source_hashes_match", "import_source_hashes_match", "import_code_matches_source"))
                or any(v.get("code_matches_source") is not True for v in imports.values())):
            raise ValueError("Shared worker source/image proof differs")
        verify_sources({k: v.get("sha256") for k, v in imports.items()})
        settings = row.get("settings", {})
        if settings.get("RESERVATION_WRITE_PIPELINE", "0") != ("1" if role == "reservation-writer" else "0"):
            raise ValueError("Writer-only pipeline setting differs")
        if role == "simulator" and any(settings.get(k) != v for k, v in {"SIMULATOR_CONCURRENCY": str(concurrency), "SIMULATOR_DISPATCH_MODE": "refill", "DB_POOL_MAX": "10"}.items()):
            raise ValueError("Simulator workload/pool changed")
        counts[role] += 1
        ids.add(row["container_id"])
    if counts != {**COUNTS, "confirmation": 1} or inventory.get("background", {}).get("simulator") != {"replicas": 1, "concurrency": concurrency, "dispatch_mode": "refill", "pool_per_replica": 10}:
        raise ValueError("Complete shared worker placement required")
    return True

"""Sealed ADR0254 overlay; historical no-retry workload remains immutable."""

import hashlib
import json

import work_envelope as policy

MANIFEST = "artifacts/customer-recovery/manifest.json"
MANIFEST_SHA256 = "2349e1b19849c20e57ed26203076635166a5fb7722d6c8202939fe9157f18331"
COORDINATOR = "artifacts/customer-recovery/coordinator.py"
OVERLAYS = {
    "scripts/checkout_journey_probe.py", "scripts/paid_ticket_load_generator.py",
    "scripts/paid_ticket_sharded_generator.py", "scripts/customer_recovery_client.py", "scripts/paid_fixture_layout.py",
}


def raw(relative):
    path = policy.ROOT / relative
    if path.is_symlink():
        raise ValueError("Recovery source symlink forbidden")
    return path.read_bytes().replace(b"\r\n", b"\n")


def sha(value):
    return hashlib.sha256(value).hexdigest()


def qualify(parent):
    content = raw(MANIFEST)
    if sha(content) != MANIFEST_SHA256:
        raise ValueError("Sealed recovery workload receipt required")
    data = json.loads(content)
    if (data["decision"] != "ADR0254" or data["max_attempts"] != 3
            or (data["rate"], data["seconds"], data["completion_deadline"]) != (84, 300, 420)
            or len(parent) != 78 or {n: sha(v) for n, v in parent.items()} != data["parent_files"]
            or set(data["overlays"]) != OVERLAYS):
        raise ValueError("Exact paid parent and recovery overlay required")
    changes = {name: raw(name) for name in OVERLAYS}
    if {n: sha(v) for n, v in changes.items()} != data["overlays"]:
        raise ValueError("Recovery workload source drift")
    coordinator = raw(COORDINATOR)
    pinned = ('FROZEN_SHA256 = "' + sha(changes["scripts/paid_ticket_sharded_generator.py"]) + '"').encode()
    if sha(coordinator) != data["coordinator_sha256"] or coordinator.count(pinned) != 1:
        raise ValueError("Recovery coordinator identity differs")
    bundle = {**parent, **changes}
    if len(bundle) != 80:
        raise ValueError("Exact 80-file recovery workload required")
    return bundle, coordinator


def enabled(goal):
    return goal is not None and goal.get("extension_decision") in {"ADR0255", "ADR0256", "ADR0259", "ADR0263", "ADR0266", "ADR0271", "ADR0277"}

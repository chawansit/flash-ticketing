"""ADR0278 explicit evidence-retention overlay; sealed historical recovery is unchanged."""
import hashlib
import json

import work_envelope as policy

MANIFEST = "artifacts/customer-timeout/manifest.json"
MANIFEST_SHA256 = "f9faa5f61768eae1db14c71b5d012fa01cc1aabf3489acc4ac0d690f1ef1401d"


def selected(goal):
    value = goal.get("timeout_retention_decision") if goal else None
    if value is not None and value != "ADR0278":
        raise ValueError("Unknown timeout-retention decision")
    return value == "ADR0278"


def qualify(parent, coordinator):
    receipt = (policy.ROOT / MANIFEST).read_bytes()
    if hashlib.sha256(receipt).hexdigest() != MANIFEST_SHA256:
        raise ValueError("Sealed timeout-retention receipt required")
    data = json.loads(receipt)
    if data["decision"] != "ADR0278" or len(parent) != 80:
        raise ValueError("Exact recovery parent required")
    from customer_recovery_bundle import MANIFEST_SHA256 as historical
    if data["parent_recovery_manifest_sha256"] != historical:
        raise ValueError("Timeout parent identity differs")
    changes = {}
    for name, expected in data["files"].items():
        path = policy.ROOT / name
        if path.is_symlink() or not path.resolve().is_relative_to(policy.ROOT.resolve()):
            raise ValueError("Canonical timeout source required")
        content = path.read_bytes()
        if hashlib.sha256(content).hexdigest() != expected:
            raise ValueError("Timeout-retention source drift")
        changes[path.name] = content
    from customer_recovery_bundle import COORDINATOR, raw
    if coordinator != raw(COORDINATOR):
        raise ValueError("Exact historical coordinator required")
    pinned = ('FROZEN_SHA256 = "' + hashlib.sha256(changes["paid_ticket_sharded_generator.py"]).hexdigest() + '"').encode()
    if changes["coordinator.py"].count(pinned) != 1:
        raise ValueError("Timeout coordinator identity differs")
    return {**parent, **{"scripts/" + name: value for name, value in changes.items()
                         if name != "coordinator.py"}}, changes["coordinator.py"]


def issuance_program(events, start):
    import math
    from uuid import UUID
    if (len(events) != 84 or len(set(events)) != 84 or any(str(UUID(v)) != v for v in events)
            or type(start) not in (int, float) or not math.isfinite(start) or start <= 0):
        raise ValueError("Exact short cohort and offered start required")
    from cce_hourly_window import QUERY
    query = QUERY.replace("3598", "298").replace("3600", "300").replace("300000", "25200").replace("302400", "25200")
    return "events=" + repr(events) + "\nstart=" + repr(start) + "\n" + query

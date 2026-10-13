"""ADR0311 sealed child; historical recovery/timeout sources remain immutable."""
import hashlib
import json

import work_envelope as policy

MANIFEST = "artifacts/customer-gateway-read-recovery/manifest.json"
MANIFEST_SHA256 = "0b5b91aea11c7cbec6271832af9aa18e30f14b71f5efb67656449d19e8015792"
CLIENT = "scripts/customer_recovery_client.py"


def raw(name):
    path = policy.ROOT / name
    if path.is_symlink():
        raise ValueError("Sealed gateway-read source symlink forbidden")
    return path.read_bytes().replace(b"\r\n", b"\n")


def sha(value):
    return hashlib.sha256(value).hexdigest()


def selected(goal):
    keys = {"gateway_read_recovery_decision", "gateway_read_recovery_contract_sha256"}
    if goal is None or not keys.intersection(goal):
        return False
    from cce_recovery_headroom import correction_selected
    if (goal.get("gateway_read_recovery_decision") != "ADR0311"
            or goal.get("gateway_read_recovery_contract_sha256") != policy.digest(contract())
            or goal.get("timeout_retention_decision") != "ADR0278"
            or goal.get("profile") != "cce_paid_comparison"
            or goal.get("offered_journeys_per_second") != 84
            or goal.get("offered_seconds") != 300
            or goal.get("maximum_experiment_seconds") != 3600
            or (goal.get("payment_slot_arm") != "candidate" and not correction_selected(goal))):
        raise ValueError("Exact gateway-read recovery scope required")
    return True


def receipt():
    content = raw(MANIFEST)
    if sha(content) != MANIFEST_SHA256:
        raise ValueError("Sealed gateway-read manifest changed")
    return json.loads(content)


def contract():
    data = receipt()
    return {"decision": "ADR0311", "manifest_sha256": MANIFEST_SHA256,
            "child_client_sha256": data["child_client_sha256"],
            "read_gateway_codes": [502, 504], "max_attempts": 3,
            "deadline_and_jitter_unchanged": True, "writes_unchanged": True,
            "helper_sha256": sha(raw("scripts/customer_gateway_read_bundle.py"))}


def qualify(parent, coordinator):
    data = receipt()
    from customer_recovery_bundle import MANIFEST_SHA256 as recovery_sha
    from customer_timeout_bundle import MANIFEST_SHA256 as timeout_sha
    optional = raw("scripts/customer_gateway_read_recovery.py")
    if (data["decision"] != "ADR0311" or len(parent) != 80
            or data["parent_files"] != {name: sha(value) for name, value in parent.items()}
            or data["parent_coordinator_sha256"] != sha(coordinator)
            or data["parent_recovery_manifest_sha256"] != recovery_sha
            or data["parent_timeout_manifest_sha256"] != timeout_sha
            or data["optional_read_source_sha256"] != sha(optional)
            or data["changed_delivered_files"] != [CLIENT]
            or (data["max_attempts"], data["rate"], data["seconds"], data["completion_deadline"]) != (3, 84, 300, 420)):
        raise ValueError("Exact sealed timeout parent and gateway-read child required")
    import_line = b"from customer_recovery_client import CustomerRecoveryClient\n"
    if optional.count(import_line) != 1:
        raise ValueError("Exact optional class import required")
    child = (parent[CLIENT] + b"\n\n" + optional.replace(import_line, b"")
             + b"\nCustomerRecoveryClient = GatewayReadRecoveryClient\n")
    if sha(child) != data["child_client_sha256"]:
        raise ValueError("Delivered gateway-read client changed")
    return {**parent, CLIENT: child}, coordinator

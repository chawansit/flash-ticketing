"""ADR0323 immutable explicit-start source and bootstrap-disabled flags."""
from cce_notification_dedup_identity import MARKER_KEYS as PARENT_MARKER_KEYS
from cce_notification_dedup_identity import verify_marker as parent_verify_marker

SOURCE = 'afd8f151f4dc45faa632aa826eab1bb635810e5b568be53715e4a6f487097592'
IMAGE_ID = 'sha256:b8689b9296c603a5bd9858fcbb9bac6b0735ffe9576b5a293ae8061eaf1b358c'
CONFIG = 'sha256:a3792d985d3ade2fcb550e3e330cd3d05822e8bbcf6ff95cbc370ec5a3767ba0'
RECEIPT_SHA256 = '199e498d2b04a131e9b205245e8a7756ff08458508fec3b69af8b3bbaed43b48'
RECEIPT = "docs/capacity/cce/explicit-transaction-start-image-2026-10-14.json"
DECISION = "ADR0323"
MARKER_KEYS = (*PARENT_MARKER_KEYS, "explicit_start_decision", "explicit_start_enabled")

def verify_marker(marker):
    arm = parent_verify_marker(marker)
    if marker.get("explicit_start_decision") != DECISION or marker.get("explicit_start_enabled") != "1":
        raise ValueError("Explicit driver-start comparison marker required")
    return arm

def bootstrap_flags(role):
    return {"ORDER_STATUS_COMMITTED_SNAPSHOT": "0", "ORDER_STATUS_NOTIFICATION_DEDUP": "0", "DB_EXPLICIT_TRANSACTION_START": "1" if role == "api" else "0"}

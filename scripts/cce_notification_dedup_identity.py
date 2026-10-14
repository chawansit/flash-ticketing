"""ADR0322 exact candidate image and explicit bootstrap-disabled notification flag."""
from cce_committed_status_identity import MARKER_KEYS as PARENT_MARKER_KEYS
from cce_committed_status_identity import verify_marker as parent_verify_marker

SOURCE = '0172c0e76b94a6abad52bcce905138077f61c5fd1e52d2cffb38e6d6963ce607'
IMAGE_ID = 'sha256:c64ef9e7df3f4864f1019cbe496ce4526654b71abd906cfd93a30e363f4ff554'
CONFIG = "sha256:5ef7a0b3017ed8413b01e520e91b0e1e276ce64279cc8e321ba7b11592ce8386"
RECEIPT_SHA256 = 'f6570ac18a3963716e73e3e7969b68f7c76f79b8ec958f6552ea7764d193a8fb'
RECEIPT = "docs/capacity/cce/ticket-notification-dedup-image-2026-10-14.json"
DECISION = "ADR0322"

MARKER_KEYS = (*PARENT_MARKER_KEYS, "notification_dedup_decision", "notification_dedup_enabled")


def verify_marker(marker):
    arm = parent_verify_marker(marker)
    if (marker.get("notification_dedup_decision") != DECISION
            or marker.get("notification_dedup_enabled") != ("1" if arm == "candidate" else "0")):
        raise ValueError("Explicit notification dedup arm required")
    return arm


def bootstrap_flags(role):
    return {"ORDER_STATUS_COMMITTED_SNAPSHOT": "0", "ORDER_STATUS_NOTIFICATION_DEDUP": "0"}

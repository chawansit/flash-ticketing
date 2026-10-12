"""ADR0298 immutable publication and consumer-only activation pins."""
SOURCE = "099bdb706c419eb18850fd7ee6a02986b489118d9f0f350a5754e113edba4104"
IMAGE_ID = "sha256:1c14f990325a5e408f63ec87b721401bd84ff844a05fbfb3449ec05df4dc06a4"
CONFIG = "sha256:eb54f55a7cb17161310c99b6c6222e64bbb45fb92c696f839dd3877fe31a8112"
RECEIPT_SHA256 = "e566044814b05b2ebaf1a8cedf0ce2701b70e6f75b87884b26c75459d5983cae"
RECEIPT = "docs/capacity/cce/committed-fulfillment-status-image-2026-10-13.json"
DECISION = "ADR0297"
MARKER_KEYS = ("committed_status_decision", "committed_status_arm", "committed_status_snapshot")


def verify_marker(marker):
    arm = marker.get("committed_status_arm")
    if (marker.get("committed_status_decision") != "ADR0298" or arm not in {"control", "candidate"}
            or marker.get("committed_status_snapshot") != ("1" if arm == "candidate" else "0")):
        raise ValueError("Explicit committed-status arm required")
    return arm


def bootstrap_flags(role):
    return {"ORDER_STATUS_COMMITTED_SNAPSHOT": "0"}

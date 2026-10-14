"""ADR0324 exact shared-image source and default-one bootstrap flags."""
from cce_explicit_start_identity import MARKER_KEYS as PARENT_MARKER_KEYS
from cce_explicit_start_identity import verify_marker as parent_verify_marker

SOURCE = '0087ce3078532a8fd05b668c4d286988a3165d95295c51ebbf2c60d4a702ce0d'
IMAGE_ID = 'sha256:4efa10e301666db5aebc58a3a218df8987307c310c55936ba1c4e3b13de921b2'
CONFIG = 'sha256:b8bbb182e31374d9bcbbfc495847df6d995224314dede9775e900134d78e1386'
RECEIPT_SHA256 = 'a07b4685cdee8df5454ecbd1d0c9ab6b00957a34af907aa695c85143133a8e51'
RECEIPT = 'docs/capacity/cce/fulfillment-transaction-batching-local-2026-10-14.json'
DECISION = 'ADR0324'
MARKER_KEYS = (*PARENT_MARKER_KEYS, "fulfillment_batch_decision", "fulfillment_batch_size")


def verify_marker(marker):
    arm = parent_verify_marker(marker)
    if marker.get("fulfillment_batch_decision") != DECISION or marker.get("fulfillment_batch_size") != "4":
        raise ValueError("Exact bounded fulfillment batch marker required")
    return arm


def bootstrap_flags(role):
    return {"ORDER_STATUS_COMMITTED_SNAPSHOT": "0", "ORDER_STATUS_NOTIFICATION_DEDUP": "0",
            "DB_EXPLICIT_TRANSACTION_START": "1" if role == "api" else "0",
            "FULFILLMENT_TRANSACTION_BATCH_SIZE": "1"}

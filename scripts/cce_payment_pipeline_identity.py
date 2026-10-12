"""ADR0284 immutable publication pins; portable to the cloud observer."""
SOURCE = "1c1db2cc0a907ff8f4cec5f6a0212d3cb073468d721ba30ccd10bcda31e6ff67"
IMAGE_ID = "sha256:af83a344af8abfdd2a42228f0fce4b11a65d9aa5a1094c75b170a04d5f973fe2"
CONFIG = "sha256:2d1a35bb3a531afe4f45367f8aea2f9208da827d2a29d26a6b1751f3c382a969"
RECEIPT_SHA256 = "12e04ee1c4b5aa5be46c573458533bdcd6a5c9f59464dc338be27039e820f33e"
RECEIPT = "docs/capacity/cce/payment-write-pipeline-image-2026-10-12.json"
DECISION = "ADR0284"


def verify_marker(marker):
    arm = marker.get("payment_pipeline_arm")
    if (marker.get("bootstrap_payment_write_pipeline") != "0"
            or marker.get("payment_pipeline_decision") != DECISION or arm not in {"control", "candidate"}
            or marker.get("payment_write_pipeline") != ("1" if arm == "candidate" else "0")):
        raise ValueError("Explicit payment pipeline arm and activation required")
    return arm

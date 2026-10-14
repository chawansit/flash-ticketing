"""ADR0319 immutable, locally tested and remotely pulled payment-context image."""
from cce_committed_status_identity import MARKER_KEYS, bootstrap_flags, verify_marker  # noqa: F401

SOURCE = 'bcd847eedb734ee523facd440c0c5a2964cbc61c1e7465b5d96892346eba8b09'
IMAGE_ID = 'sha256:a5fa40681b48f4d3e8a080ea81cedc1e187722c12ff08a7bd3b1dd1c271f76eb'
CONFIG = 'sha256:132fba2fd0e2a28e38b85cd14ffa046869c11e8b6d5513f806dfb62bbcbda1fd'
RECEIPT_SHA256 = '54402d09a26c4bd2e462619af29ac7fa13340a7e9b4c41e80d9b24c55403e40a'
RECEIPT = 'docs/capacity/cce/measured-payment-context-image-2026-10-14.json'
DECISION = 'ADR0319'

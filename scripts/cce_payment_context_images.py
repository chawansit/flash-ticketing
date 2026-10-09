"""ADR0249 exact immutable images for the post-lock payment-context factor."""
import hashlib
import re

import work_envelope as policy
from prepare_payment_context_sources import pairs

PROOF = 'docs/capacity/cce/payment-context-images-2026-10-09.json'
PROOF_SHA256 = 'f42a77d925d257a84481f1b2e4a4c42273ff7b4db207f4c4dca169c59511fad5'


def image_pair():
    path = policy.ROOT / PROOF
    if path.is_symlink() or hashlib.sha256(path.read_bytes().replace(b"\r\n", b"\n")).hexdigest() != PROOF_SHA256:
        raise ValueError("Exact payment image pair receipt required")
    proof = policy.read(path)
    _, expected = pairs()
    from cce_transaction_profile import image_pair as original_image_pair
    old = original_image_pair()
    if (proof.get("decision") != "ADR0249" or proof.get("registry_published") is not True
            or proof.get("cloud_load_started") is not False
            or proof.get("registry_credentials_removed") is not True
            or proof.get("parent_is_accepted_control") is not True
            or proof.get("only_changed_method") != "PostgresReservations.initiate_payment"
            or proof.get("platform") != old["platform"]
            or proof.get("manifest_media_type") != old["manifest_media_type"]
            or proof.get("common_environment_changes") != {"DB_FAILURE_DIAGNOSTICS": "1"}
            or set(proof.get("images", {})) != {"control", "candidate"}
            or proof["images"]["control"] != old["images"]["control"]):
        raise ValueError("Exact accepted control and one-method candidate required")
    for arm, item in proof["images"].items():
        digest, config = item["registry_manifest_digest"], item["registry_configuration_digest"]
        if (not all(re.fullmatch(r"sha256:[0-9a-f]{64}", v) for v in (digest, config))
                or digest == config
                or item["registry_image"] != "swr.ap-southeast-2.myhuaweicloud.com/chawansit/flash-ticketing@" + digest
                or item["runtime_sources_sha256"] != expected["runtime_sources"][arm]
                or item["immutable_registry_verification_passed"] is not True
                or item["runtime_user"] != "ticketing"
                or item["registry_pulled_runtime_validation"] != old["images"]["control"]["registry_pulled_runtime_validation"]
                or proof["runtime_proofs"][arm] != {"pass": True, "modules": 22}):
            raise ValueError("Qualified payment image/source/configuration required")
    if proof["images"]["candidate"]["registry_manifest_digest"] == proof["images"]["control"]["registry_manifest_digest"]:
        raise ValueError("Distinct one-method candidate required")
    return proof

"""ADR0319 exact post-lock payment correction on the restored shared-pool reference."""
import hashlib

import cce_measured_payment_context_identity as pins
import work_envelope as policy

KEYS = {"payment_context_decision", "payment_context_receipt_sha256", "payment_context_contract_sha256"}
BASELINE = "250aa2541308119f8da10a7ba3ebdf8024d99fb8a1fffbb796e8eead55fbfa2a"


def image():
    path = policy.ROOT / pins.RECEIPT
    if path.is_symlink() or hashlib.sha256(path.read_bytes().replace(b"\r\n", b"\n")).hexdigest() != pins.RECEIPT_SHA256:
        raise ValueError("Exact reviewed payment-context publication receipt required")
    value = policy.read(path)
    import cce_committed_status_profile as parent
    old = parent.image()
    sources, original = value["runtime_source_sha256"], old["runtime_source_sha256"]
    if (value.get("decision") != pins.DECISION or value.get("source_identity_sha256") != pins.SOURCE
            or value.get("registry_manifest_digest") != pins.IMAGE_ID
            or value.get("registry_configuration_digest") != pins.CONFIG
            or value.get("registry_image") != "swr.ap-southeast-2.myhuaweicloud.com/chawansit/flash-ticketing@" + pins.IMAGE_ID
            or value.get("registry_published") is not True or value.get("registry_pulled_runtime_verified") is not True
            or value.get("parent_registry_image") != old["registry_image"]
            or value.get("parent_runtime_source_sha256") != original
            or set(sources) != set(original) or len(sources) != 22
            or [k for k in original if sources[k] != original[k]] != ["src/ticketing/infrastructure/reservations.py"]
            or value.get("dependency_input_sha256") != old["dependency_input_sha256"]):
        raise ValueError("Single-method immutable payment-context candidate required")
    return {**value, "runtime_sources_sha256": sources}


def contract():
    names = ("cce_measured_payment_context_profile.py", "cce_measured_payment_context_identity.py")
    return {"decision": "ADR0319", "receipt_sha256": pins.RECEIPT_SHA256,
            "candidate_source_sha256": pins.SOURCE, "candidate_manifest": pins.IMAGE_ID,
            "baseline_result_sha256": BASELINE, "backend_connections": 24,
            "api_general_payment_slots": [2, 2], "reservation_writers": 4, "writer_batch_size": 4,
            "offered_rate": 102, "offered_seconds": 300, "generator_concurrency": 500,
            "single_application_factor": "post_lock_payment_context",
            "sources": {name: hashlib.sha256((policy.ROOT / "scripts" / name).read_bytes().replace(
                b"\r\n", b"\n")).hexdigest() for name in names}}


def selected(goal):
    if goal is None or not KEYS.intersection(goal):
        return False
    view = goal.get("_headroom_runtime_view") is True
    expected = {"payment_context_decision": "ADR0319", "payment_context_receipt_sha256": pins.RECEIPT_SHA256,
                "payment_context_contract_sha256": policy.digest(contract()), "baseline_sha256": BASELINE,
                "extension_decision": "ADR0271" if view else "ADR0277",
                "profile": "cce_paid_comparison" if view else "cce_ticket_target",
                "offered_journeys_per_second": 84 if view else 102, "offered_seconds": 300,
                "maximum_paid_stages": 1, "maximum_experiment_seconds": 3600,
                "maximum_pods": 18 if view else 19, "generator_concurrency": 500,
                "acquisition_budget": 20, "pod_vcpu": 1, "pod_memory_gib": 2,
                "reservation_writer_scale_decision": "ADR0315", "payment_slot_arm": "control",
                "committed_status_arm": "candidate"}
    if (any(type(goal.get(k)) is not type(v) or goal.get(k) != v for k, v in expected.items())
            or any(k in goal for k in ("backend_partition_decision", "writer_batch_decision", "payment_pipeline_decision"))):
        raise ValueError("Exact fixed-budget shared-pool payment-context comparison required")
    image()
    return True

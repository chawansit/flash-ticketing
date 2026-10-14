"""ADR0322 exact bounded notification correction on the restored shared-pool reference."""
import hashlib

import cce_notification_dedup_identity as pins
import work_envelope as policy

KEYS = {"notification_dedup_decision", "notification_dedup_receipt_sha256", "notification_dedup_contract_sha256"}
BASELINE = "3d5e62a71e367cb340f62c74b652917c3297cc9da5f7f07a5838cac740064a1e"


def image():
    path = policy.ROOT / pins.RECEIPT
    if path.is_symlink() or hashlib.sha256(path.read_bytes().replace(b"\r\n", b"\n")).hexdigest() != pins.RECEIPT_SHA256:
        raise ValueError("Exact reviewed notification-dedup publication receipt required")
    value = policy.read(path)
    import cce_writer_sale_context_profile as parent
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
            or {k for k in original if sources[k] != original[k]} != {"src/ticketing/config.py", "src/ticketing/workers.py",
                    "src/ticketing/infrastructure/order_status_projector.py", "src/ticketing/observability.py"}
            or value.get("feature_default") is not False or value.get("verified_modules") != 22
            or value.get("local_runtime_verification", {}).get("all_modules_imported") is not True
            or value.get("dependency_input_sha256") != old["dependency_input_sha256"]):
        raise ValueError("Bounded-notification immutable notification-dedup candidate required")
    return {**value, "runtime_sources_sha256": sources}


def contract():
    names = ("cce_notification_dedup_profile.py", "cce_notification_dedup_identity.py")
    return {"decision": "ADR0322", "receipt_sha256": pins.RECEIPT_SHA256,
            "candidate_source_sha256": pins.SOURCE, "candidate_manifest": pins.IMAGE_ID,
            "baseline_result_sha256": BASELINE, "backend_connections": 24,
            "api_general_payment_slots": [2, 2], "reservation_writers": 4, "writer_batch_size": 4,
            "offered_rate": 102, "offered_seconds": 300, "generator_concurrency": 500,
            "single_application_factor": "bounded_ticket_notification_refresh_suppression",
            "notification_hint_seconds": 30, "maximum_notification_hints": 1024,
            "customer_cache_freshness_unchanged": True,
            "sources": {name: hashlib.sha256((policy.ROOT / "scripts" / name).read_bytes().replace(
                b"\r\n", b"\n")).hexdigest() for name in names}}


def selected(goal):
    if goal is None or not KEYS.intersection(goal):
        return False
    view = goal.get("_headroom_runtime_view") is True
    expected = {"notification_dedup_decision": "ADR0322", "notification_dedup_receipt_sha256": pins.RECEIPT_SHA256,
                "notification_dedup_contract_sha256": policy.digest(contract()), "baseline_sha256": BASELINE,
                "extension_decision": "ADR0271" if view else "ADR0277",
                "profile": "cce_paid_comparison" if view else "cce_ticket_target",
                "offered_journeys_per_second": 84 if view else 102, "offered_seconds": 300,
                "maximum_paid_stages": 1, "maximum_experiment_seconds": 3600,
                "maximum_pods": 18 if view else 19, "generator_concurrency": 500,
                "acquisition_budget": 20, "pod_vcpu": 1, "pod_memory_gib": 2,
                "reservation_writer_scale_decision": "ADR0315", "payment_slot_arm": "control",
                "committed_status_arm": "candidate"}
    if (any(type(goal.get(k)) is not type(v) or goal.get(k) != v for k, v in expected.items())
            or any(k in goal for k in ("backend_partition_decision", "writer_batch_decision", "payment_pipeline_decision", "payment_context_decision", "writer_sale_context_decision"))):
        raise ValueError("Exact fixed-budget shared-pool notification-dedup comparison required")
    image()
    return True


def require_parent():
    """Use the financially reconciled, restored ADR0321 run without claiming a passing capacity gate."""
    relative = "tmp/adr0151-7b637d7b8a0f/cce-comparison.private.json"
    report = policy.read(policy.ROOT / relative)
    ledger = "bounded_cce_ticket_target__384290e57344"
    state = policy.read(policy.STATE)[ledger]
    entries = [e for e in policy.journal(policy.envelope())["experiments"] if e.get("ledger") == ledger]
    stage = report.get("paid_stage", {})
    if (policy.digest(report) != BASELINE or len(entries) != 1
            or entries[0].get("status") != "FAILED_RESTORED"
            or entries[0].get("result_sha256") != BASELINE or state.get("active_run")
            or state.get("cce_paid_result_sha256") != BASELINE
            or report.get("binding_sha256") != policy.digest(state["binding"])
            or report.get("pass") is not False
            or any(report.get(k) is not True for k in ("restoration_complete", "integrity_verified",
                                                       "transport_credentials_cleared"))
            or stage.get("financial", {}).get("pass") is not True
            or stage.get("global_queues", {}).get("pass") is not True
            or stage.get("customer", {}).get("fulfilled") != 30600
            or stage.get("customer", {}).get("generator_drops") != 0):
        raise ValueError("Exact restored and reconciled ADR0321 comparison reference required")
    return relative, report

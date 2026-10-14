"""ADR0323 exact explicit driver start on the reconciled ADR0322 reference."""
import hashlib

import cce_explicit_start_identity as pins
import work_envelope as policy

KEYS = {"explicit_start_decision", "explicit_start_receipt_sha256", "explicit_start_contract_sha256"}
BASELINE = "cbb3eda22fa6990483b4be9c89bef9a9a69df6184ccc406f3801101b6c43a84c"


def image():
    path = policy.ROOT / pins.RECEIPT
    if path.is_symlink() or hashlib.sha256(path.read_bytes().replace(b"\r\n", b"\n")).hexdigest() != pins.RECEIPT_SHA256:
        raise ValueError("Exact reviewed explicit-start publication receipt required")
    value = policy.read(path)
    import cce_notification_dedup_profile as parent
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
                    "src/ticketing/api.py", "src/ticketing/infrastructure/postgres.py"}
            or value.get("feature_default") is not False or value.get("verified_modules") != 22
            or value.get("local_runtime_verification", {}).get("all_modules_imported") is not True
            or value.get("dependency_input_sha256") != old["dependency_input_sha256"]):
        raise ValueError("Single-start immutable driver candidate required")
    return {**value, "runtime_sources_sha256": sources}


def contract():
    names = ("cce_explicit_start_profile.py", "cce_explicit_start_identity.py")
    return {"decision": "ADR0323", "receipt_sha256": pins.RECEIPT_SHA256,
            "candidate_source_sha256": pins.SOURCE, "candidate_manifest": pins.IMAGE_ID,
            "baseline_result_sha256": BASELINE, "backend_connections": 24,
            "api_general_payment_slots": [2, 2], "reservation_writers": 4, "writer_batch_size": 4,
            "offered_rate": 102, "offered_seconds": 300, "generator_concurrency": 500,
            "single_application_factor": "one_explicit_driver_transaction_start",
            "driver_autocommit": True, "explicit_financial_begin_commit_preserved": True,
            "customer_cache_freshness_unchanged": True,
            "sources": {name: hashlib.sha256((policy.ROOT / "scripts" / name).read_bytes().replace(
                b"\r\n", b"\n")).hexdigest() for name in names}}


def selected(goal):
    if goal is None or not KEYS.intersection(goal):
        return False
    view = goal.get("_headroom_runtime_view") is True
    expected = {"explicit_start_decision": "ADR0323", "explicit_start_receipt_sha256": pins.RECEIPT_SHA256,
                "explicit_start_contract_sha256": policy.digest(contract()), "baseline_sha256": BASELINE,
                "extension_decision": "ADR0271" if view else "ADR0277",
                "profile": "cce_paid_comparison" if view else "cce_ticket_target",
                "offered_journeys_per_second": 84 if view else 102, "offered_seconds": 300,
                "maximum_paid_stages": 1, "maximum_experiment_seconds": 3600,
                "maximum_pods": 18 if view else 19, "generator_concurrency": 500,
                "acquisition_budget": 20, "pod_vcpu": 1, "pod_memory_gib": 2,
                "reservation_writer_scale_decision": "ADR0315", "payment_slot_arm": "control",
                "committed_status_arm": "candidate"}
    if (any(type(goal.get(k)) is not type(v) or goal.get(k) != v for k, v in expected.items())
            or any(k in goal for k in ("backend_partition_decision", "writer_batch_decision", "payment_pipeline_decision", "payment_context_decision", "writer_sale_context_decision", "notification_dedup_decision"))):
        raise ValueError("Exact fixed-budget explicit-start comparison required")
    image()
    return True


def require_parent():
    """Retained failed load is a reference, never a passing-control permit."""
    import cce_headroom_recovery as recovery
    relative = "tmp/" + recovery.NOTIFICATION_RUN + "/cce-comparison.private.json"
    report = policy.read(policy.ROOT / relative)
    state = policy.read(policy.STATE)[recovery.NOTIFICATION_LEDGER]
    entries = [e for e in policy.journal(policy.envelope())["experiments"] if e["ledger"] == recovery.NOTIFICATION_LEDGER]
    proof = policy.read(policy.ROOT / "tmp" / recovery.NOTIFICATION_RUN / "headroom-recovery.private.json")
    if (policy.digest(report) != BASELINE or len(entries) != 1 or entries[0].get("status") != "FAILED_RESTORED"
            or entries[0].get("result_sha256") != BASELINE or state.get("cce_paid_result_sha256") != BASELINE
            or state.get("active_run") or report.get("pass") is not False
            or report.get("binding_sha256") != policy.digest(state["binding"])
            or entries[0].get("recovery", {}).get("sha256") != policy.digest(proof)
            or proof.get("original_result_sha256") != BASELINE or proof.get("pass") is not True
            or proof.get("capacity_qualified") is not False
            or proof.get("gates") != dict.fromkeys(recovery.GATES, True)
            or proof.get("fresh_financial", {}).get("pass") is not True
            or report["paid_stage"]["customer"]["dispatched"] != 30581
            or report["paid_stage"]["customer"]["generator_drops"] != 19):
        raise ValueError("Exact independently reconciled ADR0322 failed reference required")
    return relative, report

"""ADR0336 API-only image overlay over the unchanged ADR0335 workload/workers."""

import hashlib

import work_envelope as policy

SOURCE = "scripts/cce_auth_dispatch_profile.py"
LOCAL = "docs/capacity/cce/authentication-dispatch-local-2026-10-17.json"
LOCAL_SHA256 = "453dc1a12c224017d6a1c8bac8fb3075eb06810424285ade77867f3defa24bc5"
PUBLICATION = "docs/capacity/cce/authentication-dispatch-image-2026-10-17.json"
IMAGE = "sha256:cde635d647a83758065d6e7bb6d36efaed79aa7baeb0dab91368dcd047aecde3"
CONFIG = "sha256:527aec6fa481e6774d4508e420cf3e394155cc8ec9ba12021971e351101044d0"
IDENTITY = "bf8bc89d0dac9da1f318976fcecdb7899f2f883cd0fea1cab9f3d67d6140e477"
REFERENCE_RUN = "adr0151-3fa2832bb77c"
REFERENCE_LEDGER = "bounded_cce_ticket_target__93dca53926e6"
REFERENCE = "784f559ae4a76271170d72fb78dc2d00a8a34216822cf9a025cebb2f664ab17f"
RECOVERY_SHA256 = "4f2e2cbc470e2717df21646e66c09c2b6269874dbdfc995e9ece28062e0d39b6"
KEYS = {"auth_dispatch_decision", "auth_dispatch_receipt_sha256", "auth_dispatch_contract_sha256"}
CHANGED = {
    SOURCE,
    "scripts/cce_transaction_profile.py",
    "scripts/cce_recovery_headroom.py",
    "scripts/run_cce_paid_comparison.py",
    "scripts/cce_paid_stage.py",
    "scripts/cce_generator_process_scaling.py",
    "scripts/cce_throughput_push.py",
    "scripts/cce_reproduction_overlay.py",
    "artifacts/cce-transaction-runner/manifest.json",
}
ADDED = {SOURCE, LOCAL, PUBLICATION}


def file_sha(relative):
    path = policy.ROOT / relative
    if path.is_symlink():
        raise ValueError("Regular reviewed authentication-overlay input required")
    return hashlib.sha256(path.read_bytes().replace(b"\r\n", b"\n")).hexdigest()


def contract():
    return {
        "decision": "ADR0336",
        "api_manifest": IMAGE,
        "api_configuration": CONFIG,
        "api_source": IDENTITY,
        "local_receipt_sha256": LOCAL_SHA256,
        "publication_receipt_sha256": file_sha(PUBLICATION),
        "reference_result_sha256": REFERENCE,
        "native_api_only": True,
        "worker_image_decision": "ADR0324",
        "offered_rate": 160,
        "seconds": 300,
        "concurrency": 800,
        "generator_processes": 4,
        "http_connections": 500,
        "http_clients": 16,
        "physical_backends": 24,
        "api_pools": [2, 2],
        "admission": 20,
        "api_pods": 4,
        "worker_pods": 15,
        "maximum_paid_stages": 1,
        "maximum_experiment_seconds": 3600,
        "sources": {name: file_sha(name) for name in sorted(CHANGED)},
    }


def selected(goal):
    if goal is None or not KEYS.intersection(goal):
        return False
    view = goal.get("_headroom_runtime_view") is True
    expected = {
        "auth_dispatch_decision": "ADR0336",
        "auth_dispatch_receipt_sha256": LOCAL_SHA256,
        "auth_dispatch_contract_sha256": policy.digest(contract()),
        "generator_process_decision": "ADR0335",
        "generator_concurrency": 800,
        "throughput_push_decision": "ADR0329",
        "fulfillment_batch_decision": "ADR0324",
        "offered_journeys_per_second": 84 if view else 160,
        "offered_seconds": 300,
        "profile": "cce_paid_comparison" if view else "cce_ticket_target",
        "extension_decision": "ADR0271" if view else "ADR0277",
        "maximum_paid_stages": 1,
        "maximum_experiment_seconds": 3600,
        "maximum_pods": 18 if view else 19,
        "acquisition_budget": 20,
        "pod_vcpu": 1,
        "pod_memory_gib": 2,
        "comparison_arm": "correction",
        "committed_status_arm": "candidate",
        "payment_slot_arm": "control",
        "primary_bootstrap_decision": "ADR0330",
        "fixture_clock_decision": "ADR0334",
    }
    if any(type(goal.get(k)) is not type(v) or goal.get(k) != v for k, v in expected.items()):
        raise ValueError("Exact source-bound API-only authentication comparison required")
    return True


def image():
    from cce_fulfillment_batch_profile import image as parent_image

    old = parent_image()
    if file_sha(LOCAL) != LOCAL_SHA256:
        raise ValueError("Reviewed local authentication image evidence changed")
    local = policy.read(policy.ROOT / LOCAL)
    value = policy.read(policy.ROOT / PUBLICATION)
    if (
        value.get("registry_published") is not True
        or value.get("registry_pulled_runtime_verified") is not True
    ):
        raise ValueError(
            "Authentication candidate must be published and independently pulled before admission"
        )
    expected = {
        "decision": "ADR0336",
        "registry_manifest_digest": IMAGE,
        "registry_configuration_digest": CONFIG,
        "source_identity_sha256": IDENTITY,
        "registry_image": "swr.ap-southeast-2.myhuaweicloud.com/chawansit/flash-ticketing@" + IMAGE,
        "parent_registry_image": old["registry_image"],
        "parent_runtime_source_sha256": old["runtime_source_sha256"],
        "runtime_source_sha256": local["runtime_source_sha256"],
        "dependency_input_sha256": old["dependency_input_sha256"],
        "platform": {"os": "linux", "architecture": "amd64"},
    }
    before, after = old["runtime_source_sha256"], value.get("runtime_source_sha256", {})
    if (
        any(value.get(k) != v for k, v in expected.items())
        or len(after) != 22
        or set(before) != set(after)
        or {k for k in before if before[k] != after[k]} != {"src/ticketing/api.py"}
        or value.get("local_runtime_verification") != local["local_runtime_verification"]
    ):
        raise ValueError("Only the exact verified authentication module may differ")
    return {**value, "runtime_sources_sha256": after}


def reference():
    path = "tmp/" + REFERENCE_RUN + "/cce-comparison.private.json"
    report = policy.read(policy.ROOT / path)
    proof = policy.read(policy.ROOT / "docs/capacity/cce/generator-occupancy-800-recovery-2026-10-17.json")
    scope = policy.read(policy.STATE)[REFERENCE_LEDGER]
    entries = [
        e for e in policy.journal(policy.envelope())["experiments"] if e.get("ledger") == REFERENCE_LEDGER
    ]
    if (
        policy.digest(report) != REFERENCE
        or len(entries) != 1
        or entries[0].get("status") != "FAILED_RESTORED"
        or entries[0].get("result_sha256") != REFERENCE
        or scope.get("active_run")
        or scope.get("paid_runs_started") != 1
        or entries[0].get("paid_runs_started") != 1
        or report.get("pass") is not False
        or report.get("binding_sha256") != policy.digest(scope["binding"])
        or proof.get("pass") is not True
        or proof.get("original_result_sha256") != REFERENCE
        or proof.get("financial", {}).get("pass") is not True
        or proof.get("financial", {}).get("tickets") != 39151
        or entries[0].get("recovery", {}).get("sha256") != policy.digest(proof)
        or policy.digest(proof) != RECOVERY_SHA256
        or proof.get("queues", {}).get("pass") is not True
        or any(
            proof.get(k) is not True
            for k in (
                "runtime_restored_and_stable",
                "namespace_absent",
                "helpers_absent",
                "generator_idle",
                "original_result_preserved",
            )
        )
        or any(
            proof.get(k, {}).get("pass") is not True for k in ("safety_financial", "native_safety_financial")
        )
    ):
        raise ValueError("Exact failed ADR0335 result and independent operational recovery required")
    return path, report, scope["binding"]


def extend_plan(goal, plan):
    if not selected(goal):
        return plan
    path, report, _ = reference()
    candidate = image()
    return {
        **plan,
        "kind": "api_authentication_dispatch_comparison",
        "single_changed_factor": "local_jwt_dependency_thread_handoff",
        "baseline_evidence": path,
        "baseline_sha256": policy.digest(report),
        "baseline_is_passing_control": False,
        "matched_performance_comparison": True,
        "api_overlay_contract": contract(),
        "api_overlay_manifest": candidate["registry_manifest_digest"],
        "worker_image_unchanged": True,
        "duration_reason": "One160/s300s API-only correction against retained failed/reconciled800-slot evidence; no capacity qualification inferred.",
    }


def validate_binding(goal, binding):
    if not selected(goal):
        raise ValueError("Explicit authentication overlay required")
    image()
    _, _, old = reference()
    if binding.get("cce_auth_dispatch_contract") != contract() or binding.get("baseline_sha256") != REFERENCE:
        raise ValueError("Authentication image/reference must be reserved before deployment")
    varied = {
        "cce_auth_dispatch_contract",
        "baseline_sha256",
        "cce_paid_entry_sources",
        "cce_paid_core_sources",
        "cce_manifest_sha256",
        "cce_headroom_contract",
        "cce_throughput_push_contract",
        "cce_generator_process_contract",
        "cce_verification_contract_sha256",
        "cce_primary_bootstrap_contract",
    }
    if any(old.get(k) != binding.get(k) for k in old.keys() | binding.keys() if k not in varied):
        raise ValueError("Authentication comparison changed resources, settings, worker image or budgets")
    import cce_paid_stage as paid
    import run_cce_paid_comparison as core

    for key, expected in (
        ("cce_paid_entry_sources", core.identity()),
        ("cce_paid_core_sources", paid.identity()),
    ):
        before, after = old[key], binding.get(key, {})
        if (
            after != expected
            or set(after) != set(before) | ADDED
            or any(before[k] != after[k] for k in before if k not in CHANGED)
        ):
            raise ValueError("Authentication runner source changed outside the reviewed image integration")
    import cce_generator_process_scaling as generator
    import cce_primary_bootstrap as bootstrap
    import cce_recovery_headroom as headroom
    import cce_throughput_push as push
    import cce_verification_contract as verification

    if not headroom.selected(goal):
        raise ValueError("Full fixed-budget parent profile required")
    generator.validate_binding(goal, binding)
    expected = {
        "cce_headroom_contract": headroom.contract(goal),
        "cce_throughput_push_contract": push.contract(goal),
        "cce_generator_process_contract": generator.contract(goal),
        "cce_verification_contract_sha256": policy.digest(verification.contract()),
        "cce_primary_bootstrap_contract": bootstrap.contract(),
    }
    if any(binding.get(k) != v for k, v in expected.items()):
        raise ValueError("Fresh source-bound authentication comparison contracts required")
    before, after = old["cce_primary_bootstrap_contract"], binding["cce_primary_bootstrap_contract"]
    if (
        {k: v for k, v in before.items() if k != "sources"}
        != {k: v for k, v in after.items() if k != "sources"}
        or set(before["sources"]) != set(after["sources"])
        or any(before["sources"][k] != after["sources"][k] for k in before["sources"] if k not in CHANGED)
    ):
        raise ValueError("ECS bootstrap contract must remain unchanged")
    return True
